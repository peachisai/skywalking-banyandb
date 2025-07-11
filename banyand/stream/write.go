// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package stream

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

type writeCallback struct {
	l                   *logger.Logger
	schemaRepo          *schemaRepo
	maxDiskUsagePercent int
}

func setUpWriteCallback(l *logger.Logger, schemaRepo *schemaRepo, maxDiskUsagePercent int) bus.MessageListener {
	if maxDiskUsagePercent > 100 {
		maxDiskUsagePercent = 100
	}
	return &writeCallback{
		l:                   l,
		schemaRepo:          schemaRepo,
		maxDiskUsagePercent: maxDiskUsagePercent,
	}
}

func (w *writeCallback) CheckHealth() *common.Error {
	if w.maxDiskUsagePercent < 1 {
		return common.NewErrorWithStatus(modelv1.Status_STATUS_DISK_FULL, "stream is readonly because \"stream-max-disk-usage-percent\" is 0")
	}
	diskPercent := observability.GetPathUsedPercent(w.schemaRepo.path)
	if diskPercent < w.maxDiskUsagePercent {
		return nil
	}
	w.l.Warn().Int("maxPercent", w.maxDiskUsagePercent).Int("diskPercent", diskPercent).Msg("disk usage is too high, stop writing")
	return common.NewErrorWithStatus(modelv1.Status_STATUS_DISK_FULL, "disk usage is too high, stop writing")
}

func (w *writeCallback) handle(dst map[string]*elementsInGroup, writeEvent *streamv1.InternalWriteRequest,
	docIDBuilder *strings.Builder,
) (map[string]*elementsInGroup, error) {
	t := writeEvent.Request.Element.Timestamp.AsTime().Local()
	if err := timestamp.Check(t); err != nil {
		return nil, fmt.Errorf("invalid timestamp: %w", err)
	}
	ts := t.UnixNano()
	eg, err := w.prepareElementsInGroup(dst, writeEvent, ts)
	if err != nil {
		return nil, err
	}
	et, err := w.prepareElementsInTable(eg, writeEvent, ts)
	if err != nil {
		return nil, err
	}
	err = w.processElements(et, eg, writeEvent, docIDBuilder, ts)
	if err != nil {
		return nil, err
	}
	return dst, nil
}

func (w *writeCallback) prepareElementsInGroup(dst map[string]*elementsInGroup, writeEvent *streamv1.InternalWriteRequest, ts int64) (*elementsInGroup, error) {
	gn := writeEvent.Request.Metadata.Group
	tsdb, err := w.schemaRepo.loadTSDB(gn)
	if err != nil {
		return nil, fmt.Errorf("cannot load tsdb for group %s: %w", gn, err)
	}

	eg, ok := dst[gn]
	if !ok {
		eg = &elementsInGroup{
			tsdb:        tsdb,
			tables:      make([]*elementsInTable, 0),
			segments:    make([]storage.Segment[*tsTable, option], 0),
			docIDsAdded: make(map[uint64]struct{}), // Initialize the map
		}
		dst[gn] = eg
	}
	if eg.latestTS < ts {
		eg.latestTS = ts
	}
	return eg, nil
}

func (w *writeCallback) prepareElementsInTable(eg *elementsInGroup, writeEvent *streamv1.InternalWriteRequest, ts int64) (*elementsInTable, error) {
	var et *elementsInTable
	for i := range eg.tables {
		if eg.tables[i].timeRange.Contains(ts) {
			et = eg.tables[i]
			break
		}
	}

	if et == nil {
		var segment storage.Segment[*tsTable, option]
		for _, seg := range eg.segments {
			if seg.GetTimeRange().Contains(ts) {
				segment = seg
				break
			}
		}
		if segment == nil {
			var err error
			segment, err = eg.tsdb.CreateSegmentIfNotExist(time.Unix(0, ts))
			if err != nil {
				return nil, fmt.Errorf("cannot create segment: %w", err)
			}
			eg.segments = append(eg.segments, segment)
		}

		shardID := common.ShardID(writeEvent.ShardId)
		tstb, err := segment.CreateTSTableIfNotExist(shardID)
		if err != nil {
			return nil, fmt.Errorf("cannot create ts table: %w", err)
		}

		et = &elementsInTable{
			timeRange: segment.GetTimeRange(),
			tsTable:   tstb,
			elements:  generateElements(),
		}
		et.elements.reset()
		eg.tables = append(eg.tables, et)
	}
	return et, nil
}

func (w *writeCallback) processElements(et *elementsInTable, eg *elementsInGroup, writeEvent *streamv1.InternalWriteRequest,
	docIDBuilder *strings.Builder, ts int64,
) error {
	req := writeEvent.Request

	et.elements.timestamps = append(et.elements.timestamps, ts)
	docIDBuilder.Reset()
	docIDBuilder.WriteString(req.Metadata.Name)
	docIDBuilder.WriteByte('|')
	docIDBuilder.WriteString(req.Element.ElementId)
	eID := convert.HashStr(docIDBuilder.String())
	et.elements.elementIDs = append(et.elements.elementIDs, eID)

	stm, ok := w.schemaRepo.loadStream(writeEvent.GetRequest().GetMetadata())
	if !ok {
		return fmt.Errorf("cannot find stream definition: %s", writeEvent.GetRequest().GetMetadata())
	}

	fLen := len(req.Element.GetTagFamilies())
	if fLen < 1 {
		return fmt.Errorf("%s has no tag family", req)
	}
	if fLen > len(stm.schema.GetTagFamilies()) {
		return fmt.Errorf("%s has more tag families than %s", req.Metadata, stm.schema)
	}

	series := &pbv1.Series{
		Subject:      req.Metadata.Name,
		EntityValues: writeEvent.EntityValues,
	}
	if err := series.Marshal(); err != nil {
		return fmt.Errorf("cannot marshal series: %w", err)
	}
	et.elements.seriesIDs = append(et.elements.seriesIDs, series.ID)

	is := stm.indexSchema.Load().(indexSchema)
	tagFamilies := make([]tagValues, 0, len(stm.schema.TagFamilies))
	indexedTags := make(map[string]map[string]struct{})
	var fields []index.Field

	if len(is.indexRuleLocators.TagFamilyTRule) != len(stm.GetSchema().GetTagFamilies()) {
		return fmt.Errorf("metadata crashed, tag family rule length %d, tag family length %d",
			len(is.indexRuleLocators.TagFamilyTRule), len(stm.GetSchema().GetTagFamilies()))
	}

	for i := range stm.GetSchema().GetTagFamilies() {
		var tagFamily *modelv1.TagFamilyForWrite
		if len(req.Element.TagFamilies) <= i {
			tagFamily = pbv1.NullTagFamily
		} else {
			tagFamily = req.Element.TagFamilies[i]
		}
		tfr := is.indexRuleLocators.TagFamilyTRule[i]
		tagFamilySpec := stm.GetSchema().GetTagFamilies()[i]
		tf := tagValues{
			tag: tagFamilySpec.Name,
		}
		indexedTags[tagFamilySpec.Name] = make(map[string]struct{})

		for j := range tagFamilySpec.Tags {
			var tagValue *modelv1.TagValue
			if tagFamily == pbv1.NullTagFamily || len(tagFamily.Tags) <= j {
				tagValue = pbv1.NullTagValue
			} else {
				tagValue = tagFamily.Tags[j]
			}

			t := tagFamilySpec.Tags[j]
			indexed := false
			if r, ok := tfr[t.Name]; ok && tagValue != pbv1.NullTagValue {
				if r.GetType() == databasev1.IndexRule_TYPE_INVERTED {
					fields = appendField(fields, index.FieldKey{
						IndexRuleID: r.GetMetadata().GetId(),
						Analyzer:    r.Analyzer,
						SeriesID:    series.ID,
					}, t.Type, tagValue, r.GetNoSort())
				} else if r.GetType() == databasev1.IndexRule_TYPE_SKIPPING {
					indexed = true
				}
			}
			_, isEntity := is.indexRuleLocators.EntitySet[t.Name]
			if tagFamilySpec.Tags[j].IndexedOnly || isEntity {
				continue
			}
			tv := encodeTagValue(t.Name, t.Type, tagValue)
			tv.indexed = indexed
			tf.values = append(tf.values, tv)
		}
		if len(tf.values) > 0 {
			tagFamilies = append(tagFamilies, tf)
		}
	}
	et.elements.tagFamilies = append(et.elements.tagFamilies, tagFamilies)

	et.docs = append(et.docs, index.Document{
		DocID:     eID,
		Fields:    fields,
		Timestamp: ts,
	})

	docID := uint64(series.ID)
	if _, exists := eg.docIDsAdded[docID]; !exists {
		eg.docs = append(eg.docs, index.Document{
			DocID:        docID,
			EntityValues: series.Buffer,
		})
		eg.docIDsAdded[docID] = struct{}{}
	}

	return nil
}

func (w *writeCallback) Rev(_ context.Context, message bus.Message) (resp bus.Message) {
	events, ok := message.Data().([]any)
	if !ok {
		w.l.Warn().Msg("invalid event data type")
		return
	}
	if len(events) < 1 {
		w.l.Warn().Msg("empty event")
		return
	}
	groups := make(map[string]*elementsInGroup)
	var builder strings.Builder
	for i := range events {
		var writeEvent *streamv1.InternalWriteRequest
		switch e := events[i].(type) {
		case *streamv1.InternalWriteRequest:
			writeEvent = e
		case []byte:
			writeEvent = &streamv1.InternalWriteRequest{}
			if err := proto.Unmarshal(e, writeEvent); err != nil {
				w.l.Error().Err(err).RawJSON("written", e).Msg("fail to unmarshal event")
				continue
			}
		default:
			w.l.Warn().Msg("invalid event data type")
			continue
		}
		var err error
		if groups, err = w.handle(groups, writeEvent, &builder); err != nil {
			w.l.Error().Err(err).Msg("cannot handle write event")
			groups = make(map[string]*elementsInGroup)
			continue
		}
	}
	for i := range groups {
		g := groups[i]
		for j := range g.tables {
			es := g.tables[j]
			es.tsTable.mustAddElements(es.elements)
			releaseElements(es.elements)
			if len(es.docs) > 0 {
				index := es.tsTable.Index()
				if err := index.Write(es.docs); err != nil {
					w.l.Error().Err(err).Msg("cannot write element index")
				}
			}
		}
		if len(g.docs) > 0 {
			for _, segment := range g.segments {
				if err := segment.IndexDB().Insert(g.docs); err != nil {
					w.l.Error().Err(err).Msg("cannot write index")
				}
				segment.DecRef()
			}
		}
		g.tsdb.Tick(g.latestTS)
	}
	return
}

func encodeTagValue(name string, tagType databasev1.TagType, tagVal *modelv1.TagValue) *tagValue {
	tv := generateTagValue()
	tv.tag = name
	switch tagType {
	case databasev1.TagType_TAG_TYPE_INT:
		tv.valueType = pbv1.ValueTypeInt64
		if tagVal.GetInt() != nil {
			tv.value = convert.Int64ToBytes(tagVal.GetInt().GetValue())
		}
	case databasev1.TagType_TAG_TYPE_STRING:
		tv.valueType = pbv1.ValueTypeStr
		if tagVal.GetStr() != nil {
			tv.value = convert.StringToBytes(tagVal.GetStr().GetValue())
		}
	case databasev1.TagType_TAG_TYPE_DATA_BINARY:
		tv.valueType = pbv1.ValueTypeBinaryData
		if tagVal.GetBinaryData() != nil {
			tv.value = bytes.Clone(tagVal.GetBinaryData())
		}
	case databasev1.TagType_TAG_TYPE_INT_ARRAY:
		tv.valueType = pbv1.ValueTypeInt64Arr
		if tagVal.GetIntArray() == nil {
			return tv
		}
		tv.valueArr = make([][]byte, len(tagVal.GetIntArray().Value))
		for i := range tagVal.GetIntArray().Value {
			tv.valueArr[i] = convert.Int64ToBytes(tagVal.GetIntArray().Value[i])
		}
	case databasev1.TagType_TAG_TYPE_STRING_ARRAY:
		tv.valueType = pbv1.ValueTypeStrArr
		if tagVal.GetStrArray() == nil {
			return tv
		}
		tv.valueArr = make([][]byte, len(tagVal.GetStrArray().Value))
		for i := range tagVal.GetStrArray().Value {
			tv.valueArr[i] = []byte(tagVal.GetStrArray().Value[i])
		}
	default:
		logger.Panicf("unsupported tag value type: %T", tagVal.GetValue())
	}
	return tv
}

func appendField(dest []index.Field, fieldKey index.FieldKey, tagType databasev1.TagType, tagVal *modelv1.TagValue, noSort bool) []index.Field {
	switch tagType {
	case databasev1.TagType_TAG_TYPE_INT:
		v := tagVal.GetInt()
		if v == nil {
			return dest
		}
		f := index.NewIntField(fieldKey, v.Value)
		f.NoSort = noSort
		dest = append(dest, f)
	case databasev1.TagType_TAG_TYPE_STRING:
		v := tagVal.GetStr()
		if v == nil {
			return dest
		}
		f := index.NewStringField(fieldKey, v.Value)
		f.NoSort = noSort
		dest = append(dest, f)
	case databasev1.TagType_TAG_TYPE_DATA_BINARY:
		v := tagVal.GetBinaryData()
		if v == nil {
			return dest
		}
		f := index.NewBytesField(fieldKey, v)
		f.NoSort = noSort
		dest = append(dest, f)
	case databasev1.TagType_TAG_TYPE_INT_ARRAY:
		if tagVal.GetIntArray() == nil {
			return dest
		}
		for i := range tagVal.GetIntArray().Value {
			f := index.NewIntField(fieldKey, tagVal.GetIntArray().Value[i])
			f.NoSort = noSort
			dest = append(dest, f)
		}
	case databasev1.TagType_TAG_TYPE_STRING_ARRAY:
		if tagVal.GetStrArray() == nil {
			return dest
		}
		for i := range tagVal.GetStrArray().Value {
			f := index.NewStringField(fieldKey, tagVal.GetStrArray().Value[i])
			f.NoSort = noSort
			dest = append(dest, f)
		}
	default:
		logger.Panicf("unsupported tag value type: %T", tagVal.GetValue())
	}
	return dest
}
