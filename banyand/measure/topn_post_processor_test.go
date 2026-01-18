package measure

import (
	"testing"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestBlockCursor_MergeTopNResult(t *testing.T) {
	tests := []struct {
		name        string
		srcTopNVal  *TopNValue
		destTopNVal *TopNValue
		sort        modelv1.Sort
		topN        int32
		wantTopNVal *TopNValue
	}{
		{
			name: "Test block merge TopN result",
			srcTopNVal: &TopNValue{
				valueName:      "value",
				entityTagNames: []string{"entity_id"},
				values:         []int64{1000, 200, 300, 400, 500},
				entities: [][]*modelv1.TagValue{
					{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "entity_1"}}}},
					{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "entity_2"}}}},
					{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "entity_3"}}}},
					{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "entity_4"}}}},
					{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "entity_5"}}}},
				},
			},
			destTopNVal: &TopNValue{
				valueName:      "value",
				entityTagNames: []string{"entity_id"},
				values:         []int64{550, 200, 500, 600, 400},
				entities: [][]*modelv1.TagValue{
					{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "entity_3"}}}},
					{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "entity_4"}}}},
					{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "entity_5"}}}},
					{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "entity_6"}}}},
					{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "entity_7"}}}},
				},
			},
			sort: modelv1.Sort_SORT_DESC,
			topN: 3,
			wantTopNVal: &TopNValue{
				valueName:      "value",
				entityTagNames: []string{"entity_id"},
				values:         []int64{550, 600, 1000},
				entities: [][]*modelv1.TagValue{
					{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "entity_3"}}}},
					{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "entity_6"}}}},
					{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "entity_1"}}}},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srcBuf, err := tt.srcTopNVal.marshal(make([]byte, 0, 256))
			require.NoError(t, err)

			destBuf, err := tt.destTopNVal.marshal(make([]byte, 0, 256))
			require.NoError(t, err)

			bc := &blockCursor{
				bm:          blockMetadata{seriesID: 1},
				tagFamilies: nil,
				fields: columnFamily{
					columns: []column{
						{name: "value", valueType: pbv1.ValueTypeBinaryData, values: [][]byte{destBuf}},
					},
				},
				timestamps:     []int64{1},
				versions:       []int64{2},
				idx:            0,
				schemaTagTypes: map[string]pbv1.ValueType{},
			}

			result := &model.MeasureResult{
				Timestamps: []int64{1},
				Versions:   []int64{1},
				TagFamilies: []model.TagFamily{
					{Name: "_topN", Tags: []model.Tag{
						{Name: "name", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
				},
				Fields: []model.Field{
					{
						Name:   "value",
						Values: []*modelv1.FieldValue{{Value: &modelv1.FieldValue_BinaryData{BinaryData: srcBuf}}},
					},
				},
			}

			topNPostAggregator := CreateTopNPostProcessor(tt.topN, modelv1.AggregationFunction_AGGREGATION_FUNCTION_UNSPECIFIED, tt.sort)

			expectedTagValue := "endpoint_resp_time-service"
			storedIndexValue := map[common.SeriesID]map[string]*modelv1.TagValue{
				1: {
					"name": {
						Value: &modelv1.TagValue_Str{
							Str: &modelv1.Str{Value: expectedTagValue},
						},
					},
				},
			}

			bc.mergeTopNResult(result, storedIndexValue, topNPostAggregator)

			tagValue := result.TagFamilies[0].Tags[0].Values[0]
			require.Equal(t, expectedTagValue, tagValue.GetStr().GetValue())

			mergedFieldValue := result.Fields[0].Values[0]
			require.NotNil(t, mergedFieldValue.GetBinaryData())

			gotTopNVal := GenerateTopNValue()
			defer ReleaseTopNValue(gotTopNVal)
			decoder := GenerateTopNValuesDecoder()
			defer ReleaseTopNValuesDecoder(decoder)

			err = gotTopNVal.Unmarshal(mergedFieldValue.GetBinaryData(), decoder)
			require.NoError(t, err)

			require.Equal(t, tt.wantTopNVal.valueName, gotTopNVal.valueName)
			require.Equal(t, tt.wantTopNVal.entityTagNames, gotTopNVal.entityTagNames)
			require.Equal(t, tt.wantTopNVal.values, gotTopNVal.values)
			diff := cmp.Diff(tt.wantTopNVal.entities, gotTopNVal.entities, protocmp.Transform())
			require.Empty(t, diff, "entities differ: %s", diff)
		})
	}
}
