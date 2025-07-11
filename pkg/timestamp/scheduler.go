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

package timestamp

import (
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/pkg/errors"
	"github.com/robfig/cron/v3"

	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

var (
	// ErrSchedulerClosed indicates the scheduler is closed.
	ErrSchedulerClosed = errors.New("the scheduler is closed")

	// ErrTaskDuplicated indicates registered task already exists.
	ErrTaskDuplicated = errors.New("the task is duplicated")
)

// SchedulerAction is an executable when a trigger is fired
// now is the trigger time, logger has a context indicating the task's identity.
type SchedulerAction func(now time.Time, logger *logger.Logger) bool

// Scheduler maintains a registry of tasks and their duty cycle.
// It also provides a Trigger method to fire a task that is scheduled by a MockClock.
type Scheduler struct {
	clock Clock
	l     *logger.Logger
	tasks map[string]*task
	sync.RWMutex
	isMock bool
	closed bool
}

// NewScheduler returns an instance of Scheduler.
func NewScheduler(parent *logger.Logger, clock Clock) *Scheduler {
	var isMock bool
	if _, ok := clock.(MockClock); ok {
		isMock = true
	}
	return &Scheduler{
		isMock: isMock,
		l:      parent.Named("scheduler"),
		clock:  clock,
		tasks:  make(map[string]*task),
	}
}

// Register adds the given task's SchedulerAction to the Scheduler,
// and associate the given schedule expression.
func (s *Scheduler) Register(name string, options cron.ParseOption, expr string, action SchedulerAction) error {
	s.Lock()
	defer s.Unlock()
	if s.closed {
		return ErrSchedulerClosed
	}
	if _, ok := s.tasks[name]; ok {
		return errors.WithMessage(ErrTaskDuplicated, name)
	}
	parser := cron.NewParser(options)
	schedule, err := parser.Parse(expr)
	if err != nil {
		return err
	}
	var clock Clock
	if s.isMock {
		mc := NewMockClock()
		mc.Set(s.clock.Now())
		clock = mc
	} else {
		clock = s.clock
	}
	t := newTask(s.l.Named(name), name, clock, schedule, action)
	s.tasks[name] = t
	go func() {
		t.run()
		t.close()
		s.Lock()
		defer s.Unlock()
		delete(s.tasks, name)
	}()
	return nil
}

// Trigger fire a task that is scheduled by a MockTime.
// A real clock-based task will ignore this trigger, and return false.
// If the task's name is unknown, it returns false.
func (s *Scheduler) Trigger(name string) bool {
	if !s.isMock {
		return false
	}
	var t *task
	var ok bool
	s.RLock()
	t, ok = s.tasks[name]
	s.RUnlock()
	if !ok {
		return false
	}
	c := t.clock.(MockClock)
	c.Set(s.clock.Now())
	return true
}

// Interval returns the duration between two consecutive executions of the task.
// If the task is not registered, it returns false.
func (s *Scheduler) Interval(name string) (interval time.Duration, next time.Time, exist bool) {
	var t *task
	var ok bool
	s.RLock()
	t, ok = s.tasks[name]
	s.RUnlock()
	if !ok {
		return
	}
	t1 := t.schedule.Next(t.clock.Now())
	t2 := t.schedule.Next(t1)
	return t2.Sub(t1), t1, true
}

// Closed returns whether the Scheduler is closed.
func (s *Scheduler) Closed() bool {
	s.RLock()
	defer s.RUnlock()
	return s.closed
}

// Close the Scheduler and shut down all registered tasks.
func (s *Scheduler) Close() {
	s.Lock()
	defer s.Unlock()
	s.closed = true
	for k, t := range s.tasks {
		t.close()
		delete(s.tasks, k)
	}
}

// Metrics returns the metrics of all registered tasks.
func (s *Scheduler) Metrics() map[string]*SchedulerMetrics {
	s.RLock()
	defer s.RUnlock()
	m := make(map[string]*SchedulerMetrics, len(s.tasks))
	for k, t := range s.tasks {
		m[k] = t.metrics
	}
	return m
}

type task struct {
	clock    Clock
	schedule cron.Schedule
	closer   *run.Closer
	l        *logger.Logger
	action   SchedulerAction
	metrics  *SchedulerMetrics
	name     string
}

func newTask(l *logger.Logger, name string, clock clock.Clock, schedule cron.Schedule, action SchedulerAction) *task {
	return &task{
		l:        l,
		name:     name,
		clock:    clock,
		schedule: schedule,
		action:   action,
		closer:   run.NewCloser(0),
		metrics:  &SchedulerMetrics{},
	}
}

func (t *task) run() {
	if !t.closer.AddRunning() {
		return
	}
	defer t.closer.Done()
	now := t.clock.Now()
	t.l.Info().Str("name", t.name).Time("now", now).Msg("start")
	t.metrics.TotalJobsStarted.Add(1)
	defer t.metrics.TotalJobsFinished.Add(1)
	for {
		next := t.schedule.Next(now)
		d := next.Sub(now)
		if e := t.l.Debug(); e.Enabled() {
			e.Str("name", t.name).Time("now", now).Time("next", next).Dur("dur", d).Msg("schedule to")
		}
		timer := t.clock.Timer(d)
		select {
		case now = <-timer.C:
			if e := t.l.Debug(); e.Enabled() {
				e.Str("name", t.name).Time("now", now).Msg("wake")
			}
			if !func() (ret bool) {
				t.metrics.TotalTasksStarted.Add(1)
				start := time.Now()
				defer func() {
					t.metrics.TotalTasksFinished.Add(1)
					t.metrics.TotalTaskLatencyInNanoseconds.Add(time.Since(start).Nanoseconds())
					if r := recover(); r != nil {
						t.l.Error().Str("name", t.name).Interface("panic", r).Str("stack", string(debug.Stack())).Msg("panic")
						ret = true
						t.metrics.TotalTasksPanic.Add(1)
					}
				}()
				resultCh := make(chan bool, 1)
				timeoutCh := t.clock.Timer(5 * time.Minute).C

				go func() {
					resultCh <- t.action(now, t.l)
				}()

				select {
				case result := <-resultCh:
					return result
				case <-timeoutCh:
					t.l.Error().Str("name", t.name).Msg("action timed out")
					t.metrics.TotalTasksTimeout.Add(1)
					return true
				}
			}() {
				t.l.Info().Str("name", t.name).Msg("action stops the task")
				return
			}
		case <-t.closer.CloseNotify():
			timer.Stop()
			t.l.Info().Str("name", t.name).Msg("closed")
			return
		}
	}
}

func (t *task) close() {
	t.closer.CloseThenWait()
}

// SchedulerMetrics collects the metrics of a Scheduler.
type SchedulerMetrics struct {
	TotalJobsStarted              atomic.Uint64
	TotalJobsFinished             atomic.Uint64
	TotalTasksStarted             atomic.Uint64
	TotalTasksFinished            atomic.Uint64
	TotalTasksPanic               atomic.Uint64
	TotalTasksTimeout             atomic.Uint64
	TotalTaskLatencyInNanoseconds atomic.Int64
}
