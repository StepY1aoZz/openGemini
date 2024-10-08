// Code generated by tmpl; DO NOT EDIT.
// https://github.com/benbjohnson/tmpl
//
// Source: statistics.tmpl

// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package statistics

import (
	"sync"
	"time"

	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics/opsStat"
)

type StreamWindowStatistics struct {
	mu  sync.RWMutex
	buf []byte

	tags map[string]string
}

var instanceStreamWindowStatistics = &StreamWindowStatistics{}

func NewStreamWindowStatistics() *StreamWindowStatistics {
	return instanceStreamWindowStatistics
}

func (s *StreamWindowStatistics) Init(tags map[string]string) {
	s.tags = make(map[string]string)
	for k, v := range tags {
		s.tags[k] = v
	}
}

func (s *StreamWindowStatistics) Collect(buffer []byte) ([]byte, error) {
	data := map[string]interface{}{
		"window": "",
	}

	buffer = AddPointToBuffer("stream_window", s.tags, data, buffer)
	if len(s.buf) > 0 {
		s.mu.Lock()
		buffer = append(buffer, s.buf...)
		s.buf = s.buf[:0]
		s.mu.Unlock()
	}

	return buffer, nil
}

func (s *StreamWindowStatistics) CollectOps() []opsStat.OpsStatistic {
	data := map[string]interface{}{}

	return []opsStat.OpsStatistic{
		{
			Name:   "stream_window",
			Tags:   s.tags,
			Values: data,
		},
	}
}

func (s *StreamWindowStatistics) Push(item *StreamWindowStatItem) {
	if !item.Validate() {
		return
	}

	data := item.Values()
	tags := item.Tags()
	AllocTagMap(tags, s.tags)

	s.mu.Lock()
	s.buf = AddPointToBuffer("stream_window", tags, data, s.buf)
	s.mu.Unlock()
}

type StreamWindowStatItem struct {
	validateHandle func(item *StreamWindowStatItem) bool

	WindowIn               int64
	WindowProcess          int64
	WindowSkip             int64
	WindowFlushCost        int64
	WindowFlushMarshalCost int64
	WindowFlushWriteCost   int64
	WindowUpdateCost       int64
	WindowOutMinTime       int64
	WindowOutMaxTime       int64
	WindowStartTime        int64
	WindowEndTime          int64
	WindowGroupKeyCount    int64

	StreamID string

	begin    time.Time
	duration int64
}

func (s *StreamWindowStatItem) Duration() int64 {
	if s.duration == 0 {
		s.duration = time.Since(s.begin).Milliseconds()
	}
	return s.duration
}

func (s *StreamWindowStatItem) Push() {
	NewStreamWindowStatistics().Push(s)
}

func (s *StreamWindowStatItem) Validate() bool {
	if s.validateHandle == nil {
		return true
	}
	return s.validateHandle(s)
}

func (s *StreamWindowStatItem) Values() map[string]interface{} {
	return map[string]interface{}{
		"WindowIn":               s.WindowIn,
		"WindowProcess":          s.WindowProcess,
		"WindowSkip":             s.WindowSkip,
		"WindowFlushCost":        s.WindowFlushCost,
		"WindowFlushMarshalCost": s.WindowFlushMarshalCost,
		"WindowFlushWriteCost":   s.WindowFlushWriteCost,
		"WindowUpdateCost":       s.WindowUpdateCost,
		"WindowOutMinTime":       s.WindowOutMinTime,
		"WindowOutMaxTime":       s.WindowOutMaxTime,
		"WindowStartTime":        s.WindowStartTime,
		"WindowEndTime":          s.WindowEndTime,
		"WindowGroupKeyCount":    s.WindowGroupKeyCount,
		"Duration":               s.Duration(),
	}
}

func (s *StreamWindowStatItem) Tags() map[string]string {
	return map[string]string{
		"StreamID": s.StreamID,
	}
}
