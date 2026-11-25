/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package processor_test

import (
	"github.com/apache/iotdb-client-go/v2/subscription/payload"
	subprocessor "github.com/apache/iotdb-client-go/v2/subscription/processor"

	"errors"
	"math"
	"testing"
	"time"

	"github.com/apache/iotdb-client-go/v2/subscription"
)

func newTestWatermarkProcessor(t *testing.T, config subprocessor.WatermarkProcessorConfig) *subprocessor.WatermarkProcessor {
	t.Helper()
	processor, err := subprocessor.NewWatermarkProcessor(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(processor.Reset)
	return processor
}

func TestWatermarkOrdersWholeMessagesAfterEveryRegionAdvances(t *testing.T) {
	processor := newTestWatermarkProcessor(t, subprocessor.WatermarkProcessorConfig{
		MaxOutOfOrderness: 10, Timeout: time.Hour, StaleRegionTimeout: -1,
	})
	high := processorRecord("topic", "a", 1, processorTreeBatch([]int64{200}))
	low := processorRecord("topic", "b", 2, processorTreeBatch([]int64{50}))
	middle := processorRecord("topic", "a", 3, processorTreeBatch([]int64{100, 1}))
	high.context.CommitID = payload.InvalidCommitID
	high.context.Writer = &payload.WriterPosition{ID: payload.WriterID{RegionID: "a", NodeID: 7}, Progress: payload.WriterProgress{PhysicalTime: 200, LocalSeq: 1}}
	messages, err := processor.Process([]payload.SubscriptionMessage{high, low, middle})
	if err != nil || len(messages) != 0 {
		t.Fatalf("premature delivery: %d messages, error=%v", len(messages), err)
	}
	buffer := processor.Buffered()
	if buffer.Count != 3 || len(buffer.Contexts) != 3 {
		t.Fatalf("renewal snapshot lost buffered deliveries: %+v", buffer)
	}
	legacy := make(map[int64]bool)
	writerRenewed := false
	for _, context := range buffer.Contexts {
		if context.Writer != nil {
			writerRenewed = context.Writer.ID.NodeID == 7 && context.Writer.Progress.LocalSeq == 1
		} else {
			legacy[context.CommitID] = true
		}
	}
	if !writerRenewed || !legacy[2] || !legacy[3] {
		t.Fatalf("renewal snapshot omitted an independent delivery: %+v", buffer.Contexts)
	}
	messages, err = processor.Process([]payload.SubscriptionMessage{processorWatermark("topic", "b", 250)})
	if err != nil {
		t.Fatal(err)
	}
	requireProcessorCommitIDs(t, messages, 2, 3)
	batches, err := messages[1].Records()
	if err != nil || len(batches) != 1 || batches[0].Len() != 2 {
		t.Fatalf("message was split instead of emitted whole: batches=%v error=%v", batches, err)
	}
	releaseProcessorMessages(messages)
	if buffer = processor.Buffered(); buffer.Count != 1 || len(buffer.Contexts) != 1 || buffer.Contexts[0].Writer == nil {
		t.Fatalf("remaining writer delivery is not renewable: %+v", buffer)
	}
	messages, err = processor.Process([]payload.SubscriptionMessage{processorWatermark("topic", "a", 300)})
	if err != nil || len(messages) != 1 || messages[0] != high {
		t.Fatalf("high message not released after all sources advanced: messages=%v error=%v", messages, err)
	}
	releaseProcessorMessages(messages)
}

func TestWatermarkResetTopicDropsOnlyItsRecordsAndOrderingSource(t *testing.T) {
	processor := newTestWatermarkProcessor(t, subprocessor.WatermarkProcessorConfig{
		MaxOutOfOrderness: 10, Timeout: time.Hour, StaleRegionTimeout: -1,
	})
	a := processorRecord("a", "region", 1, processorTreeBatch([]int64{20}))
	b := processorRecord("b", "region", 2, processorTreeBatch([]int64{100}))
	messages, err := processor.Process([]payload.SubscriptionMessage{a, b})
	if err != nil || len(messages) != 0 {
		t.Fatalf("initial delivery=%v error=%v", messages, err)
	}
	processor.ResetTopic("a")
	if _, err := a.Records(); !errors.Is(err, subscription.ErrDataReleased) {
		t.Fatalf("reset topic retained its borrowed data: %v", err)
	}
	if _, err := b.Records(); err != nil {
		t.Fatalf("other topic's record was released: %v", err)
	}
	buffer := processor.Buffered()
	if buffer.Count != 1 || len(buffer.Contexts) != 1 || buffer.Contexts[0].Topic != "b" {
		t.Fatalf("reset corrupted other topic's renewal: %+v", buffer)
	}
	messages, err = processor.Process([]payload.SubscriptionMessage{processorWatermark("b", "region", 120)})
	if err != nil {
		t.Fatal(err)
	}
	requireProcessorCommitIDs(t, messages, 2)
	releaseProcessorMessages(messages)
}

func TestWatermarkForceFlushDoesNotWaitForRegionProgress(t *testing.T) {
	for _, test := range []struct {
		name    string
		timeout time.Duration
		limit   int64
	}{
		{name: "capacity", timeout: time.Hour, limit: 1},
		{name: "zero_timeout", timeout: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			processor := newTestWatermarkProcessor(t, subprocessor.WatermarkProcessorConfig{
				MaxOutOfOrderness: 1000, Timeout: test.timeout, MaxBufferBytes: test.limit, StaleRegionTimeout: -1,
			})
			high := processorRecord("topic", "region", 2, processorTreeBatch([]int64{20}))
			low := processorRecord("topic", "region", 1, processorTreeBatch([]int64{10}))
			messages, err := processor.Process([]payload.SubscriptionMessage{high, low})
			if err != nil {
				t.Fatal(err)
			}
			requireProcessorCommitIDs(t, messages, 1, 2)
			releaseProcessorMessages(messages)
			if buffer := processor.Buffered(); buffer.Count != 0 || len(buffer.Contexts) != 0 {
				t.Fatalf("flushed records remain renewable: %+v", buffer)
			}
		})
	}
}

func TestWatermarkTimestampSubtractionCannotWrapAndDeliverEarly(t *testing.T) {
	processor := newTestWatermarkProcessor(t, subprocessor.WatermarkProcessorConfig{
		MaxOutOfOrderness: 10, Timeout: time.Hour, StaleRegionTimeout: -1,
	})
	_, err := processor.Process([]payload.SubscriptionMessage{processorWatermark("topic", "region", math.MinInt64+5)})
	if err != nil {
		t.Fatal(err)
	}
	record := processorRecord("topic", "region", 1, processorTreeBatch([]int64{math.MinInt64 + 1}))
	messages, err := processor.Process([]payload.SubscriptionMessage{record})
	if err != nil || len(messages) != 0 {
		t.Fatalf("underflow delivered an unready message: messages=%v error=%v", messages, err)
	}
	messages, err = processor.Flush()
	if err != nil {
		t.Fatal(err)
	}
	requireProcessorCommitIDs(t, messages, 1)
	releaseProcessorMessages(messages)
}

func TestWatermarkReadFailureDoesNotAdvanceOrTakeOwnership(t *testing.T) {
	processor := newTestWatermarkProcessor(t, subprocessor.WatermarkProcessorConfig{
		MaxOutOfOrderness: 10, Timeout: time.Hour, StaleRegionTimeout: -1,
	})
	seed := processorRecord("topic", "a", 1, processorTreeBatch([]int64{100}))
	if messages, err := processor.Process([]payload.SubscriptionMessage{seed}); err != nil || len(messages) != 0 {
		t.Fatalf("initial delivery=%v error=%v", messages, err)
	}
	readFailure := errors.New("input timestamp unavailable")
	brokenBatch := processorTreeBatch([]int64{600})
	brokenBatch.timestampErr = readFailure
	broken := processorRecord("topic", "a", 3, brokenBatch)
	good := processorRecord("topic", "a", 2, processorTreeBatch([]int64{500}))
	messages, err := processor.Process([]payload.SubscriptionMessage{
		processorWatermark("topic", "b", 1000), good, broken,
	})
	if !errors.Is(err, readFailure) || len(messages) != 0 {
		t.Fatalf("failed input delivered data or lost error: messages=%v error=%v", messages, err)
	}
	for _, input := range []*processorInputMessage{good, broken} {
		if _, err := input.Records(); err != nil {
			t.Fatalf("failed round consumed caller-owned input: %v", err)
		}
		defer input.Release()
	}
	next := processorRecord("topic", "a", 4, processorTreeBatch([]int64{101}))
	messages, err = processor.Process([]payload.SubscriptionMessage{next})
	if err != nil || len(messages) != 0 {
		t.Fatalf("failed round advanced ordering: messages=%v error=%v", messages, err)
	}
	messages, err = processor.Flush()
	if err != nil {
		t.Fatal(err)
	}
	requireProcessorCommitIDs(t, messages, 1, 4)
	releaseProcessorMessages(messages)
}
