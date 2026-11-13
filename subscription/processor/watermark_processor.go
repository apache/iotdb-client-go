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

package processor

import (
	"github.com/apache/iotdb-client-go/subscription/payload"
	"github.com/apache/iotdb-client-go/subscription/internal/record"

	"container/heap"
	"errors"
	"math"
	"sync"
	"time"
)

// WatermarkProcessorConfig controls client-side message reordering. Timestamp
// distances use server precision; wall-clock waits use time.Duration.
type WatermarkProcessorConfig struct {
	// MaxOutOfOrderness is subtracted from the minimum active region timestamp.
	// It must be nonnegative and uses the same unit as payload.RecordBatch.Timestamp.
	MaxOutOfOrderness int64
	// Timeout force-flushes buffered records when this long has passed without
	// output. Zero flushes on every Process call that produces no ordered output.
	Timeout time.Duration
	// StaleRegionTimeout excludes a region whose timestamp has not increased
	// for this long. Zero selects 30 seconds; a negative value disables exclusion.
	StaleRegionTimeout time.Duration
	// MaxBufferBytes limits estimated retained message bytes. Zero selects
	// 64 MiB. Exceeding the limit after ordered emission flushes the remainder.
	MaxBufferBytes int64
}

// WatermarkProcessor buffers whole record messages in maximum-timestamp order.
// Its watermark is the minimum latest timestamp across active topic/node/region
// sources, minus MaxOutOfOrderness. Only increasing timestamps refresh source
// activity; repeated server watermarks cannot keep a stalled source active.
// All-stale sources leave the watermark unchanged. Timeout and capacity flushing
// prevent indefinite buffering, but can emit records beyond the watermark.
// This is a reference processor; large out-of-order streams may require a
// downstream stream processing framework. Use one instance per consumer.
type WatermarkProcessor struct {
	mu sync.Mutex
	config WatermarkProcessorConfig
	buffer watermarkMessageHeap
	regions map[watermarkRegion]watermarkRegionProgress
	bufferedBytes int64
	lastEmit time.Time
	watermark int64
	hasWatermark bool
}

type watermarkRegion struct {
	topic string
	node int32
	region string
}

type watermarkRegionProgress struct {
	timestamp int64
	advanced time.Time
}

type watermarkBufferedMessage struct {
	message payload.SubscriptionMessage
	maximum int64
	size int64
}

type watermarkMessageHeap []watermarkBufferedMessage

func (h watermarkMessageHeap) Len() int { return len(h) }
func (h watermarkMessageHeap) Less(i, j int) bool { return h[i].maximum < h[j].maximum }
func (h watermarkMessageHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *watermarkMessageHeap) Push(value any) {
	*h = append(*h, value.(watermarkBufferedMessage))
}
func (h *watermarkMessageHeap) Pop() any {
	last := len(*h) - 1
	value := (*h)[last]
	(*h)[last] = watermarkBufferedMessage{}
	*h = (*h)[:last]
	return value
}

// NewWatermarkProcessor validates configuration without taking message ownership.
// Timeout must be nonnegative. StaleRegionTimeout and MaxBufferBytes select their
// documented defaults independently. Emission is message-level, not a global
// row sort; Rows still sorts each batch independently.
func NewWatermarkProcessor(config WatermarkProcessorConfig) (*WatermarkProcessor, error) {
	if config.MaxOutOfOrderness < 0 || config.Timeout < 0 || config.MaxBufferBytes < 0 {
		return nil, errors.New("subscription: watermark distance, timeout, and buffer limit must be nonnegative")
	}
	if config.StaleRegionTimeout == 0 {
		config.StaleRegionTimeout = 30 * time.Second
	}
	if config.MaxBufferBytes == 0 {
		config.MaxBufferBytes = 64 * 1024 * 1024
	}
	return &WatermarkProcessor{
		config: config,
		regions: make(map[watermarkRegion]watermarkRegionProgress),
		lastEmit: time.Now(),
	}, nil
}

// Process retains records until their maximum timestamp is at most the
// watermark, or a timeout/capacity flush occurs. Watermark events update source
// progress and are consumed. Empty input still evaluates staleness and timeout.
// An input-access error leaves processor state and input ownership unchanged.
func (p *WatermarkProcessor) Process(messages []payload.SubscriptionMessage) ([]payload.SubscriptionMessage, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	incoming := make([]watermarkBufferedMessage, 0, len(messages))
	for _, message := range messages {
		if message == nil {
			return nil, errors.New("subscription: watermark processor received a nil message")
		}
		if timestamp, ok := message.Watermark(); ok {
			incoming = append(incoming, watermarkBufferedMessage{message: message, maximum: timestamp})
			continue
		}
		batches, err := message.Records()
		if err != nil {
			return nil, err
		}
		maximum, found := int64(math.MinInt64), false
		for _, batch := range batches {
			if batch == nil {
				return nil, errors.New("subscription: watermark processor received a nil batch")
			}
			for row := 0; row < batch.Len(); row++ {
				timestamp, err := batch.Timestamp(row)
				if err != nil {
					return nil, err
				}
				if !found || timestamp > maximum {
					maximum = timestamp
				}
				found = true
			}
		}
		// As in Java, an empty message falls back to the wall clock in ms.
		if !found {
			maximum = now.UnixMilli()
		}
		size := message.EstimatedSize()
		if size < 0 {
			size = 0
		}
		incoming = append(incoming, watermarkBufferedMessage{message: message, maximum: maximum, size: size})
	}
	for _, entry := range incoming {
		context := entry.message.CommitContext()
		key := watermarkRegion{topic: context.Topic, node: context.DataNodeID, region: context.RegionID}
		previous, ok := p.regions[key]
		if !ok || entry.maximum > previous.timestamp {
			p.regions[key] = watermarkRegionProgress{timestamp: entry.maximum, advanced: now}
		}
		if entry.message.Kind() == payload.WatermarkMessage {
			entry.message.Release()
			continue
		}
		heap.Push(&p.buffer, entry)
		p.bufferedBytes += entry.size
	}
	p.updateWatermark(now)
	var emitted []payload.SubscriptionMessage
	for len(p.buffer) > 0 && p.hasWatermark && p.buffer[0].maximum <= p.watermark {
		entry := heap.Pop(&p.buffer).(watermarkBufferedMessage)
		p.bufferedBytes -= entry.size
		emitted = append(emitted, entry.message)
	}
	if p.bufferedBytes > p.config.MaxBufferBytes ||
		(len(emitted) == 0 && len(p.buffer) > 0 && now.Sub(p.lastEmit) >= p.config.Timeout) {
		emitted = append(emitted, p.flushLocked(now)...)
	}
	if len(emitted) > 0 {
		p.lastEmit = now
	}
	return emitted, nil
}

// Flush transfers every buffered record in maximum-timestamp order without
// resetting region progress or the current watermark.
func (p *WatermarkProcessor) Flush() ([]payload.SubscriptionMessage, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.flushLocked(time.Now()), nil
}

func (p *WatermarkProcessor) flushLocked(now time.Time) []payload.SubscriptionMessage {
	messages := make([]payload.SubscriptionMessage, 0, len(p.buffer))
	for len(p.buffer) > 0 {
		messages = append(messages, heap.Pop(&p.buffer).(watermarkBufferedMessage).message)
	}
	p.bufferedBytes = 0
	p.lastEmit = now
	return messages
}

// Buffered atomically snapshots the retained count and every renewable context.
func (p *WatermarkProcessor) Buffered() ProcessorBuffer {
	p.mu.Lock()
	defer p.mu.Unlock()
	result := ProcessorBuffer{Count: len(p.buffer)}
	for _, entry := range p.buffer {
		context := entry.message.CommitContext()
		if record.Committable(context) {
			result.Contexts = append(result.Contexts, context)
		}
	}
	return result
}

// Watermark reports the current ordering watermark and whether one is known.
// It can move backward when new regions appear or after a topic reset, as in Java.
func (p *WatermarkProcessor) Watermark() (int64, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.watermark, p.hasWatermark
}

// Reset releases buffered records and clears all ordering progress.
func (p *WatermarkProcessor) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, entry := range p.buffer {
		entry.message.Release()
	}
	p.buffer = nil
	p.regions = make(map[watermarkRegion]watermarkRegionProgress)
	p.bufferedBytes = 0
	p.lastEmit = time.Now()
	p.watermark, p.hasWatermark = 0, false
}

// ResetTopic releases only this topic's records and recomputes the watermark
// from the remaining active sources. Other topics keep their buffered records.
func (p *WatermarkProcessor) ResetTopic(topic string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	retained := p.buffer[:0]
	for _, entry := range p.buffer {
		if entry.message.CommitContext().Topic == topic {
			p.bufferedBytes -= entry.size
			entry.message.Release()
		} else {
			retained = append(retained, entry)
		}
	}
	clear(p.buffer[len(retained):])
	p.buffer = retained
	heap.Init(&p.buffer)
	for key := range p.regions {
		if key.topic == topic {
			delete(p.regions, key)
		}
	}
	p.watermark, p.hasWatermark = 0, false
	p.updateWatermark(time.Now())
}

func (p *WatermarkProcessor) updateWatermark(now time.Time) {
	var minimum int64
	found := false
	for _, progress := range p.regions {
		if p.config.StaleRegionTimeout >= 0 && now.Sub(progress.advanced) > p.config.StaleRegionTimeout {
			continue
		}
		if !found || progress.timestamp < minimum {
			minimum, found = progress.timestamp, true
		}
	}
	if found {
		// Saturate subtraction to preserve ordering at the timestamp boundary.
		p.watermark = math.MinInt64
		if minimum >= math.MinInt64 + p.config.MaxOutOfOrderness {
			p.watermark = minimum - p.config.MaxOutOfOrderness
		}
		p.hasWatermark = true
	}
}
