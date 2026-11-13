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

package subscription

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/apache/iotdb-client-go/v2/client"
)

// ColumnAlignProcessor forward-fills NULL columns with the latest known non-null
// value for the same topic, device, and column. It does not retain messages.
// A cached value fills a NULL only when its timestamp is strictly earlier;
// an observed non-null value updates the cache at equal or greater timestamps.
// Processing follows raw Tablet row order, matching Java, rather than Rows order.
// Tree devices use the device path; table devices use database, table, and TAG
// values for each row, including tables with no TAG columns. Use one instance
// per consumer. Cache size grows with observed devices and columns until reset.
type ColumnAlignProcessor struct {
	mu sync.Mutex
	dialect Dialect
	lastValues map[string]map[columnAlignDevice]map[columnAlignColumn]columnAlignValue
}

type columnAlignDevice struct {
	database string
	target string
	tags string
}

type columnAlignColumn struct {
	name string
	dataType client.TSDataType
}

type columnAlignValue struct {
	timestamp int64
	value any
}

type columnAlignMessage struct {
	message SubscriptionMessage
	batches []columnAlignBatch
}

type columnAlignBatch struct {
	database string
	timeSelected bool
	tablet decodedSubscriptionTablet
}

// NewColumnAlignProcessor creates a processor for one model. An empty dialect
// selects TreeDialect, matching Java's no-argument constructor. Table consumers
// must select TableDialect. Different databases never share cached table values.
func NewColumnAlignProcessor(dialect Dialect) (*ColumnAlignProcessor, error) {
	if dialect == "" {
		dialect = TreeDialect
	}
	if dialect != TreeDialect && dialect != TableDialect {
		return nil, fmt.Errorf("subscription: invalid column alignment dialect %q", dialect)
	}
	return &ColumnAlignProcessor{
		dialect: dialect,
		lastValues: make(map[string]map[columnAlignDevice]map[columnAlignColumn]columnAlignValue),
	}, nil
}

// Process returns record messages with aligned, independently owned buffers.
// It consumes and releases the original record messages only after every input
// has been read successfully. Kind, commit identity, schema, raw row order, and
// time projection are preserved. Watermarks pass through unchanged. Cached byte
// values are copied so releasing an earlier message cannot invalidate the cache.
// Access failures or model mismatches leave cache and input ownership unchanged.
func (p *ColumnAlignProcessor) Process(messages []SubscriptionMessage) ([]SubscriptionMessage, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	incoming := make([]columnAlignMessage, 0, len(messages))
	for _, message := range messages {
		if message == nil {
			return nil, errors.New("subscription: column alignment received a nil message")
		}
		entry := columnAlignMessage{message: message}
		if message.Kind() == WatermarkMessage {
			incoming = append(incoming, entry)
			continue
		}
		batches, err := message.Records()
		if err != nil {
			return nil, err
		}
		for _, batch := range batches {
			if batch == nil {
				return nil, errors.New("subscription: column alignment received a nil batch")
			}
			if batch.Dialect() != p.dialect {
				return nil, fmt.Errorf("subscription: column alignment dialect %q does not match batch dialect %q", p.dialect, batch.Dialect())
			}
			tablet := decodedSubscriptionTablet{
				Target: batch.Target(),
				Columns: append([]Column(nil), batch.Columns()...),
				RowCount: batch.Len(),
			}
			tablet.Timestamps = make([]int64, tablet.RowCount)
			tablet.Values = make([][]any, len(tablet.Columns))
			for column := range tablet.Values {
				tablet.Values[column] = make([]any, tablet.RowCount)
			}
			for row := 0; row < tablet.RowCount; row++ {
				tablet.Timestamps[row], err = batch.Timestamp(row)
				if err != nil {
					return nil, err
				}
				for column := range tablet.Columns {
					value, err := batch.Value(row, column)
					if err != nil {
						return nil, err
					}
					tablet.Values[column][row] = cloneColumnAlignValue(value)
				}
			}
			entry.batches = append(entry.batches, columnAlignBatch{
				database: batch.Database(),
				timeSelected: batch.TimeSelected(),
				tablet: tablet,
			})
		}
		incoming = append(incoming, entry)
	}
	result := make([]SubscriptionMessage, 0, len(incoming))
	for _, entry := range incoming {
		if entry.message.Kind() == WatermarkMessage {
			result = append(result, entry.message)
			continue
		}
		context := entry.message.CommitContext()
		var batches []RecordBatch
		for _, batch := range entry.batches {
			p.fillTablet(context.Topic, batch.database, &batch.tablet)
			batches = append(batches, newSubscriptionRecordBatch(p.dialect, batch.database, batch.tablet, batch.timeSelected))
		}
		result = append(result, newSubscriptionRecordMessage(context, batches))
	}
	for _, entry := range incoming {
		if entry.message.Kind() != WatermarkMessage {
			entry.message.Release()
		}
	}
	return result, nil
}

func (p *ColumnAlignProcessor) fillTablet(topic, database string, tablet *decodedSubscriptionTablet) {
	topicCache := p.lastValues[topic]
	if topicCache == nil {
		topicCache = make(map[columnAlignDevice]map[columnAlignColumn]columnAlignValue)
		p.lastValues[topic] = topicCache
	}
	for row := 0; row < tablet.RowCount; row++ {
		timestamp := tablet.Timestamps[row]
		device := columnAlignDevice{target: tablet.Target}
		if p.dialect == TableDialect {
			device.database = database
			device.tags = columnAlignTags(tablet, row)
		}
		cache := topicCache[device]
		if cache == nil {
			cache = make(map[columnAlignColumn]columnAlignValue)
			topicCache[device] = cache
		}
		for column, schema := range tablet.Columns {
			// A schema type change cannot reuse an incompatible cached value.
			key := columnAlignColumn{name: schema.Name, dataType: schema.DataType}
			cached, ok := cache[key]
			value := tablet.Values[column][row]
			if value == nil {
				if ok && cached.timestamp < timestamp {
					tablet.Values[column][row] = cloneColumnAlignValue(cached.value)
				}
			} else if !ok || timestamp >= cached.timestamp {
				cache[key] = columnAlignValue{timestamp: timestamp, value: cloneColumnAlignValue(value)}
			}
		}
	}
}

// columnAlignTags encodes ordered TAG segments without delimiter collisions.
// Trailing NULLs are ignored, matching TsFile's table device identity; a NULL
// segment remains distinct from an empty string and from embedded delimiters.
func columnAlignTags(tablet *decodedSubscriptionTablet, row int) string {
	var tags []any
	lastNonNull := 0
	for column, schema := range tablet.Columns {
		if schema.Category == client.TAG {
			value := tablet.Values[column][row]
			tags = append(tags, value)
			if value != nil {
				lastNonNull = len(tags)
			}
		}
	}
	var key strings.Builder
	for _, value := range tags[:lastNonNull] {
		if value == nil {
			key.WriteString("-1:")
			continue
		}
		segment := fmt.Sprint(value)
		if binary, ok := value.([]byte); ok {
			segment = string(binary)
		}
		key.WriteString(strconv.Itoa(len(segment)))
		key.WriteByte(':')
		key.WriteString(segment)
	}
	return key.String()
}

func cloneColumnAlignValue(value any) any {
	if binary, ok := value.([]byte); ok {
		return append([]byte(nil), binary...)
	}
	return value
}

// Flush returns no messages because alignment does not buffer deliveries.
func (p *ColumnAlignProcessor) Flush() ([]SubscriptionMessage, error) {
	return nil, nil
}

// Reset clears cached values for every topic.
func (p *ColumnAlignProcessor) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lastValues = make(map[string]map[columnAlignDevice]map[columnAlignColumn]columnAlignValue)
}

// ResetTopic clears only the target topic's cached alignment values.
func (p *ColumnAlignProcessor) ResetTopic(topic string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.lastValues, topic)
}
