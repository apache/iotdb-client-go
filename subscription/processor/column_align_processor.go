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
	"bytes"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/apache/iotdb-client-go/subscription"
	"github.com/apache/iotdb-client-go/subscription/internal/record"
	"github.com/apache/iotdb-client-go/subscription/payload"
	"github.com/apache/iotdb-client-go/v2/client"
)

// ColumnAlignProcessor forward-fills NULL columns with the latest known non-null
// value for the same topic, device, and column. Decoded subscription messages
// are changed in place under one batch lock; user-provided RecordBatch
// implementations use a copying fallback. It does not retain messages.
type ColumnAlignProcessor struct {
	mu         sync.Mutex
	dialect    subscription.Dialect
	lastValues map[string]map[columnAlignDeviceKey][]columnAlignDeviceEntry
}

type columnAlignDeviceKey struct {
	database string
	target   string
	tagHash  uint64
}

type columnAlignDeviceEntry struct {
	tags   []any
	values map[columnAlignColumn]columnAlignValue
}

type columnAlignColumn struct {
	name     string
	dataType client.TSDataType
}

type columnAlignValue struct {
	timestamp int64
	value     any
}

type columnAlignTablet interface {
	target() string
	rowCount() int
	columns() []payload.Column
	timestampAt(int) int64
	valueAt(int, int) (any, bool)
	setValueAt(int, int, any) bool
}

type tabletReader struct{ tablet *record.Tablet }

func (r tabletReader) target() string { return r.tablet.Target }
func (r tabletReader) rowCount() int { return r.tablet.RowCount }
func (r tabletReader) columns() []payload.Column { return r.tablet.Columns }
func (r tabletReader) timestampAt(row int) int64 {
	if r.tablet.Timestamps == nil {
		return 0
	}
	return r.tablet.Timestamps[row]
}
func (r tabletReader) valueAt(row, column int) (any, bool) {
	if column < 0 || column >= len(r.tablet.Values) {
		return nil, false
	}
	return r.tablet.Values[column].Value(row)
}
func (r tabletReader) setValueAt(row, column int, value any) bool {
	if column < 0 || column >= len(r.tablet.Values) {
		return false
	}
	return r.tablet.Values[column].Set(row, value)
}

type tabletMutator struct{ tablet record.TabletMutator }

func (m tabletMutator) target() string { return m.tablet.Target() }
func (m tabletMutator) rowCount() int { return m.tablet.RowCount() }
func (m tabletMutator) columns() []payload.Column { return m.tablet.Columns() }
func (m tabletMutator) timestampAt(row int) int64 { return m.tablet.Timestamp(row) }
func (m tabletMutator) valueAt(row, column int) (any, bool) { return m.tablet.Value(row, column) }
func (m tabletMutator) setValueAt(row, column int, value any) bool {
	return m.tablet.SetValue(row, column, value)
}

// NewColumnAlignProcessor creates a processor for one model. An empty dialect
// selects subscription.TreeDialect, matching Java's no-argument constructor.
func NewColumnAlignProcessor(dialect subscription.Dialect) (*ColumnAlignProcessor, error) {
	if dialect == "" {
		dialect = subscription.TreeDialect
	}
	if dialect != subscription.TreeDialect && dialect != subscription.TableDialect {
		return nil, fmt.Errorf("subscription: invalid column alignment dialect %q", dialect)
	}
	return &ColumnAlignProcessor{
		dialect:    dialect,
		lastValues: make(map[string]map[columnAlignDeviceKey][]columnAlignDeviceEntry),
	}, nil
}

// Process returns aligned messages. For decoded subscription messages it
// transfers the existing typed buffers to the result instead of copying the
// Tablet. Kind, commit identity, schema, raw row order, and time projection are
// preserved. Watermarks pass through unchanged.
func (p *ColumnAlignProcessor) Process(messages []payload.SubscriptionMessage) ([]payload.SubscriptionMessage, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.validate(messages); err != nil {
		return nil, err
	}
	processed, owned, err := record.TryMutateRecordMessages(messages, func(context payload.CommitContext, tablet record.TabletMutator) error {
		return p.fillTablet(context.Topic, tablet.Database(), tabletMutator{tablet: tablet})
	})
	if owned {
		if err != nil {
			return nil, err
		}
		return processed, nil
	}
	return p.copyAndAlign(messages)
}

func (p *ColumnAlignProcessor) validate(messages []payload.SubscriptionMessage) error {
	for _, message := range messages {
		if message == nil {
			return errors.New("subscription: column alignment received a nil message")
		}
		if message.Kind() == payload.WatermarkMessage {
			continue
		}
		batches, err := message.Records()
		if err != nil {
			return err
		}
		for _, batch := range batches {
			if batch == nil {
				return errors.New("subscription: column alignment received a nil batch")
			}
			if batch.Dialect() != p.dialect {
				return fmt.Errorf("subscription: column alignment dialect %q does not match batch dialect %q", p.dialect, batch.Dialect())
			}
		}
	}
	return nil
}

func (p *ColumnAlignProcessor) copyAndAlign(messages []payload.SubscriptionMessage) ([]payload.SubscriptionMessage, error) {
	incoming := make([]columnAlignCopiedMessage, 0, len(messages))
	for _, message := range messages {
		if message.Kind() == payload.WatermarkMessage {
			incoming = append(incoming, columnAlignCopiedMessage{message: message})
			continue
		}
		batches, err := message.Records()
		if err != nil {
			return nil, err
		}
		entry := columnAlignCopiedMessage{message: message}
		for _, batch := range batches {
			if batch == nil {
				return nil, errors.New("subscription: column alignment received a nil batch")
			}
			if batch.Dialect() != p.dialect {
				return nil, fmt.Errorf("subscription: column alignment dialect %q does not match batch dialect %q", p.dialect, batch.Dialect())
			}
			tablet := record.Tablet{
				Target:   batch.Target(),
				Columns:  append([]payload.Column(nil), batch.Columns()...),
				RowCount: batch.Len(),
			}
			tablet.Timestamps = make([]int64, tablet.RowCount)
			tablet.Values = make([]record.TabletColumn, len(tablet.Columns))
			for column, schema := range tablet.Columns {
				tablet.Values[column] = record.NewTabletColumn(schema.DataType, tablet.RowCount)
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
					if !tablet.Values[column].Set(row, cloneColumnAlignValue(value)) {
						return nil, fmt.Errorf("subscription: column alignment cannot store value for column %d", column)
					}
				}
			}
			entry.batches = append(entry.batches, columnAlignCopiedBatch{
				database:     batch.Database(),
				timeSelected: batch.TimeSelected(),
				tablet:       tablet,
			})
		}
		incoming = append(incoming, entry)
	}
	result := make([]payload.SubscriptionMessage, 0, len(incoming))
	for _, entry := range incoming {
		if entry.message.Kind() == payload.WatermarkMessage {
			result = append(result, entry.message)
			continue
		}
		context := record.ReadCommitContext(entry.message)
		var batches []payload.RecordBatch
		for _, batch := range entry.batches {
			if err := p.fillTablet(context.Topic, batch.database, tabletReader{tablet: &batch.tablet}); err != nil {
				return nil, err
			}
			batches = append(batches, record.NewRecordBatch(p.dialect, batch.database, batch.tablet, batch.timeSelected))
		}
		result = append(result, record.NewRecordMessage(context, batches))
	}
	for _, entry := range incoming {
		if entry.message.Kind() != payload.WatermarkMessage {
			entry.message.Release()
		}
	}
	return result, nil
}

type columnAlignCopiedMessage struct {
	message payload.SubscriptionMessage
	batches []columnAlignCopiedBatch
}

type columnAlignCopiedBatch struct {
	database     string
	timeSelected bool
	tablet       record.Tablet
}

func (p *ColumnAlignProcessor) fillTablet(topic, database string, tablet columnAlignTablet) error {
	topicCache := p.lastValues[topic]
	if topicCache == nil {
		topicCache = make(map[columnAlignDeviceKey][]columnAlignDeviceEntry)
		p.lastValues[topic] = topicCache
	}
	for row := 0; row < tablet.rowCount(); row++ {
		key := columnAlignDeviceKey{target: tablet.target()}
		if p.dialect == subscription.TableDialect {
			key.database = database
			key.tagHash = columnAlignTagHash(tablet, row)
		}
		bucket := topicCache[key]
		cache, entry := columnAlignFindDevice(tablet, row, bucket, p.dialect == subscription.TableDialect)
		if cache == nil {
			cache = make(map[columnAlignColumn]columnAlignValue)
			entry.values = cache
			topicCache[key] = append(bucket, entry)
		}
		timestamp := tablet.timestampAt(row)
		for column, schema := range tablet.columns() {
			cellKey := columnAlignColumn{name: schema.Name, dataType: schema.DataType}
			cached, ok := cache[cellKey]
			value, valid := tablet.valueAt(row, column)
			if !valid {
				return fmt.Errorf("subscription: column alignment has no value for column %d", column)
			}
			if value == nil {
				if ok && cached.timestamp < timestamp && !tablet.setValueAt(row, column, cloneColumnAlignValue(cached.value)) {
					return fmt.Errorf("subscription: column alignment cannot fill column %d", column)
				}
			} else if !ok || timestamp >= cached.timestamp {
				cache[cellKey] = columnAlignValue{timestamp: timestamp, value: cloneColumnAlignValue(value)}
			}
		}
	}
	return nil
}

func columnAlignFindDevice(tablet columnAlignTablet, row int, bucket []columnAlignDeviceEntry, table bool) (map[columnAlignColumn]columnAlignValue, columnAlignDeviceEntry) {
	if !table {
		if len(bucket) == 0 {
			return nil, columnAlignDeviceEntry{}
		}
		return bucket[0].values, bucket[0]
	}
	for _, entry := range bucket {
		if columnAlignTagsEqual(tablet, row, entry.tags) {
			return entry.values, entry
		}
	}
	tags := columnAlignTags(tablet, row)
	return nil, columnAlignDeviceEntry{tags: tags}
}

const (
	columnAlignHashOffset uint64 = 14695981039346656037
	columnAlignHashPrime  uint64 = 1099511628211
)

func columnAlignTagHash(tablet columnAlignTablet, row int) uint64 {
	lastNonNull := -1
	for index, schema := range tablet.columns() {
		if schema.Category != client.TAG {
			continue
		}
		value, _ := tablet.valueAt(row, index)
		if value != nil {
			lastNonNull = index
		}
	}
	hash := columnAlignHashOffset
	for column, schema := range tablet.columns() {
		if column > lastNonNull {
			break
		}
		if schema.Category != client.TAG {
			continue
		}
		value, _ := tablet.valueAt(row, column)
		hash = columnAlignHashValue(hash, value)
	}
	return hash
}

func columnAlignTags(tablet columnAlignTablet, row int) []any {
	tags := make([]any, 0)
	lastNonNull := 0
	for column, schema := range tablet.columns() {
		if schema.Category != client.TAG {
			continue
		}
		value, _ := tablet.valueAt(row, column)
		tags = append(tags, cloneColumnAlignValue(value))
		if value != nil {
			lastNonNull = len(tags)
		}
	}
	return tags[:lastNonNull]
}

func columnAlignTagsEqual(tablet columnAlignTablet, row int, tags []any) bool {
	index := 0
	lastNonNull := 0
	for column, schema := range tablet.columns() {
		if schema.Category != client.TAG {
			continue
		}
		value, _ := tablet.valueAt(row, column)
		if index >= len(tags) {
			if value != nil {
				return false
			}
		} else if !columnAlignEqualValue(tags[index], value) {
			return false
		}
		index++
		if value != nil {
			lastNonNull = index
		}
	}
	return lastNonNull == len(tags) && index >= len(tags)
}

func columnAlignHashValue(hash uint64, value any) uint64 {
	mix := func(value uint64) uint64 {
		for index := 0; index < 8; index++ {
			hash ^= (value >> uint(index*8)) & 0xff
			hash *= columnAlignHashPrime
		}
		return hash
	}
	if value == nil {
		return mix(0)
	}
	switch value := value.(type) {
	case bool:
		if value {
			return mix(2)
		}
		return mix(1)
	case int32:
		return mix(3) ^ mix(uint64(uint32(value)))
	case int64:
		return mix(4) ^ mix(uint64(value))
	case float32:
		return mix(5) ^ mix(uint64(math.Float32bits(value)))
	case float64:
		return mix(6) ^ mix(math.Float64bits(value))
	case string:
		hash = mix(7)
		for index := 0; index < len(value); index++ {
			hash ^= uint64(value[index])
			hash *= columnAlignHashPrime
		}
		return hash
	case []byte:
		hash = mix(8)
		for _, item := range value {
			hash ^= uint64(item)
			hash *= columnAlignHashPrime
		}
		return hash
	case time.Time:
		return mix(9) ^ mix(uint64(value.Year())) ^ mix(uint64(value.Month())) ^ mix(uint64(value.Day()))
	default:
		return mix(10)
	}
}

func columnAlignEqualValue(left, right any) bool {
	switch left := left.(type) {
	case nil:
		return right == nil
	case bool:
		value, ok := right.(bool)
		return ok && left == value
	case int32:
		value, ok := right.(int32)
		return ok && left == value
	case int64:
		value, ok := right.(int64)
		return ok && left == value
	case float32:
		value, ok := right.(float32)
		return ok && math.Float32bits(left) == math.Float32bits(value)
	case float64:
		value, ok := right.(float64)
		return ok && math.Float64bits(left) == math.Float64bits(value)
	case string:
		value, ok := right.(string)
		return ok && left == value
	case []byte:
		value, ok := right.([]byte)
		return ok && bytes.Equal(left, value)
	case time.Time:
		value, ok := right.(time.Time)
		return ok && left.Equal(value)
	default:
		return false
	}
}

func cloneColumnAlignValue(value any) any {
	if binary, ok := value.([]byte); ok {
		return append([]byte{}, binary...)
	}
	return value
}

func (p *ColumnAlignProcessor) Flush() ([]payload.SubscriptionMessage, error) {
	return nil, nil
}

func (p *ColumnAlignProcessor) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lastValues = make(map[string]map[columnAlignDeviceKey][]columnAlignDeviceEntry)
}

func (p *ColumnAlignProcessor) ResetTopic(topic string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.lastValues, topic)
}
