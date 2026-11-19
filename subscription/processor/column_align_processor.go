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

// columnAlignTablet separates backing-column availability from per-cell NULL so
// processors can skip columns that a decoded Tablet omitted entirely.
type columnAlignTablet interface {
	target() string
	rowCount() int
	columnCount() int
	columnName(int) string
	columnDataType(int) client.TSDataType
	columnCategory(int) client.ColumnCategory
	timestampAt(int) int64
	hasColumnValues(int) bool
	rawValueAt(int, int) (any, bool)
	setValueAt(int, int, any) bool
}

type tabletReader struct{ tablet *client.Tablet }

func (r tabletReader) target() string               { return r.tablet.GetTargetName() }
func (r tabletReader) rowCount() int                { return r.tablet.Len() }
func (r tabletReader) columnCount() int             { return r.tablet.ColumnCount() }
func (r tabletReader) columnName(column int) string { return r.tablet.ColumnName(column) }
func (r tabletReader) columnDataType(column int) client.TSDataType {
	return r.tablet.ColumnDataType(column)
}
func (r tabletReader) columnCategory(column int) client.ColumnCategory {
	return r.tablet.ColumnCategory(column)
}
func (r tabletReader) timestampAt(row int) int64 {
	return r.tablet.GetTimestampAt(row)
}
func (r tabletReader) hasColumnValues(column int) bool {
	return r.tablet.HasColumnValues(column)
}
func (r tabletReader) rawValueAt(row, column int) (any, bool) {
	value, err := r.tablet.GetRawValueAt(column, row)
	return value, err == nil
}
func (r tabletReader) setValueAt(row, column int, value any) bool {
	return r.tablet.SetValueAt(value, column, row) == nil
}

type tabletMutator struct{ tablet record.TabletMutator }

func (m tabletMutator) target() string               { return m.tablet.Target() }
func (m tabletMutator) rowCount() int                { return m.tablet.RowCount() }
func (m tabletMutator) columnCount() int             { return m.tablet.ColumnCount() }
func (m tabletMutator) columnName(column int) string { return m.tablet.ColumnName(column) }
func (m tabletMutator) columnDataType(column int) client.TSDataType {
	return m.tablet.ColumnDataType(column)
}
func (m tabletMutator) columnCategory(column int) client.ColumnCategory {
	return m.tablet.ColumnCategory(column)
}
func (m tabletMutator) timestampAt(row int) int64 { return m.tablet.Timestamp(row) }
func (m tabletMutator) hasColumnValues(column int) bool {
	return m.tablet.HasColumnValues(column)
}
func (m tabletMutator) rawValueAt(row, column int) (any, bool) {
	return m.tablet.RawValue(row, column)
}
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
			rowCount := batch.Len()
			timestamps := make([]int64, rowCount)
			columnCount := batch.ColumnCount()
			values := make([]interface{}, columnCount)
			schemas := make([]*client.MeasurementSchema, columnCount)
			categories := make([]client.ColumnCategory, columnCount)
			for column := 0; column < columnCount; column++ {
				name, err := batch.ColumnName(column)
				if err != nil {
					return nil, err
				}
				dataType, err := batch.ColumnDataType(column)
				if err != nil {
					return nil, err
				}
				category, err := batch.ColumnCategory(column)
				if err != nil {
					return nil, err
				}
				schemas[column] = &client.MeasurementSchema{Measurement: name, DataType: dataType}
				categories[column] = category
				values[column] = newColumnAlignValues(dataType, rowCount)
			}
			data := client.NewDecodedTablet(batch.Target(), schemas, categories, rowCount, timestamps, values, nil)
			reader := tabletReader{tablet: data}
			for row := 0; row < rowCount; row++ {
				timestamps[row], err = batch.Timestamp(row)
				if err != nil {
					return nil, err
				}
				for column := 0; column < columnCount; column++ {
					value, err := batch.Value(row, column)
					if err != nil {
						return nil, err
					}
					if !reader.setValueAt(row, column, cloneColumnAlignValue(value)) {
						return nil, fmt.Errorf("subscription: column alignment cannot store value for column %d", column)
					}
				}
			}
			entry.batches = append(entry.batches, columnAlignCopiedBatch{
				database:     batch.Database(),
				timeSelected: batch.TimeSelected(),
				tablet:       data,
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
			if err := p.fillTablet(context.Topic, batch.database, tabletReader{tablet: batch.tablet}); err != nil {
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

func newColumnAlignValues(dataType client.TSDataType, rows int) interface{} {
	switch dataType {
	case client.BOOLEAN:
		return make([]bool, rows)
	case client.INT32:
		return make([]int32, rows)
	case client.INT64, client.TIMESTAMP:
		return make([]int64, rows)
	case client.FLOAT:
		return make([]float32, rows)
	case client.DOUBLE:
		return make([]float64, rows)
	case client.DATE:
		return make([]int32, rows)
	default:
		return make([][]byte, rows)
	}
}

type columnAlignCopiedMessage struct {
	message payload.SubscriptionMessage
	batches []columnAlignCopiedBatch
}

type columnAlignCopiedBatch struct {
	database     string
	timeSelected bool
	tablet       *client.Tablet
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
		for column := 0; column < tablet.columnCount(); column++ {
			if !tablet.hasColumnValues(column) {
				continue
			}
			cellKey := columnAlignColumn{name: tablet.columnName(column), dataType: tablet.columnDataType(column)}
			cached, ok := cache[cellKey]
			value, valid := tablet.rawValueAt(row, column)
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
	for column := 0; column < tablet.columnCount(); column++ {
		if tablet.columnCategory(column) != client.TAG {
			continue
		}
		value, _ := tablet.rawValueAt(row, column)
		if value != nil {
			lastNonNull = column
		}
	}
	hash := columnAlignHashOffset
	for column := 0; column < tablet.columnCount(); column++ {
		if column > lastNonNull {
			break
		}
		if tablet.columnCategory(column) != client.TAG {
			continue
		}
		value, _ := tablet.rawValueAt(row, column)
		hash = columnAlignHashValue(hash, value)
	}
	return hash
}

func columnAlignTags(tablet columnAlignTablet, row int) []any {
	tags := make([]any, 0)
	lastNonNull := 0
	for column := 0; column < tablet.columnCount(); column++ {
		if tablet.columnCategory(column) != client.TAG {
			continue
		}
		value, _ := tablet.rawValueAt(row, column)
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
	for column := 0; column < tablet.columnCount(); column++ {
		if tablet.columnCategory(column) != client.TAG {
			continue
		}
		value, _ := tablet.rawValueAt(row, column)
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
