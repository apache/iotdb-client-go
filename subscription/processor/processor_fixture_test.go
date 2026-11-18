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
	"github.com/apache/iotdb-client-go/subscription/payload"

	"fmt"
	"iter"
	"sort"
	"testing"

	"github.com/apache/iotdb-client-go/subscription"
	"github.com/apache/iotdb-client-go/v2/client"
)

// processorInputMessage is a caller-owned implementation of the public message
// contract. Release reclaims its borrowed buffers to exercise ownership transfer.
type processorInputMessage struct {
	context payload.CommitContext
	batches []*processorInputBatch
	watermark *int64
	released bool
}

func processorRecord(topic, region string, commitID int64, batches ...*processorInputBatch) *processorInputMessage {
	return &processorInputMessage{
		context: payload.CommitContext{Topic: topic, GroupID: "group", DataNodeID: 1, RegionID: region, CommitID: commitID},
		batches: batches,
	}
}

func processorWatermark(topic, region string, timestamp int64) *processorInputMessage {
	message := processorRecord(topic, region, payload.InvalidCommitID)
	message.watermark = &timestamp
	return message
}

func (m *processorInputMessage) Kind() payload.SubscriptionMessageKind {
	if m.watermark != nil {
		return payload.WatermarkMessage
	}
	return payload.RecordMessage
}

func (m *processorInputMessage) CommitContext() payload.CommitContext {
	result := m.context
	if result.Writer != nil {
		writer := *result.Writer
		result.Writer = &writer
	}
	return result
}

func (m *processorInputMessage) Records() ([]payload.RecordBatch, error) {
	if m.watermark != nil {
		return nil, subscription.ErrIncompatibleMessage
	}
	if m.released {
		return nil, subscription.ErrDataReleased
	}
	result := make([]payload.RecordBatch, len(m.batches))
	for index, batch := range m.batches {
		result[index] = batch
	}
	return result, nil
}

func (m *processorInputMessage) Watermark() (int64, bool) {
	if m.watermark == nil {
		return 0, false
	}
	return *m.watermark, true
}

func (m *processorInputMessage) EstimatedSize() int64 { return 64 }

func (m *processorInputMessage) Release() {
	if m.released {
		return
	}
	m.released = true
	for _, batch := range m.batches {
		for _, row := range batch.values {
			for _, value := range row {
				if binary, ok := value.([]byte); ok {
					clear(binary)
				}
			}
		}
		batch.released = true
		batch.values = nil
		batch.timestamps = nil
	}
	m.batches = nil
}

// processorInputBatch stores rows, independently of the processor's column
// storage. Optional read errors represent failures in a caller-provided reader.
type processorInputBatch struct {
	dialect subscription.Dialect
	database string
	target string
	columnNames []string
	columnTypes []client.TSDataType
	columnCategories []client.ColumnCategory
	timestamps []int64
	values [][]any
	timestampErr error
	valueErr error
	released bool
}

func processorTreeBatch(timestamps []int64, values ...any) *processorInputBatch {
	rows := make([][]any, len(timestamps))
	for row := range rows {
		var value any
		if row < len(values) {
			value = values[row]
		}
		rows[row] = []any{value}
	}
	return &processorInputBatch{
		dialect: subscription.TreeDialect,
		target: "root.db.device",
		columnNames: []string{"value"},
		columnTypes: []client.TSDataType{client.INT64},
		columnCategories: []client.ColumnCategory{client.FIELD},
		timestamps: timestamps,
		values: rows,
	}
}

func processorTableRecord(topic, database, table string, tags []any, timestamp int64, value any) *processorInputMessage {
	columnNames := make([]string, 0, len(tags)+1)
	columnTypes := make([]client.TSDataType, 0, len(tags)+1)
	columnCategories := make([]client.ColumnCategory, 0, len(tags)+1)
	for index := range tags {
		columnNames = append(columnNames, fmt.Sprintf("tag%d", index))
		columnTypes = append(columnTypes, client.STRING)
		columnCategories = append(columnCategories, client.TAG)
	}
	columnNames = append(columnNames, "value")
	columnTypes = append(columnTypes, client.INT64)
	columnCategories = append(columnCategories, client.FIELD)
	row := append([]any(nil), tags...)
	row = append(row, value)
	return processorRecord(topic, "region", timestamp, &processorInputBatch{
		dialect: subscription.TableDialect,
		database: database,
		target: table,
		columnNames: columnNames,
		columnTypes: columnTypes,
		columnCategories: columnCategories,
		timestamps: []int64{timestamp},
		values: [][]any{row},
	})
}

func (b *processorInputBatch) Dialect() subscription.Dialect { return b.dialect }
func (b *processorInputBatch) Database() string { return b.database }
func (b *processorInputBatch) Target() string { return b.target }
func (b *processorInputBatch) ColumnCount() int { return len(b.columnNames) }
func (b *processorInputBatch) ColumnName(column int) (string, error) {
	if b.released {
		return "", subscription.ErrDataReleased
	}
	if column < 0 || column >= len(b.columnNames) {
		return "", fmt.Errorf("input column %d out of range", column)
	}
	return b.columnNames[column], nil
}
func (b *processorInputBatch) ColumnDataType(column int) (client.TSDataType, error) {
	if b.released {
		return client.UNKNOWN, subscription.ErrDataReleased
	}
	if column < 0 || column >= len(b.columnTypes) {
		return client.UNKNOWN, fmt.Errorf("input column %d out of range", column)
	}
	return b.columnTypes[column], nil
}
func (b *processorInputBatch) ColumnCategory(column int) (client.ColumnCategory, error) {
	if b.released {
		return client.FIELD, subscription.ErrDataReleased
	}
	if column < 0 || column >= len(b.columnCategories) {
		return client.FIELD, fmt.Errorf("input column %d out of range", column)
	}
	return b.columnCategories[column], nil
}
func (b *processorInputBatch) TimeSelected() bool { return true }
func (b *processorInputBatch) Len() int { return len(b.timestamps) }

func (b *processorInputBatch) Timestamp(row int) (int64, error) {
	if b.released {
		return 0, subscription.ErrDataReleased
	}
	if b.timestampErr != nil {
		return 0, b.timestampErr
	}
	if row < 0 || row >= len(b.timestamps) {
		return 0, fmt.Errorf("input row %d out of range", row)
	}
	return b.timestamps[row], nil
}

func (b *processorInputBatch) Value(row, column int) (any, error) {
	if b.released {
		return nil, subscription.ErrDataReleased
	}
	if b.valueErr != nil {
		return nil, b.valueErr
	}
	if row < 0 || row >= len(b.values) || column < 0 || column >= len(b.columnNames) {
		return nil, fmt.Errorf("input cell (%d,%d) out of range", row, column)
	}
	return b.values[row][column], nil
}

func (b *processorInputBatch) Rows() iter.Seq2[payload.Row, error] {
	return func(yield func(payload.Row, error) bool) {
		if b.released {
			yield(payload.Row{}, subscription.ErrDataReleased)
			return
		}
		order := make([]int, b.Len())
		for index := range order {
			order[index] = index
		}
		sort.SliceStable(order, func(i, j int) bool { return b.timestamps[order[i]] < b.timestamps[order[j]] })
		for _, index := range order {
			timestamp, err := b.Timestamp(index)
			if err != nil {
				yield(payload.Row{}, err)
				return
			}
			values := make([]any, len(b.columnNames))
			for column := range values {
				values[column], err = b.Value(index, column)
				if err != nil {
					yield(payload.Row{}, err)
					return
				}
			}
			if !yield(payload.Row{Timestamp: &timestamp, Values: values}, nil) {
				return
			}
		}
	}
}

func releaseProcessorMessages(messages []payload.SubscriptionMessage) {
	for _, message := range messages {
		message.Release()
	}
}

func requireProcessorCommitIDs(t *testing.T, messages []payload.SubscriptionMessage, expected ...int64) {
	t.Helper()
	if len(messages) != len(expected) {
		t.Fatalf("delivered %d messages, want %d", len(messages), len(expected))
	}
	for index, message := range messages {
		if id := message.CommitContext().CommitID; id != expected[index] {
			t.Fatalf("message %d commit ID = %d, want %d", index, id, expected[index])
		}
		if _, err := message.Records(); err != nil {
			t.Fatalf("delivered message %d is unreadable: %v", index, err)
		}
	}
}

func processorOutputValue(t *testing.T, messages []payload.SubscriptionMessage, row, column int) any {
	t.Helper()
	if len(messages) != 1 {
		t.Fatalf("delivered %d messages, want one", len(messages))
	}
	batches, err := messages[0].Records()
	if err != nil || len(batches) != 1 {
		t.Fatalf("output batches = %d, error = %v", len(batches), err)
	}
	value, err := batches[0].Value(row, column)
	if err != nil {
		t.Fatal(err)
	}
	return value
}
