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

package record

import (
	"fmt"
	"iter"
	"sort"
	"sync"

	"github.com/apache/iotdb-client-go/subscription"
	"github.com/apache/iotdb-client-go/subscription/payload"
	"github.com/apache/iotdb-client-go/v2/client"
)

// subscriptionMessage keeps immutable delivery metadata separate from releasable
// record buffers. Its factories take ownership of decoded batches.
type subscriptionMessage struct {
	mu        sync.RWMutex
	kind      payload.SubscriptionMessageKind
	context   payload.CommitContext
	watermark int64
	batches   []payload.RecordBatch
	released  bool
}

// NewRecordMessage takes ownership of batches and snapshots commit metadata.
func NewRecordMessage(commitContext payload.CommitContext, batches []payload.RecordBatch) payload.SubscriptionMessage {
	return &subscriptionMessage{
		kind:    payload.RecordMessage,
		context: CloneCommitContext(commitContext),
		batches: append([]payload.RecordBatch(nil), batches...),
	}
}

// NewWatermarkMessage creates a metadata-only timestamp progress signal.
func NewWatermarkMessage(commitContext payload.CommitContext, timestamp int64) payload.SubscriptionMessage {
	return &subscriptionMessage{
		kind:      payload.WatermarkMessage,
		context:   CloneCommitContext(commitContext),
		watermark: timestamp,
	}
}

// CloneCommitContext copies the optional writer identity and progress.
func CloneCommitContext(commitContext payload.CommitContext) payload.CommitContext {
	if commitContext.Writer != nil {
		writer := *commitContext.Writer
		commitContext.Writer = &writer
	}
	return commitContext
}

func (m *subscriptionMessage) Kind() payload.SubscriptionMessageKind {
	return m.kind
}

func (m *subscriptionMessage) CommitContext() payload.CommitContext {
	return CloneCommitContext(m.context)
}

// ReadCommitContext avoids copying immutable metadata of an internally owned
// message. External message implementations keep their public snapshot contract.
func ReadCommitContext(message payload.SubscriptionMessage) payload.CommitContext {
	if internal, ok := message.(*subscriptionMessage); ok {
		return internal.context
	}
	return message.CommitContext()
}

func (m *subscriptionMessage) Records() ([]payload.RecordBatch, error) {
	if m.kind != payload.RecordMessage {
		return nil, subscription.ErrIncompatibleMessage
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.released {
		return nil, subscription.ErrDataReleased
	}
	return append([]payload.RecordBatch(nil), m.batches...), nil
}

func (m *subscriptionMessage) Watermark() (int64, bool) {
	return m.watermark, m.kind == payload.WatermarkMessage
}

func (m *subscriptionMessage) EstimatedSize() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	// Include a rough object/metadata allowance and retained variable data.
	// This estimate is intended for buffering limits, not heap accounting.
	size := int64(128) + int64(len(m.kind)) + int64(len(m.context.Topic)) +
		int64(len(m.context.GroupID)) + int64(len(m.context.RegionID))
	if m.context.Writer != nil {
		size += 32 + int64(len(m.context.Writer.ID.RegionID))
	}
	size += int64(len(m.batches)) * 16
	for _, batch := range m.batches {
		if records, ok := batch.(*subscriptionRecordBatch); ok {
			size += records.estimatedSize()
		}
	}
	return size
}

func (m *subscriptionMessage) Release() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.released {
		return
	}
	m.released = true
	for _, batch := range m.batches {
		if records, ok := batch.(*subscriptionRecordBatch); ok {
			records.release()
		}
	}
	m.batches = nil
}

// subscriptionRecordBatch preserves the decoded Tablet's indexed order and uses
// a separate stable permutation for user-visible iteration. Immutable target
// metadata remains available after release; column accessors and Len return zero.
type subscriptionRecordBatch struct {
	mu           sync.RWMutex
	dialect      subscription.Dialect
	database     string
	target       string
	timeSelected bool
	rowCount     int
	tablet       *client.Tablet
	rowOrder     []int
	released     bool
}

// NewRecordBatch takes ownership of decoded arrays and preserves indexed order.
func NewRecordBatch(dialect subscription.Dialect, database string, tablet *client.Tablet, timeSelected bool) payload.RecordBatch {
	if dialect == subscription.TreeDialect {
		database = ""
		timeSelected = true
	}
	rowCount := tablet.Len()
	batch := &subscriptionRecordBatch{
		dialect:      dialect,
		database:     database,
		target:       tablet.GetTargetName(),
		timeSelected: timeSelected,
		rowCount:     rowCount,
		tablet:       tablet,
	}
	// Sorted timestamps already have the required stable order, so only retain a
	// permutation when the decoded Tablet is actually out of order.
	ordered := true
	for index := 1; index < rowCount; index++ {
		if tablet.GetTimestampAt(index) < tablet.GetTimestampAt(index-1) {
			ordered = false
			break
		}
	}
	if !ordered {
		batch.rowOrder = make([]int, rowCount)
		for index := range batch.rowOrder {
			batch.rowOrder[index] = index
		}
		sort.SliceStable(batch.rowOrder, func(left, right int) bool {
			return batch.tablet.GetTimestampAt(batch.rowOrder[left]) < batch.tablet.GetTimestampAt(batch.rowOrder[right])
		})
	}
	return batch
}

func (b *subscriptionRecordBatch) Dialect() subscription.Dialect {
	return b.dialect
}

func (b *subscriptionRecordBatch) Database() string {
	return b.database
}

func (b *subscriptionRecordBatch) Target() string {
	return b.target
}

func (b *subscriptionRecordBatch) ColumnCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.released || b.tablet == nil {
		return 0
	}
	return b.tablet.ColumnCount()
}

func (b *subscriptionRecordBatch) ColumnName(column int) (string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.released {
		return "", subscription.ErrDataReleased
	}
	if column < 0 || column >= b.tablet.ColumnCount() {
		return "", fmt.Errorf("subscription: column index %d out of range", column)
	}
	return b.tablet.ColumnName(column), nil
}

func (b *subscriptionRecordBatch) ColumnDataType(column int) (client.TSDataType, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.released {
		return client.UNKNOWN, subscription.ErrDataReleased
	}
	if column < 0 || column >= b.tablet.ColumnCount() {
		return client.UNKNOWN, fmt.Errorf("subscription: column index %d out of range", column)
	}
	return b.tablet.ColumnDataType(column), nil
}

func (b *subscriptionRecordBatch) ColumnCategory(column int) (client.ColumnCategory, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.released {
		return client.FIELD, subscription.ErrDataReleased
	}
	if column < 0 || column >= b.tablet.ColumnCount() {
		return client.FIELD, fmt.Errorf("subscription: column index %d out of range", column)
	}
	return b.tablet.ColumnCategory(column), nil
}

func (b *subscriptionRecordBatch) TimeSelected() bool {
	return b.timeSelected
}

func (b *subscriptionRecordBatch) Len() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.rowCount
}

func (b *subscriptionRecordBatch) Timestamp(row int) (int64, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.released {
		return 0, subscription.ErrDataReleased
	}
	if row < 0 || row >= b.rowCount {
		return 0, fmt.Errorf("subscription: row index %d out of range", row)
	}
	return b.tablet.GetTimestampAt(row), nil
}

func (b *subscriptionRecordBatch) Value(row, column int) (any, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.released {
		return nil, subscription.ErrDataReleased
	}
	if row < 0 || row >= b.rowCount {
		return nil, fmt.Errorf("subscription: row index %d out of range", row)
	}
	if column < 0 || column >= b.tablet.ColumnCount() {
		return nil, fmt.Errorf("subscription: column index %d out of range", column)
	}
	return b.valueLocked(row, column)
}

func (b *subscriptionRecordBatch) valueLocked(row, column int) (any, error) {
	return b.tablet.GetValueAt(column, row)
}

func (b *subscriptionRecordBatch) Rows() iter.Seq2[payload.Row, error] {
	return func(yield func(payload.Row, error) bool) {
		for position := 0; ; position++ {
			b.mu.RLock()
			if b.released {
				b.mu.RUnlock()
				yield(payload.Row{}, subscription.ErrDataReleased)
				return
			}
			if position >= b.rowCount {
				b.mu.RUnlock()
				return
			}
			index := position
			if b.rowOrder != nil {
				index = b.rowOrder[position]
			}
			row := payload.Row{Values: make([]any, b.tablet.ColumnCount())}
			if b.timeSelected {
				timestamp := b.tablet.GetTimestampAt(index)
				row.Timestamp = &timestamp
			}
			var err error
			for column := range row.Values {
				row.Values[column], err = b.valueLocked(index, column)
				if err != nil {
					break
				}
			}
			b.mu.RUnlock()
			// The caller may release the message or stop iteration from yield.
			// Never hold a batch lock while invoking caller-controlled code.
			if err != nil {
				yield(payload.Row{}, err)
				return
			}
			if !yield(row, nil) {
				return
			}
		}
	}
}

func (b *subscriptionRecordBatch) release() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.released {
		return
	}
	b.released = true
	b.rowCount = 0
	b.tablet = nil
	b.rowOrder = nil
}

func (b *subscriptionRecordBatch) estimatedSize() int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.estimatedSizeLocked()
}

func (b *subscriptionRecordBatch) estimatedSizeLocked() int64 {
	size := int64(128) + int64(len(b.dialect)) + int64(len(b.database)) +
		int64(len(b.target)) + int64(len(b.rowOrder))*8
	if b.tablet != nil {
		size += b.tablet.EstimatedSize()
	}
	return size
}

// Committable reports whether a delivery has writer progress or a legacy ID.
func Committable(context payload.CommitContext) bool {
	return context.Writer != nil || context.CommitID != payload.InvalidCommitID
}

// IsNilMessage recognizes a nil public interface or a typed nil native message.
// It preserves the commit boundary check without exposing concrete message types.
func IsNilMessage(message payload.SubscriptionMessage) bool {
	if message == nil {
		return true
	}
	value, ok := message.(*subscriptionMessage)
	return ok && value == nil
}
