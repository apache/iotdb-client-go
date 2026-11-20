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
	"errors"

	"github.com/apache/iotdb-client-go/v2/subscription"
	"github.com/apache/iotdb-client-go/v2/subscription/payload"
	"github.com/apache/iotdb-client-go/v2/client"
)

// TabletMutator provides one exclusive lock across an in-place Tablet mutation.
// It is intentionally internal to the subscription packages and never changes
// the public RecordBatch contract.
type TabletMutator struct {
	batch *subscriptionRecordBatch
}

func (m TabletMutator) Dialect() subscription.Dialect { return m.batch.dialect }
func (m TabletMutator) Database() string              { return m.batch.database }
func (m TabletMutator) Target() string                { return m.batch.target }
func (m TabletMutator) TimeSelected() bool            { return m.batch.timeSelected }
func (m TabletMutator) RowCount() int                 { return m.batch.rowCount }
func (m TabletMutator) ColumnCount() int              { return m.batch.tablet.ColumnCount() }
func (m TabletMutator) ColumnName(column int) string  { return m.batch.tablet.ColumnName(column) }
func (m TabletMutator) ColumnDataType(column int) client.TSDataType {
	return m.batch.tablet.ColumnDataType(column)
}
func (m TabletMutator) ColumnCategory(column int) client.ColumnCategory {
	return m.batch.tablet.ColumnCategory(column)
}

// HasColumnValues reports whether the column has decoded backing storage. It is
// an internal read-only capability used to avoid writing into omitted columns.
func (m TabletMutator) HasColumnValues(column int) bool {
	return column >= 0 && column < m.batch.tablet.ColumnCount() && m.batch.tablet.HasColumnValues(column)
}

func (m TabletMutator) Timestamp(row int) int64 {
	return m.batch.tablet.GetTimestampAt(row)
}

func (m TabletMutator) Value(row, column int) (any, bool) {
	if column < 0 || column >= m.batch.tablet.ColumnCount() {
		return nil, false
	}
	value, err := m.batch.tablet.GetValueAt(column, row)
	return value, err == nil
}

// RawValue returns the decoded backing value without converting TEXT or STRING
// storage to string. Returned byte slices are borrowed from the Tablet.
func (m TabletMutator) RawValue(row, column int) (any, bool) {
	if column < 0 || column >= m.batch.tablet.ColumnCount() {
		return nil, false
	}
	value, err := m.batch.tablet.GetRawValueAt(column, row)
	return value, err == nil
}

func (m TabletMutator) SetValue(row, column int, value any) bool {
	if column < 0 || column >= m.batch.tablet.ColumnCount() {
		return false
	}
	return m.batch.tablet.SetValueAt(value, column, row) == nil
}

// TryMutateRecordMessages mutates decoded record buffers in place and transfers
// their ownership to replacement messages. The bool result is false without
// mutation when an input is not an internally owned record message; callers can
// then use a copying fallback for user-provided implementations.
func TryMutateRecordMessages(messages []payload.SubscriptionMessage, mutate func(payload.CommitContext, TabletMutator) error) ([]payload.SubscriptionMessage, bool, error) {
	seen := make(map[*subscriptionMessage]struct{}, len(messages))
	seenBatches := make(map[*subscriptionRecordBatch]struct{})
	owned := make([]*subscriptionMessage, len(messages))
	type ownedBatch struct {
		context payload.CommitContext
		batch   *subscriptionRecordBatch
	}
	var batches []ownedBatch
	for index, message := range messages {
		if message == nil {
			return nil, true, errors.New("subscription: mutation received a nil message")
		}
		if message.Kind() == payload.WatermarkMessage {
			continue
		}
		internal, ok := message.(*subscriptionMessage)
		if !ok {
			return nil, false, nil
		}
		if _, duplicate := seen[internal]; duplicate {
			return nil, false, nil
		}
		seen[internal] = struct{}{}
		internal.mu.RLock()
		if internal.released {
			internal.mu.RUnlock()
			return nil, true, subscription.ErrDataReleased
		}
		internalBatches := append([]payload.RecordBatch(nil), internal.batches...)
		internal.mu.RUnlock()
		owned[index] = internal
		for _, batch := range internalBatches {
			internalBatch, ok := batch.(*subscriptionRecordBatch)
			if !ok {
				return nil, false, nil
			}
			if _, duplicate := seenBatches[internalBatch]; duplicate {
				return nil, false, nil
			}
			seenBatches[internalBatch] = struct{}{}
			batches = append(batches, ownedBatch{context: internal.context, batch: internalBatch})
		}
	}
	for _, owned := range batches {
		batch := owned.batch
		batch.mu.RLock()
		if batch.released {
			batch.mu.RUnlock()
			return nil, true, subscription.ErrDataReleased
		}
		batch.mu.RUnlock()
	}
	for _, owned := range batches {
		batch := owned.batch
		batch.mu.Lock()
		if batch.released {
			batch.mu.Unlock()
			return nil, true, subscription.ErrDataReleased
		}
		err := mutate(owned.context, TabletMutator{batch: batch})
		batch.mu.Unlock()
		if err != nil {
			return nil, true, err
		}
	}
	result := make([]payload.SubscriptionMessage, len(messages))
	for index, message := range messages {
		if message.Kind() == payload.WatermarkMessage {
			result[index] = message
			continue
		}
		internal := owned[index]
		result[index] = &subscriptionMessage{
			kind:    payload.RecordMessage,
			context: CloneCommitContext(internal.context),
			batches: append([]payload.RecordBatch(nil), internal.batches...),
		}
		internal.mu.Lock()
		internal.released = true
		internal.batches = nil
		internal.mu.Unlock()
	}
	return result, true, nil
}

// TabletReader exposes decoded timestamps and typed columns while the batch is
// locked. Callers must not retain the reader after the callback returns.
type TabletReader struct {
	batch *subscriptionRecordBatch
}

func (r TabletReader) Database() string             { return r.batch.database }
func (r TabletReader) Target() string               { return r.batch.target }
func (r TabletReader) RowCount() int                { return r.batch.rowCount }
func (r TabletReader) ColumnCount() int             { return r.batch.tablet.ColumnCount() }
func (r TabletReader) ColumnName(column int) string { return r.batch.tablet.ColumnName(column) }
func (r TabletReader) ColumnDataType(column int) client.TSDataType {
	return r.batch.tablet.ColumnDataType(column)
}
func (r TabletReader) ColumnCategory(column int) client.ColumnCategory {
	return r.batch.tablet.ColumnCategory(column)
}
func (r TabletReader) Timestamp(row int) int64 {
	return r.batch.tablet.GetTimestampAt(row)
}
func (r TabletReader) Value(row, column int) (any, bool) {
	if column < 0 || column >= r.batch.tablet.ColumnCount() {
		return nil, false
	}
	value, err := r.batch.tablet.GetValueAt(column, row)
	return value, err == nil
}
func (r TabletReader) EstimatedSize() int64 { return r.batch.estimatedSizeLocked() }

// TryReadRecordMessage visits decoded batches with one read lock per batch. It
// returns false without invoking visit when message is not internally owned.
func TryReadRecordMessage(message payload.SubscriptionMessage, visit func(payload.CommitContext, TabletReader) error) (bool, error) {
	if message == nil {
		return true, errors.New("subscription: reader received a nil message")
	}
	if message.Kind() != payload.RecordMessage {
		return true, subscription.ErrIncompatibleMessage
	}
	internal, ok := message.(*subscriptionMessage)
	if !ok {
		return false, nil
	}
	internal.mu.RLock()
	released := internal.released
	context := internal.context
	batches := append([]payload.RecordBatch(nil), internal.batches...)
	internal.mu.RUnlock()
	if released {
		return true, subscription.ErrDataReleased
	}
	for _, batch := range batches {
		internalBatch, ok := batch.(*subscriptionRecordBatch)
		if !ok {
			return false, nil
		}
		internalBatch.mu.RLock()
		if internalBatch.released {
			internalBatch.mu.RUnlock()
			return true, subscription.ErrDataReleased
		}
		err := visit(context, TabletReader{batch: internalBatch})
		internalBatch.mu.RUnlock()
		if err != nil {
			return true, err
		}
	}
	return true, nil
}
