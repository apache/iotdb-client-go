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

	"github.com/apache/iotdb-client-go/subscription"
	"github.com/apache/iotdb-client-go/subscription/payload"
)

// TabletMutator provides one exclusive lock across an in-place Tablet mutation.
// It is intentionally internal to the subscription module and never changes the
// public RecordBatch contract.
type TabletMutator struct {
	batch *subscriptionRecordBatch
}

func (m TabletMutator) Dialect() subscription.Dialect { return m.batch.dialect }
func (m TabletMutator) Database() string { return m.batch.database }
func (m TabletMutator) Target() string { return m.batch.target }
func (m TabletMutator) TimeSelected() bool { return m.batch.timeSelected }
func (m TabletMutator) RowCount() int { return m.batch.rowCount }
func (m TabletMutator) Columns() []payload.Column { return m.batch.columns }

func (m TabletMutator) Timestamp(row int) int64 {
	if m.batch.timestamps == nil {
		return 0
	}
	return m.batch.timestamps[row]
}

func (m TabletMutator) Value(row, column int) (any, bool) {
	if column < 0 || column >= len(m.batch.columns) {
		return nil, false
	}
	if column >= len(m.batch.values) {
		return nil, true
	}
	return m.batch.values[column].Value(row)
}

func (m TabletMutator) SetValue(row, column int, value any) bool {
	if column < 0 || column >= len(m.batch.columns) {
		return false
	}
	if column >= len(m.batch.values) {
		values := make([]TabletColumn, len(m.batch.columns))
		copy(values, m.batch.values)
		m.batch.values = values
	}
	if !m.batch.values[column].present() {
		m.batch.values[column] = NewTabletColumn(m.batch.columns[column].DataType, m.batch.rowCount)
	}
	return m.batch.values[column].Set(row, value)
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
		batch *subscriptionRecordBatch
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
		if batch.timestamps != nil && len(batch.timestamps) != batch.rowCount {
			batch.mu.RUnlock()
			return nil, true, errors.New("subscription: Tablet timestamp count does not match row count")
		}
		for column := 0; column < len(batch.columns) && column < len(batch.values); column++ {
			values := &batch.values[column]
			if values.present() && values.DataType != batch.columns[column].DataType {
				batch.mu.RUnlock()
				return nil, true, errors.New("subscription: Tablet column type does not match schema")
			}
			if values.present() && values.Rows() != batch.rowCount {
				batch.mu.RUnlock()
				return nil, true, errors.New("subscription: Tablet column row count does not match Tablet")
			}
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
			kind: payload.RecordMessage,
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

func (r TabletReader) Database() string { return r.batch.database }
func (r TabletReader) Target() string { return r.batch.target }
func (r TabletReader) RowCount() int { return r.batch.rowCount }
func (r TabletReader) Columns() []payload.Column { return r.batch.columns }
func (r TabletReader) Timestamp(row int) int64 {
	if r.batch.timestamps == nil {
		return 0
	}
	return r.batch.timestamps[row]
}
func (r TabletReader) Value(row, column int) (any, bool) {
	if column < 0 || column >= len(r.batch.columns) {
		return nil, false
	}
	if column >= len(r.batch.values) {
		return nil, true
	}
	return r.batch.values[column].Value(row)
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
