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

package payload

import (
	"iter"

	"github.com/apache/iotdb-client-go/v2/client"
	"github.com/apache/iotdb-client-go/v2/subscription"
)

// SubscriptionMessageKind identifies a record or watermark message independently
// of the protocol's numeric response types.
type SubscriptionMessageKind string

const (
	// RecordMessage carries decoded records for application consumption.
	RecordMessage SubscriptionMessageKind = "records"
	// WatermarkMessage carries a server-side timestamp-progress signal for
	// client-side processing rather than application record delivery.
	WatermarkMessage SubscriptionMessageKind = "watermark"
)

// SubscriptionMessage exposes records or an internal watermark signal. Poll results and
// push callbacks contain records only; processors can also receive watermarks.
// TsFile messages are never constructed: decoding them returns
// subscription.ErrUnsupportedTsFile instead.
type SubscriptionMessage interface {
	// Kind returns the message kind used to select the applicable payload view.
	Kind() SubscriptionMessageKind
	// CommitContext returns a caller-owned snapshot of the routing and progress
	// information needed to acknowledge this message. It remains available
	// after Release so record buffers can be freed before committing.
	CommitContext() CommitContext
	// Records returns the record-handler views of the message's decoded
	// Tablets. A message can contain batches from multiple databases and tables;
	// each batch preserves its own target, schema, and time-column projection.
	// The returned data is borrowed and read-only until Release.
	//
	// Records returns subscription.ErrIncompatibleMessage for a watermark and
	// subscription.ErrDataReleased after the record buffers have been released.
	Records() ([]RecordBatch, error)
	// Watermark returns the timestamp carried by a watermark message. The
	// boolean is false for other kinds, replacing Java's exception when a
	// watermark timestamp is requested from a record message.
	Watermark() (timestamp int64, ok bool)
	// EstimatedSize estimates this message's heap memory usage in bytes,
	// including its decoded record buffers. The estimate is useful for
	// processor buffering limits and is not a serialized payload size.
	EstimatedSize() int64
	// Release drops record buffers and is idempotent. Kind, CommitContext, and
	// Watermark remain usable, so a released message can still be committed.
	Release()
}

// RecordBatch is the record-handler view of one decoded Tablet. Returned data is
// borrowed, read-only, and valid until its message is released. Copy row values
// and byte slices before retaining them beyond that lifetime. All indexes are
// zero-based. Rows uses the record handler's user-visible ordering; raw indexed
// access addresses the decoded Tablet's row order.
type RecordBatch interface {
	// Dialect identifies whether this batch contains tree-model or table-model
	// data. Sharing this interface does not merge the models' target semantics.
	Dialect() subscription.Dialect
	// Database returns the database associated with table-model data. It is
	// empty for tree-model data, which is addressed by its device path.
	Database() string
	// Target returns the tree-model device path or table-model table name
	// associated with this decoded Tablet.
	Target() string
	// ColumnCount returns the number of data columns. The time column is
	// represented separately, even when selected.
	ColumnCount() int
	// ColumnName returns the measurement name at column. An out-of-range index
	// returns an error.
	ColumnName(column int) (string, error)
	// ColumnDataType returns the data type at column. An out-of-range index
	// returns an error.
	ColumnDataType(column int) (client.TSDataType, error)
	// ColumnCategory returns the table category at column. An out-of-range index
	// returns an error.
	ColumnCategory(column int) (client.ColumnCategory, error)
	// TimeSelected reports whether timestamps are exposed in the batch's
	// user-visible rows. This is resolved per batch, including any table-specific
	// projection, rather than assumed to be uniform across the whole message.
	TimeSelected() bool
	// Len returns the number of rows in the decoded Tablet, not its backing
	// buffer capacity.
	Len() int
	// Timestamp exposes the raw timestamp for processing even when the time
	// column is excluded from user-visible rows. Its unit is server precision.
	// The row index refers to the original Tablet order; an out-of-range index
	// returns an error.
	Timestamp(row int) (int64, error)
	// Value returns the value at the zero-based raw row and data-column indexes.
	// A nil value denotes SQL NULL; non-null representations are described by
	// Row. An out-of-range row or column returns an error.
	Value(row, column int) (any, error)
	// Rows iterates the user-visible records in ascending timestamp order,
	// retaining original row order for equal timestamps. It does not reorder
	// the underlying Tablet used by Timestamp and Value. Iteration failures are
	// reported through the second yielded value.
	Rows() iter.Seq2[Row, error]
}

// Row contains data columns in RecordBatch column order. A nil Timestamp means
// that the time column was not selected; a nil value is SQL NULL. Values use
// bool, int32, int64, float32, float64, string (TEXT/STRING), time.Time (DATE),
// or []byte (BLOB/OBJECT). TIMESTAMP values remain int64 in server precision.
// Rows are borrowed views with the same lifetime as their message.
type Row struct {
	// Timestamp is the selected timestamp in server precision, or nil when the
	// time column was not selected. It is not an entry in Values.
	Timestamp *int64
	// Values follows RecordBatch column order and preserves null entries as nil.
	// The slice and any byte values are borrowed from the owning message.
	Values []any
}
