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
	"time"

	"github.com/apache/iotdb-client-go/subscription/payload"
	"github.com/apache/iotdb-client-go/v2/client"
)

// Tablet follows TsFile's columnar stream representation. Values are stored in
// typed backing arrays and materialized as any only at the public accessor.
type Tablet struct {
	Target     string
	Columns    []payload.Column
	RowCount   int
	Timestamps []int64
	Values     []TabletColumn
}

// TabletColumn is one typed Tablet column. Exactly one value slice is populated
// according to DataType. A nil zero-value column represents an absent column.
type TabletColumn struct {
	DataType     client.TSDataType
	Booleans     []bool
	Int32s       []int32
	Int64s       []int64
	Float32s     []float32
	Float64s     []float64
	Dates        []int32
	Strings      []string
	Binaries     [][]byte
	Nulls        []uint64
	VariableBytes int64
}

// NewTabletColumn allocates the backing array required by dataType.
func NewTabletColumn(dataType client.TSDataType, rows int) TabletColumn {
	column := TabletColumn{DataType: dataType}
	switch dataType {
	case client.BOOLEAN:
		column.Booleans = make([]bool, rows)
	case client.INT32:
		column.Int32s = make([]int32, rows)
	case client.INT64, client.TIMESTAMP:
		column.Int64s = make([]int64, rows)
	case client.FLOAT:
		column.Float32s = make([]float32, rows)
	case client.DOUBLE:
		column.Float64s = make([]float64, rows)
	case client.DATE:
		column.Dates = make([]int32, rows)
	case client.TEXT, client.STRING:
		column.Strings = make([]string, rows)
	case client.BLOB, client.OBJECT:
		column.Binaries = make([][]byte, rows)
	}
	return column
}

// Set stores a non-null value without introducing a per-cell interface slot.
// It returns false when the value or storage type does not match the column.
func (c *TabletColumn) Set(row int, value any) bool {
	if !c.hasRow(row) {
		return false
	}
	if value == nil {
		c.SetNull(row)
		return true
	}
	switch c.DataType {
	case client.BOOLEAN:
		typed, ok := value.(bool)
		if !ok || row >= len(c.Booleans) {
			return false
		}
		c.Booleans[row] = typed
	case client.INT32:
		typed, ok := value.(int32)
		if !ok || row >= len(c.Int32s) {
			return false
		}
		c.Int32s[row] = typed
	case client.INT64, client.TIMESTAMP:
		typed, ok := value.(int64)
		if !ok || row >= len(c.Int64s) {
			return false
		}
		c.Int64s[row] = typed
	case client.FLOAT:
		typed, ok := value.(float32)
		if !ok || row >= len(c.Float32s) {
			return false
		}
		c.Float32s[row] = typed
	case client.DOUBLE:
		typed, ok := value.(float64)
		if !ok || row >= len(c.Float64s) {
			return false
		}
		c.Float64s[row] = typed
	case client.DATE:
		typed, ok := value.(time.Time)
		if !ok || row >= len(c.Dates) {
			return false
		}
		c.Dates[row] = int32(typed.Year()*10000 + int(typed.Month())*100 + typed.Day())
	case client.TEXT, client.STRING:
		typed, ok := value.(string)
		if !ok || row >= len(c.Strings) {
			return false
		}
		c.VariableBytes += int64(len(typed) - len(c.Strings[row]))
		c.Strings[row] = typed
	case client.BLOB, client.OBJECT:
		typed, ok := value.([]byte)
		if !ok || row >= len(c.Binaries) {
			return false
		}
		c.VariableBytes += int64(len(typed) - len(c.Binaries[row]))
		c.Binaries[row] = append([]byte{}, typed...)
	default:
		return false
	}
	c.setNull(row, false)
	return true
}

// Value materializes one typed value for the public RecordBatch accessor. The
// second result is false when the column has no storage for row.
func (c *TabletColumn) Value(row int) (any, bool) {
	if row < 0 {
		return nil, false
	}
	if !c.present() {
		return nil, true
	}
	switch c.DataType {
	case client.BOOLEAN:
		if row >= len(c.Booleans) {
			return nil, false
		}
		if c.IsNull(row) {
			return nil, true
		}
		return c.Booleans[row], true
	case client.INT32:
		if row >= len(c.Int32s) {
			return nil, false
		}
		if c.IsNull(row) {
			return nil, true
		}
		return c.Int32s[row], true
	case client.INT64, client.TIMESTAMP:
		if row >= len(c.Int64s) {
			return nil, false
		}
		if c.IsNull(row) {
			return nil, true
		}
		return c.Int64s[row], true
	case client.FLOAT:
		if row >= len(c.Float32s) {
			return nil, false
		}
		if c.IsNull(row) {
			return nil, true
		}
		return c.Float32s[row], true
	case client.DOUBLE:
		if row >= len(c.Float64s) {
			return nil, false
		}
		if c.IsNull(row) {
			return nil, true
		}
		return c.Float64s[row], true
	case client.DATE:
		if row >= len(c.Dates) {
			return nil, false
		}
		if c.IsNull(row) {
			return nil, true
		}
		date := c.Dates[row]
		year, month, day := int(date/10000), time.Month((date/100)%100), int(date%100)
		value := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
		if value.Year() != year || value.Month() != month || value.Day() != day {
			return nil, false
		}
		return value, true
	case client.TEXT, client.STRING:
		if row >= len(c.Strings) {
			return nil, false
		}
		if c.IsNull(row) {
			return nil, true
		}
		return c.Strings[row], true
	case client.BLOB, client.OBJECT:
		if row >= len(c.Binaries) {
			return nil, false
		}
		if c.IsNull(row) {
			return nil, true
		}
		return c.Binaries[row], true
	case client.UNKNOWN:
		return nil, true
	default:
		return nil, false
	}
}

// SetNull marks an SQL NULL cell without changing the backing array length.
func (c *TabletColumn) SetNull(row int) {
	if !c.hasRow(row) {
		return
	}
	c.setNull(row, true)
}

// IsNull reports whether row is SQL NULL. An absent column is all NULL.
func (c *TabletColumn) IsNull(row int) bool {
	if !c.hasRow(row) {
		return true
	}
	if len(c.Nulls) == 0 {
		return false
	}
	word := row / 64
	return word < len(c.Nulls) && c.Nulls[word]&(uint64(1)<<uint(row%64)) != 0
}

func (c *TabletColumn) hasRow(row int) bool {
	return row >= 0 && ((row < len(c.Booleans)) || (row < len(c.Int32s)) ||
		(row < len(c.Int64s)) || (row < len(c.Float32s)) ||
		(row < len(c.Float64s)) || (row < len(c.Dates)) ||
		(row < len(c.Strings)) || (row < len(c.Binaries)))
}

func (c *TabletColumn) setNull(row int, null bool) {
	if !c.hasRow(row) {
		return
	}
	rows := c.Rows()
	words := (rows + 63) / 64
	if len(c.Nulls) < words {
		nulls := make([]uint64, words)
		copy(nulls, c.Nulls)
		c.Nulls = nulls
	}
	word, mask := row/64, uint64(1)<<uint(row%64)
	if null {
		c.Nulls[word] |= mask
	} else {
		c.Nulls[word] &^= mask
	}
}

// Rows returns the backing row count for a populated column.
func (c *TabletColumn) Rows() int {
	switch c.DataType {
	case client.BOOLEAN:
		return len(c.Booleans)
	case client.INT32:
		return len(c.Int32s)
	case client.INT64, client.TIMESTAMP:
		return len(c.Int64s)
	case client.FLOAT:
		return len(c.Float32s)
	case client.DOUBLE:
		return len(c.Float64s)
	case client.DATE:
		return len(c.Dates)
	case client.TEXT, client.STRING:
		return len(c.Strings)
	case client.BLOB, client.OBJECT:
		return len(c.Binaries)
	default:
		return 0
	}
}


func (c *TabletColumn) present() bool {
	switch c.DataType {
	case client.BOOLEAN:
		return c.Booleans != nil
	case client.INT32:
		return c.Int32s != nil
	case client.INT64, client.TIMESTAMP:
		return c.Int64s != nil
	case client.FLOAT:
		return c.Float32s != nil
	case client.DOUBLE:
		return c.Float64s != nil
	case client.DATE:
		return c.Dates != nil
	case client.TEXT, client.STRING:
		return c.Strings != nil
	case client.BLOB, client.OBJECT:
		return c.Binaries != nil
	default:
		return false
	}
}

func (c *TabletColumn) estimatedSize() int64 {
	if !c.present() {
		return 0
	}
	size := int64(64) + int64(len(c.Nulls))*8 + c.VariableBytes
	switch c.DataType {
	case client.BOOLEAN:
		size += int64(len(c.Booleans))
	case client.INT32, client.FLOAT, client.DATE:
		size += int64(len(c.Int32s))*4 + int64(len(c.Float32s))*4 + int64(len(c.Dates))*4
	case client.INT64, client.TIMESTAMP, client.DOUBLE:
		size += int64(len(c.Int64s))*8 + int64(len(c.Float64s))*8
	case client.TEXT, client.STRING:
		size += int64(len(c.Strings))*16
	case client.BLOB, client.OBJECT:
		size += int64(len(c.Binaries))*24
	default:
		return 0
	}
	return size
}
