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

package client

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"sort"
	"time"
)

type MeasurementSchema struct {
	Measurement string
	DataType    TSDataType
}

type ColumnCategory int8

const (
	TAG = iota
	FIELD
	ATTRIBUTE
)

type Tablet struct {
	insertTargetName   string
	measurementSchemas []*MeasurementSchema
	columnCategories   []ColumnCategory
	timestamps         []int64
	values             []interface{}
	bitMaps            []*BitMap
	maxRowNumber       int
	variableBytes      int64
	fromDecodedPayload bool
	RowSize            int
}

func (t *Tablet) Len() int {
	return t.RowSize
}

func (t *Tablet) Swap(i, j int) {
	for index, schema := range t.measurementSchemas {
		if t.values[index] == nil {
			continue
		}
		switch schema.DataType {
		case BOOLEAN:
			sortedSlice := t.values[index].([]bool)
			sortedSlice[i], sortedSlice[j] = sortedSlice[j], sortedSlice[i]
		case INT32, DATE:
			sortedSlice := t.values[index].([]int32)
			sortedSlice[i], sortedSlice[j] = sortedSlice[j], sortedSlice[i]
		case INT64, TIMESTAMP:
			sortedSlice := t.values[index].([]int64)
			sortedSlice[i], sortedSlice[j] = sortedSlice[j], sortedSlice[i]
		case FLOAT:
			sortedSlice := t.values[index].([]float32)
			sortedSlice[i], sortedSlice[j] = sortedSlice[j], sortedSlice[i]
		case DOUBLE:
			sortedSlice := t.values[index].([]float64)
			sortedSlice[i], sortedSlice[j] = sortedSlice[j], sortedSlice[i]
		case TEXT, BLOB, STRING, OBJECT:
			sortedSlice := t.values[index].([][]byte)
			sortedSlice[i], sortedSlice[j] = sortedSlice[j], sortedSlice[i]
		}
	}
	if t.bitMaps != nil {
		for _, bitMap := range t.bitMaps {
			if bitMap != nil {
				isNilI := bitMap.IsMarked(i)
				isNilJ := bitMap.IsMarked(j)
				if isNilI {
					bitMap.Mark(j)
				} else {
					bitMap.UnMark(j)
				}
				if isNilJ {
					bitMap.Mark(i)
				} else {
					bitMap.UnMark(i)
				}
			}
		}
	}
	t.timestamps[i], t.timestamps[j] = t.timestamps[j], t.timestamps[i]
}

func (t *Tablet) Less(i, j int) bool {
	return t.timestamps[i] < t.timestamps[j]
}

func (t *Tablet) SetTimestamp(timestamp int64, rowIndex int) {
	t.timestamps[rowIndex] = timestamp
}

// SetTimestampAt is a bounds-checked alternative to SetTimestamp. It validates
// rowIndex and returns an "illegal argument rowIndex" error instead of
// panicking with an index-out-of-range when rowIndex is negative or beyond the
// tablet's capacity, consistent with SetValueAt and GetValueAt. The existing
// SetTimestamp is preserved unchanged for source compatibility.
func (t *Tablet) SetTimestampAt(timestamp int64, rowIndex int) error {
	if rowIndex < 0 || rowIndex >= t.maxRowNumber {
		return fmt.Errorf("illegal argument rowIndex %d", rowIndex)
	}
	t.timestamps[rowIndex] = timestamp
	return nil
}

// SetValueAt stores a typed value or NULL. OBJECT values on a normal insert
// Tablet must use SetObjectValueAt so their EOF and offset are encoded. Tablets
// created from decoded subscription payloads also accept raw OBJECT bytes here
// because those bytes are already in the wire representation.
func (t *Tablet) SetValueAt(value interface{}, columnIndex, rowIndex int) error {

	if columnIndex < 0 || columnIndex >= len(t.measurementSchemas) {
		return fmt.Errorf("illegal argument columnIndex %d", columnIndex)
	}

	if rowIndex < 0 || rowIndex >= t.maxRowNumber {
		return fmt.Errorf("illegal argument rowIndex %d", rowIndex)
	}

	if value == nil {
		// Init the bitMap to mark nil value
		if t.bitMaps == nil {
			t.bitMaps = make([]*BitMap, len(t.values))
		}
		if t.bitMaps[columnIndex] == nil {
			t.bitMaps[columnIndex] = NewBitMap(t.maxRowNumber)
		}
		// Mark the nil value position
		t.bitMaps[columnIndex].Mark(rowIndex)
		return nil
	}
	if t.values[columnIndex] == nil {
		return fmt.Errorf("column %d has no decoded values", columnIndex)
	}

	switch t.measurementSchemas[columnIndex].DataType {
	case BOOLEAN:
		values := t.values[columnIndex].([]bool)
		switch v := value.(type) {
		case bool:
			values[rowIndex] = v
		case *bool:
			values[rowIndex] = *v
		default:
			return fmt.Errorf("illegal argument value %v %v", value, reflect.TypeOf(value))
		}
	case INT32:
		values := t.values[columnIndex].([]int32)
		switch v := value.(type) {
		case int32:
			values[rowIndex] = v
		case *int32:
			values[rowIndex] = *v
		default:
			return fmt.Errorf("illegal argument value %v %v", value, reflect.TypeOf(value))
		}
	case INT64, TIMESTAMP:
		values := t.values[columnIndex].([]int64)
		switch v := value.(type) {
		case int64:
			values[rowIndex] = v
		case *int64:
			values[rowIndex] = *v
		default:
			return fmt.Errorf("illegal argument value %v %v", value, reflect.TypeOf(value))
		}
	case FLOAT:
		values := t.values[columnIndex].([]float32)
		switch v := value.(type) {
		case float32:
			values[rowIndex] = v
		case *float32:
			values[rowIndex] = *v
		default:
			return fmt.Errorf("illegal argument value %v %v", value, reflect.TypeOf(value))
		}
	case DOUBLE:
		values := t.values[columnIndex].([]float64)
		switch v := value.(type) {
		case float64:
			values[rowIndex] = v
		case *float64:
			values[rowIndex] = *v
		default:
			return fmt.Errorf("illegal argument value %v %v", value, reflect.TypeOf(value))
		}
	case TEXT, STRING:
		values := t.values[columnIndex].([][]byte)
		switch v := value.(type) {
		case string:
			t.variableBytes += int64(len(v) - len(values[rowIndex]))
			values[rowIndex] = []byte(v)
		case []byte:
			t.variableBytes += int64(len(v) - len(values[rowIndex]))
			values[rowIndex] = v
		default:
			return fmt.Errorf("illegal argument value %v %v", value, reflect.TypeOf(value))
		}
	case BLOB:
		values := t.values[columnIndex].([][]byte)
		switch v := value.(type) {
		case []byte:
			t.variableBytes += int64(len(v) - len(values[rowIndex]))
			values[rowIndex] = v
		default:
			return fmt.Errorf("illegal argument value %v %v", value, reflect.TypeOf(value))
		}
	case OBJECT:
		if !t.fromDecodedPayload {
			return fmt.Errorf("OBJECT values must be set with SetObjectValueAt")
		}
		values := t.values[columnIndex].([][]byte)
		switch v := value.(type) {
		case string:
			t.variableBytes += int64(len(v) - len(values[rowIndex]))
			values[rowIndex] = []byte(v)
		case []byte:
			t.variableBytes += int64(len(v) - len(values[rowIndex]))
			values[rowIndex] = v
		default:
			return fmt.Errorf("illegal argument value %v %v", value, reflect.TypeOf(value))
		}
	case DATE:
		values := t.values[columnIndex].([]int32)
		switch v := value.(type) {
		case time.Time:
			val, err := DateToInt32(v)
			if err != nil {
				return err
			}
			values[rowIndex] = val
		default:
			return fmt.Errorf("illegal argument value %v %v", value, reflect.TypeOf(value))
		}
	}
	t.unmarkNullValueAt(columnIndex, rowIndex)
	return nil
}

func (t *Tablet) unmarkNullValueAt(columnIndex, rowIndex int) {
	if t.bitMaps != nil && t.bitMaps[columnIndex] != nil {
		t.bitMaps[columnIndex].UnMark(rowIndex)
	}
}

// SetObjectValueAt writes a segment of an OBJECT column value. An OBJECT value can be
// written in multiple segments so that a large object does not need to be fully loaded
// into memory: each segment is wrapped into a 9-byte header (1 byte isEOF flag followed
// by an 8-byte big-endian offset) and then the raw content, consistent with the Java
// Tablet.addValue(rowIndex, columnIndex, isEOF, offset, content). Segments of the same
// object must be written in order with ascending offsets, and the last segment must set
// isEOF to true.
//
// Parameters:
//   - isEOF: Whether this segment is the last one of the object.
//   - offset: The offset of this segment within the whole object.
//   - content: The raw bytes of this segment.
//   - columnIndex: The column index of the OBJECT column.
//   - rowIndex: The row index to write the segment into.
//
// Returns:
//   - err: An error if the column/row index is invalid or the column is not of type OBJECT.
func (t *Tablet) SetObjectValueAt(isEOF bool, offset int64, content []byte, columnIndex, rowIndex int) error {
	if columnIndex < 0 || columnIndex >= len(t.measurementSchemas) {
		return fmt.Errorf("illegal argument columnIndex %d", columnIndex)
	}
	if rowIndex < 0 || rowIndex >= t.maxRowNumber {
		return fmt.Errorf("illegal argument rowIndex %d", rowIndex)
	}
	if t.measurementSchemas[columnIndex].DataType != OBJECT {
		return fmt.Errorf("column %d must be of type OBJECT", columnIndex)
	}
	if t.values[columnIndex] == nil {
		return fmt.Errorf("column %d has no decoded values", columnIndex)
	}
	value := make([]byte, len(content)+9)
	if isEOF {
		value[0] = 1
	}
	binary.BigEndian.PutUint64(value[1:9], uint64(offset))
	copy(value[9:], content)
	t.variableBytes += int64(len(value) - len(t.values[columnIndex].([][]byte)[rowIndex]))
	t.values[columnIndex].([][]byte)[rowIndex] = value
	t.unmarkNullValueAt(columnIndex, rowIndex)
	return nil
}

func (t *Tablet) GetMaxRowNumber() int {
	return t.maxRowNumber
}

// HasColumnValues reports whether columnIndex has a value backing array. A
// decoded Tablet may omit an entire column; such a column reads as NULL and
// rejects non-NULL SetValueAt calls.
func (t *Tablet) HasColumnValues(columnIndex int) bool {
	if columnIndex < 0 || columnIndex >= len(t.values) {
		return false
	}
	return t.values[columnIndex] != nil
}

// GetValueAt returns the value at the zero-based column and row indexes. TEXT
// and STRING values are converted to string for the public API; use
// GetRawValueAt to access their decoded byte storage without that allocation.
func (t *Tablet) GetValueAt(columnIndex, rowIndex int) (interface{}, error) {
	value, err := t.GetRawValueAt(columnIndex, rowIndex)
	if err != nil || value == nil {
		return value, err
	}
	switch t.measurementSchemas[columnIndex].DataType {
	case TEXT, STRING:
		return string(value.([]byte)), nil
	default:
		return value, nil
	}
}

// GetRawValueAt is the allocation-conscious counterpart to GetValueAt for
// processor hot paths. TEXT and STRING values are returned as borrowed []byte
// storage instead of allocating a string; all other types use the GetValueAt
// representation. The returned slice must not be mutated and remains valid only
// while the Tablet retains its decoded buffers.
func (t *Tablet) GetRawValueAt(columnIndex, rowIndex int) (interface{}, error) {
	if columnIndex < 0 || columnIndex >= len(t.measurementSchemas) {
		return nil, fmt.Errorf("illegal argument columnIndex %d", columnIndex)
	}

	if rowIndex < 0 || rowIndex >= t.maxRowNumber {
		return nil, fmt.Errorf("illegal argument rowIndex %d", rowIndex)
	}

	schema := t.measurementSchemas[columnIndex]

	if t.bitMaps != nil && t.bitMaps[columnIndex] != nil && t.bitMaps[columnIndex].IsMarked(rowIndex) {
		return nil, nil
	}
	if t.values[columnIndex] == nil {
		return nil, nil
	}
	switch schema.DataType {
	case BOOLEAN:
		return t.values[columnIndex].([]bool)[rowIndex], nil
	case INT32:
		return t.values[columnIndex].([]int32)[rowIndex], nil
	case INT64, TIMESTAMP:
		return t.values[columnIndex].([]int64)[rowIndex], nil
	case FLOAT:
		return t.values[columnIndex].([]float32)[rowIndex], nil
	case DOUBLE:
		return t.values[columnIndex].([]float64)[rowIndex], nil
	case TEXT, STRING, BLOB, OBJECT:
		return t.values[columnIndex].([][]byte)[rowIndex], nil
	case DATE:
		return Int32ToDate(t.values[columnIndex].([]int32)[rowIndex])
	default:
		return nil, fmt.Errorf("illegal datatype %v", schema.DataType)
	}
}

// GetTimestampAt returns the timestamp at rowIndex. Decoded Tablets use zero for
// rows whose timestamp flag was absent from the wire payload. The caller must
// provide an index within RowSize.
func (t *Tablet) GetTimestampAt(rowIndex int) int64 {
	return t.timestamps[rowIndex]
}

// GetTargetName returns the tree device path or table name for this Tablet.
func (t *Tablet) GetTargetName() string {
	return t.insertTargetName
}

// ColumnCount returns the number of data columns.
func (t *Tablet) ColumnCount() int {
	return len(t.measurementSchemas)
}

// ColumnName returns the measurement name at index. The caller must provide a
// valid column index.
func (t *Tablet) ColumnName(index int) string {
	return t.measurementSchemas[index].Measurement
}

// ColumnDataType returns the data type at index. The caller must provide a valid
// column index.
func (t *Tablet) ColumnDataType(index int) TSDataType {
	return t.measurementSchemas[index].DataType
}

// ColumnCategory returns the table column category at index. Tree Tablets
// without an explicit category list use FIELD. The caller must provide a valid
// column index.
func (t *Tablet) ColumnCategory(index int) ColumnCategory {
	if index >= len(t.columnCategories) {
		return FIELD
	}
	return t.columnCategories[index]
}

// EstimatedSize returns an approximate retained heap size for the Tablet.
func (t *Tablet) EstimatedSize() int64 {
	size := int64(160) + int64(len(t.insertTargetName)) + int64(len(t.timestamps))*8 +
		int64(len(t.measurementSchemas))*64 + int64(len(t.columnCategories))*8 + t.variableBytes
	rows := int64(t.maxRowNumber)
	for column, schema := range t.measurementSchemas {
		size += int64(len(schema.Measurement))
		if t.values[column] == nil {
			continue
		}
		switch schema.DataType {
		case BOOLEAN:
			size += rows
		case INT32, FLOAT, DATE:
			size += rows * 4
		case INT64, TIMESTAMP, DOUBLE:
			size += rows * 8
		case TEXT, STRING, BLOB, OBJECT:
			size += rows * 24
		}
	}
	for _, bitmap := range t.bitMaps {
		if bitmap != nil {
			size += int64(len(bitmap.bits))
		}
	}
	return size
}

func (t *Tablet) GetTimestampBytes() []byte {
	buff := &bytes.Buffer{}
	binary.Write(buff, binary.BigEndian, t.timestamps[0:t.RowSize])
	return buff.Bytes()
}

func (t *Tablet) GetMeasurements() []string {
	measurements := make([]string, len(t.measurementSchemas))
	for i, s := range t.measurementSchemas {
		measurements[i] = s.Measurement
	}
	return measurements
}

func (t *Tablet) getDataTypes() []int32 {
	types := make([]int32, len(t.measurementSchemas))
	for i, s := range t.measurementSchemas {
		types[i] = int32(s.DataType)
	}
	return types
}

func (t *Tablet) getColumnCategories() []int8 {
	columnCategories := make([]int8, len(t.columnCategories))
	for i := range t.columnCategories {
		columnCategories[i] = int8(t.columnCategories[i])
	}
	return columnCategories
}

func (t *Tablet) getValuesBytes() ([]byte, error) {
	buff := &bytes.Buffer{}
	for i, schema := range t.measurementSchemas {
		switch schema.DataType {
		case BOOLEAN:
			binary.Write(buff, binary.BigEndian, t.values[i].([]bool)[0:t.RowSize])
		case INT32, DATE:
			binary.Write(buff, binary.BigEndian, t.values[i].([]int32)[0:t.RowSize])
		case INT64, TIMESTAMP:
			binary.Write(buff, binary.BigEndian, t.values[i].([]int64)[0:t.RowSize])
		case FLOAT:
			binary.Write(buff, binary.BigEndian, t.values[i].([]float32)[0:t.RowSize])
		case DOUBLE:
			binary.Write(buff, binary.BigEndian, t.values[i].([]float64)[0:t.RowSize])
		case TEXT, STRING, BLOB, OBJECT:
			for _, s := range t.values[i].([][]byte)[0:t.RowSize] {
				binary.Write(buff, binary.BigEndian, int32(len(s)))
				binary.Write(buff, binary.BigEndian, s)
			}
		default:
			return nil, fmt.Errorf("illegal datatype %v", schema.DataType)
		}
	}
	if t.bitMaps != nil {
		for _, bitMap := range t.bitMaps {
			columnHasNil := bitMap != nil && !bitMap.IsAllUnmarked()
			binary.Write(buff, binary.BigEndian, columnHasNil)
			if columnHasNil {
				// Need to maintain consistency with the calculation method on the IoTDB side.
				binary.Write(buff, binary.BigEndian, bitMap.GetBits()[0:t.RowSize/8+1])
			}
		}
	}
	return buff.Bytes(), nil
}

func (t *Tablet) Sort() error {
	sort.Sort(t)
	return nil
}

func (t *Tablet) Reset() {
	t.RowSize = 0
	t.bitMaps = nil
}

func NewTablet(insertTargetName string, measurementSchemas []*MeasurementSchema, maxRowNumber int) (*Tablet, error) {
	tablet := &Tablet{
		insertTargetName:   insertTargetName,
		measurementSchemas: measurementSchemas,
		maxRowNumber:       maxRowNumber,
	}
	tablet.timestamps = make([]int64, maxRowNumber)
	tablet.values = make([]interface{}, len(measurementSchemas))
	for i, schema := range tablet.measurementSchemas {
		switch schema.DataType {
		case BOOLEAN:
			tablet.values[i] = make([]bool, maxRowNumber)
		case INT32, DATE:
			tablet.values[i] = make([]int32, maxRowNumber)
		case INT64, TIMESTAMP:
			tablet.values[i] = make([]int64, maxRowNumber)
		case FLOAT:
			tablet.values[i] = make([]float32, maxRowNumber)
		case DOUBLE:
			tablet.values[i] = make([]float64, maxRowNumber)
		case TEXT, STRING, BLOB, OBJECT:
			tablet.values[i] = make([][]byte, maxRowNumber)
		default:
			return nil, fmt.Errorf("illegal datatype %v", schema.DataType)
		}
	}
	return tablet, nil
}

func NewRelationalTablet(tableName string, measurementSchemas []*MeasurementSchema, columnCategories []ColumnCategory, maxRowNumber int) (*Tablet, error) {
	tablet, err := NewTablet(tableName, measurementSchemas, maxRowNumber)
	if err != nil {
		return nil, err
	}
	tablet.columnCategories = columnCategories
	return tablet, nil
}

// NewDecodedTablet constructs a Tablet from data already validated by a wire
// decoder. It takes ownership of the supplied slices and does not copy typed
// values or NULL bitmaps. The caller must ensure that schemas, categories,
// values, and nullBitmaps have matching lengths, that each non-nil value slice
// uses the storage type required by its schema, and that every bitmap can
// represent rowCount rows. A nil values entry represents a column absent from
// the decoded payload. A nil timestamps slice is replaced with a zero-filled
// slice of rowCount entries. No other defensive validation is performed.
func NewDecodedTablet(
	insertTargetName string,
	measurementSchemas []*MeasurementSchema,
	columnCategories []ColumnCategory,
	rowCount int,
	timestamps []int64,
	values []interface{},
	nullBitmaps [][]byte,
) *Tablet {
	tablet := &Tablet{
		insertTargetName:   insertTargetName,
		measurementSchemas: measurementSchemas,
		columnCategories:   columnCategories,
		timestamps:         timestamps,
		values:             values,
		maxRowNumber:       rowCount,
		fromDecodedPayload: true,
		RowSize:            rowCount,
	}
	if tablet.timestamps == nil {
		tablet.timestamps = make([]int64, rowCount)
	}
	for column, schema := range measurementSchemas {
		switch schema.DataType {
		case TEXT, STRING, BLOB, OBJECT:
			if columnValues, ok := values[column].([][]byte); ok {
				for _, value := range columnValues {
					tablet.variableBytes += int64(len(value))
				}
			}
		}
	}
	if nullBitmaps != nil {
		tablet.bitMaps = make([]*BitMap, len(nullBitmaps))
		for column, bits := range nullBitmaps {
			if bits != nil {
				tablet.bitMaps[column] = &BitMap{size: rowCount, bits: bits}
			}
		}
	}
	return tablet
}
