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
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"time"
)

// DecodeTablet decodes one serialized Tablet from the beginning of data. The
// returned byte count identifies the end of the Tablet so callers can decode
// multiple Tablets from one payload. Trailing bytes are not validated here.
func DecodeTablet(data []byte) (*Tablet, int, error) {
	reader := &tabletDecoder{data: data}
	tablet, err := decodeTablet(reader)
	if err != nil {
		return nil, 0, err
	}
	return tablet, len(data) - reader.Remaining(), nil
}

func decodeTablet(reader *tabletDecoder) (*Tablet, error) {
	target := reader.ReadString()
	rowCount := reader.ReadInt32()
	if reader.Err() != nil {
		return nil, reader.Err()
	}
	if rowCount < 0 {
		return nil, fmt.Errorf("tablet: invalid row count %d", rowCount)
	}
	rows := int(rowCount)

	var schemas []*MeasurementSchema
	var categories []ColumnCategory
	if reader.ReadBool() {
		columnCount := reader.ReadCount(1)
		for index := int32(0); index < columnCount && reader.Err() == nil; index++ {
			hasSchema := reader.ReadBool()
			if reader.Err() != nil {
				return nil, reader.Err()
			}
			if !hasSchema {
				return nil, fmt.Errorf("tablet: null schema at column %d is not supported", index)
			}
			schema := &MeasurementSchema{Measurement: reader.ReadString(), DataType: TSDataType(reader.ReadByte())}
			// Encoding and compression describe the source representation; the
			// serialized Tablet already contains uncompressed typed arrays.
			reader.ReadByte()
			reader.ReadByte()
			properties := reader.ReadCount(8)
			for property := int32(0); property < properties && reader.Err() == nil; property++ {
				reader.ReadString()
				reader.ReadString()
			}
			category := reader.ReadByte()
			if reader.Err() != nil {
				break
			}
			switch category {
			case 0:
				categories = append(categories, TAG)
			case 1:
				categories = append(categories, FIELD)
			case 2:
				categories = append(categories, ATTRIBUTE)
			default:
				return nil, fmt.Errorf("tablet: unsupported column category %d", category)
			}
			if _, err := tabletValueSize(schema.DataType); err != nil {
				return nil, err
			}
			schemas = append(schemas, schema)
		}
	}
	if reader.Err() != nil {
		return nil, reader.Err()
	}

	var timestamps []int64
	if reader.ReadBool() {
		if err := requireTabletValues(reader, rows, 8); err != nil {
			return nil, err
		}
		timestamps = make([]int64, rows)
		for row := range timestamps {
			timestamps[row] = reader.ReadInt64()
		}
	}

	nullBitmaps := make([][]byte, len(schemas))
	if reader.ReadBool() {
		for column := range schemas {
			if !reader.ReadBool() {
				continue
			}
			size := reader.ReadInt32()
			length := reader.ReadCount(1)
			if reader.Err() != nil {
				break
			}
			if size < rowCount || int64(length) < (int64(rowCount)+7)/8 {
				return nil, fmt.Errorf("tablet: bitmap cannot represent %d rows", rowCount)
			}
			nullBitmaps[column] = append([]byte(nil), reader.Take(int(length))...)
		}
	}
	if reader.Err() != nil {
		return nil, reader.Err()
	}

	values := make([]interface{}, len(schemas))
	if reader.ReadBool() {
		for column, schema := range schemas {
			if !reader.ReadBool() {
				continue
			}
			decoded, err := decodeTabletColumn(reader, schema.DataType, rows)
			if err != nil {
				return nil, err
			}
			values[column] = decoded
		}
	}
	if reader.Err() != nil {
		return nil, reader.Err()
	}
	return NewDecodedTablet(target, schemas, categories, rows, timestamps, values, nullBitmaps), nil
}

func tabletValueSize(dataType TSDataType) (int, error) {
	switch dataType {
	case BOOLEAN, TEXT, STRING, BLOB, OBJECT:
		return 1, nil
	case INT32, FLOAT, DATE:
		return 4, nil
	case INT64, DOUBLE, TIMESTAMP:
		return 8, nil
	default:
		return 0, fmt.Errorf("tablet: unsupported data type %d", dataType)
	}
}

func requireTabletValues(reader *tabletDecoder, count, size int) error {
	if reader.Err() != nil {
		return reader.Err()
	}
	if count > reader.Remaining()/size {
		reader.SetError(fmt.Errorf("tablet: array of %d entries exceeds remaining payload: %w", count, io.ErrUnexpectedEOF))
		return reader.Err()
	}
	return nil
}

func decodeTabletColumn(reader *tabletDecoder, dataType TSDataType, rows int) (interface{}, error) {
	size, err := tabletValueSize(dataType)
	if err != nil {
		return nil, err
	}
	if err := requireTabletValues(reader, rows, size); err != nil {
		return nil, err
	}
	switch dataType {
	case BOOLEAN:
		values := make([]bool, rows)
		for row := range values {
			values[row] = reader.ReadBool()
		}
		return values, reader.Err()
	case INT32:
		values := make([]int32, rows)
		for row := range values {
			values[row] = reader.ReadInt32()
		}
		return values, reader.Err()
	case INT64, TIMESTAMP:
		values := make([]int64, rows)
		for row := range values {
			values[row] = reader.ReadInt64()
		}
		return values, reader.Err()
	case FLOAT:
		values := make([]float32, rows)
		for row := range values {
			values[row] = math.Float32frombits(uint32(reader.ReadInt32()))
		}
		return values, reader.Err()
	case DOUBLE:
		values := make([]float64, rows)
		for row := range values {
			values[row] = math.Float64frombits(uint64(reader.ReadInt64()))
		}
		return values, reader.Err()
	case DATE:
		values := make([]int32, rows)
		for row := range values {
			date := reader.ReadInt32()
			if reader.Err() != nil {
				return nil, reader.Err()
			}
			year, month, day := int(date/10000), time.Month((date/100)%100), int(date%100)
			value := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
			if value.Year() != year || value.Month() != month || value.Day() != day {
				return nil, fmt.Errorf("tablet: invalid DATE value %d", date)
			}
			values[row] = date
		}
		return values, reader.Err()
	case TEXT, STRING, BLOB, OBJECT:
		values := make([][]byte, rows)
		for row := range values {
			if reader.ReadBool() {
				length := reader.ReadCount(1)
				values[row] = append([]byte{}, reader.Take(int(length))...)
			} else {
				values[row] = []byte{}
			}
		}
		return values, reader.Err()
	default:
		return nil, fmt.Errorf("tablet: unsupported data type %d", dataType)
	}
}

type tabletDecoder struct {
	data   []byte
	offset int
	err    error
}

func (d *tabletDecoder) Remaining() int {
	if d.err != nil || d.offset < 0 || d.offset > len(d.data) {
		return 0
	}
	return len(d.data) - d.offset
}

func (d *tabletDecoder) Take(count int) []byte {
	if d.err != nil {
		return nil
	}
	if count < 0 || d.offset < 0 || d.offset > len(d.data) || count > len(d.data)-d.offset {
		d.err = io.ErrUnexpectedEOF
		return nil
	}
	value := d.data[d.offset : d.offset+count]
	d.offset += count
	return value
}

func (d *tabletDecoder) ReadByte() int8 {
	value := d.Take(1)
	if d.err != nil {
		return 0
	}
	return int8(value[0])
}

func (d *tabletDecoder) ReadBool() bool {
	return d.ReadByte() == 1
}

func (d *tabletDecoder) ReadInt32() int32 {
	value := d.Take(4)
	if d.err != nil {
		return 0
	}
	return int32(binary.BigEndian.Uint32(value))
}

func (d *tabletDecoder) ReadInt64() int64 {
	value := d.Take(8)
	if d.err != nil {
		return 0
	}
	return int64(binary.BigEndian.Uint64(value))
}

func (d *tabletDecoder) ReadString() string {
	length := d.ReadInt32()
	if d.err != nil || length == -1 {
		return ""
	}
	if length < -1 {
		d.err = fmt.Errorf("tablet: invalid string length %d", length)
		return ""
	}
	return string(d.Take(int(length)))
}

func (d *tabletDecoder) ReadCount(minimumEntryBytes int) int32 {
	count := d.ReadInt32()
	if d.err != nil {
		return 0
	}
	if count < 0 {
		d.err = fmt.Errorf("tablet: invalid collection size %d", count)
		return 0
	}
	if int64(count) > int64(d.Remaining()/minimumEntryBytes) {
		d.err = fmt.Errorf("tablet: collection size %d exceeds remaining payload: %w", count, io.ErrUnexpectedEOF)
		return 0
	}
	return count
}

func (d *tabletDecoder) Err() error {
	return d.err
}

func (d *tabletDecoder) SetError(err error) {
	if d.err == nil {
		d.err = err
	}
}
