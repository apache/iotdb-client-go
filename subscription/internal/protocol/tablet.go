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

package protocol

import (
	"fmt"
	"io"
	"math"
	"time"

	"github.com/apache/iotdb-client-go/subscription/internal/record"
	"github.com/apache/iotdb-client-go/subscription/payload"
	"github.com/apache/iotdb-client-go/v2/client"
)

func decodeSubscriptionTablet(reader *subscriptionWireReader) (record.Tablet, error) {
	tablet := record.Tablet{Target: reader.ReadString()}
	rowCount := reader.ReadInt32()
	if reader.Err() != nil {
		return record.Tablet{}, reader.Err()
	}
	if rowCount < 0 {
		return record.Tablet{}, fmt.Errorf("subscription: invalid Tablet row count %d", rowCount)
	}
	tablet.RowCount = int(rowCount)
	if reader.ReadBool() {
		columnCount := reader.ReadCount(1, false)
		for index := int32(0); index < columnCount && reader.Err() == nil; index++ {
			hasSchema := reader.ReadBool()
			if reader.Err() != nil {
				return record.Tablet{}, reader.Err()
			}
			if !hasSchema {
				return record.Tablet{}, fmt.Errorf("subscription: null Tablet schema at column %d is not supported by the record handler", index)
			}
			column := payload.Column{Name: reader.ReadString(), DataType: client.TSDataType(reader.ReadByte())}
			// Encoding and compression are metadata in this uncompressed array
			// representation; they do not select a value decoder here.
			reader.ReadByte()
			reader.ReadByte()
			properties := reader.ReadCount(8, false)
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
				column.Category = client.TAG
			case 1:
				column.Category = client.FIELD
			case 2:
				column.Category = client.ATTRIBUTE
			default:
				return record.Tablet{}, fmt.Errorf("subscription: unsupported Tablet column category %d", category)
			}
			if _, err := subscriptionTabletValueSize(column.DataType); err != nil {
				return record.Tablet{}, err
			}
			tablet.Columns = append(tablet.Columns, column)
		}
	}
	if reader.Err() != nil {
		return record.Tablet{}, reader.Err()
	}
	if reader.ReadBool() {
		if err := requireSubscriptionTabletValues(reader, tablet.RowCount, 8); err != nil {
			return record.Tablet{}, err
		}
		tablet.Timestamps = make([]int64, tablet.RowCount)
		for row := range tablet.Timestamps {
			tablet.Timestamps[row] = reader.ReadInt64()
		}
	}
	bitmaps := make([][]byte, len(tablet.Columns))
	if reader.ReadBool() {
		for column := range tablet.Columns {
			if !reader.ReadBool() {
				continue
			}
			size := reader.ReadInt32()
			length := reader.ReadCount(1, false)
			if reader.Err() != nil {
				break
			}
			if size < rowCount || int64(length) < (int64(rowCount)+7)/8 {
				return record.Tablet{}, fmt.Errorf("subscription: Tablet bitmap cannot represent %d rows", rowCount)
			}
			bitmaps[column] = reader.Take(int(length))
		}
	}
	if reader.Err() != nil {
		return record.Tablet{}, reader.Err()
	}
	tablet.Values = make([][]any, len(tablet.Columns))
	if reader.ReadBool() {
		for column, schema := range tablet.Columns {
			if !reader.ReadBool() {
				continue
			}
			values, err := decodeSubscriptionTabletColumn(reader, schema.DataType, tablet.RowCount)
			if err != nil {
				return record.Tablet{}, err
			}
			for row := range values {
				if bitmap := bitmaps[column]; bitmap != nil && bitmap[row/8]&(1<<uint(row%8)) != 0 {
					values[row] = nil
				}
			}
			tablet.Values[column] = values
		}
	}
	if reader.Err() != nil {
		return record.Tablet{}, reader.Err()
	}
	return tablet, nil
}

// subscriptionTabletValueSize returns the minimum wire bytes per array entry.
// Binary-backed types use a presence byte before their optional length and data.
func subscriptionTabletValueSize(dataType client.TSDataType) (int, error) {
	switch dataType {
	case client.BOOLEAN, client.TEXT, client.STRING, client.BLOB, client.OBJECT:
		return 1, nil
	case client.INT32, client.FLOAT, client.DATE:
		return 4, nil
	case client.INT64, client.DOUBLE, client.TIMESTAMP:
		return 8, nil
	default:
		return 0, fmt.Errorf("subscription: unsupported Tablet data type %d", dataType)
	}
}

func requireSubscriptionTabletValues(reader *subscriptionWireReader, count, size int) error {
	if reader.Err() != nil {
		return reader.Err()
	}
	if count > reader.Remaining()/size {
		reader.SetError(fmt.Errorf("subscription: Tablet array of %d entries exceeds remaining payload: %w", count, io.ErrUnexpectedEOF))
		return reader.Err()
	}
	return nil
}

func decodeSubscriptionTabletColumn(reader *subscriptionWireReader, dataType client.TSDataType, rows int) ([]any, error) {
	size, err := subscriptionTabletValueSize(dataType)
	if err != nil {
		return nil, err
	}
	if err := requireSubscriptionTabletValues(reader, rows, size); err != nil {
		return nil, err
	}
	values := make([]any, rows)
	for row := range values {
		switch dataType {
		case client.BOOLEAN:
			values[row] = reader.ReadBool()
		case client.INT32:
			values[row] = reader.ReadInt32()
		case client.INT64, client.TIMESTAMP:
			values[row] = reader.ReadInt64()
		case client.FLOAT:
			values[row] = math.Float32frombits(uint32(reader.ReadInt32()))
		case client.DOUBLE:
			values[row] = math.Float64frombits(uint64(reader.ReadInt64()))
		case client.DATE:
			date := reader.ReadInt32()
			if reader.Err() != nil {
				return nil, reader.Err()
			}
			year, month, day := int(date/10000), time.Month((date/100)%100), int(date%100)
			value := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
			if value.Year() != year || value.Month() != month || value.Day() != day {
				return nil, fmt.Errorf("subscription: invalid Tablet DATE value %d", date)
			}
			values[row] = value
		case client.TEXT, client.STRING, client.BLOB, client.OBJECT:
			var binary []byte
			if reader.ReadBool() {
				length := reader.ReadCount(1, false)
				binary = reader.Take(int(length))
			}
			if dataType == client.TEXT || dataType == client.STRING {
				values[row] = string(binary)
			} else {
				// Type.deserializeArray uses Binary.EMPTY_VALUE for a missing
				// Binary. SQL NULL is supplied separately by the Tablet bitmap.
				values[row] = append([]byte{}, binary...)
			}
		}
		if reader.Err() != nil {
			return nil, reader.Err()
		}
	}
	return values, nil
}
