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

package rpcsession

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/apache/iotdb-client-go/v2/client"
)

// decodeSubscriptionRows reads the string fields used by SHOW TOPIC and SHOW
// SUBSCRIPTIONS. Topic and subscription column-count policies belong to callers.
func decodeSubscriptionRows(buffers [][]byte, dataTypes []string, columnMapping []int32, columnCount int) (rows [][]string, err error) {
	// The shared decoder and column accessors may panic on malformed lengths or
	// indexes. Contain that behavior at this untrusted result-data boundary.
	defer func() {
		if recovered := recover(); recovered != nil {
			rows = nil
			err = fmt.Errorf("subscription: invalid SHOW result: %v", recovered)
		}
	}()
	if columnCount != 2 && columnCount != 4 {
		return nil, fmt.Errorf("subscription: unsupported SHOW field count %d", columnCount)
	}
	if len(dataTypes) < columnCount {
		return nil, fmt.Errorf("subscription: SHOW result has %d types for %d requested fields", len(dataTypes), columnCount)
	}
	if len(columnMapping) > 0 && len(columnMapping) < columnCount {
		return nil, fmt.Errorf("subscription: SHOW result has %d column mappings for %d requested fields", len(columnMapping), columnCount)
	}
	for logical := 0; logical < columnCount; logical++ {
		if dataTypes[logical] != "TEXT" && dataTypes[logical] != "STRING" {
			return nil, fmt.Errorf("subscription: SHOW field %d has non-string type %q", logical, dataTypes[logical])
		}
		if len(columnMapping) > 0 && columnMapping[logical] < 0 {
			return nil, fmt.Errorf("subscription: SHOW field %d maps to invalid physical column %d", logical, columnMapping[logical])
		}
	}

	for blockIndex, buffer := range buffers {
		wireTypes, validationErr := validateSubscriptionTsBlock(buffer)
		if validationErr != nil {
			return nil, fmt.Errorf("subscription: invalid SHOW block %d: %w", blockIndex, validationErr)
		}
		physicalIndexes := make([]int32, columnCount)
		for logical := range physicalIndexes {
			physical := int32(logical)
			if len(columnMapping) > 0 {
				physical = columnMapping[logical]
			}
			if physical < 0 || int64(physical) >= int64(len(wireTypes)) {
				return nil, fmt.Errorf("subscription: SHOW field %d maps to invalid physical column %d", logical, physical)
			}
			dataType := client.TSDataType(wireTypes[physical])
			if dataType != client.TEXT && dataType != client.STRING {
				return nil, fmt.Errorf("subscription: SHOW field %d maps to non-string physical column %d", logical, physical)
			}
			physicalIndexes[logical] = physical
		}

		block, decodeErr := client.DeserializeTsBlock(buffer)
		if decodeErr != nil {
			return nil, fmt.Errorf("subscription: decode SHOW block %d: %w", blockIndex, decodeErr)
		}
		if block == nil || block.GetPositionCount() < 0 || block.GetValueColumnCount() != int32(len(wireTypes)) {
			return nil, fmt.Errorf("subscription: invalid decoded SHOW block %d", blockIndex)
		}
		columns := make([]client.Column, columnCount)
		for logical, physical := range physicalIndexes {
			column := block.GetColumn(physical)
			if column == nil || column.GetPositionCount() != block.GetPositionCount() {
				return nil, fmt.Errorf("subscription: invalid SHOW column %d in block %d", physical, blockIndex)
			}
			columns[logical] = column
		}
		for position := int32(0); position < block.GetPositionCount(); position++ {
			row := make([]string, columnCount)
			for logical, column := range columns {
				if column.IsNull(position) {
					// Java Field.getStringValue uses this representation for SQL NULL.
					row[logical] = "null"
					continue
				}
				value, valueErr := column.GetBinary(position)
				if valueErr != nil {
					return nil, fmt.Errorf("subscription: read SHOW field %d in block %d: %w", logical, blockIndex, valueErr)
				}
				if value == nil {
					return nil, fmt.Errorf("subscription: SHOW field %d in block %d has a nil non-null value", logical, blockIndex)
				}
				row[logical] = value.GetStringValue()
			}
			rows = append(rows, row)
		}
	}
	return rows, nil
}

// validateSubscriptionTsBlock checks the fixed header before handing result data
// to the shared decoder. Value-column decoding remains owned by the client module.
func validateSubscriptionTsBlock(data []byte) ([]byte, error) {
	if len(data) < 9 {
		return nil, io.ErrUnexpectedEOF
	}
	columnCount := int64(int32(binary.BigEndian.Uint32(data[:4])))
	if columnCount < 0 || columnCount > (int64(len(data))-9)/2 {
		return nil, fmt.Errorf("invalid TsBlock value-column count %d", columnCount)
	}
	typesEnd := 4 + int(columnCount)
	positionCount := int32(binary.BigEndian.Uint32(data[typesEnd : typesEnd+4]))
	if positionCount < 0 {
		return nil, fmt.Errorf("invalid TsBlock position count %d", positionCount)
	}
	return data[4:typesEnd], nil
}
