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
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/apache/iotdb-client-go/rpc"
)

const (
	startIndex          = 2
	flag                = 0x80
)

var (
	tsTypeMap map[string]TSDataType = map[string]TSDataType{
		"BOOLEAN": BOOLEAN,
		"INT32":   INT32,
		"INT64":   INT64,
		"FLOAT":   FLOAT,
		"DOUBLE":  DOUBLE,
		"TEXT":    TEXT,
	}
)

type IoTDBRpcDataSet struct {
	columnCount                int
	sessionId                  int64
	queryId                    int64
	lastReadWasNull            bool
	rowsIndex                  int
	queryDataSet               *rpc.TSQueryDataSet
	sql                        string
	fetchSize                  int32
	columnNameList             []string
	columnTypeList             []TSDataType
	columnOrdinalMap           map[string]int32
	columnTypeDeduplicatedList []TSDataType
	currentBitmap              []byte
	time                       []byte
	values                     [][]byte
	client                     *rpc.TSIServiceClient
	emptyResultSet             bool
	ignoreTimeStamp            bool
}

func (s *IoTDBRpcDataSet) getColumnIndex(columnName string) int32 {
	return s.columnOrdinalMap[columnName] - startIndex
}

func (s *IoTDBRpcDataSet) getColumnType(columnName string) TSDataType {
	return s.columnTypeDeduplicatedList[s.getColumnIndex(columnName)]
}

func (s *IoTDBRpcDataSet) isNull(columnIndex int, rowIndex int) bool {
	bitmap := s.currentBitmap[columnIndex]
	shift := rowIndex % 8
	return ((flag >> shift) & (bitmap & 0xff)) == 0
}

func (s *IoTDBRpcDataSet) constructOneRow() error {
	// simulating buffer, read 8 bytes from data set and discard first 8 bytes which have been read.
	s.time = s.queryDataSet.Time[:8]
	s.queryDataSet.Time = s.queryDataSet.Time[8:]

	for i := 0; i < len(s.queryDataSet.BitmapList); i++ {
		bitmapBuffer := s.queryDataSet.BitmapList[i]
		if s.rowsIndex%8 == 0 {
			s.currentBitmap[i] = bitmapBuffer[0]
			s.queryDataSet.BitmapList[i] = bitmapBuffer[1:]
		}
		if !s.isNull(i, s.rowsIndex) {
			valueBuffer := s.queryDataSet.ValueList[i]
			dataType := s.columnTypeDeduplicatedList[i]
			switch dataType {
			case BOOLEAN:
				s.values[i] = valueBuffer[:1]
				s.queryDataSet.ValueList[i] = valueBuffer[1:]
				break
			case INT32:
				s.values[i] = valueBuffer[:4]
				s.queryDataSet.ValueList[i] = valueBuffer[4:]
				break
			case INT64:
				s.values[i] = valueBuffer[:8]
				s.queryDataSet.ValueList[i] = valueBuffer[8:]
				break
			case FLOAT:
				s.values[i] = valueBuffer[:4]
				s.queryDataSet.ValueList[i] = valueBuffer[4:]
				break
			case DOUBLE:
				s.values[i] = valueBuffer[:8]
				s.queryDataSet.ValueList[i] = valueBuffer[8:]
				break
			case TEXT:
				length := bytesToInt32(valueBuffer[:4])
				s.values[i] = valueBuffer[4 : 4+length]
				s.queryDataSet.ValueList[i] = valueBuffer[4+length:]
			default:
				return fmt.Errorf("unsupported data type %d", dataType)
			}
		}
	}
	s.rowsIndex++
	return nil
}

func (s *IoTDBRpcDataSet) GetTimestamp() int64 {
	return bytesToInt64(s.time)
}

func (s *IoTDBRpcDataSet) getText(columnName string) string {
	if columnName == TimestampColumnName {
		return time.Unix(0, bytesToInt64(s.time)*1000000).Format(time.RFC3339)
	}

	columnIndex := s.getColumnIndex(columnName)
	if columnIndex < 0 || int(columnIndex) >= len(s.values) || s.isNull(int(columnIndex), s.rowsIndex-1) {
		s.lastReadWasNull = true
		return ""
	}
	s.lastReadWasNull = false
	return s.getString(int(columnIndex), s.columnTypeDeduplicatedList[columnIndex])
}

func (s *IoTDBRpcDataSet) getString(columnIndex int, dataType TSDataType) string {
	valueBytes := s.values[columnIndex]
	switch dataType {
	case BOOLEAN:
		if valueBytes[0] != 0 {
			return "true"
		}
		return "false"
	case INT32:
		return fmt.Sprintf("%v", bytesToInt32(valueBytes))
	case INT64:
		return fmt.Sprintf("%v", bytesToInt64(valueBytes))
	case FLOAT:
		bits := binary.BigEndian.Uint32(valueBytes)
		return fmt.Sprintf("%v", math.Float32frombits(bits))
	case DOUBLE:
		bits := binary.BigEndian.Uint64(valueBytes)
		return fmt.Sprintf("%v", math.Float64frombits(bits))
	case TEXT:
		return string(valueBytes)
	default:
		return ""
	}
}

func (s *IoTDBRpcDataSet) getValue(columnName string) interface{} {
	columnIndex := int(s.getColumnIndex(columnName))
	if s.isNull(columnIndex, s.rowsIndex-1) {
		return nil
	}

	dataType := s.getColumnType(columnName)
	valueBytes := s.values[columnIndex]
	switch dataType {
	case BOOLEAN:
		return bool(valueBytes[0] != 0)
	case INT32:
		return bytesToInt32(valueBytes)
	case INT64:
		return bytesToInt64(valueBytes)
	case FLOAT:
		bits := binary.BigEndian.Uint32(valueBytes)
		return math.Float32frombits(bits)
	case DOUBLE:
		bits := binary.BigEndian.Uint64(valueBytes)
		return math.Float64frombits(bits)
	case TEXT:
		return string(valueBytes)
	default:
		return nil
	}
}

func (s *IoTDBRpcDataSet) getRowRecord() *RowRecord {
	fields := make([]*Field, s.columnCount)
	for i := 0; i < s.columnCount; i++ {
		columnName := s.columnNameList[i]
		field := Field{
			name:     columnName,
			dataType: s.getColumnType(columnName),
			value:    s.getValue(columnName),
		}
		fields[i] = &field
	}
	return &RowRecord{
		timestamp: s.GetTimestamp(),
		fields:    fields,
	}
}

func (s *IoTDBRpcDataSet) getBool(columnName string) bool {
	columnIndex := s.getColumnIndex(columnName)
	if !s.isNull(int(columnIndex), s.rowsIndex-1) {
		return s.values[columnIndex][0] != 0
	}
	s.lastReadWasNull = true
	return false
}

func (s *IoTDBRpcDataSet) scan(dest ...interface{}) error {
	count := s.columnCount
	if count > len(dest) {
		count = len(dest)
	}

	for i := 0; i < count; i++ {
		columnName := s.columnNameList[i]
		columnIndex := int(s.getColumnIndex(columnName))
		if s.isNull(columnIndex, s.rowsIndex-1) {
			continue
		}

		dataType := s.getColumnType(columnName)
		d := dest[i]
		valueBytes := s.values[columnIndex]
		switch dataType {
		case BOOLEAN:
			switch t := d.(type) {
			case *bool:
				*t = bool(valueBytes[0] != 0)
			case *string:
				if valueBytes[0] != 0 {
					*t = "true"
				} else {
					*t = "false"
				}
			default:
				return fmt.Errorf("dest[%d] types must be *bool or *string", i)
			}

		case INT32:
			switch t := d.(type) {
			case *int32:
				*t = bytesToInt32(valueBytes)
			case *string:
				*t = strconv.FormatInt(int64(bytesToInt32(valueBytes)), 10)
			default:
				return fmt.Errorf("dest[%d] types must be *int32 or *string", i)
			}
		case INT64:
			switch t := d.(type) {
			case *int64:
				*t = bytesToInt64(valueBytes)
			case *string:
				*t = strconv.FormatInt(bytesToInt64(valueBytes), 10)
			default:
				return fmt.Errorf("dest[%d] types must be *int64 or *string", i)
			}
		case FLOAT:
			switch t := d.(type) {
			case *float32:
				bits := binary.BigEndian.Uint32(valueBytes)
				*t = math.Float32frombits(bits)
			case *string:
				bits := binary.BigEndian.Uint32(valueBytes)
				*t = fmt.Sprintf("%v", math.Float32frombits(bits))
			default:
				return fmt.Errorf("dest[%d] types must be *float32 or *string", i)
			}
		case DOUBLE:
			switch t := d.(type) {
			case *float64:
				bits := binary.BigEndian.Uint64(valueBytes)
				*t = math.Float64frombits(bits)
			case *string:
				bits := binary.BigEndian.Uint64(valueBytes)
				*t = fmt.Sprintf("%v", math.Float64frombits(bits))
			default:
				return fmt.Errorf("dest[%d] types must be *float64 or *string", i)
			}
		case TEXT:
			switch t := d.(type) {
			case *string:
				*t = string(valueBytes)
			default:
				return fmt.Errorf("dest[%d] types must be *string", i)
			}
		default:
			return nil
		}
	}
	return nil
}

func (s *IoTDBRpcDataSet) getFloat(columnName string) float32 {
	columnIndex := s.getColumnIndex(columnName)
	if !s.isNull(int(columnIndex), s.rowsIndex-1) {
		s.lastReadWasNull = false
		bits := binary.BigEndian.Uint32(s.values[columnIndex])
		return math.Float32frombits(bits)
	}
	s.lastReadWasNull = true
	return 0
}

func (s *IoTDBRpcDataSet) getDouble(columnName string) float64 {
	columnIndex := s.getColumnIndex(columnName)

	if !s.isNull(int(columnIndex), s.rowsIndex-1) {
		s.lastReadWasNull = false
		bits := binary.BigEndian.Uint64(s.values[columnIndex])
		return math.Float64frombits(bits)
	}
	s.lastReadWasNull = true
	return 0
}

func (s *IoTDBRpcDataSet) getInt32(columnName string) int32 {
	columnIndex := s.getColumnIndex(columnName)
	if !s.isNull(int(columnIndex), s.rowsIndex-1) {
		s.lastReadWasNull = false
		return bytesToInt32(s.values[columnIndex])
	}

	s.lastReadWasNull = true
	return 0
}

func (s *IoTDBRpcDataSet) getInt64(columnName string) int64 {
	if columnName == TimestampColumnName {
		return bytesToInt64(s.time)
	}

	columnIndex := s.getColumnIndex(columnName)
	bys := s.values[columnIndex]

	if !s.isNull(int(columnIndex), s.rowsIndex-1) {
		s.lastReadWasNull = false
		return bytesToInt64(bys)
	}
	s.lastReadWasNull = true
	return 0
}

func (s *IoTDBRpcDataSet) hasCachedResults() bool {
	return (s.queryDataSet != nil && len(s.queryDataSet.Time) > 0)
}

func (s *IoTDBRpcDataSet) next() (bool, error) {
	if s.hasCachedResults() {
		s.constructOneRow()
		return true, nil
	}
	if s.emptyResultSet {
		return false, nil
	}

	r, err := s.fetchResults()
	if err == nil && r {
		s.constructOneRow()
		return true, nil
	}
	return false, nil
}

func (s *IoTDBRpcDataSet) fetchResults() (bool, error) {
	s.rowsIndex = 0
	req := rpc.TSFetchResultsReq{s.sessionId, s.sql, s.fetchSize, s.queryId, true}
	resp, err := s.client.FetchResults(context.Background(), &req)
	if err != nil {
		return false, err
	}
	if !resp.HasResultSet {
		s.emptyResultSet = true
	} else {
		s.queryDataSet = resp.GetQueryDataSet()
	}
	return resp.HasResultSet, nil
}

func (s *IoTDBRpcDataSet) Close() error {
	//it doesn't have any resources to close
	return nil
}

func NewIoTDBRpcDataSet(sql string, columnNameList []string, columnTypes []string,
	columnNameIndex map[string]int32,
	queryId int64, client *rpc.TSIServiceClient, sessionId int64, queryDataSet *rpc.TSQueryDataSet,
	ignoreTimeStamp bool, fetchSize int32) *IoTDBRpcDataSet {

	ds := &IoTDBRpcDataSet{
		sql:             sql,
		columnNameList:  columnNameList,
		ignoreTimeStamp: ignoreTimeStamp,
		queryId:         queryId,
		client:          client,
		sessionId:       sessionId,
		queryDataSet:    queryDataSet,
		fetchSize:       fetchSize,
		currentBitmap:   make([]byte, len(columnNameList)),
		values:          make([][]byte, len(columnTypes)),
		columnCount:     len(columnNameList),
	}

	ds.columnTypeList = make([]TSDataType, 0)

	// deduplicate and map
	ds.columnOrdinalMap = make(map[string]int32)
	if !ignoreTimeStamp {
		ds.columnOrdinalMap[TimestampColumnName] = 1
	}

	if columnNameIndex != nil {
		ds.columnTypeDeduplicatedList = make([]TSDataType, len(columnNameIndex))
		for i, name := range columnNameList {
			columnTypeString := columnTypes[i]
			columnDataType := tsTypeMap[columnTypeString]
			ds.columnTypeList = append(ds.columnTypeList, columnDataType)
			if _, exists := ds.columnOrdinalMap[name]; !exists {
				index := columnNameIndex[name]
				ds.columnOrdinalMap[name] = index + startIndex
				ds.columnTypeDeduplicatedList[index] = tsTypeMap[columnTypeString]
			}
		}
	} else {
		ds.columnTypeDeduplicatedList = make([]TSDataType, ds.columnCount)
		index := startIndex
		for i := 0; i < len(columnNameList); i++ {
			name := columnNameList[i]
			dataType := tsTypeMap[columnTypes[i]]
			ds.columnTypeList = append(ds.columnTypeList, dataType)
			ds.columnTypeDeduplicatedList[i] = dataType
			if _, exists := ds.columnOrdinalMap[name]; !exists {
				ds.columnOrdinalMap[name] = int32(index)
				index++
			}
		}
	}
	return ds
}
