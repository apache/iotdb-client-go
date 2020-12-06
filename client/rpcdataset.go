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
	TIMESTAMP_STR = "Time"
	START_INDEX   = 2
	FLAG          = 0x80
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
	columnTypeList             []int32
	columnOrdinalMap           map[string]int32
	columnTypeDeduplicatedList []int32
	columnNameIndexMap         map[string]int32
	columnTypeMap              map[string]int32
	currentBitmap              []byte
	time                       []byte
	value                      [][]byte
	client                     *rpc.TSIServiceClient
	emptyResultSet             bool
	ignoreTimeStamp            bool
}

func (s *IoTDBRpcDataSet) isNull(columnIndex int, rowIndex int) bool {
	bitmap := s.currentBitmap[columnIndex]
	shift := rowIndex % 8
	return ((FLAG >> shift) & (bitmap & 0xff)) == 0
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
				s.value[i] = valueBuffer[:1]
				s.queryDataSet.ValueList[i] = valueBuffer[1:]
				break
			case INT32:
				s.value[i] = valueBuffer[:4]
				s.queryDataSet.ValueList[i] = valueBuffer[4:]
				break
			case INT64:
				s.value[i] = valueBuffer[:8]
				s.queryDataSet.ValueList[i] = valueBuffer[8:]
				break
			case FLOAT:
				s.value[i] = valueBuffer[:4]
				s.queryDataSet.ValueList[i] = valueBuffer[4:]
				break
			case DOUBLE:
				s.value[i] = valueBuffer[:8]
				s.queryDataSet.ValueList[i] = valueBuffer[8:]
				break
			case TEXT:
				length := bytesToInt32(valueBuffer[:4])
				s.value[i] = valueBuffer[4 : 4+length]
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
	if columnName == TIMESTAMP_STR {
		return time.Unix(0, bytesToInt64(s.time)*1000000).Format(time.RFC3339)
	}

	index := s.columnOrdinalMap[columnName] - START_INDEX
	if index < 0 || int(index) >= len(s.value) || s.isNull(int(index), s.rowsIndex-1) {
		s.lastReadWasNull = true
		return ""
	}
	s.lastReadWasNull = false
	return s.getString(int(index), s.columnTypeDeduplicatedList[index])
}

func (s *IoTDBRpcDataSet) getString(index int, dataType int32) string {
	switch dataType {
	case BOOLEAN:
		if s.value[index][0] != 0 {
			return "true"
		}
		return "false"
	case INT32:
		return fmt.Sprintf("%v", bytesToInt32(s.value[index]))
	case INT64:
		return fmt.Sprintf("%v", bytesToInt64(s.value[index]))
	case FLOAT:
		bits := binary.BigEndian.Uint32(s.value[index])
		return fmt.Sprintf("%v", math.Float32frombits(bits))
	case DOUBLE:
		bits := binary.BigEndian.Uint64(s.value[index])
		return fmt.Sprintf("%v", math.Float64frombits(bits))
	case TEXT:
		return string(s.value[index])
	default:
		return ""
	}
}

func (s *IoTDBRpcDataSet) getValue(columnName string) interface{} {
	index := int(s.columnOrdinalMap[columnName] - START_INDEX)
	if s.isNull(index, s.rowsIndex-1) {
		return nil
	}
	dataType := s.columnTypeMap[columnName]
	switch dataType {
	case BOOLEAN:
		return bool(s.value[index][0] != 0)
	case INT32:
		return bytesToInt32(s.value[index])
	case INT64:
		return bytesToInt64(s.value[index])
	case FLOAT:
		bits := binary.BigEndian.Uint32(s.value[index])
		return math.Float32frombits(bits)
	case DOUBLE:
		bits := binary.BigEndian.Uint64(s.value[index])
		return math.Float64frombits(bits)
	case TEXT:
		return string(s.value[index])
	default:
		return nil
	}
}

func (s *IoTDBRpcDataSet) GetRowRecord() *RowRecord {
	fields := make([]*Field, s.columnCount)
	for i := 0; i < s.columnCount; i++ {
		columnName := s.columnNameList[i]
		field := Field{
			name:     columnName,
			dataType: s.columnTypeMap[columnName],
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
	index := s.columnOrdinalMap[columnName] - START_INDEX
	if !s.isNull(int(index), s.rowsIndex-1) {
		s.lastReadWasNull = false
		return s.value[index][0] != 0
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
		index := int(s.columnOrdinalMap[columnName] - START_INDEX)
		if s.isNull(index, s.rowsIndex-1) {
			continue
		}

		dataType := s.columnTypeMap[columnName]
		d := dest[i]

		switch dataType {
		case BOOLEAN:
			switch t := d.(type) {
			case *bool:
				*t = bool(s.value[index][0] != 0)
			case *string:
				if s.value[index][0] != 0 {
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
				*t = bytesToInt32(s.value[index])
			case *string:
				*t = strconv.FormatInt(int64(bytesToInt32(s.value[index])), 10)
			default:
				return fmt.Errorf("dest[%d] types must be *int32 or *string", i)
			}
		case INT64:
			switch t := d.(type) {
			case *int64:
				*t = bytesToInt64(s.value[index])
			case *string:
				*t = strconv.FormatInt(bytesToInt64(s.value[index]), 10)
			default:
				return fmt.Errorf("dest[%d] types must be *int64 or *string", i)
			}
		case FLOAT:
			switch t := d.(type) {
			case *float32:
				bits := binary.BigEndian.Uint32(s.value[index])
				*t = math.Float32frombits(bits)
			case *string:
				bits := binary.BigEndian.Uint32(s.value[index])
				*t = fmt.Sprintf("%v", math.Float32frombits(bits))
			default:
				return fmt.Errorf("dest[%d] types must be *float32 or *string", i)
			}
		case DOUBLE:
			switch t := d.(type) {
			case *float64:
				bits := binary.BigEndian.Uint64(s.value[index])
				*t = math.Float64frombits(bits)
			case *string:
				bits := binary.BigEndian.Uint64(s.value[index])
				*t = fmt.Sprintf("%v", math.Float64frombits(bits))
			default:
				return fmt.Errorf("dest[%d] types must be *float64 or *string", i)
			}
		case TEXT:
			switch t := d.(type) {
			case *string:
				*t = string(s.value[index])
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
	index := s.columnOrdinalMap[columnName] - START_INDEX
	if !s.isNull(int(index), s.rowsIndex-1) {
		s.lastReadWasNull = false
		bits := binary.BigEndian.Uint32(s.value[index])
		return math.Float32frombits(bits)
	}
	s.lastReadWasNull = true
	return 0
}

func (s *IoTDBRpcDataSet) getDouble(columnName string) float64 {
	index := s.columnOrdinalMap[columnName] - START_INDEX

	if !s.isNull(int(index), s.rowsIndex-1) {
		s.lastReadWasNull = false
		bits := binary.BigEndian.Uint64(s.value[index])
		return math.Float64frombits(bits)
	}
	s.lastReadWasNull = true
	return 0
}

func (s *IoTDBRpcDataSet) getInt32(columnName string) int32 {
	index := s.columnOrdinalMap[columnName] - START_INDEX
	if !s.isNull(int(index), s.rowsIndex-1) {
		s.lastReadWasNull = false
		return bytesToInt32(s.value[index])
	}

	s.lastReadWasNull = true
	return 0
}

func (s *IoTDBRpcDataSet) getInt64(columnName string) int64 {
	if columnName == TIMESTAMP_STR {
		return bytesToInt64(s.time)
	}

	index := s.columnOrdinalMap[columnName] - START_INDEX
	bys := s.value[index]

	if !s.isNull(int(index), s.rowsIndex-1) {
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
	//   RpcUtils.verifySuccess(resp.getStatus());
	if !resp.HasResultSet {
		s.emptyResultSet = true
	} else {
		s.queryDataSet = resp.GetQueryDataSet()
	}
	return resp.HasResultSet, nil
}

func NewIoTDBRpcDataSet(sql string, columnNameList []string, columnTypes []string,
	columnNameIndex map[string]int32,
	queryId int64, client *rpc.TSIServiceClient, sessionId int64, queryDataSet *rpc.TSQueryDataSet,
	ignoreTimeStamp bool) *IoTDBRpcDataSet {
	typeMap := map[string]int32{
		"BOOLEAN": BOOLEAN,
		"INT32":   INT32,
		"INT64":   INT64,
		"FLOAT":   FLOAT,
		"DOUBLE":  DOUBLE,
		"TEXT":    TEXT,
	}

	ds := &IoTDBRpcDataSet{
		sql:                sql,
		columnNameList:     columnNameList,
		columnNameIndexMap: columnNameIndex,
		ignoreTimeStamp:    ignoreTimeStamp,
		queryId:            queryId,
		client:             client,
		sessionId:          sessionId,
		queryDataSet:       queryDataSet,
		fetchSize:          1024,
		currentBitmap:      make([]byte, len(columnNameList)),
		value:              make([][]byte, len(columnTypes)),
		columnCount:        len(columnNameList),
	}

	ds.columnTypeList = make([]int32, 0)
	ds.columnTypeMap = make(map[string]int32)

	// deduplicate and map
	ds.columnOrdinalMap = make(map[string]int32)
	if !ignoreTimeStamp {
		ds.columnOrdinalMap[TIMESTAMP_STR] = 1
	}

	if columnNameIndex != nil {
		ds.columnTypeDeduplicatedList = make([]int32, len(columnNameIndex))
		for i, name := range columnNameList {
			columnTypeString := columnTypes[i]
			columnDataType := typeMap[columnTypeString]
			ds.columnTypeMap[name] = columnDataType
			ds.columnTypeList = append(ds.columnTypeList, columnDataType)
			if _, exists := ds.columnOrdinalMap[name]; !exists {
				index := columnNameIndex[name]
				ds.columnOrdinalMap[name] = index + START_INDEX
				ds.columnTypeDeduplicatedList[index] = typeMap[columnTypeString]
			}
		}
	} else {
		ds.columnTypeDeduplicatedList = make([]int32, ds.columnCount)
		index := START_INDEX
		for i := 0; i < len(columnNameList); i++ {
			name := columnNameList[i]
			dataType := typeMap[columnTypes[i]]
			ds.columnTypeList = append(ds.columnTypeList, dataType)
			ds.columnTypeMap[name] = dataType
			ds.columnTypeDeduplicatedList[i] = dataType
			if _, exists := ds.columnOrdinalMap[name]; !exists {
				ds.columnOrdinalMap[name] = int32(index)
				index++
			}
		}
	}
	return ds
}
