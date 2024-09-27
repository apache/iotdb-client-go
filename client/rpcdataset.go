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
	"errors"
	"fmt"
	"github.com/apache/iotdb-client-go/common"
	"math"
	"time"

	"github.com/apache/iotdb-client-go/rpc"
)

const (
	startIndex = 2
	flag       = 0x80
)

var (
	errClosed error                 = errors.New("DataSet is Closed")
	tsTypeMap map[string]TSDataType = map[string]TSDataType{
		"BOOLEAN":   BOOLEAN,
		"INT32":     INT32,
		"INT64":     INT64,
		"FLOAT":     FLOAT,
		"DOUBLE":    DOUBLE,
		"TEXT":      TEXT,
		"TIMESTAMP": TIMESTAMP,
		"DATE":      DATE,
		"BLOB":      BLOB,
		"STRING":    STRING,
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
	client                     *rpc.IClientRPCServiceClient
	emptyResultSet             bool
	ignoreTimeStamp            bool
	closed                     bool
	timeoutMs                  *int64
}

func (s *IoTDBRpcDataSet) getColumnIndex(columnName string) int32 {
	if s.closed {
		return -1
	}
	return s.columnOrdinalMap[columnName] - startIndex
}

func (s *IoTDBRpcDataSet) getColumnType(columnName string) TSDataType {
	if s.closed {
		return UNKNOWN
	}
	return s.columnTypeDeduplicatedList[s.getColumnIndex(columnName)]
}

func (s *IoTDBRpcDataSet) isNull(columnIndex int, rowIndex int) bool {
	if s.closed {
		return true
	}
	bitmap := s.currentBitmap[columnIndex]
	shift := rowIndex % 8
	return ((flag >> shift) & (bitmap & 0xff)) == 0
}

func (s *IoTDBRpcDataSet) constructOneRow() error {
	if s.closed {
		return errClosed
	}

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
			case INT32, DATE:
				s.values[i] = valueBuffer[:4]
				s.queryDataSet.ValueList[i] = valueBuffer[4:]
			case INT64, TIMESTAMP:
				s.values[i] = valueBuffer[:8]
				s.queryDataSet.ValueList[i] = valueBuffer[8:]
			case FLOAT:
				s.values[i] = valueBuffer[:4]
				s.queryDataSet.ValueList[i] = valueBuffer[4:]
			case DOUBLE:
				s.values[i] = valueBuffer[:8]
				s.queryDataSet.ValueList[i] = valueBuffer[8:]
			case TEXT, BLOB, STRING:
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
	if s.closed {
		return -1
	}
	return bytesToInt64(s.time)
}

func (s *IoTDBRpcDataSet) getText(columnName string) string {
	if s.closed {
		return ""
	}
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
	if s.closed {
		return ""
	}
	valueBytes := s.values[columnIndex]
	switch dataType {
	case BOOLEAN:
		if valueBytes[0] != 0 {
			return "true"
		}
		return "false"
	case INT32:
		return int32ToString(bytesToInt32(valueBytes))
	case INT64, TIMESTAMP:
		return int64ToString(bytesToInt64(valueBytes))
	case FLOAT:
		bits := binary.BigEndian.Uint32(valueBytes)
		return float32ToString(math.Float32frombits(bits))
	case DOUBLE:
		bits := binary.BigEndian.Uint64(valueBytes)
		return float64ToString(math.Float64frombits(bits))
	case TEXT, STRING:
		return string(valueBytes)
	case BLOB:
		return bytesToHexString(valueBytes)
	case DATE:
		date, err := bytesToDate(valueBytes)
		if err != nil {
			return ""
		}
		return date.Format(time.DateOnly)
	default:
		return ""
	}
}

func (s *IoTDBRpcDataSet) getValue(columnName string) interface{} {
	if s.closed {
		return nil
	}
	columnIndex := int(s.getColumnIndex(columnName))
	if s.isNull(columnIndex, s.rowsIndex-1) {
		return nil
	}

	dataType := s.getColumnType(columnName)
	valueBytes := s.values[columnIndex]
	switch dataType {
	case BOOLEAN:
		return valueBytes[0] != 0
	case INT32:
		return bytesToInt32(valueBytes)
	case INT64, TIMESTAMP:
		return bytesToInt64(valueBytes)
	case FLOAT:
		bits := binary.BigEndian.Uint32(valueBytes)
		return math.Float32frombits(bits)
	case DOUBLE:
		bits := binary.BigEndian.Uint64(valueBytes)
		return math.Float64frombits(bits)
	case TEXT, STRING:
		return string(valueBytes)
	case BLOB:
		return valueBytes
	case DATE:
		date, err := bytesToDate(valueBytes)
		if err != nil {
			return nil
		}
		return date
	default:
		return nil
	}
}

func (s *IoTDBRpcDataSet) getRowRecord() (*RowRecord, error) {
	if s.closed {
		return nil, errClosed
	}

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
	}, nil
}

func (s *IoTDBRpcDataSet) getBool(columnName string) bool {
	if s.closed {
		return false
	}
	columnIndex := s.getColumnIndex(columnName)
	if !s.isNull(int(columnIndex), s.rowsIndex-1) {
		return s.values[columnIndex][0] != 0
	}
	s.lastReadWasNull = true
	return false
}

func (s *IoTDBRpcDataSet) scan(dest ...interface{}) error {
	if s.closed {
		return errClosed
	}

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
				*t = valueBytes[0] != 0
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
				*t = int32ToString(bytesToInt32(valueBytes))
			default:
				return fmt.Errorf("dest[%d] types must be *int32 or *string", i)
			}
		case INT64, TIMESTAMP:
			switch t := d.(type) {
			case *int64:
				*t = bytesToInt64(valueBytes)
			case *string:
				*t = int64ToString(bytesToInt64(valueBytes))
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
				*t = float32ToString(math.Float32frombits(bits))
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
				*t = float64ToString(math.Float64frombits(bits))
			default:
				return fmt.Errorf("dest[%d] types must be *float64 or *string", i)
			}
		case TEXT, STRING:
			switch t := d.(type) {
			case *[]byte:
				*t = valueBytes
			case *string:
				*t = string(valueBytes)
			default:
				return fmt.Errorf("dest[%d] types must be *[]byte or *string", i)
			}
		case BLOB:
			switch t := d.(type) {
			case *[]byte:
				*t = valueBytes
			case *string:
				*t = bytesToHexString(valueBytes)
			default:
				return fmt.Errorf("dest[%d] types must be *[]byte or *string", i)
			}
		case DATE:
			switch t := d.(type) {
			case *time.Time:
				*t, _ = bytesToDate(valueBytes)
			case *string:
				*t = int32ToString(bytesToInt32(valueBytes))
				date, err := bytesToDate(valueBytes)
				if err != nil {
					*t = ""
				}
				*t = date.Format(time.DateOnly)
			default:
				return fmt.Errorf("dest[%d] types must be *time.Time or *string", i)
			}
		default:
			return nil
		}
	}
	return nil
}

func (s *IoTDBRpcDataSet) getFloat(columnName string) float32 {
	if s.closed {
		return 0
	}
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
	if s.closed {
		return 0
	}
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
	if s.closed {
		return 0
	}
	columnIndex := s.getColumnIndex(columnName)
	if !s.isNull(int(columnIndex), s.rowsIndex-1) {
		s.lastReadWasNull = false
		return bytesToInt32(s.values[columnIndex])
	}

	s.lastReadWasNull = true
	return 0
}

func (s *IoTDBRpcDataSet) getInt64(columnName string) int64 {
	if s.closed {
		return 0
	}
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
	if s.closed {
		return false
	}
	return s.queryDataSet != nil && len(s.queryDataSet.Time) > 0
}

func (s *IoTDBRpcDataSet) next() (bool, error) {
	if s.closed {
		return false, errClosed
	}

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
	if s.closed {
		return false, errClosed
	}
	s.rowsIndex = 0
	req := rpc.TSFetchResultsReq{
		SessionId: s.sessionId,
		Statement: s.sql,
		FetchSize: s.fetchSize,
		QueryId:   s.queryId,
		IsAlign:   true,
		Timeout:   s.timeoutMs,
	}
	resp, err := s.client.FetchResults(context.Background(), &req)

	if err != nil {
		return false, err
	}

	if err = VerifySuccess(resp.Status); err != nil {
		return false, err
	}

	if !resp.HasResultSet {
		s.emptyResultSet = true
	} else {
		s.queryDataSet = resp.GetQueryDataSet()
	}
	return resp.HasResultSet, nil
}

func (s *IoTDBRpcDataSet) IsClosed() bool {
	return s.closed
}

func (s *IoTDBRpcDataSet) Close() (err error) {
	if s.IsClosed() {
		return nil
	}
	if s.client != nil {
		closeRequest := &rpc.TSCloseOperationReq{
			SessionId: s.sessionId,
			QueryId:   &s.queryId,
		}

		var status *common.TSStatus
		status, err = s.client.CloseOperation(context.Background(), closeRequest)
		if err == nil {
			err = VerifySuccess(status)
		}
	}

	s.columnCount = 0
	s.sessionId = -1
	s.queryId = -1
	s.rowsIndex = -1
	s.queryDataSet = nil
	s.sql = ""
	s.fetchSize = 0
	s.columnNameList = nil
	s.columnTypeList = nil
	s.columnOrdinalMap = nil
	s.columnTypeDeduplicatedList = nil
	s.currentBitmap = nil
	s.time = nil
	s.values = nil
	s.client = nil
	s.emptyResultSet = true
	s.closed = true
	return err
}

func NewIoTDBRpcDataSet(sql string, columnNameList []string, columnTypes []string,
	columnNameIndex map[string]int32,
	queryId int64, client *rpc.IClientRPCServiceClient, sessionId int64, queryDataSet *rpc.TSQueryDataSet,
	ignoreTimeStamp bool, fetchSize int32, timeoutMs *int64) *IoTDBRpcDataSet {

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
		closed:          false,
		timeoutMs:       timeoutMs,
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
