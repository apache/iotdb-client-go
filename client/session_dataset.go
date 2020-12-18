/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
	"errors"
	"github.com/apache/iotdb-client-go/rpc"
)

type SessionDataSet struct {
	Sql             string
	ColumnNameList  []string
	ColumnTypeList  []string
	ColumnNameIndex map[string]int32
	QueryId         int64
	SessionId       int64
	IgnoreTimeStamp bool
	Client          *rpc.TSIServiceClient
	QueryDataSet    *rpc.TSQueryDataSet
	ioTDBRpcDataSet *IoTDBRpcDataSet
}

func NewSessionDataSet(dataSet *SessionDataSet) *SessionDataSet {
	var sessionDataSet = SessionDataSet{
		Sql:             dataSet.Sql,
		ColumnNameList:  dataSet.ColumnNameList,
		ColumnTypeList:  dataSet.ColumnTypeList,
		ColumnNameIndex: dataSet.ColumnNameIndex,
		QueryId:         dataSet.QueryId,
		SessionId:       dataSet.SessionId,
		IgnoreTimeStamp: dataSet.IgnoreTimeStamp,
		Client:          dataSet.Client,
		QueryDataSet:    dataSet.QueryDataSet,
	}
	sessionDataSet.ioTDBRpcDataSet = NewIoTDBRpcDataSet(&sessionDataSet, DefaultFetchSize)
	return &sessionDataSet
}

func (s *SessionDataSet) getFetchSize() int32 {

	return s.ioTDBRpcDataSet.FetchSize
}

func (s *SessionDataSet) setFetchSize(fetchSize int32) {
	s.ioTDBRpcDataSet.FetchSize = fetchSize
}

func (s *SessionDataSet) GetColumnNames() []string {
	return s.ioTDBRpcDataSet.ColumnNameList
}

func (s *SessionDataSet) GetColumnTypes() []string {
	return s.ioTDBRpcDataSet.ColumnTypeList
}

func (s *SessionDataSet) HasNext() bool {
	return s.ioTDBRpcDataSet.next()
}

func (s *SessionDataSet) Next() (*RowRecord, error) {
	if !s.ioTDBRpcDataSet.HasCachedRecord && !s.HasNext() {
		return nil, nil
	}
	s.ioTDBRpcDataSet.HasCachedRecord = false
	return s.constructRowRecordFromValueArray()
}

func (s *SessionDataSet) constructRowRecordFromValueArray() (*RowRecord, error) {
	var outFields []Field
	var err error
	for i := 0; i < s.ioTDBRpcDataSet.getColumnSize(); i++ {
		var field Field
		var index = i + 1
		var datasetColumnIndex = i + StarIndex
		if s.ioTDBRpcDataSet.IgnoreTimeStamp {
			index--
			datasetColumnIndex--
		}
		var loc = s.ioTDBRpcDataSet.ColumnOrdinalMap[s.ioTDBRpcDataSet.ColumnNameList[index]] - StarIndex
		dataSetIsNil, err := s.ioTDBRpcDataSet.isNil(datasetColumnIndex)
		if err != nil {
			return nil, err
		}
		if !dataSetIsNil {
			valueBytes := s.ioTDBRpcDataSet.Values[loc]
			dataType := s.ioTDBRpcDataSet.ColumnTypeDeduplicatedList[loc]
			bytesBuffer := bytes.NewBuffer(valueBytes)
			field = NewField(dataType)
			switch dataType {
			case "BOOLEAN":
				var booleanValue bool
				binary.Read(bytesBuffer, binary.BigEndian, &booleanValue)
				field.SetBoolV(booleanValue)
				break
			case "INT32":
				var intValue int32
				binary.Read(bytesBuffer, binary.BigEndian, &intValue)
				field.SetIntV(intValue)
				break
			case "INT64":
				var longValue int64
				binary.Read(bytesBuffer, binary.BigEndian, &longValue)
				field.SetLongV(longValue)
				break
			case "FLOAT":
				var floatValue float32
				binary.Read(bytesBuffer, binary.BigEndian, &floatValue)
				field.SetFloatV(floatValue)
				break
			case "DOUBLE":
				var doubleValue float64
				binary.Read(bytesBuffer, binary.BigEndian, &doubleValue)
				field.SetDoubleV(doubleValue)
				break
			case "TEXT":
				field.SetBinaryV(valueBytes)
				break
			default:
				return nil, errors.New("unsupported data type " + dataType)
			}
		} else {
			field = NewField("")
		}
		outFields = append(outFields, field)
	}
	bytesBuffer := bytes.NewBuffer(s.ioTDBRpcDataSet.time)
	var timeStamp int64
	binary.Read(bytesBuffer, binary.BigEndian, &timeStamp)
	return &RowRecord{
		Timestamp: timeStamp,
		Fields:    outFields,
	}, err
}

func (s *SessionDataSet) CloseOperationHandle() {
	s.ioTDBRpcDataSet.close()
}
