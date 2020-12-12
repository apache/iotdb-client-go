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

import "github.com/apache/iotdb-client-go/rpc"

const (TimestampColumnName = "Time")

type SessionDataSet struct {
	ioTDBRpcDataSet *IoTDBRpcDataSet
}

func (s *SessionDataSet) Next() (bool, error) {
	return s.ioTDBRpcDataSet.next()
}

func (s *SessionDataSet) GetText(columnName string) string {
	return s.ioTDBRpcDataSet.getText(columnName)
}

func (s *SessionDataSet) GetBool(columnName string) bool {
	return s.ioTDBRpcDataSet.getBool(columnName)
}

func (s *SessionDataSet) Scan(dest ...interface{}) error {
	return s.ioTDBRpcDataSet.scan(dest...)
}

func (s *SessionDataSet) GetFloat(columnName string) float32 {
	return s.ioTDBRpcDataSet.getFloat(columnName)
}

func (s *SessionDataSet) GetDouble(columnName string) float64 {
	return s.ioTDBRpcDataSet.getDouble(columnName)
}

func (s *SessionDataSet) GetInt32(columnName string) int32 {
	return s.ioTDBRpcDataSet.getInt32(columnName)
}

func (s *SessionDataSet) GetInt64(columnName string) int64 {
	return s.ioTDBRpcDataSet.getInt64(columnName)
}

func (s *SessionDataSet) GetTimestamp() int64 {
	return s.ioTDBRpcDataSet.GetTimestamp()
}

func (s *SessionDataSet) GetValue(columnName string) interface{} {
	return s.ioTDBRpcDataSet.getValue(columnName)
}

func (s *SessionDataSet) GetRowRecord() *RowRecord {
	return s.ioTDBRpcDataSet.getRowRecord()
}

func (s *SessionDataSet) GetColumnCount() int {
	return s.ioTDBRpcDataSet.columnCount
}

func (s *SessionDataSet) GetColumnDataType(columnIndex int) TSDataType {
	return s.ioTDBRpcDataSet.columnTypeList[columnIndex]
}

func (s *SessionDataSet) GetColumnName(columnIndex int) string {
	return s.ioTDBRpcDataSet.columnNameList[columnIndex]
}

func (s *SessionDataSet) IsIgnoreTimeStamp() bool {
	return s.ioTDBRpcDataSet.ignoreTimeStamp
}

func (s *SessionDataSet) Close() error {
	return s.ioTDBRpcDataSet.Close()
}

func NewSessionDataSet(sql string, columnNameList []string, columnTypeList []string,
	columnNameIndex map[string]int32,
	queryId int64, client *rpc.TSIServiceClient, sessionId int64, queryDataSet *rpc.TSQueryDataSet,
	ignoreTimeStamp bool, fetchSize int32) *SessionDataSet {

	return &SessionDataSet{
		ioTDBRpcDataSet: NewIoTDBRpcDataSet(sql, columnNameList, columnTypeList,
			columnNameIndex,
			queryId, client, sessionId, queryDataSet,
			ignoreTimeStamp, fetchSize),
	}
}
