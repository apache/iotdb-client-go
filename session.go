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

package iotdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"github.com/apache/iotdb-client-go/rpc"
	"github.com/apache/thrift/lib/go/thrift"
	"net"
	"strconv"
	"time"
)

const protocolVersion = rpc.TSProtocolVersion_IOTDB_SERVICE_PROTOCOL_V3

var lengthError = errors.New("deviceIds, times, measurementsList and valuesList's size should be equal")

func (s *Session) Open(enableRPCCompression bool, connectionTimeoutInMs int) error {
	var protocolFactory thrift.TProtocolFactory
	var err error
	s.trans, err = thrift.NewTSocketTimeout(net.JoinHostPort(s.config.Host, s.config.Port), time.Duration(connectionTimeoutInMs))
	if err != nil {
		return err
	}
	s.trans = thrift.NewTFramedTransport(s.trans)
	if !s.trans.IsOpen() {
		err = s.trans.Open()
		if err != nil {
			return err
		}
	}
	if enableRPCCompression {
		protocolFactory = thrift.NewTCompactProtocolFactory()
	} else {
		protocolFactory = thrift.NewTBinaryProtocolFactoryDefault()
	}
	iProtocol := protocolFactory.GetProtocol(s.trans)
	oProtocol := protocolFactory.GetProtocol(s.trans)
	s.client = rpc.NewTSIServiceClient(thrift.NewTStandardClient(iProtocol, oProtocol))
	tSOpenSessionReq := rpc.TSOpenSessionReq{
		ClientProtocol: protocolVersion,
		ZoneId: s.config.ZoneId,
		Username: &s.config.User,
		Password: &s.config.Passwd,
	}
	tSOpenSessionResp, err := s.client.OpenSession(context.Background(), &tSOpenSessionReq)
	if err != nil {
		return err
	}
	s.sessionId = tSOpenSessionResp.GetSessionId()
	s.requestStatementId, err = s.client.RequestStatementId(context.Background(), s.sessionId)
	if err != nil {
		return err
	}
	s.SetTimeZone(s.config.ZoneId)
	return err
}

func (s *Session) CheckTimeseriesExists(path string) bool {
	dataSet, _ := s.ExecuteQueryStatement("SHOW TIMESERIES " + path)
	result := dataSet.HasNext()
	dataSet.CloseOperationHandle()
	return result
}

func (s *Session) Close() error {
	tSCloseSessionReq := rpc.NewTSCloseSessionReq()
	tSCloseSessionReq.SessionId = s.sessionId
	status, err := s.client.CloseSession(context.Background(), tSCloseSessionReq)
	s.trans.Close()
	if err != nil {
		return err
	}
	err = verifySuccess(status)
	if err != nil {
		return err
	}
	return nil
}

/*
 *set one storage group
 *
 *param
 *storageGroupId: string, storage group name (starts from root)
 *
 */
func (s *Session) SetStorageGroup(storageGroupId string) error {
	status, err := s.client.SetStorageGroup(context.Background(), s.sessionId, storageGroupId)
	if err != nil {
		return err
	}
	err = verifySuccess(status)
	if err != nil {
		return err
	}
	return nil
}

/*
 *delete one storage group
 *
 *param
 *storageGroupId: string, storage group name (starts from root)
 *
 */
func (s *Session) DeleteStorageGroup(storageGroupId string) error {
	status, err := s.client.DeleteStorageGroups(context.Background(), s.sessionId, []string{storageGroupId})
	if err != nil {
		return err
	}
	err = verifySuccess(status)
	if err != nil {
		return err
	}
	return nil
}

/*
 *delete multiple storage group
 *
 *param
 *storageGroupIds: []string, paths of the target storage groups
 *
 */
func (s *Session) DeleteStorageGroups(storageGroupIds []string) error {
	status, err := s.client.DeleteStorageGroups(context.Background(), s.sessionId, storageGroupIds)
	if err != nil {
		return err
	}
	err = verifySuccess(status)
	if err != nil {
		return err
	}
	return nil
}

/*
 *create single time series
 *
 *params
 *path: string, complete time series path (starts from root)
 *dataType: int32, data type for this time series
 *encoding: int32, data type for this time series
 *compressor: int32, compressing type for this time series
 *
 */
func (s *Session) CreateTimeseries(path string, dataType int32, encoding int32, compressor int32) error {
	request := rpc.TSCreateTimeseriesReq{
		SessionId: s.sessionId,
		Path: path, DataType: dataType,
		Encoding: encoding,
		Compressor: compressor,
	}
	status, err := s.client.CreateTimeseries(context.Background(), &request)
	if err != nil {
		return err
	}
	err = verifySuccess(status)
	if err != nil {
		return err
	}
	return nil
}

/*
 *create multiple time series
 *
 *params
 *paths: []string, complete time series paths (starts from root)
 *dataTypes: []int32, data types for time series
 *encodings: []int32, encodings for time series
 *compressors: []int32, compressing types for time series
 *
 */
func (s *Session) CreateMultiTimeseries(paths []string, dataTypes []int32, encodings []int32, compressors []int32) error {
	request := rpc.TSCreateMultiTimeseriesReq{
		SessionId: s.sessionId,
		Paths: paths,
		DataTypes: dataTypes,
		Encodings: encodings,
		Compressors: compressors,
	}
	status, err := s.client.CreateMultiTimeseries(context.Background(), &request)
	if err != nil {
		return err
	}
	err = verifySuccess(status)
	if err != nil {
		return err
	}
	return nil
}

/*
 *delete multiple time series, including data and schema
 *
 *params
 *paths: []string, time series paths, which should be complete (starts from root)
 *
 */
func (s *Session) DeleteTimeseries(paths []string) error {
	status, err := s.client.DeleteTimeseries(context.Background(), s.sessionId, paths)
	if err != nil {
		return err
	}
	err = verifySuccess(status)
	if err != nil {
		return err
	}
	return nil
}

/*
 *delete all startTime <= data <= endTime in multiple time series
 *
 *params
 *paths: []string, time series array that the data in
 *startTime: int64, start time of deletion range
 *endTime: int64, end time of deletion range
 *
 */
func (s *Session) DeleteData(paths []string, startTime int64, endTime int64) error {
	request := rpc.TSDeleteDataReq{SessionId: s.sessionId,
		Paths: paths,
		StartTime: startTime,
		EndTime: endTime,
	}
	status, err := s.client.DeleteData(context.Background(), &request)
	if err != nil {
		return err
	}
	err = verifySuccess(status)
	if err != nil {
		return err
	}
	return nil
}

/*
 *special case for inserting one row of String (TEXT) value
 *
 *params
 *deviceId: string, time series path for device
 *measurements: []string, sensor names
 *values: []string, values to be inserted, for each sensor
 *timestamp: int64, indicate the timestamp of the row of data
 *
 */
func (s *Session) InsertStringRecord(deviceId string, measurements []string, values []string, timestamp int64) error {
	request := rpc.TSInsertStringRecordReq{
		SessionId: s.sessionId,
		DeviceId: deviceId,
		Measurements: measurements,
		Values: values,
		Timestamp: timestamp,
	}
	status, err := s.client.InsertStringRecord(context.Background(), &request)
	if err != nil {
		return err
	}
	err = verifySuccess(status)
	if err != nil {
		return err
	}
	return nil
}

/**
 * This method NOT insert data into database and the server just return after accept the request,
 * this method should be used to test other time cost in iotdb
 */
func (s *Session) TestInsertStringRecord(deviceId string, measurements []string, values []string, timestamp int64) error {
	request := rpc.TSInsertStringRecordReq{
		SessionId: s.sessionId,
		DeviceId: deviceId,
		Measurements: measurements,
		Values: values,
		Timestamp: timestamp,
	}
	status, err := s.client.TestInsertStringRecord(context.Background(), &request)
	if err != nil {
		return err
	}
	err = verifySuccess(status)
	if err != nil {
		return err
	}
	return nil
}

/*
 *special case for inserting multiple rows of String (TEXT) value
 *
 *params
 *deviceIds: []string, time series paths for device
 *measurements: [][]string, each element of outer list indicates measurements of a device
 *values: [][]interface{}, values to be inserted, for each device
 *timestamps: []int64, timestamps for records
 *
 */
func (s *Session) InsertStringRecords(deviceIds []string, measurements [][]string, values [][]string,
	timestamps []int64) error {
	request := rpc.TSInsertStringRecordsReq{
		SessionId: s.sessionId,
		DeviceIds: deviceIds,
		MeasurementsList: measurements,
		ValuesList: values,
		Timestamps: timestamps,
	}
	status, err := s.client.InsertStringRecords(context.Background(), &request)
	if err != nil {
		return err
	}
	err = verifySuccess(status)
	if err != nil {
		return err
	}
	return nil
}

/**
 * This method NOT insert data into database and the server just return after accept the request,
 * this method should be used to test other time cost in iotdb
 */
func (s *Session) TestInsertStringRecords(deviceIds []string, measurements [][]string, values [][]string,
	timestamps []int64) error {
	request := rpc.TSInsertStringRecordsReq{
		SessionId: s.sessionId,
		DeviceIds: deviceIds,
		MeasurementsList: measurements,
		ValuesList: values,
		Timestamps: timestamps,
	}
	status, err := s.client.TestInsertStringRecords(context.Background(), &request)
	if err != nil {
		return err
	}
	err = verifySuccess(status)
	if err != nil {
		return err
	}
	return nil
}

/*
 *insert one row of record into database, if you want improve your performance, please use insertTablet method
 *
 *params
 *deviceId: string, time series path for device
 *measurements: []string, sensor names
 *dataTypes: []int32, list of dataType, indicate the data type for each sensor
 *values: []interface{}, values to be inserted, for each sensor
 *timestamp: int64, indicate the timestamp of the row of data
 *
 */
func (s *Session) InsertRecord(deviceId string, measurements []string, dataTypes []int32, values []interface{},
	timestamp int64) error {
	request := s.genInsertRecordReq(deviceId, measurements, dataTypes, values, timestamp)
	status, err := s.client.InsertRecord(context.Background(), request)
	if err != nil {
		return err
	}
	err = verifySuccess(status)
	if err != nil {
		return err
	}
	return nil
}

/**
 * This method NOT insert data into database and the server just return after accept the request,
 * this method should be used to test other time cost in iotdb
 */
func (s *Session) TestInsertRecord(deviceId string, measurements []string, dataTypes []int32, values []interface{},
	timestamp int64) error {
	request := s.genInsertRecordReq(deviceId, measurements, dataTypes, values, timestamp)
	status, err := s.client.TestInsertRecord(context.Background(), request)
	if err != nil {
		return err
	}
	err = verifySuccess(status)
	if err != nil {
		return err
	}
	return nil
}

func (s *Session) genInsertRecordReq(deviceId string, measurements []string, dataTypes []int32, values []interface{},
	timestamp int64) *rpc.TSInsertRecordReq {
	request := rpc.TSInsertRecordReq{
		SessionId: s.sessionId,
		DeviceId: deviceId,
		Measurements: measurements,
		Timestamp: timestamp,
	}
	request.Values = valuesToBytes(dataTypes, values)
	return &request
}

/*
 *insert multiple rows of data, records are independent to each other, in other words, there's no relationship
 *between those records
 *
 *params
 *deviceIds: []string, time series paths for device
 *measurements: [][]string, each element of outer list indicates measurements of a device
 *dataTypes: [][]int32, each element of outer list indicates sensor data types of a device
 *values: [][]interface{}, values to be inserted, for each device
 *timestamps: []int64, timestamps for records
 *
 */
func (s *Session) InsertRecords(deviceIds []string, measurements [][]string, dataTypes [][]int32, values [][]interface{},
	timestamps []int64) error {
	request, err := s.genInsertRecordsReq(deviceIds, measurements, dataTypes, values, timestamps)
	if err != nil {
		return err
	} else {
		status, err := s.client.InsertRecords(context.Background(), request)
		if err != nil {
			return err
		}
		err = verifySuccess(status)
		if err != nil {
			return err
		}
		return nil
	}

}

/**
 * This method NOT insert data into database and the server just return after accept the request,
 * this method should be used to test other time cost in iotdb
 */
func (s *Session) TestInsertRecords(deviceIds []string, measurements [][]string, dataTypes [][]int32, values [][]interface{},
	timestamps []int64) error {
	request, err := s.genInsertRecordsReq(deviceIds, measurements, dataTypes, values, timestamps)
	if err != nil {
		return err
	} else {
		status, err := s.client.TestInsertRecords(context.Background(), request)
		if err != nil {
			return err
		}
		err = verifySuccess(status)
		if err != nil {
			return err
		}
		return nil
	}
}

func (s *Session) genInsertRecordsReq(deviceIds []string, measurements [][]string, dataTypes [][]int32, values [][]interface{},
	timestamps []int64) (*rpc.TSInsertRecordsReq, error) {
	length := len(deviceIds)
	if length != len(timestamps) || length != len(measurements) || length != len(values) {
		return nil, lengthError
	}
	request := rpc.TSInsertRecordsReq{
		SessionId: s.sessionId,
		DeviceIds: deviceIds,
		MeasurementsList: measurements,
		Timestamps: timestamps,
	}
	v := make([][]byte, length)
	for i := 0; i < len(measurements); i++ {
		v[i] = valuesToBytes(dataTypes[i], values[i])
	}
	request.ValuesList = v
	return &request, nil
}

/*
 *insert one tablet, in a tablet, for each timestamp, the number of measurements is same
 *
 *params
 *tablet: utils.Tablet, a tablet specified above
 *
 */
func (s *Session) InsertTablet(tablet Tablet) error {
	tablet.SortTablet()
	request := s.genInsertTabletReq(tablet)
	status, err := s.client.InsertTablet(context.Background(), request)
	if err != nil {
		return err
	}
	err = verifySuccess(status)
	if err != nil {
		return err
	}
	return nil
}

/**
 * This method NOT insert data into database and the server just return after accept the request,
 * this method should be used to test other time cost in iotdb
 */
func (s *Session) TestInsertTablet(tablet Tablet) error {
	tablet.SortTablet()
	request := s.genInsertTabletReq(tablet)
	status, err := s.client.TestInsertTablet(context.Background(), request)
	if err != nil {
		return err
	}
	err = verifySuccess(status)
	if err != nil {
		return err
	}
	return nil
}

func (s *Session) genInsertTabletReq(tablet Tablet) *rpc.TSInsertTabletReq {
	request := rpc.TSInsertTabletReq{
		SessionId: s.sessionId,
		DeviceId: tablet.GetDeviceId(),
		Types: tablet.GetTypes(),
		Measurements: tablet.Measurements,
		Values: tablet.GetBinaryValues(),
		Timestamps: tablet.GetBinaryTimestamps(),
		Size: tablet.GetRowNumber(),
	}
	return &request
}

/*
 *insert multiple tablets, tablets are independent to each other
 *
 *params
 *tablets: []utils.Tablet, list of tablets
 *
 */
func (s *Session) InsertTablets(tablets []Tablet) error {
	for index := range tablets {
		tablets[index].SortTablet()
	}
	request := s.genInsertTabletsReq(tablets)
	status, err := s.client.InsertTablets(context.Background(), request)
	if err != nil {
		return err
	}
	err = verifySuccess(status)
	if err != nil {
		return err
	}
	return nil
}

/**
 * This method NOT insert data into database and the server just return after accept the request,
 * this method should be used to test other time cost in iotdb
 */
func (s *Session) TestInsertTablets(tablets []Tablet) error {
	for index := range tablets {
		tablets[index].SortTablet()
	}
	request := s.genInsertTabletsReq(tablets)
	status, err := s.client.TestInsertTablets(context.Background(), request)
	if err != nil {
		return err
	}
	err = verifySuccess(status)
	if err != nil {
		return err
	}
	return nil
}

func (s *Session) genInsertTabletsReq(tablets []Tablet) *rpc.TSInsertTabletsReq {
	var (
		length           = len(tablets)
		deviceIds        = make([]string, length)
		measurementsList = make([][]string, length)
		valuesList       = make([][]byte, length)
		timestampsList   = make([][]byte, length)
		typesList        = make([][]int32, length)
		sizeList         = make([]int32, length)
	)
	for index, tablet := range tablets {
		deviceIds[index] = tablet.GetDeviceId()
		measurementsList[index] = tablet.GetMeasurements()
		valuesList[index] = tablet.GetBinaryValues()
		timestampsList[index] = tablet.GetBinaryTimestamps()
		typesList[index] = tablet.GetTypes()
		sizeList[index] = tablet.GetRowNumber()
	}
	request := rpc.TSInsertTabletsReq{
		SessionId: s.sessionId,
		DeviceIds: deviceIds,
		TypesList: typesList,
		MeasurementsList: measurementsList,
		ValuesList: valuesList,
		TimestampsList: timestampsList,
		SizeList: sizeList,
	}
	return &request
}

func valuesToBytes(dataTypes []int32, values []interface{}) []byte {
	buf := bytes.NewBuffer([]byte{})
	for i := 0; i < len(dataTypes); i++ {
		dataType := int16(dataTypes[i])
		binary.Write(buf, binary.BigEndian, dataType)
		switch dataTypes[i] {
		case 0, 1, 2, 3, 4:
			binary.Write(buf, binary.BigEndian, values[i])
			break
		case 5:
			tmp := (int32)(len(values[i].(string)))
			binary.Write(buf, binary.BigEndian, tmp)
			buf.WriteString(values[i].(string))
			break
		}
	}
	return buf.Bytes()
}

func (s *Session) ExecuteStatement(sql string) (*SessionDataSet, error) {
	request := rpc.TSExecuteStatementReq{
		SessionId: s.sessionId,
		Statement: sql,
		StatementId: s.requestStatementId,
		FetchSize: &s.config.FetchSize,
	}
	resp, err := s.client.ExecuteStatement(context.Background(), &request)
	dataSet := s.genDataSet(sql, resp)
	sessionDataSet := NewSessionDataSet(dataSet)
	return sessionDataSet, err
}

func (s *Session) genDataSet(sql string, resp *rpc.TSExecuteStatementResp) *SessionDataSet {
	dataSet := SessionDataSet{
		Sql:             sql,
		ColumnNameList:  resp.GetColumns(),
		ColumnTypeList:  resp.GetDataTypeList(),
		ColumnNameIndex: resp.GetColumnNameIndexMap(),
		QueryId:         resp.GetQueryId(),
		SessionId:       s.sessionId,
		IgnoreTimeStamp: resp.GetIgnoreTimeStamp(),
		Client:          s.client,
		QueryDataSet:    resp.GetQueryDataSet(),
	}
	return &dataSet
}

func (s *Session) ExecuteQueryStatement(sql string) (*SessionDataSet, error) {
	request := rpc.TSExecuteStatementReq{
		SessionId: s.sessionId,
		Statement: sql,
		StatementId: s.requestStatementId,
		FetchSize: &s.config.FetchSize,
	}
	resp, err := s.client.ExecuteQueryStatement(context.Background(), &request)
	dataSet := s.genDataSet(sql, resp)
	sessionDataSet := NewSessionDataSet(dataSet)
	return sessionDataSet, err
}

func (s *Session) ExecuteUpdateStatement(sql string) (*SessionDataSet, error) {
	request := rpc.TSExecuteStatementReq{
		SessionId: s.sessionId,
		Statement: sql,
		StatementId: s.requestStatementId,
		FetchSize: &s.config.FetchSize,
	}
	resp, err := s.client.ExecuteUpdateStatement(context.Background(), &request)
	dataSet := s.genDataSet(sql, resp)
	sessionDataSet := NewSessionDataSet(dataSet)
	return sessionDataSet, err
}

func (s *Session) ExecuteBatchStatement(inserts []string) error {
	request := rpc.TSExecuteBatchStatementReq{
		SessionId:  s.sessionId,
		Statements: inserts,
	}
	status, err := s.client.ExecuteBatchStatement(context.Background(), &request)
	if err != nil {
		return err
	}
	err = verifySuccess(status)
	if err != nil {
		return err
	}
	return nil
}

func (s *Session) ExecuteRawDataQuery(paths []string, startTime int64, endTime int64) (*SessionDataSet, error) {
	request := rpc.TSRawDataQueryReq{
		SessionId:   s.sessionId,
		Paths:       paths,
		FetchSize:   &s.config.FetchSize,
		StartTime:   startTime,
		EndTime:     endTime,
		StatementId: s.requestStatementId,
	}
	resp, err := s.client.ExecuteRawDataQuery(context.Background(), &request)
	dataSet := s.genDataSet("", resp)
	sessionDataSet := NewSessionDataSet(dataSet)
	return sessionDataSet, err
}

func (s *Session) GetTimeZone() (string, error) {
	if s.config.ZoneId != "" {
		return s.config.ZoneId, nil
	} else {
		resp, err := s.client.GetTimeZone(context.Background(), s.sessionId)
		return resp.TimeZone, err
	}
}

func (s *Session) SetTimeZone(timeZone string) error {
	request := rpc.TSSetTimeZoneReq{SessionId: s.sessionId, TimeZone: timeZone}
	status, err := s.client.SetTimeZone(context.Background(), &request)
	if err != nil {
		return err
	}
	err = verifySuccess(status)
	if err != nil {
		return err
	}
	s.config.ZoneId = timeZone
	return nil
}

func verifySuccess(status *rpc.TSStatus) error {
	if status.GetCode() == MULTIPLE_ERROR {
		return VerifySuccess(status.GetSubStatus())
	}
	if status.GetCode() != SUCCESS_STATUS {
		return errors.New(strconv.Itoa(int(status.GetCode())) + ": " + status.GetMessage())
	}
	return nil
}

func VerifySuccess(statuses []*rpc.TSStatus) error {
	for _, status := range statuses {
		if status.GetCode() != SUCCESS_STATUS {
			return errors.New(strconv.Itoa(int(status.GetCode())) + ": " + status.GetMessage())
		}
	}
	return nil
}
