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
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"reflect"
	"sort"
	"time"

	"github.com/apache/iotdb-client-go/rpc"
	"github.com/apache/thrift/lib/go/thrift"
)

const (
	DefaultTimeZone  = "Asia/Shanghai"
	DefaultFetchSize = 1024
)

var errLength = errors.New("deviceIds, times, measurementsList and valuesList's size should be equal")

type Config struct {
	Host      string
	Port      string
	UserName  string
	Password  string
	FetchSize int32
	TimeZone  string
}

type Session struct {
	config             *Config
	client             *rpc.TSIServiceClient
	sessionId          int64
	trans              thrift.TTransport
	requestStatementId int64
}

func (s *Session) Open(enableRPCCompression bool, connectionTimeoutInMs int) error {
	if s.config.FetchSize <= 0 {
		s.config.FetchSize = DefaultFetchSize
	}
	if s.config.TimeZone == "" {
		s.config.TimeZone = DefaultTimeZone
	}

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
	iprot := protocolFactory.GetProtocol(s.trans)
	oprot := protocolFactory.GetProtocol(s.trans)
	s.client = rpc.NewTSIServiceClient(thrift.NewTStandardClient(iprot, oprot))
	req := rpc.TSOpenSessionReq{ClientProtocol: rpc.TSProtocolVersion_IOTDB_SERVICE_PROTOCOL_V3, ZoneId: s.config.TimeZone, Username: &s.config.UserName,
		Password: &s.config.Password}
	resp, err := s.client.OpenSession(context.Background(), &req)
	if err != nil {
		return err
	}
	s.sessionId = resp.GetSessionId()
	s.requestStatementId, err = s.client.RequestStatementId(context.Background(), s.sessionId)
	if err != nil {
		return err
	}

	s.SetTimeZone(s.config.TimeZone)
	s.config.TimeZone, err = s.GetTimeZone()
	return err
}

func (s *Session) Close() (r *rpc.TSStatus, err error) {
	req := rpc.NewTSCloseSessionReq()
	req.SessionId = s.sessionId
	r, err = s.client.CloseSession(context.Background(), req)
	if err != nil {
		return nil, err
	}
	return nil, s.trans.Close()
}

/*
 *set one storage group
 *param
 *storageGroupId: string, storage group name (starts from root)
 *return
 *error: correctness of operation
 */
func (s *Session) SetStorageGroup(storageGroupId string) (r *rpc.TSStatus, err error) {
	r, err = s.client.SetStorageGroup(context.Background(), s.sessionId, storageGroupId)
	return r, err
}

/*
 *delete one storage group
 *param
 *storageGroupId: string, storage group name (starts from root)
 *return
 *error: correctness of operation
 */
func (s *Session) DeleteStorageGroup(storageGroupId string) (r *rpc.TSStatus, err error) {
	r, err = s.client.DeleteStorageGroups(context.Background(), s.sessionId, []string{storageGroupId})
	return r, err
}

/*
 *delete multiple storage group
 *param
 *storageGroupIds: []string, paths of the target storage groups
 *return
 *error: correctness of operation
 */
func (s *Session) DeleteStorageGroups(storageGroupIds ...string) (r *rpc.TSStatus, err error) {
	r, err = s.client.DeleteStorageGroups(context.Background(), s.sessionId, storageGroupIds)
	return r, err
}

/*
 *create single time series
 *params
 *path: string, complete time series path (starts from root)
 *dataType: int32, data type for this time series
 *encoding: int32, data type for this time series
 *compressor: int32, compressing type for this time series
 *return
 *error: correctness of operation
 */
func (s *Session) CreateTimeseries(path string, dataType TSDataType, encoding TSEncoding, compressor TSCompressionType, attributes map[string]string, tags map[string]string) (r *rpc.TSStatus, err error) {
	request := rpc.TSCreateTimeseriesReq{SessionId: s.sessionId, Path: path, DataType: int32(dataType), Encoding: int32(encoding),
		Compressor: int32(compressor), Attributes: attributes, Tags: tags}
	status, err := s.client.CreateTimeseries(context.Background(), &request)
	return status, err
}

/*
 *create multiple time series
 *params
 *paths: []string, complete time series paths (starts from root)
 *dataTypes: []int32, data types for time series
 *encodings: []int32, encodings for time series
 *compressors: []int32, compressing types for time series
 *return
 *error: correctness of operation
 */
func (s *Session) CreateMultiTimeseries(paths []string, dataTypes []TSDataType, encodings []TSEncoding, compressors []TSCompressionType) (r *rpc.TSStatus, err error) {
	destTypes := make([]int32, len(dataTypes))
	for i, t := range dataTypes {
		destTypes[i] = int32(t)
	}

	destEncodings := make([]int32, len(encodings))
	for i, e := range encodings {
		destEncodings[i] = int32(e)
	}

	destCompressions := make([]int32, len(compressors))
	for i, e := range compressors {
		destCompressions[i] = int32(e)
	}

	request := rpc.TSCreateMultiTimeseriesReq{SessionId: s.sessionId, Paths: paths, DataTypes: destTypes,
		Encodings: destEncodings, Compressors: destCompressions}
	r, err = s.client.CreateMultiTimeseries(context.Background(), &request)

	return r, err
}

/*
 *delete multiple time series, including data and schema
 *params
 *paths: []string, time series paths, which should be complete (starts from root)
 *return
 *error: correctness of operation
 */
func (s *Session) DeleteTimeseries(paths []string) (r *rpc.TSStatus, err error) {
	r, err = s.client.DeleteTimeseries(context.Background(), s.sessionId, paths)
	return r, err
}

/*
 *delete all startTime <= data <= endTime in multiple time series
 *params
 *paths: []string, time series array that the data in
 *startTime: int64, start time of deletion range
 *endTime: int64, end time of deletion range
 *return
 *error: correctness of operation
 */
func (s *Session) DeleteData(paths []string, startTime int64, endTime int64) (r *rpc.TSStatus, err error) {
	request := rpc.TSDeleteDataReq{SessionId: s.sessionId, Paths: paths, StartTime: startTime, EndTime: endTime}
	r, err = s.client.DeleteData(context.Background(), &request)
	return r, err
}

/*
 *special case for inserting one row of String (TEXT) value
 *params
 *deviceId: string, time series path for device
 *measurements: []string, sensor names
 *values: []string, values to be inserted, for each sensor
 *timestamp: int64, indicate the timestamp of the row of data
 *return
 *error: correctness of operation
 */
func (s *Session) InsertStringRecord(deviceId string, measurements []string, values []string, timestamp int64) (r *rpc.TSStatus, err error) {
	request := rpc.TSInsertStringRecordReq{SessionId: s.sessionId, DeviceId: deviceId, Measurements: measurements,
		Values: values, Timestamp: timestamp}
	r, err = s.client.InsertStringRecord(context.Background(), &request)
	return r, err
}

func (s *Session) GetTimeZone() (string, error) {
	resp, err := s.client.GetTimeZone(context.Background(), s.sessionId)
	if err != nil {
		return "", err
	}
	return resp.TimeZone, nil
}

func (s *Session) SetTimeZone(timeZone string) (r *rpc.TSStatus, err error) {
	request := rpc.TSSetTimeZoneReq{SessionId: s.sessionId, TimeZone: timeZone}
	r, err = s.client.SetTimeZone(context.Background(), &request)
	s.config.TimeZone = timeZone
	return r, err
}

func (s *Session) ExecuteStatement(sql string) (*SessionDataSet, error) {
	request := rpc.TSExecuteStatementReq{
		SessionId:   s.sessionId,
		Statement:   sql,
		StatementId: s.requestStatementId,
		FetchSize:   &s.config.FetchSize,
	}
	resp, err := s.client.ExecuteStatement(context.Background(), &request)
	return s.genDataSet(sql, resp), err
}

func (s *Session) ExecuteQueryStatement(sql string, timeoutMs int64) (*SessionDataSet, error) {
	request := rpc.TSExecuteStatementReq{SessionId: s.sessionId, Statement: sql, StatementId: s.requestStatementId,
		FetchSize: &s.config.FetchSize, Timeout: &timeoutMs}
	if resp, err := s.client.ExecuteQueryStatement(context.Background(), &request); err == nil {
		if statusErr := VerifySuccess(resp.Status); statusErr == nil {
			return NewSessionDataSet(sql, resp.Columns, resp.DataTypeList, resp.ColumnNameIndexMap, *resp.QueryId, s.client, s.sessionId, resp.QueryDataSet, resp.IgnoreTimeStamp != nil && *resp.IgnoreTimeStamp, s.config.FetchSize, timeoutMs), err
		} else {
			return nil, statusErr
		}
	} else {
		return nil, err
	}
}

func (s *Session) genTSInsertRecordReq(deviceId string, time int64,
	measurements []string,
	types []TSDataType,
	values []interface{}) (*rpc.TSInsertRecordReq, error) {
	request := &rpc.TSInsertRecordReq{}
	request.SessionId = s.sessionId
	request.DeviceId = deviceId
	request.Timestamp = time
	request.Measurements = measurements

	if bys, err := valuesToBytes(types, values); err == nil {
		request.Values = bys
	} else {
		return nil, err
	}
	return request, nil
}

func (s *Session) InsertRecord(deviceId string, measurements []string, dataTypes []TSDataType, values []interface{}, timestamp int64) (r *rpc.TSStatus, err error) {
	request, err := s.genTSInsertRecordReq(deviceId, timestamp, measurements, dataTypes, values)
	if err != nil {
		return nil, err
	}
	r, err = s.client.InsertRecord(context.Background(), request)
	return r, err
}

type deviceData struct {
	timestamps        []int64
	measurementsSlice [][]string
	dataTypesSlice    [][]TSDataType
	valuesSlice       [][]interface{}
}

func (d *deviceData) Len() int {
	return len(d.timestamps)
}

func (d *deviceData) Less(i, j int) bool {
	return d.timestamps[i] < d.timestamps[j]
}

func (d *deviceData) Swap(i, j int) {
	d.timestamps[i], d.timestamps[j] = d.timestamps[j], d.timestamps[i]
	d.measurementsSlice[i], d.measurementsSlice[j] = d.measurementsSlice[j], d.measurementsSlice[i]
	d.dataTypesSlice[i], d.dataTypesSlice[j] = d.dataTypesSlice[j], d.dataTypesSlice[i]
	d.valuesSlice[i], d.valuesSlice[j] = d.valuesSlice[j], d.valuesSlice[i]
}

// InsertRecordsOfOneDevice Insert multiple rows, which can reduce the overhead of network. This method is just like jdbc
// executeBatch, we pack some insert request in batch and send them to server. If you want improve
// your performance, please see insertTablet method
// Each row is independent, which could have different deviceId, time, number of measurements
func (s *Session) InsertRecordsOfOneDevice(deviceId string, timestamps []int64, measurementsSlice [][]string, dataTypesSlice [][]TSDataType, valuesSlice [][]interface{}, sorted bool) (r *rpc.TSStatus, err error) {
	length := len(timestamps)
	if len(measurementsSlice) != length || len(dataTypesSlice) != length || len(valuesSlice) != length {
		return nil, errors.New("timestamps, measurementsSlice and valuesSlice's size should be equal")
	}

	if !sorted {
		sort.Sort(&deviceData{
			timestamps:        timestamps,
			measurementsSlice: measurementsSlice,
			dataTypesSlice:    dataTypesSlice,
			valuesSlice:       valuesSlice,
		})
	}

	valuesList := make([][]byte, length)
	for i := 0; i < length; i++ {
		if valuesList[i], err = valuesToBytes(dataTypesSlice[i], valuesSlice[i]); err != nil {
			return nil, err
		}
	}

	request := &rpc.TSInsertRecordsOfOneDeviceReq{
		SessionId:        s.sessionId,
		DeviceId:         deviceId,
		Timestamps:       timestamps,
		MeasurementsList: measurementsSlice,
		ValuesList:       valuesList,
	}
	return s.client.InsertRecordsOfOneDevice(context.Background(), request)
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
func (s *Session) InsertRecords(deviceIds []string, measurements [][]string, dataTypes [][]TSDataType, values [][]interface{},
	timestamps []int64) (r *rpc.TSStatus, err error) {
	request, err := s.genInsertRecordsReq(deviceIds, measurements, dataTypes, values, timestamps)
	if err != nil {
		return nil, err
	} else {
		return s.client.InsertRecords(context.Background(), request)
	}
}

/*
 * InsertTablets insert multiple tablets, tablets are independent to each other
 *params
 *tablets: []*client.Tablet, list of tablets
 */
func (s *Session) InsertTablets(tablets []*Tablet, sorted bool) (r *rpc.TSStatus, err error) {
	if !sorted {
		for _, t := range tablets {
			if err := t.Sort(); err != nil {
				return nil, err
			}
		}
	}
	request, err := s.genInsertTabletsReq(tablets)
	if err != nil {
		return nil, err
	}
	return s.client.InsertTablets(context.Background(), request)
}

func (s *Session) ExecuteBatchStatement(inserts []string) (r *rpc.TSStatus, err error) {
	request := rpc.TSExecuteBatchStatementReq{
		SessionId:  s.sessionId,
		Statements: inserts,
	}
	return s.client.ExecuteBatchStatement(context.Background(), &request)
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
	return s.genDataSet("", resp), err
}

func (s *Session) ExecuteUpdateStatement(sql string) (*SessionDataSet, error) {
	request := rpc.TSExecuteStatementReq{
		SessionId:   s.sessionId,
		Statement:   sql,
		StatementId: s.requestStatementId,
		FetchSize:   &s.config.FetchSize,
	}
	resp, err := s.client.ExecuteUpdateStatement(context.Background(), &request)
	return s.genDataSet(sql, resp), err
}

func (s *Session) genDataSet(sql string, resp *rpc.TSExecuteStatementResp) *SessionDataSet {
	return NewSessionDataSet(sql, resp.Columns, resp.DataTypeList, resp.ColumnNameIndexMap, *resp.QueryId, s.client, s.sessionId, resp.QueryDataSet, resp.IgnoreTimeStamp != nil && *resp.IgnoreTimeStamp, s.config.FetchSize, 0)
}

func (s *Session) genInsertTabletsReq(tablets []*Tablet) (*rpc.TSInsertTabletsReq, error) {
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
		deviceIds[index] = tablet.deviceId
		measurementsList[index] = tablet.GetMeasurements()

		values, err := tablet.getValuesBytes()
		if err != nil {
			return nil, err
		}

		valuesList[index] = values
		timestampsList[index] = tablet.GetTimestampBytes()
		typesList[index] = tablet.getDataTypes()
		sizeList[index] = int32(tablet.rowCount)
	}
	request := rpc.TSInsertTabletsReq{
		SessionId:        s.sessionId,
		DeviceIds:        deviceIds,
		TypesList:        typesList,
		MeasurementsList: measurementsList,
		ValuesList:       valuesList,
		TimestampsList:   timestampsList,
		SizeList:         sizeList,
	}
	return &request, nil
}

func (s *Session) genInsertRecordsReq(deviceIds []string, measurements [][]string, dataTypes [][]TSDataType, values [][]interface{},
	timestamps []int64) (*rpc.TSInsertRecordsReq, error) {
	length := len(deviceIds)
	if length != len(timestamps) || length != len(measurements) || length != len(values) {
		return nil, errLength
	}
	request := rpc.TSInsertRecordsReq{
		SessionId:        s.sessionId,
		DeviceIds:        deviceIds,
		MeasurementsList: measurements,
		Timestamps:       timestamps,
	}
	v := make([][]byte, length)
	for i := 0; i < len(measurements); i++ {
		if bys, err := valuesToBytes(dataTypes[i], values[i]); err == nil {
			v[i] = bys
		} else {
			return nil, err
		}
	}
	request.ValuesList = v
	return &request, nil
}

func valuesToBytes(dataTypes []TSDataType, values []interface{}) ([]byte, error) {
	buff := &bytes.Buffer{}
	for i, t := range dataTypes {
		binary.Write(buff, binary.BigEndian, byte(t))
		v := values[i]
		if v == nil {
			return nil, fmt.Errorf("values[%d] can't be nil", i)
		}

		switch t {
		case BOOLEAN:
			switch v.(type) {
			case bool:
				binary.Write(buff, binary.BigEndian, v)
			default:
				return nil, fmt.Errorf("values[%d] %v(%v) must be bool", i, v, reflect.TypeOf(v))
			}
		case INT32:
			switch v.(type) {
			case int32:
				binary.Write(buff, binary.BigEndian, v)
			default:
				return nil, fmt.Errorf("values[%d] %v(%v) must be int32", i, v, reflect.TypeOf(v))
			}
		case INT64:
			switch v.(type) {
			case int64:
				binary.Write(buff, binary.BigEndian, v)
			default:
				return nil, fmt.Errorf("values[%d] %v(%v) must be int64", i, v, reflect.TypeOf(v))
			}
		case FLOAT:
			switch v.(type) {
			case float32:
				binary.Write(buff, binary.BigEndian, v)
			default:
				return nil, fmt.Errorf("values[%d] %v(%v) must be float32", i, v, reflect.TypeOf(v))
			}
		case DOUBLE:
			switch v.(type) {
			case float64:
				binary.Write(buff, binary.BigEndian, v)
			default:
				return nil, fmt.Errorf("values[%d] %v(%v) must be float64", i, v, reflect.TypeOf(v))
			}
		case TEXT:
			switch s := v.(type) {
			case string:
				size := len(s)
				binary.Write(buff, binary.BigEndian, int32(size))
				binary.Write(buff, binary.BigEndian, []byte(s))
			default:
				return nil, fmt.Errorf("values[%d] %v(%v) must be string", i, v, reflect.TypeOf(v))
			}
		default:
			return nil, fmt.Errorf("types[%d] is incorrect, it must in (BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT)", i)
		}
	}
	return buff.Bytes(), nil
}

func (s *Session) InsertTablet(tablet *Tablet, sorted bool) (r *rpc.TSStatus, err error) {
	if !sorted {
		if err := tablet.Sort(); err != nil {
			return nil, err
		}
	}
	request, err := s.genTSInsertTabletReq(tablet)
	if err != nil {
		return nil, err
	}
	return s.client.InsertTablet(context.Background(), request)
}

func (s *Session) genTSInsertTabletReq(tablet *Tablet) (*rpc.TSInsertTabletReq, error) {
	if values, err := tablet.getValuesBytes(); err == nil {
		request := &rpc.TSInsertTabletReq{
			SessionId:    s.sessionId,
			DeviceId:     tablet.deviceId,
			Measurements: tablet.GetMeasurements(),
			Values:       values,
			Timestamps:   tablet.GetTimestampBytes(),
			Types:        tablet.getDataTypes(),
			Size:         int32(tablet.rowCount),
		}
		return request, nil
	} else {
		return nil, err
	}
}

func (s *Session) GetSessionId() int64 {
	return s.sessionId
}

func NewSession(config *Config) *Session {
	return &Session{config: config}
}
