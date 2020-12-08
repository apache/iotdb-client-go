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
	"net"
	"time"

	"github.com/apache/iotdb-client-go/rpc"
	"github.com/apache/thrift/lib/go/thrift"
)

const (
	DEFAULT_TIME_ZONE  = "Asia/Shanghai"
	DEFAULT_FETCH_SIZE = 1024
)

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
	isClose            bool
	trans              thrift.TTransport
	requestStatementId int64
}

func (s *Session) Open(enableRPCCompression bool, connectionTimeoutInMs int) error {
	if s.config.FetchSize <= 0 {
		s.config.FetchSize = DEFAULT_FETCH_SIZE
	}
	if s.config.TimeZone == "" {
		s.config.TimeZone = DEFAULT_TIME_ZONE
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
 *
 *param
 *storageGroupId: string, storage group name (starts from root)
 *
 *return
 *error: correctness of operation
 */
func (s *Session) SetStorageGroup(storageGroupId string) (r *rpc.TSStatus, err error) {
	r, err = s.client.SetStorageGroup(context.Background(), s.sessionId, storageGroupId)
	return r, err
}

/*
 *delete one storage group
 *
 *param
 *storageGroupId: string, storage group name (starts from root)
 *
 *return
 *error: correctness of operation
 */
func (s *Session) DeleteStorageGroup(storageGroupId string) (r *rpc.TSStatus, err error) {
	r, err = s.client.DeleteStorageGroups(context.Background(), s.sessionId, []string{storageGroupId})
	return r, err
}

/*
 *delete multiple storage group
 *
 *param
 *storageGroupIds: []string, paths of the target storage groups
 *
 *return
 *error: correctness of operation
 */
func (s *Session) DeleteStorageGroups(storageGroupIds []string) (r *rpc.TSStatus, err error) {
	r, err = s.client.DeleteStorageGroups(context.Background(), s.sessionId, storageGroupIds)
	return r, err
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
 *return
 *error: correctness of operation
 */
func (s *Session) CreateTimeseries(path string, dataType int32, encoding int32, compressor int32, attributes map[string]string, tags map[string]string) (r *rpc.TSStatus, err error) {
	request := rpc.TSCreateTimeseriesReq{SessionId: s.sessionId, Path: path, DataType: dataType, Encoding: encoding,
		Compressor: compressor, Attributes: attributes, Tags: tags}
	status, err := s.client.CreateTimeseries(context.Background(), &request)
	return status, err
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
 *return
 *error: correctness of operation
 */
func (s *Session) CreateMultiTimeseries(paths []string, dataTypes []int32, encodings []int32, compressors []int32) (r *rpc.TSStatus, err error) {
	request := rpc.TSCreateMultiTimeseriesReq{SessionId: s.sessionId, Paths: paths, DataTypes: dataTypes,
		Encodings: encodings, Compressors: compressors}
	r, err = s.client.CreateMultiTimeseries(context.Background(), &request)

	return r, err
}

/*
 *delete multiple time series, including data and schema
 *
 *params
 *paths: []string, time series paths, which should be complete (starts from root)
 *
 *return
 *error: correctness of operation
 */
func (s *Session) DeleteTimeseries(paths []string) (r *rpc.TSStatus, err error) {
	r, err = s.client.DeleteTimeseries(context.Background(), s.sessionId, paths)
	return r, err
}

/*
 *delete all startTime <= data <= endTime in multiple time series
 *
 *params
 *paths: []string, time series array that the data in
 *startTime: int64, start time of deletion range
 *endTime: int64, end time of deletion range
 *
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
 *
 *params
 *deviceId: string, time series path for device
 *measurements: []string, sensor names
 *values: []string, values to be inserted, for each sensor
 *timestamp: int64, indicate the timestamp of the row of data
 *
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
	if s.config.TimeZone != "" {
		return s.config.TimeZone, nil
	} else {
		resp, err := s.client.GetTimeZone(context.Background(), s.sessionId)
		if err != nil {
			return "", err
		}
		return resp.TimeZone, nil
	}
}

func (s *Session) SetTimeZone(timeZone string) (r *rpc.TSStatus, err error) {
	request := rpc.TSSetTimeZoneReq{SessionId: s.sessionId, TimeZone: timeZone}
	r, err = s.client.SetTimeZone(context.Background(), &request)
	s.config.TimeZone = timeZone
	return r, err
}

func (s *Session) ExecuteStatement(sql string) (*rpc.TSExecuteStatementResp, error) {
	request := rpc.TSExecuteStatementReq{SessionId: s.sessionId, Statement: sql, StatementId: s.requestStatementId,
		FetchSize: &s.config.FetchSize}
	resp, err := s.client.ExecuteStatement(context.Background(), &request)
	return resp, err
}

func (s *Session) ExecuteQueryStatement(sql string) (*SessionDataSet, error) {
	request := rpc.TSExecuteStatementReq{SessionId: s.sessionId, Statement: sql, StatementId: s.requestStatementId,
		FetchSize: &s.config.FetchSize}
	resp, err := s.client.ExecuteQueryStatement(context.Background(), &request)
	return NewSessionDataSet(sql, resp.Columns, resp.DataTypeList, resp.ColumnNameIndexMap, *resp.QueryId, s.client, s.sessionId, resp.QueryDataSet, resp.IgnoreTimeStamp != nil && *resp.IgnoreTimeStamp), err
}

func (s *Session) genTSInsertRecordReq(deviceId string, time int64,
	measurements []string,
	types []int32,
	values []interface{}) *rpc.TSInsertRecordReq {
	request := &rpc.TSInsertRecordReq{}
	request.SessionId = s.sessionId
	request.DeviceId = deviceId
	request.Timestamp = time
	request.Measurements = measurements

	buff := &bytes.Buffer{}
	for i, t := range types {
		binary.Write(buff, binary.BigEndian, int16(t))
		if t == TEXT {
			text := values[i].(string)
			size := len(text)
			binary.Write(buff, binary.BigEndian, int32(size))
			binary.Write(buff, binary.BigEndian, []byte(text))
		} else {
			binary.Write(buff, binary.BigEndian, values[i])
		}
	}
	request.Values = buff.Bytes()
	return request
}

func (s *Session) InsertRecord(deviceId string, measurements []string, dataTypes []int32, values []interface{}, timestamp int64) (r *rpc.TSStatus, err error) {
	request := s.genTSInsertRecordReq(deviceId, timestamp, measurements, dataTypes, values)
	r, err = s.client.InsertRecord(context.Background(), request)
	return r, err
}

func NewSession(config *Config) *Session {
	return &Session{config: config}
}
