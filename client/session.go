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
	DefaultUser      = "root"
	DefaultPasswd    = "root"
	DefaultZoneId    = "Asia/Shanghai"
	protocolVersion  = rpc.TSProtocolVersion_IOTDB_SERVICE_PROTOCOL_V3
	DefaultFetchSize = 1024
)

type Session struct {
	Host               string
	Port               string
	User               string
	Passwd             string
	FetchSize          int32
	ZoneId             string
	client             *rpc.TSIServiceClient
	sessionId          int64
	isClose            bool
	trans              thrift.TTransport
	requestStatementId int64
}

func (s *Session) Open(enableRPCCompression bool, connectionTimeoutInMs int) error {
	var protocolFactory thrift.TProtocolFactory
	var err error
	s.trans, err = thrift.NewTSocketTimeout(net.JoinHostPort(s.Host, s.Port), time.Duration(connectionTimeoutInMs))
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
	s.ZoneId = DefaultZoneId
	tSOpenSessionReq := rpc.TSOpenSessionReq{ClientProtocol: protocolVersion, ZoneId: s.ZoneId, Username: &s.User,
		Password: &s.Passwd}
	tSOpenSessionResp, err := s.client.OpenSession(context.Background(), &tSOpenSessionReq)
	if err != nil {
		return err
	}
	s.sessionId = tSOpenSessionResp.GetSessionId()
	s.requestStatementId, err = s.client.RequestStatementId(context.Background(), s.sessionId)
	s.FetchSize = DefaultFetchSize
	if err != nil {
		return err
	}
	if s.ZoneId != "" {
		s.SetTimeZone(s.ZoneId)
	} else {
		s.ZoneId, err = s.GetTimeZone()
	}
	return err
}

func (s *Session) Close() error {
	tSCloseSessionReq := rpc.NewTSCloseSessionReq()
	tSCloseSessionReq.SessionId = s.sessionId
	_, err := s.client.CloseSession(context.Background(), tSCloseSessionReq)
	if err != nil {
		return err
	}
	s.trans.Close()
	return nil
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
func (s *Session) SetStorageGroup(storageGroupId string) error {
	_, err := s.client.SetStorageGroup(context.Background(), s.sessionId, storageGroupId)
	return err
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
func (s *Session) DeleteStorageGroup(storageGroupId string) error {
	_, err := s.client.DeleteStorageGroups(context.Background(), s.sessionId, []string{storageGroupId})
	return err
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
func (s *Session) DeleteStorageGroups(storageGroupIds []string) error {
	_, err := s.client.DeleteStorageGroups(context.Background(), s.sessionId, storageGroupIds)
	return err
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
func (s *Session) CreateMultiTimeseries(paths []string, dataTypes []int32, encodings []int32, compressors []int32) error {
	request := rpc.TSCreateMultiTimeseriesReq{SessionId: s.sessionId, Paths: paths, DataTypes: dataTypes,
		Encodings: encodings, Compressors: compressors}
	_, err := s.client.CreateMultiTimeseries(context.Background(), &request)

	return err
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
func (s *Session) DeleteTimeseries(paths []string) error {
	_, err := s.client.DeleteTimeseries(context.Background(), s.sessionId, paths)
	return err
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
func (s *Session) DeleteData(paths []string, startTime int64, endTime int64) error {
	request := rpc.TSDeleteDataReq{SessionId: s.sessionId, Paths: paths, StartTime: startTime, EndTime: endTime}
	_, err := s.client.DeleteData(context.Background(), &request)

	return err
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
func (s *Session) InsertStringRecord(deviceId string, measurements []string, values []string, timestamp int64) error {
	request := rpc.TSInsertStringRecordReq{SessionId: s.sessionId, DeviceId: deviceId, Measurements: measurements,
		Values: values, Timestamp: timestamp}
	_, err := s.client.InsertStringRecord(context.Background(), &request)
	return err
}

func (s *Session) GetTimeZone() (string, error) {
	if s.ZoneId != "" {
		return s.ZoneId, nil
	} else {
		resp, err := s.client.GetTimeZone(context.Background(), s.sessionId)
		if err != nil {
			return "", err
		}
		return resp.TimeZone, nil
	}
}

func (s *Session) SetTimeZone(timeZone string) error {
	request := rpc.TSSetTimeZoneReq{SessionId: s.sessionId, TimeZone: timeZone}
	_, err := s.client.SetTimeZone(context.Background(), &request)
	s.ZoneId = timeZone
	return err
}

func (s *Session) ExecuteStatement(sql string) (*rpc.TSExecuteStatementResp, error) {
	request := rpc.TSExecuteStatementReq{SessionId: s.sessionId, Statement: sql, StatementId: s.requestStatementId,
		FetchSize: &s.FetchSize}
	resp, err := s.client.ExecuteStatement(context.Background(), &request)
	return resp, err
}

func (s *Session) ExecuteQueryStatement(sql string) (*SessionDataSet, error) {
	request := rpc.TSExecuteStatementReq{SessionId: s.sessionId, Statement: sql, StatementId: s.requestStatementId,
		FetchSize: &s.FetchSize}
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

func (s *Session) InsertRecord(deviceId string, measurements []string, dataTypes []int32, values []interface{}, timestamp int64) error {
	request := s.genTSInsertRecordReq(deviceId, timestamp, measurements, dataTypes, values)
	_, err := s.client.InsertRecord(context.Background(), request)
	return err
}

type DialOption interface {
	apply(*Session)
}

type FuncOption struct {
	f func(*Session)
}

func (O *FuncOption) apply(s *Session) {
	O.f(s)
}

func newFuncOption(f func(*Session)) *FuncOption {
	return &FuncOption{
		f: f,
	}
}

func withUser(user string) DialOption {
	return newFuncOption(func(session *Session) {
		session.User = user
	})
}

func withPasswd(passwd string) DialOption {
	return newFuncOption(func(session *Session) {
		session.Passwd = passwd
	})
}

func withFetchSize(fetchSize int32) DialOption {
	return newFuncOption(func(session *Session) {
		session.FetchSize = fetchSize
	})
}

func defaultOptions() Session {
	return Session{
		User:      DefaultUser,
		Passwd:    DefaultPasswd,
		FetchSize: DefaultFetchSize,
		ZoneId:    DefaultZoneId,
	}
}

type SessionConn struct {
	session Session
}

func NewSession(host string, port string, opts ...DialOption) Session {
	sessionConn := &SessionConn{
		session: defaultOptions(),
	}
	for _, opt := range opts {
		opt.apply(&sessionConn.session)
	}
	sessionConn.session.Host = host
	sessionConn.session.Port = port
	return sessionConn.session
}
