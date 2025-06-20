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
	"log"
	"net"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/apache/iotdb-client-go/v2/common"
	"github.com/apache/iotdb-client-go/v2/rpc"
)

const (
	DefaultTimeZone        = "Asia/Shanghai"
	DefaultFetchSize       = 1024
	DefaultConnectRetryMax = 3
	TreeSqlDialect         = "tree"
	TableSqlDialect        = "table"
)

type Version string

const (
	V_0_12          = Version("V_0_12")
	V_0_13          = Version("V_0_13")
	V_1_0           = Version("V_1_0")
	DEFAULT_VERSION = V_1_0
)

var errLength = errors.New("deviceIds, times, measurementsList and valuesList's size should be equal")

type Config struct {
	Host            string
	Port            string
	UserName        string
	Password        string
	FetchSize       int32
	TimeZone        string
	ConnectRetryMax int
	sqlDialect      string
	Version         Version
	Database        string
}

type Session struct {
	config             *Config
	client             *rpc.IClientRPCServiceClient
	sessionId          int64
	trans              thrift.TTransport
	requestStatementId int64
	protocolFactory    thrift.TProtocolFactory
	endPointList       []endPoint
	timeFactor         int32
}

type endPoint struct {
	Host string
	Port string
}

func (s *Session) Open(enableRPCCompression bool, connectionTimeoutInMs int) error {
	if s.config.FetchSize <= 0 {
		s.config.FetchSize = DefaultFetchSize
	}
	if s.config.TimeZone == "" {
		s.config.TimeZone = DefaultTimeZone
	}

	if s.config.ConnectRetryMax <= 0 {
		s.config.ConnectRetryMax = DefaultConnectRetryMax
	}

	var err error

	// in thrift 0.14.1, this func returns two values; in thrift 0.15.0, it returns one.
	s.trans = thrift.NewTSocketConf(net.JoinHostPort(s.config.Host, s.config.Port), &thrift.TConfiguration{
		ConnectTimeout: time.Duration(connectionTimeoutInMs) * time.Millisecond, // Use 0 for no timeout
	})
	// s.trans = thrift.NewTFramedTransport(s.trans)	// deprecated
	tmp_conf := thrift.TConfiguration{MaxFrameSize: thrift.DEFAULT_MAX_FRAME_SIZE}
	s.trans = thrift.NewTFramedTransportConf(s.trans, &tmp_conf)
	if !s.trans.IsOpen() {
		err = s.trans.Open()
		if err != nil {
			return err
		}
	}
	s.protocolFactory = getProtocolFactory(enableRPCCompression)
	iprot := s.protocolFactory.GetProtocol(s.trans)
	oprot := s.protocolFactory.GetProtocol(s.trans)
	s.client = rpc.NewIClientRPCServiceClient(thrift.NewTStandardClient(iprot, oprot))
	req := rpc.TSOpenSessionReq{
		ClientProtocol: rpc.TSProtocolVersion_IOTDB_SERVICE_PROTOCOL_V3, ZoneId: s.config.TimeZone, Username: s.config.UserName,
		Password: &s.config.Password,
	}
	req.Configuration = make(map[string]string)
	req.Configuration["sql_dialect"] = s.config.sqlDialect
	if s.config.Version == "" {
		req.Configuration["version"] = string(DEFAULT_VERSION)
	} else {
		req.Configuration["version"] = string(s.config.Version)
	}
	if s.config.Database != "" {
		req.Configuration["db"] = s.config.Database
	}
	resp, err := s.client.OpenSession(context.Background(), &req)
	if err != nil {
		return err
	}
	s.sessionId = resp.GetSessionId()
	s.requestStatementId, err = s.client.RequestStatementId(context.Background(), s.sessionId)
	if timeFactor, err := getTimeFactor(resp); err != nil {
		return err
	} else {
		s.timeFactor = timeFactor
	}
	return err
}

type ClusterConfig struct {
	NodeUrls        []string //ip:port
	UserName        string
	Password        string
	FetchSize       int32
	TimeZone        string
	ConnectRetryMax int
	sqlDialect      string
	Database        string
}

func (s *Session) OpenCluster(enableRPCCompression bool) error {
	if s.config.FetchSize <= 0 {
		s.config.FetchSize = DefaultFetchSize
	}
	if s.config.TimeZone == "" {
		s.config.TimeZone = DefaultTimeZone
	}

	if s.config.ConnectRetryMax <= 0 {
		s.config.ConnectRetryMax = DefaultConnectRetryMax
	}

	var err error

	s.protocolFactory = getProtocolFactory(enableRPCCompression)
	iprot := s.protocolFactory.GetProtocol(s.trans)
	oprot := s.protocolFactory.GetProtocol(s.trans)
	s.client = rpc.NewIClientRPCServiceClient(thrift.NewTStandardClient(iprot, oprot))
	req := rpc.TSOpenSessionReq{
		ClientProtocol: rpc.TSProtocolVersion_IOTDB_SERVICE_PROTOCOL_V3, ZoneId: s.config.TimeZone, Username: s.config.UserName,
		Password: &s.config.Password,
	}
	req.Configuration = make(map[string]string)
	req.Configuration["sql_dialect"] = s.config.sqlDialect
	if s.config.Version == "" {
		req.Configuration["version"] = string(DEFAULT_VERSION)
	} else {
		req.Configuration["version"] = string(s.config.Version)
	}
	if s.config.Database != "" {
		req.Configuration["db"] = s.config.Database
	}

	resp, err := s.client.OpenSession(context.Background(), &req)
	if err != nil {
		return err
	}
	if timeFactor, err := getTimeFactor(resp); err != nil {
		return err
	} else {
		s.timeFactor = timeFactor
	}
	s.sessionId = resp.GetSessionId()
	s.requestStatementId, err = s.client.RequestStatementId(context.Background(), s.sessionId)
	return err
}

func getProtocolFactory(enableRPCCompression bool) thrift.TProtocolFactory {
	if enableRPCCompression {
		return thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{})
	} else {
		return thrift.NewTBinaryProtocolFactoryConf(&thrift.TConfiguration{})
	}
}

func (s *Session) Close() error {
	req := rpc.NewTSCloseSessionReq()
	req.SessionId = s.sessionId
	_, err := s.client.CloseSession(context.Background(), req)
	if err != nil {
		return err
	}
	return s.trans.Close()
}

/*
 *set one storage group
 *param
 *storageGroupId: string, storage group name (starts from root)
 *return
 *error: correctness of operation
 */
func (s *Session) SetStorageGroup(storageGroupId string) (r *common.TSStatus, err error) {
	r, err = s.client.SetStorageGroup(context.Background(), s.sessionId, storageGroupId)
	if err != nil && r == nil {
		if s.reconnect() {
			r, err = s.client.SetStorageGroup(context.Background(), s.sessionId, storageGroupId)
		}
	}
	return r, err
}

/*
 *delete one storage group
 *param
 *storageGroupId: string, storage group name (starts from root)
 *return
 *error: correctness of operation
 */
func (s *Session) DeleteStorageGroup(storageGroupId string) (r *common.TSStatus, err error) {
	r, err = s.client.DeleteStorageGroups(context.Background(), s.sessionId, []string{storageGroupId})
	if err != nil && r == nil {
		if s.reconnect() {
			r, err = s.client.DeleteStorageGroups(context.Background(), s.sessionId, []string{storageGroupId})
		}
	}
	return r, err
}

/*
 *delete multiple storage group
 *param
 *storageGroupIds: []string, paths of the target storage groups
 *return
 *error: correctness of operation
 */
func (s *Session) DeleteStorageGroups(storageGroupIds ...string) (r *common.TSStatus, err error) {
	r, err = s.client.DeleteStorageGroups(context.Background(), s.sessionId, storageGroupIds)
	if err != nil && r == nil {
		if s.reconnect() {
			r, err = s.client.DeleteStorageGroups(context.Background(), s.sessionId, storageGroupIds)
		}
	}
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
func (s *Session) CreateTimeseries(path string, dataType TSDataType, encoding TSEncoding, compressor TSCompressionType, attributes map[string]string, tags map[string]string) (r *common.TSStatus, err error) {
	request := rpc.TSCreateTimeseriesReq{
		SessionId: s.sessionId, Path: path, DataType: int32(dataType), Encoding: int32(encoding),
		Compressor: int32(compressor), Attributes: attributes, Tags: tags,
	}
	status, err := s.client.CreateTimeseries(context.Background(), &request)
	if err != nil && status == nil {
		if s.reconnect() {
			request.SessionId = s.sessionId
			status, err = s.client.CreateTimeseries(context.Background(), &request)
		}
	}
	return status, err
}

/*
 *create single aligned time series
 *params
 *prefixPath: string, time series prefix path (starts from root)
 *measurements: []string, sensor names
 *dataTypes: []int32, data types for time series
 *encodings: []int32, encodings for time series
 *compressors: []int32, compressing types for time series
 *measurementAlias: []string, sensor names alias
 *return
 *error: correctness of operation
 */
func (s *Session) CreateAlignedTimeseries(prefixPath string, measurements []string, dataTypes []TSDataType, encodings []TSEncoding, compressors []TSCompressionType, measurementAlias []string) (r *common.TSStatus, err error) {
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

	request := rpc.TSCreateAlignedTimeseriesReq{
		SessionId:        s.sessionId,
		PrefixPath:       prefixPath,
		Measurements:     measurements,
		DataTypes:        destTypes,
		Encodings:        destEncodings,
		Compressors:      destCompressions,
		MeasurementAlias: measurementAlias,
	}
	status, err := s.client.CreateAlignedTimeseries(context.Background(), &request)
	if err != nil && status == nil {
		if s.reconnect() {
			request.SessionId = s.sessionId
			status, err = s.client.CreateAlignedTimeseries(context.Background(), &request)
		}
	}
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
func (s *Session) CreateMultiTimeseries(paths []string, dataTypes []TSDataType, encodings []TSEncoding, compressors []TSCompressionType) (r *common.TSStatus, err error) {
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

	request := rpc.TSCreateMultiTimeseriesReq{
		SessionId: s.sessionId, Paths: paths, DataTypes: destTypes,
		Encodings: destEncodings, Compressors: destCompressions,
	}
	r, err = s.client.CreateMultiTimeseries(context.Background(), &request)

	if err != nil && r == nil {
		if s.reconnect() {
			request.SessionId = s.sessionId
			r, err = s.client.CreateMultiTimeseries(context.Background(), &request)
		}
	}

	return r, err
}

/*
 *delete multiple time series, including data and schema
 *params
 *paths: []string, time series paths, which should be complete (starts from root)
 *return
 *error: correctness of operation
 */
func (s *Session) DeleteTimeseries(paths []string) (r *common.TSStatus, err error) {
	r, err = s.client.DeleteTimeseries(context.Background(), s.sessionId, paths)
	if err != nil && r == nil {
		if s.reconnect() {
			r, err = s.client.DeleteTimeseries(context.Background(), s.sessionId, paths)
		}
	}
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
func (s *Session) DeleteData(paths []string, startTime int64, endTime int64) (r *common.TSStatus, err error) {
	request := rpc.TSDeleteDataReq{SessionId: s.sessionId, Paths: paths, StartTime: startTime, EndTime: endTime}
	r, err = s.client.DeleteData(context.Background(), &request)
	if err != nil && r == nil {
		if s.reconnect() {
			request.SessionId = s.sessionId
			r, err = s.client.DeleteData(context.Background(), &request)
		}
	}
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
func (s *Session) InsertStringRecord(deviceId string, measurements []string, values []string, timestamp int64) (r *common.TSStatus, err error) {
	request := rpc.TSInsertStringRecordReq{
		SessionId: s.sessionId, PrefixPath: deviceId, Measurements: measurements,
		Values: values, Timestamp: timestamp,
	}
	r, err = s.client.InsertStringRecord(context.Background(), &request)
	if err != nil && r == nil {
		if s.reconnect() {
			request.SessionId = s.sessionId
			r, err = s.client.InsertStringRecord(context.Background(), &request)
		}
	}
	return r, err
}

func (s *Session) GetTimeZone() (string, error) {
	resp, err := s.client.GetTimeZone(context.Background(), s.sessionId)
	if err != nil {
		return DefaultTimeZone, err
	}
	return resp.TimeZone, nil
}

func (s *Session) SetTimeZone(timeZone string) (r *common.TSStatus, err error) {
	request := rpc.TSSetTimeZoneReq{SessionId: s.sessionId, TimeZone: timeZone}
	r, err = s.client.SetTimeZone(context.Background(), &request)
	s.config.TimeZone = timeZone
	return r, err
}

func (s *Session) ExecuteStatementWithContext(ctx context.Context, sql string) (*SessionDataSet, error) {
	request := rpc.TSExecuteStatementReq{
		SessionId:   s.sessionId,
		Statement:   sql,
		StatementId: s.requestStatementId,
		FetchSize:   &s.config.FetchSize,
	}
	resp, err := s.client.ExecuteStatementV2(ctx, &request)

	if err != nil && resp == nil {
		if s.reconnect() {
			request.SessionId = s.sessionId
			request.StatementId = s.requestStatementId
			resp, err = s.client.ExecuteStatementV2(ctx, &request)
		}
	}
	if statusErr := VerifySuccess(resp.Status); statusErr != nil {
		return nil, statusErr
	}

	return s.genDataSet(sql, resp)
}

func (s *Session) ExecuteStatement(sql string) (*SessionDataSet, error) {
	return s.ExecuteStatementWithContext(context.Background(), sql)
}

func (s *Session) ExecuteNonQueryStatement(sql string) (r *common.TSStatus, err error) {
	request := rpc.TSExecuteStatementReq{
		SessionId:   s.sessionId,
		Statement:   sql,
		StatementId: s.requestStatementId,
		FetchSize:   &s.config.FetchSize,
	}
	resp, err := s.client.ExecuteStatementV2(context.Background(), &request)

	if err != nil && resp == nil {
		if s.reconnect() {
			request.SessionId = s.sessionId
			request.StatementId = s.requestStatementId
			resp, err = s.client.ExecuteStatementV2(context.Background(), &request)
		}
	}
	if resp.IsSetDatabase() {
		s.changeDatabase(*resp.Database)
	}

	return resp.Status, err
}

func (s *Session) changeDatabase(database string) {
	s.config.Database = database
}

func (s *Session) ExecuteQueryStatement(sql string, timeoutMs *int64) (*SessionDataSet, error) {
	request := rpc.TSExecuteStatementReq{
		SessionId: s.sessionId, Statement: sql, StatementId: s.requestStatementId,
		FetchSize: &s.config.FetchSize, Timeout: timeoutMs,
	}
	if resp, err := s.client.ExecuteQueryStatementV2(context.Background(), &request); err == nil {
		if statusErr := VerifySuccess(resp.Status); statusErr == nil {
			return NewSessionDataSet(sql, resp.Columns, resp.DataTypeList, resp.ColumnNameIndexMap, *resp.QueryId, s.requestStatementId, s.client, s.sessionId, resp.QueryResult_, resp.IgnoreTimeStamp != nil && *resp.IgnoreTimeStamp, timeoutMs, *resp.MoreData, s.config.FetchSize, s.config.TimeZone, s.timeFactor, resp.ColumnIndex2TsBlockColumnIndexList)
		} else {
			return nil, statusErr
		}
	} else {
		if s.reconnect() {
			request.SessionId = s.sessionId
			request.StatementId = s.requestStatementId
			resp, err = s.client.ExecuteQueryStatementV2(context.Background(), &request)
			if statusErr := VerifySuccess(resp.Status); statusErr == nil {
				return NewSessionDataSet(sql, resp.Columns, resp.DataTypeList, resp.ColumnNameIndexMap, *resp.QueryId, s.requestStatementId, s.client, s.sessionId, resp.QueryResult_, resp.IgnoreTimeStamp != nil && *resp.IgnoreTimeStamp, timeoutMs, *resp.MoreData, s.config.FetchSize, s.config.TimeZone, s.timeFactor, resp.GetColumnIndex2TsBlockColumnIndexList())
			} else {
				return nil, statusErr
			}
		}
		return nil, err
	}
}

func (s *Session) ExecuteAggregationQuery(paths []string, aggregations []common.TAggregationType,
	startTime *int64, endTime *int64, interval *int64,
	timeoutMs *int64,
) (*SessionDataSet, error) {
	request := rpc.TSAggregationQueryReq{
		SessionId: s.sessionId, StatementId: s.requestStatementId, Paths: paths,
		Aggregations: aggregations, StartTime: startTime, EndTime: endTime, Interval: interval, FetchSize: &s.config.FetchSize, Timeout: timeoutMs,
	}
	if resp, err := s.client.ExecuteAggregationQueryV2(context.Background(), &request); err == nil {
		if statusErr := VerifySuccess(resp.Status); statusErr == nil {
			return NewSessionDataSet("", resp.Columns, resp.DataTypeList, resp.ColumnNameIndexMap, *resp.QueryId, s.requestStatementId, s.client, s.sessionId, resp.QueryResult_, resp.IgnoreTimeStamp != nil && *resp.IgnoreTimeStamp, timeoutMs, *resp.MoreData, s.config.FetchSize, s.config.TimeZone, s.timeFactor, resp.GetColumnIndex2TsBlockColumnIndexList())
		} else {
			return nil, statusErr
		}
	} else {
		if s.reconnect() {
			request.SessionId = s.sessionId
			resp, err = s.client.ExecuteAggregationQueryV2(context.Background(), &request)
			if statusErr := VerifySuccess(resp.Status); statusErr == nil {
				return NewSessionDataSet("", resp.Columns, resp.DataTypeList, resp.ColumnNameIndexMap, *resp.QueryId, s.requestStatementId, s.client, s.sessionId, resp.QueryResult_, resp.IgnoreTimeStamp != nil && *resp.IgnoreTimeStamp, timeoutMs, *resp.MoreData, s.config.FetchSize, s.config.TimeZone, s.timeFactor, resp.GetColumnIndex2TsBlockColumnIndexList())
			} else {
				return nil, statusErr
			}
		}
		return nil, err
	}
}

func (s *Session) ExecuteAggregationQueryWithLegalNodes(paths []string, aggregations []common.TAggregationType,
	startTime *int64, endTime *int64, interval *int64,
	timeoutMs *int64, legalNodes *bool,
) (*SessionDataSet, error) {
	request := rpc.TSAggregationQueryReq{
		SessionId: s.sessionId, StatementId: s.requestStatementId, Paths: paths,
		Aggregations: aggregations, StartTime: startTime, EndTime: endTime, Interval: interval, FetchSize: &s.config.FetchSize,
		Timeout: timeoutMs, LegalPathNodes: legalNodes,
	}
	if resp, err := s.client.ExecuteAggregationQueryV2(context.Background(), &request); err == nil {
		if statusErr := VerifySuccess(resp.Status); statusErr == nil {
			return NewSessionDataSet("", resp.Columns, resp.DataTypeList, resp.ColumnNameIndexMap, *resp.QueryId, s.requestStatementId, s.client, s.sessionId, resp.QueryResult_, resp.IgnoreTimeStamp != nil && *resp.IgnoreTimeStamp, timeoutMs, *resp.MoreData, s.config.FetchSize, s.config.TimeZone, s.timeFactor, resp.GetColumnIndex2TsBlockColumnIndexList())
		} else {
			return nil, statusErr
		}
	} else {
		if s.reconnect() {
			request.SessionId = s.sessionId
			resp, err = s.client.ExecuteAggregationQueryV2(context.Background(), &request)
			if statusErr := VerifySuccess(resp.Status); statusErr == nil {
				return NewSessionDataSet("", resp.Columns, resp.DataTypeList, resp.ColumnNameIndexMap, *resp.QueryId, s.requestStatementId, s.client, s.sessionId, resp.QueryResult_, resp.IgnoreTimeStamp != nil && *resp.IgnoreTimeStamp, timeoutMs, *resp.MoreData, s.config.FetchSize, s.config.TimeZone, s.timeFactor, resp.GetColumnIndex2TsBlockColumnIndexList())
			} else {
				return nil, statusErr
			}
		}
		return nil, err
	}
}

func (s *Session) genTSInsertRecordReq(deviceId string, time int64,
	measurements []string,
	types []TSDataType,
	values []interface{},
	isAligned bool,
) (*rpc.TSInsertRecordReq, error) {
	request := &rpc.TSInsertRecordReq{}
	request.SessionId = s.sessionId
	request.PrefixPath = deviceId
	request.Timestamp = time
	request.Measurements = measurements
	request.IsAligned = &isAligned
	if bys, err := valuesToBytes(types, values); err == nil {
		request.Values = bys
	} else {
		return nil, err
	}
	return request, nil
}

func (s *Session) InsertRecord(deviceId string, measurements []string, dataTypes []TSDataType, values []interface{}, timestamp int64) (r *common.TSStatus, err error) {
	request, err := s.genTSInsertRecordReq(deviceId, timestamp, measurements, dataTypes, values, false)
	if err != nil {
		return nil, err
	}
	r, err = s.client.InsertRecord(context.Background(), request)

	if err != nil && r == nil {
		if s.reconnect() {
			request.SessionId = s.sessionId
			r, err = s.client.InsertRecord(context.Background(), request)
		}
	}

	return r, err
}

func (s *Session) InsertAlignedRecord(deviceId string, measurements []string, dataTypes []TSDataType, values []interface{}, timestamp int64) (r *common.TSStatus, err error) {
	request, err := s.genTSInsertRecordReq(deviceId, timestamp, measurements, dataTypes, values, true)
	if err != nil {
		return nil, err
	}
	r, err = s.client.InsertRecord(context.Background(), request)

	if err != nil && r == nil {
		if s.reconnect() {
			request.SessionId = s.sessionId
			r, err = s.client.InsertRecord(context.Background(), request)
		}
	}

	return r, err
}

type deviceData struct {
	timestamps        []int64
	measurementsSlice [][]string
	dataTypesSlice    [][]TSDataType
	valuesSlice       [][]interface{}
	isAligned         bool
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
// Each row is independent, which could have different insertTargetName, time, number of measurements
func (s *Session) InsertRecordsOfOneDevice(deviceId string, timestamps []int64, measurementsSlice [][]string, dataTypesSlice [][]TSDataType, valuesSlice [][]interface{}, sorted bool) (r *common.TSStatus, err error) {
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
		PrefixPath:       deviceId,
		Timestamps:       timestamps,
		MeasurementsList: measurementsSlice,
		ValuesList:       valuesList,
	}

	r, err = s.client.InsertRecordsOfOneDevice(context.Background(), request)

	if err != nil && r == nil {
		if s.reconnect() {
			request.SessionId = s.sessionId
			r, err = s.client.InsertRecordsOfOneDevice(context.Background(), request)
		}
	}

	return r, err
}

func (s *Session) InsertAlignedRecordsOfOneDevice(deviceId string, timestamps []int64, measurementsSlice [][]string, dataTypesSlice [][]TSDataType, valuesSlice [][]interface{}, sorted bool) (r *common.TSStatus, err error) {
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
	isAligned := true
	request := &rpc.TSInsertRecordsOfOneDeviceReq{
		SessionId:        s.sessionId,
		PrefixPath:       deviceId,
		Timestamps:       timestamps,
		MeasurementsList: measurementsSlice,
		ValuesList:       valuesList,
		IsAligned:        &isAligned,
	}

	r, err = s.client.InsertRecordsOfOneDevice(context.Background(), request)

	if err != nil && r == nil {
		if s.reconnect() {
			request.SessionId = s.sessionId
			r, err = s.client.InsertRecordsOfOneDevice(context.Background(), request)
		}
	}

	return r, err
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
	timestamps []int64,
) (r *common.TSStatus, err error) {
	request, err := s.genInsertRecordsReq(deviceIds, measurements, dataTypes, values, timestamps, false)
	if err != nil {
		return nil, err
	} else {
		r, err = s.client.InsertRecords(context.Background(), request)
		if err != nil && r == nil {
			if s.reconnect() {
				request.SessionId = s.sessionId
				r, err = s.client.InsertRecords(context.Background(), request)
			}
		}
		return r, err
	}
}

func (s *Session) InsertAlignedRecords(deviceIds []string, measurements [][]string, dataTypes [][]TSDataType, values [][]interface{},
	timestamps []int64,
) (r *common.TSStatus, err error) {
	request, err := s.genInsertRecordsReq(deviceIds, measurements, dataTypes, values, timestamps, true)
	if err != nil {
		return nil, err
	} else {
		r, err = s.client.InsertRecords(context.Background(), request)
		if err != nil && r == nil {
			if s.reconnect() {
				request.SessionId = s.sessionId
				r, err = s.client.InsertRecords(context.Background(), request)
			}
		}
		return r, err
	}
}

/*
 * InsertTablets insert multiple tablets, tablets are independent to each other
 *params
 *tablets: []*client.Tablet, list of tablets
 */
func (s *Session) InsertTablets(tablets []*Tablet, sorted bool) (r *common.TSStatus, err error) {
	if !sorted {
		for _, t := range tablets {
			if err := t.Sort(); err != nil {
				return nil, err
			}
		}
	}
	request, err := s.genInsertTabletsReq(tablets, false)
	if err != nil {
		return nil, err
	}
	r, err = s.client.InsertTablets(context.Background(), request)
	if err != nil && r == nil {
		if s.reconnect() {
			request.SessionId = s.sessionId
			r, err = s.client.InsertTablets(context.Background(), request)
		}
	}
	return r, err
}

func (s *Session) InsertAlignedTablets(tablets []*Tablet, sorted bool) (r *common.TSStatus, err error) {
	if !sorted {
		for _, t := range tablets {
			if err := t.Sort(); err != nil {
				return nil, err
			}
		}
	}
	request, err := s.genInsertTabletsReq(tablets, true)
	if err != nil {
		return nil, err
	}
	r, err = s.client.InsertTablets(context.Background(), request)
	if err != nil && r == nil {
		if s.reconnect() {
			request.SessionId = s.sessionId
			r, err = s.client.InsertTablets(context.Background(), request)
		}
	}
	return r, err
}

func (s *Session) ExecuteBatchStatement(inserts []string) (r *common.TSStatus, err error) {
	request := rpc.TSExecuteBatchStatementReq{
		SessionId:  s.sessionId,
		Statements: inserts,
	}
	r, err = s.client.ExecuteBatchStatement(context.Background(), &request)
	if err != nil && r == nil {
		if s.reconnect() {
			request.SessionId = s.sessionId
			r, err = s.client.ExecuteBatchStatement(context.Background(), &request)
		}
	}
	return r, err
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
	resp, err := s.client.ExecuteRawDataQueryV2(context.Background(), &request)

	if err != nil && resp == nil {
		if s.reconnect() {
			request.SessionId = s.sessionId
			request.StatementId = s.requestStatementId
			resp, err = s.client.ExecuteRawDataQueryV2(context.Background(), &request)
		}
	}

	return s.genDataSet("", resp)
}

func (s *Session) ExecuteUpdateStatement(sql string) (*SessionDataSet, error) {
	request := rpc.TSExecuteStatementReq{
		SessionId:   s.sessionId,
		Statement:   sql,
		StatementId: s.requestStatementId,
		FetchSize:   &s.config.FetchSize,
	}
	resp, err := s.client.ExecuteUpdateStatementV2(context.Background(), &request)

	if err != nil && resp == nil {
		if s.reconnect() {
			request.SessionId = s.sessionId
			request.StatementId = s.requestStatementId
			resp, err = s.client.ExecuteUpdateStatementV2(context.Background(), &request)
		}
	}

	return s.genDataSet(sql, resp)
}

func (s *Session) genDataSet(sql string, resp *rpc.TSExecuteStatementResp) (*SessionDataSet, error) {
	var queryId int64
	if resp.QueryId == nil {
		queryId = 0
	} else {
		queryId = *resp.QueryId
	}
	moreData := false
	if resp.MoreData != nil {
		moreData = *resp.MoreData
	}
	return NewSessionDataSet(sql, resp.Columns, resp.DataTypeList, resp.ColumnNameIndexMap,
		queryId, s.requestStatementId, s.client, s.sessionId, resp.QueryResult_, resp.IgnoreTimeStamp != nil && *resp.IgnoreTimeStamp, nil, moreData, s.config.FetchSize, s.config.TimeZone, s.timeFactor, resp.GetColumnIndex2TsBlockColumnIndexList())
}

func (s *Session) genInsertTabletsReq(tablets []*Tablet, isAligned bool) (*rpc.TSInsertTabletsReq, error) {
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
		deviceIds[index] = tablet.insertTargetName
		measurementsList[index] = tablet.GetMeasurements()

		values, err := tablet.getValuesBytes()
		if err != nil {
			return nil, err
		}

		valuesList[index] = values
		timestampsList[index] = tablet.GetTimestampBytes()
		typesList[index] = tablet.getDataTypes()
		sizeList[index] = int32(tablet.RowSize)
	}
	request := rpc.TSInsertTabletsReq{
		SessionId:        s.sessionId,
		PrefixPaths:      deviceIds,
		TypesList:        typesList,
		MeasurementsList: measurementsList,
		ValuesList:       valuesList,
		TimestampsList:   timestampsList,
		SizeList:         sizeList,
		IsAligned:        &isAligned,
	}
	return &request, nil
}

func (s *Session) genInsertRecordsReq(deviceIds []string, measurements [][]string, dataTypes [][]TSDataType, values [][]interface{},
	timestamps []int64, isAligned bool,
) (*rpc.TSInsertRecordsReq, error) {
	length := len(deviceIds)
	if length != len(timestamps) || length != len(measurements) || length != len(values) {
		return nil, errLength
	}
	request := rpc.TSInsertRecordsReq{
		SessionId:        s.sessionId,
		PrefixPaths:      deviceIds,
		MeasurementsList: measurements,
		Timestamps:       timestamps,
		IsAligned:        &isAligned,
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
		case INT64, TIMESTAMP:
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
		case TEXT, STRING:
			switch s := v.(type) {
			case string:
				size := len(s)
				binary.Write(buff, binary.BigEndian, int32(size))
				binary.Write(buff, binary.BigEndian, []byte(s))
			case []byte:
				size := len(s)
				binary.Write(buff, binary.BigEndian, int32(size))
				binary.Write(buff, binary.BigEndian, s)
			default:
				return nil, fmt.Errorf("values[%d] %v(%v) must be string or []byte", i, v, reflect.TypeOf(v))
			}
		case BLOB:
			switch s := v.(type) {
			case []byte:
				size := len(s)
				binary.Write(buff, binary.BigEndian, int32(size))
				binary.Write(buff, binary.BigEndian, s)
			default:
				return nil, fmt.Errorf("values[%d] %v(%v) must be []byte", i, v, reflect.TypeOf(v))
			}
		case DATE:
			switch s := v.(type) {
			case time.Time:
				date, err := DateToInt32(s)
				if err != nil {
					return nil, err
				}
				binary.Write(buff, binary.BigEndian, date)
			default:
				return nil, fmt.Errorf("values[%d] %v(%v) must be time.Time", i, v, reflect.TypeOf(v))
			}
		default:
			return nil, fmt.Errorf("types[%d] is incorrect, it must in (BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT, TIMESTAMP, BLOB, DATE, STRING)", i)
		}
	}
	return buff.Bytes(), nil
}

func (s *Session) insertRelationalTablet(tablet *Tablet) (r *common.TSStatus, err error) {
	if tablet.Len() == 0 {
		return &common.TSStatus{Code: SuccessStatus}, nil
	}
	request, err := s.genTSInsertTabletReq(tablet, true, true)
	if err != nil {
		return nil, err
	}
	request.ColumnCategories = tablet.getColumnCategories()

	r, err = s.client.InsertTablet(context.Background(), request)

	if err != nil && r == nil {
		if s.reconnect() {
			request.SessionId = s.sessionId
			r, err = s.client.InsertTablet(context.Background(), request)
		}
	}

	return r, err
}

func (s *Session) InsertTablet(tablet *Tablet, sorted bool) (r *common.TSStatus, err error) {
	if !sorted {
		if err := tablet.Sort(); err != nil {
			return nil, err
		}
	}
	request, err := s.genTSInsertTabletReq(tablet, false, false)
	if err != nil {
		return nil, err
	}

	r, err = s.client.InsertTablet(context.Background(), request)

	if err != nil && r == nil {
		if s.reconnect() {
			request.SessionId = s.sessionId
			r, err = s.client.InsertTablet(context.Background(), request)
		}
	}

	return r, err
}

func (s *Session) InsertAlignedTablet(tablet *Tablet, sorted bool) (r *common.TSStatus, err error) {
	if !sorted {
		if err := tablet.Sort(); err != nil {
			return nil, err
		}
	}
	request, err := s.genTSInsertTabletReq(tablet, true, false)
	if err != nil {
		return nil, err
	}

	r, err = s.client.InsertTablet(context.Background(), request)

	if err != nil && r == nil {
		if s.reconnect() {
			request.SessionId = s.sessionId
			r, err = s.client.InsertTablet(context.Background(), request)
		}
	}

	return r, err
}

func (s *Session) genTSInsertTabletReq(tablet *Tablet, isAligned bool, writeToTable bool) (*rpc.TSInsertTabletReq, error) {
	if values, err := tablet.getValuesBytes(); err == nil {
		request := &rpc.TSInsertTabletReq{
			SessionId:    s.sessionId,
			PrefixPath:   tablet.insertTargetName,
			Measurements: tablet.GetMeasurements(),
			Values:       values,
			Timestamps:   tablet.GetTimestampBytes(),
			Types:        tablet.getDataTypes(),
			Size:         int32(tablet.RowSize),
			IsAligned:    &isAligned,
			WriteToTable: &writeToTable,
		}
		return request, nil
	} else {
		return nil, err
	}
}

func (s *Session) GetSessionId() int64 {
	return s.sessionId
}

func NewSession(config *Config) Session {
	config.sqlDialect = TreeSqlDialect
	return newSessionWithSpecifiedSqlDialect(config)
}

func newSessionWithSpecifiedSqlDialect(config *Config) Session {
	endPointList := []endPoint{{
		Host: config.Host,
		Port: config.Port,
	}}
	return Session{
		config:       config,
		endPointList: endPointList,
	}
}

func NewClusterSession(clusterConfig *ClusterConfig) (Session, error) {
	clusterConfig.sqlDialect = TreeSqlDialect
	return newClusterSessionWithSqlDialect(clusterConfig)
}

func newClusterSessionWithSqlDialect(clusterConfig *ClusterConfig) (Session, error) {
	session := Session{}
	session.endPointList = make([]endPoint, len(clusterConfig.NodeUrls))
	for i := 0; i < len(clusterConfig.NodeUrls); i++ {
		node := endPoint{}
		node.Host = strings.Split(clusterConfig.NodeUrls[i], ":")[0]
		node.Port = strings.Split(clusterConfig.NodeUrls[i], ":")[1]
		session.endPointList[i] = node
	}
	var err error
	for i := range session.endPointList {
		ep := session.endPointList[i]
		session.trans = thrift.NewTSocketConf(net.JoinHostPort(ep.Host, ep.Port), &thrift.TConfiguration{
			ConnectTimeout: time.Duration(0), // Use 0 for no timeout
		})
		// session.trans = thrift.NewTFramedTransport(session.trans)	// deprecated
		tmp_conf := thrift.TConfiguration{MaxFrameSize: thrift.DEFAULT_MAX_FRAME_SIZE}
		session.trans = thrift.NewTFramedTransportConf(session.trans, &tmp_conf)
		if !session.trans.IsOpen() {
			err = session.trans.Open()
			if err != nil {
				log.Println(err)
			} else {
				session.config = getConfig(ep.Host, ep.Port,
					clusterConfig.UserName, clusterConfig.Password, clusterConfig.FetchSize, clusterConfig.TimeZone, clusterConfig.ConnectRetryMax, clusterConfig.Database, clusterConfig.sqlDialect)
				break
			}
		}
	}
	if !session.trans.IsOpen() {
		return session, fmt.Errorf("no server can connect")
	}
	return session, nil
}

func (s *Session) initClusterConn(node endPoint) error {
	var err error

	s.trans = thrift.NewTSocketConf(net.JoinHostPort(node.Host, node.Port), &thrift.TConfiguration{
		ConnectTimeout: time.Duration(0), // Use 0 for no timeout
	})
	if err == nil {
		// s.trans = thrift.NewTFramedTransport(s.trans)	// deprecated
		tmp_conf := thrift.TConfiguration{MaxFrameSize: thrift.DEFAULT_MAX_FRAME_SIZE}
		s.trans = thrift.NewTFramedTransportConf(s.trans, &tmp_conf)
		if !s.trans.IsOpen() {
			err = s.trans.Open()
			if err != nil {
				return err
			}
		}
	}

	if s.config.FetchSize < 1 {
		s.config.FetchSize = DefaultFetchSize
	}

	if s.config.TimeZone == "" {
		s.config.TimeZone = DefaultTimeZone
	}

	if s.config.ConnectRetryMax < 1 {
		s.config.ConnectRetryMax = DefaultConnectRetryMax
	}

	iprot := s.protocolFactory.GetProtocol(s.trans)
	oprot := s.protocolFactory.GetProtocol(s.trans)
	s.client = rpc.NewIClientRPCServiceClient(thrift.NewTStandardClient(iprot, oprot))
	req := rpc.TSOpenSessionReq{
		ClientProtocol: rpc.TSProtocolVersion_IOTDB_SERVICE_PROTOCOL_V3, ZoneId: s.config.TimeZone, Username: s.config.UserName,
		Password: &s.config.Password,
	}

	resp, err := s.client.OpenSession(context.Background(), &req)
	if err != nil {
		return err
	}
	s.sessionId = resp.GetSessionId()
	s.requestStatementId, err = s.client.RequestStatementId(context.Background(), s.sessionId)
	return err
}

func getConfig(host string, port string, userName string, passWord string, fetchSize int32, timeZone string, connectRetryMax int, database string, sqlDialect string) *Config {
	return &Config{
		Host:            host,
		Port:            port,
		UserName:        userName,
		Password:        passWord,
		FetchSize:       fetchSize,
		TimeZone:        timeZone,
		ConnectRetryMax: connectRetryMax,
		sqlDialect:      sqlDialect,
		Database:        database,
	}
}

func (s *Session) reconnect() bool {
	var err error
	connectedSuccess := false

	for i := 0; i < s.config.ConnectRetryMax; i++ {
		for i := range s.endPointList {
			ep := s.endPointList[i]
			err = s.initClusterConn(ep)
			if err == nil {
				connectedSuccess = true
				break
			} else {
				log.Println("Connection refused:", ep)
			}
		}
		if connectedSuccess {
			break
		}
	}
	return connectedSuccess
}

func (s *Session) SetFetchSize(fetchSize int32) {
	s.config.FetchSize = fetchSize
}
