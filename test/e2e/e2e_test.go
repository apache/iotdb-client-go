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

package e2e

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/apache/iotdb-client-go/v2/client"
	"github.com/apache/iotdb-client-go/v2/common"
)

type e2eTestSuite struct {
	suite.Suite
	session client.Session
}

func TestE2ETestSuite(t *testing.T) {
	suite.Run(t, &e2eTestSuite{})
}

func (s *e2eTestSuite) SetupSuite() {
	clusterConfig := client.ClusterConfig{
		NodeUrls: strings.Split("iotdb:6668,iotdb:6667,iotdb:6669", ","),
		UserName: "root",
		Password: "root",
	}
	session, err := client.NewClusterSession(&clusterConfig)
	s.Require().NoError(err)
	s.session = session
	err = s.session.Open(false, 0)
	s.Require().NoError(err)
}

func (s *e2eTestSuite) TearDownSuite() {
	s.session.Close()
}

func (s *e2eTestSuite) SetupTest() {
	r, err := s.session.SetStorageGroup("root.tsg1")
	s.checkError(r, err)
}

func (s *e2eTestSuite) TearDownTest() {
	r, err := s.session.DeleteStorageGroup("root.tsg1")
	s.checkError(r, err)
}

func (s *e2eTestSuite) checkError(status *common.TSStatus, err error) {
	s.Require().NoError(err)
	if status != nil {
		s.Require().NoError(client.VerifySuccess(status))
	}
}

func (s *e2eTestSuite) Test_WrongURL() {
	clusterConfig := client.ClusterConfig{
		NodeUrls: strings.Split("iotdb1:6667", ","),
		UserName: "root",
		Password: "root",
	}
	_, err := client.NewClusterSession(&clusterConfig)
	s.Require().Error(err)
}

func (s *e2eTestSuite) Test_NonQuery() {
	_, err := s.session.ExecuteStatement("flush")
	s.Require().NoError(err)
}

func (s *e2eTestSuite) Test_CreateTimeseries() {
	var (
		path       = "root.tsg1.dev1.status"
		dataType   = client.FLOAT
		encoding   = client.PLAIN
		compressor = client.SNAPPY
	)
	s.checkError(s.session.CreateTimeseries(path, dataType, encoding, compressor, nil, nil))
	ds, err := s.session.ExecuteQueryStatement("show timeseries root.tsg1.dev1.status", nil)

	assert := s.Require()

	assert.NoError(err)
	defer ds.Close()
	assert.True(ds.Next())
	timeseries, err := ds.GetStringByIndex(1)
	assert.NoError(err)
	assert.Equal(timeseries, "root.tsg1.dev1.status")
}

func (s *e2eTestSuite) Test_CreateAlignedTimeseries() {
	var (
		prefixPath       = "root.tsg1.dev1"
		measurements     = []string{"status", "temperature"}
		measurementAlias = []string{"sts", "temp"}
		dataTypes        = []client.TSDataType{
			client.FLOAT,
			client.FLOAT,
		}
		encodings = []client.TSEncoding{
			client.PLAIN,
			client.PLAIN,
		}
		compressors = []client.TSCompressionType{
			client.LZ4,
			client.LZ4,
		}
	)
	s.checkError(s.session.CreateAlignedTimeseries(prefixPath, measurements, dataTypes, encodings, compressors, measurementAlias))
	for i := range measurements {
		fullPath := fmt.Sprintf("root.tsg1.dev1.%s", measurements[i])
		ds, err := s.session.ExecuteQueryStatement(fmt.Sprintf("show timeseries %s", fullPath), nil)

		assert := s.Require()

		assert.NoError(err)
		defer ds.Close()
		assert.True(ds.Next())
		timeseries, err := ds.GetStringByIndex(1)
		assert.NoError(err)
		assert.Equal(timeseries, fullPath)
	}
}

func (s *e2eTestSuite) Test_InsertRecords() {
	var (
		deviceId     = []string{"root.tsg1.dev1"}
		measurements = [][]string{{"status"}}
		dataTypes    = [][]client.TSDataType{{client.TEXT}}
		values       = [][]interface{}{{"Working"}}
		timestamp    = []int64{time.Now().UTC().UnixNano() / 1000000}
	)
	s.checkError(s.session.InsertRecords(deviceId, measurements, dataTypes, values, timestamp))

	ds, err := s.session.ExecuteQueryStatement("select status from root.tsg1.dev1", nil)
	assert := s.Require()
	assert.NoError(err)
	defer ds.Close()
	assert.True(ds.Next())
	status, err := ds.GetString("root.tsg1.dev1.status")
	assert.NoError(err)
	assert.Equal(status, "Working")
}

func (s *e2eTestSuite) Test_InsertAlignedRecord() {
	var (
		deviceId     = "root.tsg2.dev1"
		measurements = []string{"status"}
		dataTypes    = []client.TSDataType{client.TEXT}
		values       = []interface{}{"Working"}
		timestamp    = time.Now().UTC().UnixNano() / 1000000
	)
	s.checkError(s.session.InsertAlignedRecord(deviceId, measurements, dataTypes, values, timestamp))

	ds, err := s.session.ExecuteQueryStatement("select status from root.tsg2.dev1", nil)
	assert := s.Require()
	assert.NoError(err)
	defer ds.Close()
	assert.True(ds.Next())
	status, err := ds.GetString("root.tsg2.dev1.status")
	assert.NoError(err)
	assert.Equal("Working", status)
}

func (s *e2eTestSuite) Test_InsertAlignedRecords() {
	var (
		deviceIds    = []string{"root.al1.dev2", "root.al1.dev3"}
		measurements = [][]string{{"status"}, {"temperature"}}
		dataTypes    = [][]client.TSDataType{{client.TEXT}, {client.STRING}}
		values       = [][]interface{}{{"33"}, {"44"}}
		timestamps   = []int64{12, 13}
	)
	s.checkError(s.session.InsertAlignedRecords(deviceIds, measurements, dataTypes, values, timestamps))
	ds, err := s.session.ExecuteQueryStatement("select temperature from root.al1.dev3", nil)
	assert := s.Require()
	assert.NoError(err)
	defer ds.Close()
	assert.True(ds.Next())
	temperature, err := ds.GetString("root.al1.dev3.temperature")
	assert.NoError(err)
	assert.Equal(temperature, "44")
}

func (s *e2eTestSuite) Test_InsertAlignedRecordsOfOneDevice() {
	ts := time.Now().UTC().UnixNano() / 1000000
	var (
		deviceId          = "root.al1.dev4"
		measurementsSlice = [][]string{
			{"restart_count", "tick_count", "price"},
			{"temperature", "description", "status"},
			{"description_blob", "date", "ts"},
		}
		dataTypes = [][]client.TSDataType{
			{client.INT32, client.INT64, client.DOUBLE},
			{client.FLOAT, client.TEXT, client.BOOLEAN},
			{client.BLOB, client.DATE, client.TIMESTAMP},
		}
		values = [][]interface{}{
			{int32(1), int64(2018), float64(1988.1)},
			{float32(12.1), "Test Device 1", false},
			{[]byte("Test Device 1"), time.Date(2024, time.Month(4), 1, 0, 0, 0, 0, time.UTC), ts},
		}
		timestamps = []int64{ts, ts - 1, ts - 2}
	)
	s.checkError(s.session.InsertAlignedRecordsOfOneDevice(deviceId, timestamps, measurementsSlice, dataTypes, values, false))
	ds, err := s.session.ExecuteQueryStatement("select * from root.al1.dev4", nil)
	assert := s.Require()
	assert.NoError(err)
	defer ds.Close()
	assert.True(ds.Next())
	date, err := ds.GetString("root.al1.dev4.date")
	assert.NoError(err)
	assert.Equal("2024-04-01", date)
}

func (s *e2eTestSuite) Test_InsertAlignedTablet() {
	var timeseries = []string{"root.ln.device1.**"}
	s.session.DeleteTimeseries(timeseries)
	if tablet, err := createTablet(12); err == nil {
		status, err := s.session.InsertAlignedTablet(tablet, false)
		s.checkError(status, err)
		tablet.Reset()
	} else {
		log.Fatal(err)
	}
	var timeout int64 = 1000
	ds, err := s.session.ExecuteQueryStatement("select count(status) from root.ln.device1", &timeout)
	assert := s.Require()
	assert.NoError(err)
	defer ds.Close()
	assert.True(ds.Next())
	status, err := ds.GetStringByIndex(1)
	assert.NoError(err)
	assert.Equal(status, "12")
	s.session.DeleteStorageGroup("root.ln.**")
}

func (s *e2eTestSuite) Test_InsertAlignedTabletWithNilValue() {
	var timeseries = []string{"root.ln.device1.**"}
	s.session.DeleteTimeseries(timeseries)
	if tablet, err := createTabletWithNil(12); err == nil {
		status, err := s.session.InsertAlignedTablet(tablet, false)
		s.checkError(status, err)
		tablet.Reset()
	} else {
		log.Fatal(err)
	}
	var timeout int64 = 1000
	ds, err := s.session.ExecuteQueryStatement("select count(status) from root.ln.device1", &timeout)
	assert := s.Require()
	assert.NoError(err)
	defer ds.Close()
	assert.True(ds.Next())
	status, err := ds.GetStringByIndex(1)
	assert.NoError(err)
	assert.Equal(status, "12")
	s.session.DeleteStorageGroup("root.ln.**")
}

func createTabletWithNil(rowCount int) (*client.Tablet, error) {
	tablet, err := client.NewTablet("root.ln.device1", []*client.MeasurementSchema{
		{
			Measurement: "restart_count",
			DataType:    client.INT32,
		}, {
			Measurement: "price",
			DataType:    client.DOUBLE,
		}, {
			Measurement: "tick_count",
			DataType:    client.INT64,
		}, {
			Measurement: "temperature",
			DataType:    client.FLOAT,
		}, {
			Measurement: "description",
			DataType:    client.TEXT,
		},
		{
			Measurement: "status",
			DataType:    client.BOOLEAN,
		},
	}, rowCount)

	if err != nil {
		return nil, err
	}
	ts := time.Now().UTC().UnixNano() / 1000000
	for row := 0; row < int(rowCount); row++ {
		ts++
		tablet.SetTimestamp(ts, row)
		tablet.SetValueAt(rand.Int31(), 0, row)
		if row%2 == 1 {
			tablet.SetValueAt(rand.Float64(), 1, row)
		} else {
			tablet.SetValueAt(nil, 1, row)
		}
		tablet.SetValueAt(rand.Int63(), 2, row)
		if row%3 == 1 {
			tablet.SetValueAt(rand.Float32(), 3, row)
		} else {
			tablet.SetValueAt(nil, 3, row)
		}
		tablet.SetValueAt(fmt.Sprintf("Test Device %d", row+1), 4, row)
		tablet.SetValueAt(bool(ts%2 == 0), 5, row)
		tablet.RowSize++
	}
	return tablet, nil
}

func createTablet(rowCount int) (*client.Tablet, error) {
	tablet, err := client.NewTablet("root.ln.device1", []*client.MeasurementSchema{
		{
			Measurement: "restart_count",
			DataType:    client.INT32,
		}, {
			Measurement: "price",
			DataType:    client.DOUBLE,
		}, {
			Measurement: "tick_count",
			DataType:    client.INT64,
		}, {
			Measurement: "temperature",
			DataType:    client.FLOAT,
		}, {
			Measurement: "description",
			DataType:    client.TEXT,
		},
		{
			Measurement: "status",
			DataType:    client.BOOLEAN,
		},
	}, rowCount)

	if err != nil {
		return nil, err
	}
	ts := time.Now().UTC().UnixNano() / 1000000
	for row := 0; row < int(rowCount); row++ {
		ts++
		tablet.SetTimestamp(ts, row)
		tablet.SetValueAt(rand.Int31(), 0, row)
		tablet.SetValueAt(rand.Float64(), 1, row)
		tablet.SetValueAt(rand.Int63(), 2, row)
		tablet.SetValueAt(rand.Float32(), 3, row)
		tablet.SetValueAt(fmt.Sprintf("Test Device %d", row+1), 4, row)
		tablet.SetValueAt(bool(ts%2 == 0), 5, row)
		tablet.RowSize++
	}
	return tablet, nil
}

func (s *e2eTestSuite) Test_InsertAlignedTablets() {
	var timeseries = []string{"root.ln.device1.**"}
	s.session.DeleteTimeseries(timeseries)
	tablet1, err := createTablet(8)
	if err != nil {
		log.Fatal(err)
	}
	tablet2, err := createTablet(4)
	if err != nil {
		log.Fatal(err)
	}

	tablets := []*client.Tablet{tablet1, tablet2}
	s.checkError(s.session.InsertAlignedTablets(tablets, false))
	tablet1.Reset()
	tablet2.Reset()

	ds, err := s.session.ExecuteQueryStatement("select count(status) from root.ln.device1", nil)
	assert := s.Require()
	assert.NoError(err)
	defer ds.Close()
	assert.True(ds.Next())
	status, err := ds.GetStringByIndex(1)
	assert.NoError(err)
	assert.Equal(status, "8")
	s.session.DeleteStorageGroup("root.ln.**")
}

func (s *e2eTestSuite) Test_FetchMoreData() {
	var timeseries = []string{"root.ln.device1.**"}
	s.session.SetFetchSize(1000)
	s.session.DeleteTimeseries(timeseries)
	writeCount := 10000
	tablet1, err := createTablet(writeCount)
	if err != nil {
		log.Fatal(err)
	}

	tablets := []*client.Tablet{tablet1}
	s.checkError(s.session.InsertAlignedTablets(tablets, false))

	ds, err := s.session.ExecuteQueryStatement("select * from root.ln.device1", nil)
	count := 0
	for {
		if hasNext, err := ds.Next(); err != nil || !hasNext {
			break
		}
		count += 1
	}
	s.Assert().Equal(writeCount, count)
	s.session.DeleteStorageGroup("root.ln.**")
}

func (s *e2eTestSuite) Test_QueryAllDataType() {
	measurementSchemas := []*client.MeasurementSchema{
		{
			Measurement: "s0",
			DataType:    client.BOOLEAN,
		},
		{
			Measurement: "s1",
			DataType:    client.INT32,
		},
		{
			Measurement: "s2",
			DataType:    client.INT64,
		},
		{
			Measurement: "s3",
			DataType:    client.FLOAT,
		},
		{
			Measurement: "s4",
			DataType:    client.DOUBLE,
		},
		{
			Measurement: "s5",
			DataType:    client.TEXT,
		},
		{
			Measurement: "s6",
			DataType:    client.TIMESTAMP,
		},
		{
			Measurement: "s7",
			DataType:    client.DATE,
		},
		{
			Measurement: "s8",
			DataType:    client.BLOB,
		},
		{
			Measurement: "s9",
			DataType:    client.STRING,
		},
	}
	tablet, err := client.NewTablet("root.tsg1.d1", measurementSchemas, 100)
	s.NoError(err)
	tablet.SetTimestamp(1, 0)
	tablet.SetValueAt(true, 0, 0)
	tablet.SetValueAt(int32(1), 1, 0)
	tablet.SetValueAt(int64(1), 2, 0)
	tablet.SetValueAt(float32(1), 3, 0)
	tablet.SetValueAt(float64(1), 4, 0)
	tablet.SetValueAt("text", 5, 0)
	tablet.SetValueAt(int64(1), 6, 0)
	expectedDate, _ := client.Int32ToDate(20250326)
	tablet.SetValueAt(expectedDate, 7, 0)
	tablet.SetValueAt([]byte{1}, 8, 0)
	tablet.SetValueAt("string", 9, 0)
	tablet.RowSize = 1

	r, err := s.session.InsertAlignedTablet(tablet, true)
	s.checkError(r, err)

	sessionDataSet, err := s.session.ExecuteQueryStatement("select s0, s1, s2, s3, s4, s5, s6, s7, s8, s9 from root.tsg1.d1 limit 1", nil)
	for {
		if hasNext, err := sessionDataSet.Next(); err != nil || !hasNext {
			break
		}
		for _, columnName := range sessionDataSet.GetColumnNames() {
			isNull, err := sessionDataSet.IsNull(columnName)
			s.NoError(err)
			s.False(isNull)
		}
		timeValue, err := sessionDataSet.GetLongByIndex(1)
		s.NoError(err)
		s.Equal(int64(1), timeValue)
		boolValue, err := sessionDataSet.GetBooleanByIndex(2)
		s.NoError(err)
		s.Equal(true, boolValue)

		intValue, err := sessionDataSet.GetIntByIndex(3)
		s.NoError(err)
		s.Equal(int32(1), intValue)

		longValue, err := sessionDataSet.GetLongByIndex(4)
		s.NoError(err)
		s.Equal(int64(1), longValue)

		floatValue, err := sessionDataSet.GetFloatByIndex(5)
		s.NoError(err)
		s.Equal(float32(1), floatValue)

		doubleValue, err := sessionDataSet.GetDoubleByIndex(6)
		s.NoError(err)
		s.Equal(float64(1), doubleValue)

		textValue, err := sessionDataSet.GetStringByIndex(7)
		s.NoError(err)
		s.Equal("text", textValue)

		timestampValue, err := sessionDataSet.GetTimestampByIndex(8)
		s.NoError(err)
		s.Equal(time.Unix(0, 1e6), timestampValue)

		dateValue, err := sessionDataSet.GetDateByIndex(9)
		s.NoError(err)
		s.Equal(expectedDate, dateValue)

		blobValue, err := sessionDataSet.GetBlobByIndex(10)
		s.NoError(err)
		s.Equal([]byte{1}, blobValue.GetValues())

		stringValue, err := sessionDataSet.GetStringByIndex(11)
		s.NoError(err)
		s.Equal("string", stringValue)
	}
	sessionDataSet.Close()

	sessionDataSet, err = s.session.ExecuteQueryStatement("select s0, s1, s2, s3, s4, s5, s6, s7, s8, s9 from root.tsg1.d1 limit 1", nil)
	for {
		if hasNext, err := sessionDataSet.Next(); err != nil || !hasNext {
			break
		}
		for _, columnName := range sessionDataSet.GetColumnNames() {
			isNull, err := sessionDataSet.IsNull(columnName)
			s.NoError(err)
			s.False(isNull)
		}
		timeValue, err := sessionDataSet.GetLong("Time")
		s.NoError(err)
		s.Equal(int64(1), timeValue)

		boolValue, err := sessionDataSet.GetBoolean("root.tsg1.d1.s0")
		s.NoError(err)
		s.Equal(true, boolValue)

		intValue, err := sessionDataSet.GetInt("root.tsg1.d1.s1")
		s.NoError(err)
		s.Equal(int32(1), intValue)

		longValue, err := sessionDataSet.GetLong("root.tsg1.d1.s2")
		s.NoError(err)
		s.Equal(int64(1), longValue)

		floatValue, err := sessionDataSet.GetFloat("root.tsg1.d1.s3")
		s.NoError(err)
		s.Equal(float32(1), floatValue)

		doubleValue, err := sessionDataSet.GetDouble("root.tsg1.d1.s4")
		s.NoError(err)
		s.Equal(float64(1), doubleValue)

		textValue, err := sessionDataSet.GetString("root.tsg1.d1.s5")
		s.NoError(err)
		s.Equal("text", textValue)

		timestampValue, err := sessionDataSet.GetTimestamp("root.tsg1.d1.s6")
		s.NoError(err)
		s.Equal(time.Unix(0, 1e6), timestampValue)

		dateValue, err := sessionDataSet.GetDate("root.tsg1.d1.s7")
		s.NoError(err)
		s.Equal(expectedDate, dateValue)

		blobValue, err := sessionDataSet.GetBlob("root.tsg1.d1.s8")
		s.NoError(err)
		s.Equal([]byte{1}, blobValue.GetValues())

		stringValue, err := sessionDataSet.GetString("root.tsg1.d1.s9")
		s.NoError(err)
		s.Equal("string", stringValue)
	}
	sessionDataSet.Close()

	sessionDataSet, err = s.session.ExecuteQueryStatement("select * from root.tsg1.d1 limit 1", nil)
	for {
		if hasNext, err := sessionDataSet.Next(); err != nil || !hasNext {
			break
		}
		for _, columnName := range sessionDataSet.GetColumnNames() {
			isNull, err := sessionDataSet.IsNull(columnName)
			s.NoError(err)
			s.False(isNull)
		}
		timeValue, err := sessionDataSet.GetObject("Time")
		s.NoError(err)
		s.Equal(time.Unix(0, 1*1e6), timeValue)

		boolValue, err := sessionDataSet.GetObject("root.tsg1.d1.s0")
		s.NoError(err)
		s.Equal(true, boolValue)

		intValue, err := sessionDataSet.GetObject("root.tsg1.d1.s1")
		s.NoError(err)
		s.Equal(int32(1), intValue)

		longValue, err := sessionDataSet.GetObject("root.tsg1.d1.s2")
		s.NoError(err)
		s.Equal(int64(1), longValue)

		floatValue, err := sessionDataSet.GetObject("root.tsg1.d1.s3")
		s.NoError(err)
		s.Equal(float32(1), floatValue)

		doubleValue, err := sessionDataSet.GetObject("root.tsg1.d1.s4")
		s.NoError(err)
		s.Equal(float64(1), doubleValue)

		textValue, err := sessionDataSet.GetObject("root.tsg1.d1.s5")
		s.NoError(err)
		s.Equal("text", textValue)

		timestampValue, err := sessionDataSet.GetObject("root.tsg1.d1.s6")
		s.NoError(err)
		s.Equal(time.Unix(0, 1*1e6), timestampValue)

		dateValue, err := sessionDataSet.GetObject("root.tsg1.d1.s7")
		s.NoError(err)
		expectedDate, _ := client.Int32ToDate(int32(20250326))
		s.Equal(expectedDate, dateValue)

		blobValue, err := sessionDataSet.GetObject("root.tsg1.d1.s8")
		s.NoError(err)
		s.Equal([]byte{1}, blobValue)

		stringValue, err := sessionDataSet.GetObject("root.tsg1.d1.s9")
		s.NoError(err)
		s.Equal("string", stringValue)
	}
	sessionDataSet.Close()
}

func (s *e2eTestSuite) Test_InvalidSQL() {
	_, err := s.session.ExecuteStatementWithContext(context.Background(), "select1 from device")
	assert := s.Require()
	assert.Error(err)
}
