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
	"fmt"
	"log"
	"math/rand"
	"testing"
	"time"

	"github.com/apache/iotdb-client-go/client"
	"github.com/apache/iotdb-client-go/rpc"
	"github.com/stretchr/testify/suite"
)

type e2eTestSuite struct {
	suite.Suite
	session client.Session
}

func TestE2ETestSuite(t *testing.T) {
	suite.Run(t, &e2eTestSuite{})
}

func (s *e2eTestSuite) SetupSuite() {
	config := &client.Config{
		Host:     "iotdb",
		Port:     "6667",
		UserName: "root",
		Password: "root",
	}

	s.session = client.NewSession(config)
	err := s.session.Open(false, 0)
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

func (s *e2eTestSuite) checkError(status *rpc.TSStatus, err error) {
	s.Require().NoError(err)
	if status != nil {
		s.Require().NoError(client.VerifySuccess(status))
	}
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
	var timeseries string
	assert.NoError(ds.Scan(&timeseries))
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
		var timeseries string
		assert.NoError(ds.Scan(&timeseries))
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
	var status string
	assert.NoError(ds.Scan(&status))
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
	var status string
	assert.NoError(ds.Scan(&status))
	assert.Equal(status, "Working")
}

func (s *e2eTestSuite) Test_InsertAlignedRecords() {
	var (
		deviceIds    = []string{"root.al1.dev2", "root.al1.dev3"}
		measurements = [][]string{{"status"}, {"temperature"}}
		dataTypes    = [][]client.TSDataType{{client.TEXT}, {client.TEXT}}
		values       = [][]interface{}{{"33"}, {"44"}}
		timestamps   = []int64{12, 13}
	)
	s.checkError(s.session.InsertAlignedRecords(deviceIds, measurements, dataTypes, values, timestamps))
	ds, err := s.session.ExecuteQueryStatement("select temperature from root.al1.dev3", nil)
	assert := s.Require()
	assert.NoError(err)
	defer ds.Close()
	assert.True(ds.Next())
	var temperature string
	assert.NoError(ds.Scan(&temperature))
	assert.Equal(temperature, "44")
}

func (s *e2eTestSuite) Test_InsertAlignedRecordsOfOneDevice() {
	ts := time.Now().UTC().UnixNano() / 1000000
	var (
		deviceId          = "root.al1.dev4"
		measurementsSlice = [][]string{
			{"restart_count", "tick_count", "price"},
			{"temperature", "description", "status"},
		}
		dataTypes = [][]client.TSDataType{
			{client.INT32, client.INT64, client.DOUBLE},
			{client.FLOAT, client.TEXT, client.BOOLEAN},
		}
		values = [][]interface{}{
			{int32(1), int64(2018), float64(1988.1)},
			{float32(12.1), "Test Device 1", false},
		}
		timestamps = []int64{ts, ts - 1}
	)
	s.checkError(s.session.InsertAlignedRecordsOfOneDevice(deviceId, timestamps, measurementsSlice, dataTypes, values, false))
	ds, err := s.session.ExecuteStatement("select temperature from root.al1.dev4")
	assert := s.Require()
	assert.NoError(err)
	defer ds.Close()
	assert.True(ds.Next())
	var status string
	assert.NoError(ds.Scan(&status))
	assert.Equal(status, "12.1")
}
func (s *e2eTestSuite) Test_InsertAlignedTablet() {
	var timeseries = []string{"root.ln.device1.*"}
	s.session.DeleteTimeseries(timeseries)
	if tablet, err := createTablet(12); err == nil {
		status, err := s.session.InsertAlignedTablet(tablet, false)
		s.checkError(status, err)
	} else {
		log.Fatal(err)
	}
	var timeout int64 = 1000
	ds, err := s.session.ExecuteQueryStatement("select count(status) from root.ln.device1", &timeout)
	assert := s.Require()
	assert.NoError(err)
	defer ds.Close()
	assert.True(ds.Next())
	var status string
	assert.NoError(ds.Scan(&status))
	assert.Equal(status, "12")
	s.session.DeleteStorageGroup("root.ln.**")
}
func createTablet(rowCount int) (*client.Tablet, error) {
	tablet, err := client.NewTablet("root.ln.device1", []*client.MeasurementSchema{
		{
			Measurement: "restart_count",
			DataType:    client.INT32,
			Encoding:    client.RLE,
			Compressor:  client.SNAPPY,
		}, {
			Measurement: "price",
			DataType:    client.DOUBLE,
			Encoding:    client.GORILLA,
			Compressor:  client.SNAPPY,
		}, {
			Measurement: "tick_count",
			DataType:    client.INT64,
			Encoding:    client.RLE,
			Compressor:  client.SNAPPY,
		}, {
			Measurement: "temperature",
			DataType:    client.FLOAT,
			Encoding:    client.GORILLA,
			Compressor:  client.SNAPPY,
		}, {
			Measurement: "description",
			DataType:    client.TEXT,
			Encoding:    client.PLAIN,
			Compressor:  client.SNAPPY,
		},
		{
			Measurement: "status",
			DataType:    client.BOOLEAN,
			Encoding:    client.RLE,
			Compressor:  client.SNAPPY,
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
	}
	return tablet, nil
}

func (s *e2eTestSuite) Test_InsertAlignedTablets() {
	var timeseries = []string{"root.ln.device1.*"}
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

	ds, err := s.session.ExecuteQueryStatement("select count(status) from root.ln.device1", nil)
	assert := s.Require()
	assert.NoError(err)
	defer ds.Close()
	assert.True(ds.Next())
	var status string
	assert.NoError(ds.Scan(&status))
	assert.Equal(status, "8")
	s.session.DeleteStorageGroup("root.ln.**")
}
