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
