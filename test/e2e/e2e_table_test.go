package e2e

import (
	"github.com/apache/iotdb-client-go/client"
	"github.com/apache/iotdb-client-go/common"
	"github.com/stretchr/testify/suite"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var (
	nodeUrls = "iotdb:6668,iotdb:6667,iotdb:6669"
	host     = "iotdb"
	port     = "6667"
	username = "root"
	password = "root"
	database = "test_db"
)

type e2eTableTestSuite struct {
	suite.Suite
	session client.ITableSession
}

func TestE2ETableTestSuite(t *testing.T) {
	suite.Run(t, &e2eTableTestSuite{})
}

func (s *e2eTableTestSuite) SetupSuite() {
	clusterConfig := client.ClusterConfig{
		NodeUrls: strings.Split(nodeUrls, ","),
		UserName: username,
		Password: password,
	}
	session, err := client.NewClusterTableSession(&clusterConfig, false)
	s.Require().NoError(err)
	s.session = session
}

func (s *e2eTableTestSuite) TearDownSuite() {
	s.session.Close()
}

func (s *e2eTableTestSuite) SetupTest() {
	s.checkError(s.session.ExecuteNonQueryStatement("create database " + database))
	s.checkError(s.session.ExecuteNonQueryStatement("use " + database))
	for i := 0; i < 10; i++ {
		s.checkError(s.session.ExecuteNonQueryStatement("create database db" + strconv.Itoa(i)))
	}
}

func (s *e2eTableTestSuite) TearDownTest() {
	s.checkError(s.session.ExecuteNonQueryStatement("drop database " + database))
	for i := 0; i < 10; i++ {
		s.checkError(s.session.ExecuteNonQueryStatement("drop database db" + strconv.Itoa(i)))
	}
}

func (s *e2eTableTestSuite) Test_CreateTableSession() {
	assert := s.Require()
	config := &client.Config{
		Host:     host,
		Port:     port,
		UserName: username,
		Password: password,
	}
	session, err := client.NewTableSession(config, false, 3000)
	assert.NoError(err)
	defer session.Close()
	s.checkError(session.ExecuteNonQueryStatement("use " + database))
}

func (s *e2eTableTestSuite) Test_CreateTableSessionWithDatabase() {
	assert := s.Require()
	config := &client.Config{
		Host:     host,
		Port:     port,
		UserName: username,
		Password: password,
		Database: database,
	}
	session, err := client.NewTableSession(config, false, 3000)
	defer session.Close()
	assert.NoError(err)
	timeoutInMs := int64(1000)
	_, err = session.ExecuteQueryStatement("show tables", &timeoutInMs)
	assert.NoError(err)
}

func (s *e2eTableTestSuite) Test_GetSessionFromTableSessionPool() {
	assert := s.Require()
	poolConfig := &client.PoolConfig{
		Host:     host,
		Port:     port,
		UserName: username,
		Password: password,
	}
	sessionPool := client.NewTableSessionPool(poolConfig, 3, 10000, 3000, false)
	defer sessionPool.Close()

	session1, err := sessionPool.GetSession()
	assert.NoError(err)
	s.checkError(session1.ExecuteNonQueryStatement("use " + database))
	session1.Close()

	// test get session timeout
	var wg sync.WaitGroup
	getNum := 4
	wg.Add(getNum)
	successNum := int32(0)
	for i := 0; i < getNum; i++ {
		go func() {
			defer wg.Done()
			session, getSessionErr := sessionPool.GetSession()
			// timeout after 3s
			if getSessionErr != nil {
				return
			}
			atomic.AddInt32(&successNum, 1)
			defer func() {
				time.Sleep(time.Second * 4)
				session.Close()
			}()
		}()
	}
	wg.Wait()
	assert.Equal(int32(3), successNum)

	// test get session
	getNum = 10
	wg.Add(getNum)
	successNum = int32(0)
	for i := 0; i < getNum; i++ {
		go func() {
			defer wg.Done()
			session, getSessionErr := sessionPool.GetSession()
			if getSessionErr != nil {
				return
			}
			atomic.AddInt32(&successNum, 1)
			defer session.Close()
			s.checkError(session.ExecuteNonQueryStatement("use " + database))
		}()
	}
	wg.Wait()
	assert.Equal(int32(10), successNum)
}

func (s *e2eTableTestSuite) Test_GetSessionFromSessionPoolWithSpecificDatabase() {
	assert := s.Require()
	poolConfig := &client.PoolConfig{
		Host:     host,
		Port:     port,
		UserName: username,
		Password: password,
		Database: database,
	}
	sessionPool := client.NewTableSessionPool(poolConfig, 3, 10000, 3000, false)
	defer sessionPool.Close()

	session1, err := sessionPool.GetSession()
	assert.NoError(err)
	s.checkError(session1.ExecuteNonQueryStatement("create table table_in_" + database + " (id1 string id, id2 string id, s1 text measurement, s2 text measurement)"))
	session1.Close()

	var wg sync.WaitGroup
	getNum := 10
	wg.Add(getNum)
	successNum := int32(0)
	for i := 0; i < getNum; i++ {
		currentDbName := "db" + strconv.Itoa(i)
		go func() {
			defer wg.Done()
			session, getSessionErr := sessionPool.GetSession()
			if getSessionErr != nil {
				return
			}
			defer session.Close()

			timeoutInMs := int64(3000)
			dataSet, queryErr := session.ExecuteQueryStatement("show tables", &timeoutInMs)
			assert.NoError(queryErr)
			assert.True(dataSet.Next())
			assert.Equal("table_in_"+database, dataSet.GetText("TableName"))

			// modify using database
			s.checkError(session.ExecuteNonQueryStatement("use " + currentDbName))
			atomic.AddInt32(&successNum, 1)
		}()
	}
	wg.Wait()
	assert.Equal(int32(10), successNum)

	// database in session should be reset to test_db
	wg.Add(getNum)
	for i := 0; i < getNum; i++ {
		go func() {
			defer wg.Done()
			session, getSessionErr := sessionPool.GetSession()
			// timeout after 3s
			if getSessionErr != nil {
				return
			}
			defer session.Close()
			atomic.AddInt32(&successNum, 1)

			timeoutInMs := int64(3000)
			dataSet, queryErr := session.ExecuteQueryStatement("show tables", &timeoutInMs)
			assert.NoError(queryErr)
			assert.True(dataSet.Next())
			assert.Equal("table_in_"+database, dataSet.GetText("TableName"))
		}()
	}
	wg.Wait()
}

func (s *e2eTableTestSuite) Test_InsertTabletAndQuery() {
	assert := s.Require()
	s.checkError(s.session.ExecuteNonQueryStatement("create table t1 (id1 string id, id2 string id, s1 text measurement, s2 text measurement)"))

	timeoutInMs := int64(10000)

	// show tables
	dataSet, err := s.session.ExecuteQueryStatement("show tables", &timeoutInMs)
	assert.NoError(err)

	hasNext, err := dataSet.Next()
	assert.NoError(err)
	assert.True(hasNext)
	assert.Equal("t1", dataSet.GetText("TableName"))

	// insert relational tablet
	tablet, err := client.NewRelationalTablet("t1", []*client.MeasurementSchema{
		{
			Measurement: "id1",
			DataType:    client.STRING,
		},
		{
			Measurement: "id2",
			DataType:    client.STRING,
		},
		{
			Measurement: "s1",
			DataType:    client.TEXT,
		},
		{
			Measurement: "s2",
			DataType:    client.TEXT,
		},
	}, []client.ColumnCategory{client.ID, client.ID, client.MEASUREMENT, client.MEASUREMENT}, 1024)
	assert.NoError(err)

	values := [][]interface{}{
		{"id1_field_1", "id2_field_1", "s1_value_1", "s2_value_1"},
		{"id1_field_1", "id2_field_1", "s1_value_2", "s2_value_2"},
		{"id1_field_1", "id2_field_1", nil, "s2_value_2"},
		{"id1_field_2", "id2_field_2", "s1_value_1", "s2_value_1"},
		{"id1_field_2", "id2_field_2", "s1_value_1", "s2_value_1"},
		{"id1_field_3", "id2_field_3", "s1_value_1", "s2_value_1"},
		{"id1_field_3", "id2_field_3", "s1_value_2", nil},
		{"id1_field_3", "id2_field_3", "s1_value_3", "s2_value_3"},
	}

	ts := int64(0)
	for row := 0; row < 8; row++ {
		tablet.SetTimestamp(ts, row)
		assert.NoError(tablet.SetValueAt(values[row][0], 0, row))
		assert.NoError(tablet.SetValueAt(values[row][1], 1, row))
		assert.NoError(tablet.SetValueAt(values[row][2], 2, row))
		assert.NoError(tablet.SetValueAt(values[row][3], 3, row))
		ts++
		tablet.RowSize++
	}
	s.checkError(s.session.Insert(tablet))

	// query
	dataSet, err = s.session.ExecuteQueryStatement("select * from t1 order by time asc", &timeoutInMs)
	assert.NoError(err)

	count := int64(0)
	for {
		hasNext, err := dataSet.Next()
		assert.NoError(err)
		if !hasNext {
			break
		}
		assert.Equal(count, dataSet.GetInt64("time"))
		assert.Equal(values[count][0], getValueFromDataSet(dataSet, "id1"))
		assert.Equal(values[count][1], getValueFromDataSet(dataSet, "id2"))
		assert.Equal(values[count][2], getValueFromDataSet(dataSet, "s1"))
		assert.Equal(values[count][3], getValueFromDataSet(dataSet, "s2"))
		count++
	}
	assert.Equal(int64(8), count)
}

func getValueFromDataSet(dataSet *client.SessionDataSet, columnName string) interface{} {
	if dataSet.IsNull(columnName) {
		return nil
	} else {
		return dataSet.GetText(columnName)
	}
}

func (s *e2eTableTestSuite) checkError(status *common.TSStatus, err error) {
	s.Require().NoError(err)
	if status != nil {
		s.Require().NoError(client.VerifySuccess(status))
	}
}
