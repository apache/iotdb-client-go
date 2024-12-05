package e2e

import (
	"github.com/apache/iotdb-client-go/client"
	"github.com/apache/iotdb-client-go/common"
	"github.com/stretchr/testify/suite"
	"strings"
	"testing"
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
		NodeUrls: strings.Split("127.0.0.1:6667", ","),
		UserName: "root",
		Password: "root",
	}
	session, err := client.NewClusterTableSession(&clusterConfig, false)
	s.Require().NoError(err)
	s.session = session
}

func (s *e2eTableTestSuite) TearDownSuite() {
	s.session.Close()
}

func (s *e2eTableTestSuite) SetupTest() {
	s.checkError(s.session.ExecuteNonQueryStatement("create database test_db"))
	s.checkError(s.session.ExecuteNonQueryStatement("use test_db"))
}

func (s *e2eTableTestSuite) TearDownTest() {
	s.checkError(s.session.ExecuteNonQueryStatement("drop database test_db"))
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
