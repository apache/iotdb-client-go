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

package main

import (
	"flag"
	"fmt"
	"github.com/apache/iotdb-client-go/common"
	"log"
	"math/rand"
	"strings"
	"time"

	"github.com/apache/iotdb-client-go/client"
)

var (
	host     string
	port     string
	user     string
	password string
)
var session client.Session

func main() {
	flag.StringVar(&host, "host", "127.0.0.1", "--host=192.168.1.100")
	flag.StringVar(&port, "port", "6667", "--port=6667")
	flag.StringVar(&user, "user", "root", "--user=root")
	flag.StringVar(&password, "password", "root", "--password=root")
	flag.Parse()
	config := &client.Config{
		Host:     host,
		Port:     port,
		UserName: user,
		Password: password,
	}
	session = client.NewSession(config)
	if err := session.Open(false, 0); err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	//connectCluster()

	setStorageGroup("root.ln1")
	deleteStorageGroup("root.ln1")

	setStorageGroup("root.ln1")
	setStorageGroup("root.ln2")
	deleteStorageGroups("root.ln1", "root.ln2")

	createTimeseries("root.sg1.dev1.status")
	deleteTimeseries("root.sg1.dev1.status")

	createTimeseriesByNonQueryStatement("create timeseries root.sg1.dev1.status with datatype = int32")
	deleteTimeseries("root.sg1.dev1.status")

	createMultiTimeseries()
	deleteTimeseries("root.sg1.dev1.temperature")

	createAlignedTimeseries("root.sg1.dev1", []string{"status", "temperature"}, []string{"sts", "temp"})
	deleteTimeseries("root.sg1.dev1.status")
	deleteTimeseries("root.sg1.dev1.temperature")

	insertStringRecord()
	deleteTimeseries("root.ln.wf02.wt02.hardware")

	insertRecord()
	deleteTimeseries("root.sg1.dev1.status")

	insertRecords()
	deleteTimeseries("root.sg1.dev1.status")

	insertTablet()
	var timeout int64 = 1000
	if ds, err := session.ExecuteQueryStatement("select * from root.ln.device1", &timeout); err == nil {
		printDevice1(ds)
		ds.Close()
	} else {
		log.Fatal(err)
	}
	deleteTimeseries("root.ln.device1.restart_count", "root.ln.device1.price", "root.ln.device1.tick_count", "root.ln.device1.temperature", "root.ln.device1.description", "root.ln.device1.status")
	insertTablets()
	deleteTimeseries("root.ln.device1.restart_count", "root.ln.device1.price", "root.ln.device1.tick_count", "root.ln.device1.temperature", "root.ln.device1.description", "root.ln.device1.status")

	insertRecord()
	deleteData()

	setTimeZone()
	if tz, err := getTimeZone(); err == nil {
		fmt.Printf("TimeZone: %s\n", tz)
	} else {
		fmt.Printf("getTimeZone ERROR: %v\n", err)
	}

	executeStatement()
	executeQueryStatement("select count(s3) from root.sg1.dev1")
	executeRawDataQuery()
	executeBatchStatement()

	var startTime int64 = 1
	var endTime int64 = 10
	var interval int64 = 2
	executeAggregationQueryStatementWithLegalNodes([]string{"root.ln.wf02.wt02.s5"}, []common.TAggregationType{common.TAggregationType_COUNT}, &startTime, &endTime, &interval)

	deleteTimeseries("root.sg1.dev1.status")
	deleteTimeseries("root.ln.wf02.wt02.s5")

	//0.12.x and newer
	insertRecordsOfOneDevice()
	deleteTimeseries("root.sg1.dev0.*")

	insertAlignedRecord()
	deleteTimeseries("root.al1.dev1.*")

	insertAlignedRecords()
	deleteTimeseries("root.al1.**")

	insertAlignedRecordsOfOneDevice()
	deleteTimeseries("root.al1.dev4.*")

	deleteStorageGroup("root.ln")
	insertAlignedTablet()
	deleteTimeseries("root.ln.device1.*")

	deleteStorageGroup("root.ln")
	insertAlignedTablets()
	deleteTimeseries("root.ln.device1.*")
}

// If your IotDB is a cluster version, you can use the following code for multi node connection
func connectCluster() {
	config := &client.ClusterConfig{
		NodeUrls: strings.Split("127.0.0.1:6667,127.0.0.1:6668,127.0.0.1:6669", ","),
		UserName: "root",
		Password: "root",
	}
	session, err := client.NewClusterSession(config)
	if err != nil {
		log.Fatal(err)
	}
	if err = session.OpenCluster(false); err != nil {
		log.Fatal(err)
	}
}

func printDevice1(sds *client.SessionDataSet) {
	showTimestamp := !sds.IsIgnoreTimeStamp()
	if showTimestamp {
		fmt.Print("Time\t\t\t\t")
	}

	for _, columnName := range sds.GetColumnNames() {
		fmt.Printf("%s\t", columnName)
	}
	fmt.Println()

	for next, err := sds.Next(); err == nil && next; next, err = sds.Next() {
		if showTimestamp {
			fmt.Printf("%s\t", sds.GetText(client.TimestampColumnName))
		}

		var restartCount int32
		var price float64
		var tickCount int64
		var temperature float32
		var description string
		var status bool

		// All of iotdb datatypes can be scan into string variables
		// var restartCount string
		// var price string
		// var tickCount string
		// var temperature string
		// var description string
		// var status string

		if err := sds.Scan(&restartCount, &tickCount, &price, &temperature, &description, &status); err != nil {
			log.Fatal(err)
		}

		whitespace := "\t\t"
		fmt.Printf("%v%s", restartCount, whitespace)
		fmt.Printf("%v%s", price, whitespace)
		fmt.Printf("%v%s", tickCount, whitespace)
		fmt.Printf("%v%s", temperature, whitespace)
		fmt.Printf("%v%s", description, whitespace)
		fmt.Printf("%v%s", status, whitespace)

		fmt.Println()
	}
}

func printDataSet0(sessionDataSet *client.SessionDataSet) {
	showTimestamp := !sessionDataSet.IsIgnoreTimeStamp()
	if showTimestamp {
		fmt.Print("Time\t\t\t\t")
	}

	for i := 0; i < sessionDataSet.GetColumnCount(); i++ {
		fmt.Printf("%s\t", sessionDataSet.GetColumnName(i))
	}
	fmt.Println()

	for next, err := sessionDataSet.Next(); err == nil && next; next, err = sessionDataSet.Next() {
		if showTimestamp {
			fmt.Printf("%s\t", sessionDataSet.GetText(client.TimestampColumnName))
		}
		for i := 0; i < sessionDataSet.GetColumnCount(); i++ {
			columnName := sessionDataSet.GetColumnName(i)
			switch sessionDataSet.GetColumnDataType(i) {
			case client.BOOLEAN:
				fmt.Print(sessionDataSet.GetBool(columnName))
			case client.INT32:
				fmt.Print(sessionDataSet.GetInt32(columnName))
			case client.INT64, client.TIMESTAMP:
				fmt.Print(sessionDataSet.GetInt64(columnName))
			case client.FLOAT:
				fmt.Print(sessionDataSet.GetFloat(columnName))
			case client.DOUBLE:
				fmt.Print(sessionDataSet.GetDouble(columnName))
			case client.TEXT, client.STRING, client.BLOB, client.DATE:
				fmt.Print(sessionDataSet.GetText(columnName))
			default:
			}
			fmt.Print("\t\t")
		}
		fmt.Println()
	}
}

func printDataSet1(sds *client.SessionDataSet) {
	showTimestamp := !sds.IsIgnoreTimeStamp()
	if showTimestamp {
		fmt.Print("Time\t\t\t\t")
	}

	for i := 0; i < sds.GetColumnCount(); i++ {
		fmt.Printf("%s\t", sds.GetColumnName(i))
	}
	fmt.Println()

	for next, err := sds.Next(); err == nil && next; next, err = sds.Next() {
		if showTimestamp {
			fmt.Printf("%s\t", sds.GetText(client.TimestampColumnName))
		}
		for i := 0; i < sds.GetColumnCount(); i++ {
			columnName := sds.GetColumnName(i)
			v := sds.GetValue(columnName)
			if v == nil {
				v = "null"
			}
			fmt.Printf("%v\t\t", v)
		}
		fmt.Println()
	}
}

func printDataSet2(sds *client.SessionDataSet) {
	showTimestamp := !sds.IsIgnoreTimeStamp()
	if showTimestamp {
		fmt.Print("Time\t\t\t\t")
	}

	for i := 0; i < sds.GetColumnCount(); i++ {
		fmt.Printf("%s\t", sds.GetColumnName(i))
	}
	fmt.Println()

	for next, err := sds.Next(); err == nil && next; next, err = sds.Next() {
		if showTimestamp {
			fmt.Printf("%s\t", sds.GetText(client.TimestampColumnName))
		}

		if record, err := sds.GetRowRecord(); err == nil {
			for _, field := range record.GetFields() {
				v := field.GetValue()
				if field.IsNull() {
					v = "null"
				}
				fmt.Printf("%v\t\t", v)
			}
			fmt.Println()
		}
	}
}

func setStorageGroup(sg string) {
	checkError(session.SetStorageGroup(sg))
}

func deleteStorageGroup(sg string) {
	checkError(session.DeleteStorageGroup(sg))
}

func deleteStorageGroups(sgs ...string) {
	checkError(session.DeleteStorageGroups(sgs...))
}

func createTimeseries(path string) {
	var (
		dataType   = client.FLOAT
		encoding   = client.PLAIN
		compressor = client.SNAPPY
	)
	checkError(session.CreateTimeseries(path, dataType, encoding, compressor, nil, nil))
}

func createTimeseriesByNonQueryStatement(sql string) {
	checkError(session.ExecuteNonQueryStatement(sql))
}

func createAlignedTimeseries(prefixPath string, measurements, measurementAlias []string) {
	var (
		dataTypes = []client.TSDataType{
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
	checkError(session.CreateAlignedTimeseries(prefixPath, measurements, dataTypes, encodings, compressors, measurementAlias))
}

func createMultiTimeseries() {
	var (
		paths       = []string{"root.sg1.dev1.temperature"}
		dataTypes   = []client.TSDataType{client.TEXT}
		encodings   = []client.TSEncoding{client.PLAIN}
		compressors = []client.TSCompressionType{client.SNAPPY}
	)
	checkError(session.CreateMultiTimeseries(paths, dataTypes, encodings, compressors))
}

func deleteTimeseries(paths ...string) {
	checkError(session.DeleteTimeseries(paths))
}

func insertStringRecord() {
	var (
		deviceId           = "root.ln.wf02.wt02"
		measurements       = []string{"hardware"}
		values             = []string{"123"}
		timestamp    int64 = 12
	)
	checkError(session.InsertStringRecord(deviceId, measurements, values, timestamp))
}

func insertRecord() {
	var (
		deviceId           = "root.sg1.dev1"
		measurements       = []string{"status"}
		values             = []interface{}{"123"}
		dataTypes          = []client.TSDataType{client.TEXT}
		timestamp    int64 = 12
	)
	checkError(session.InsertRecord(deviceId, measurements, dataTypes, values, timestamp))
}

func insertAlignedRecord() {
	var (
		deviceId           = "root.al1.dev1"
		measurements       = []string{"status"}
		values             = []interface{}{"123"}
		dataTypes          = []client.TSDataType{client.TEXT}
		timestamp    int64 = 12
	)
	checkError(session.InsertAlignedRecord(deviceId, measurements, dataTypes, values, timestamp))
	sessionDataSet, err := session.ExecuteStatement("show devices")
	if err == nil {
		printDataSet0(sessionDataSet)
		sessionDataSet.Close()
	} else {
		log.Println(err)
	}
	fmt.Println()
}

func insertRecords() {
	var (
		deviceId     = []string{"root.sg1.dev1"}
		measurements = [][]string{{"status"}}
		dataTypes    = [][]client.TSDataType{{client.TEXT}}
		values       = [][]interface{}{{"123"}}
		timestamp    = []int64{12}
	)
	checkError(session.InsertRecords(deviceId, measurements, dataTypes, values, timestamp))
}

func insertAlignedRecords() {
	var (
		deviceIds    = []string{"root.al1.dev2", "root.al1.dev3"}
		measurements = [][]string{{"status"}, {"temperature"}}
		dataTypes    = [][]client.TSDataType{{client.TEXT}, {client.TEXT}}
		values       = [][]interface{}{{"33"}, {"44"}}
		timestamps   = []int64{12, 13}
	)
	checkError(session.InsertAlignedRecords(deviceIds, measurements, dataTypes, values, timestamps))
	sessionDataSet, err := session.ExecuteStatement("show devices")
	if err == nil {
		printDataSet0(sessionDataSet)
		sessionDataSet.Close()
	} else {
		log.Println(err)
	}
	fmt.Println()
}

func insertRecordsOfOneDevice() {
	ts := time.Now().UTC().UnixNano() / 1000000
	var (
		deviceId          = "root.sg1.dev0"
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
	checkError(session.InsertRecordsOfOneDevice(deviceId, timestamps, measurementsSlice, dataTypes, values, false))
}

func insertAlignedRecordsOfOneDevice() {
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
	checkError(session.InsertAlignedRecordsOfOneDevice(deviceId, timestamps, measurementsSlice, dataTypes, values, false))
	sessionDataSet, err := session.ExecuteStatement("show devices")
	if err == nil {
		printDataSet0(sessionDataSet)
		sessionDataSet.Close()
	} else {
		log.Println(err)
	}
	sessionDataSetNew, err := session.ExecuteStatement("select restart_count,tick_count,price,temperature,description,status from  root.al1.dev4")
	if err == nil {
		printDataSet0(sessionDataSetNew)
		sessionDataSetNew.Close()
	} else {
		log.Println(err)
	}
	fmt.Println()
}

func deleteData() {
	var (
		paths           = []string{"root.sg1.dev1.status"}
		startTime int64 = 0
		endTime   int64 = 12
	)
	checkError(session.DeleteData(paths, startTime, endTime))
}

func insertTablet() {
	if tablet, err := createTablet(12); err == nil {
		status, err := session.InsertTablet(tablet, false)
		tablet.Reset()
		checkError(status, err)
	} else {
		log.Fatal(err)
	}
}

func insertAlignedTablet() {
	if tablet, err := createTablet(12); err == nil {
		status, err := session.InsertAlignedTablet(tablet, false)
		tablet.Reset()
		checkError(status, err)
	} else {
		log.Fatal(err)
	}
	var timeout int64 = 1000
	if ds, err := session.ExecuteQueryStatement("select * from root.ln.device1", &timeout); err == nil {
		printDevice1(ds)
		ds.Close()
	} else {
		log.Fatal(err)
	}
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
		if row%2 == 1 {
			tablet.SetValueAt(rand.Float64(), 1, row)
		} else {
			tablet.SetValueAt(nil, 1, row)
		}
		tablet.SetValueAt(rand.Int63(), 2, row)
		tablet.SetValueAt(rand.Float32(), 3, row)
		tablet.SetValueAt(fmt.Sprintf("Test Device %d", row+1), 4, row)
		tablet.SetValueAt(bool(ts%2 == 0), 5, row)
		tablet.RowSize++
	}
	return tablet, nil
}

func insertTablets() {
	tablet1, err := createTablet(8)
	if err != nil {
		log.Fatal(err)
	}
	tablet2, err := createTablet(4)
	if err != nil {
		log.Fatal(err)
	}

	tablets := []*client.Tablet{tablet1, tablet2}
	checkError(session.InsertTablets(tablets, false))
}

func insertAlignedTablets() {
	tablet1, err := createTablet(8)
	if err != nil {
		log.Fatal(err)
	}
	tablet2, err := createTablet(4)
	if err != nil {
		log.Fatal(err)
	}

	tablets := []*client.Tablet{tablet1, tablet2}
	checkError(session.InsertAlignedTablets(tablets, false))
}

func setTimeZone() {
	var timeZone = "GMT"
	session.SetTimeZone(timeZone)
}

func getTimeZone() (string, error) {
	return session.GetTimeZone()
}

func executeStatement() {
	var sql = "show storage group"
	sessionDataSet, err := session.ExecuteStatement(sql)
	if err == nil {
		printDataSet0(sessionDataSet)
		sessionDataSet.Close()
	} else {
		log.Println(err)
	}
}

func executeQueryStatement(sql string) {
	var timeout int64 = 1000
	sessionDataSet, err := session.ExecuteQueryStatement(sql, &timeout)
	if err == nil {
		printDataSet1(sessionDataSet)
		sessionDataSet.Close()
	} else {
		log.Println(err)
	}
}

func executeAggregationQueryStatementWithLegalNodes(paths []string, aggregations []common.TAggregationType,
	startTime *int64, endTime *int64, interval *int64) {
	var timeout int64 = 1000
	var legal bool = true
	sessionDataSet, err := session.ExecuteAggregationQueryWithLegalNodes(paths, aggregations, startTime, endTime, interval, &timeout, &legal)
	if err == nil {
		printDataSet1(sessionDataSet)
		sessionDataSet.Close()
	} else {
		log.Println(err)
	}
}

func executeRawDataQuery() {
	session.ExecuteUpdateStatement("insert into root.ln.wf02.wt02(time,s5) values(1,true)")
	var (
		paths           = []string{"root.ln.wf02.wt02.s5"}
		startTime int64 = 1
		endTime   int64 = 200
	)
	sessionDataSet, err := session.ExecuteRawDataQuery(paths, startTime, endTime)
	if err == nil {
		printDataSet2(sessionDataSet)
		sessionDataSet.Close()
	} else {
		log.Println(err)
	}
}

func executeBatchStatement() {
	var sqls = []string{"insert into root.ln.wf02.wt02(time,s5) values(1,true)",
		"insert into root.ln.wf02.wt02(time,s5) values(2,true)"}
	checkError(session.ExecuteBatchStatement(sqls))
	var (
		paths     []string = []string{"root.ln.wf02.wt02.s5"}
		startTime int64    = 1
		endTime   int64    = 200
	)
	sessionDataSet, err := session.ExecuteRawDataQuery(paths, startTime, endTime)
	if err == nil {
		printDataSet2(sessionDataSet)
		sessionDataSet.Close()
	} else {
		log.Println(err)
	}
}

func checkError(status *common.TSStatus, err error) {
	if err != nil {
		log.Fatal(err)
	}

	if status != nil {
		if err = client.VerifySuccess(status); err != nil {
			log.Println(err)
		}
	}
}
