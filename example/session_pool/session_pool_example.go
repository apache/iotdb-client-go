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
	"log"
	"math/rand"
	"strings"
	"time"

	"github.com/apache/iotdb-client-go/v2/client"
)

var (
	host     string
	port     string
	user     string
	password string
)
var sessionPool client.SessionPool

func main() {
	flag.StringVar(&host, "host", "127.0.0.1", "--host=192.168.1.100")
	flag.StringVar(&port, "port", "6667", "--port=6667")
	flag.StringVar(&user, "user", "root", "--user=root")
	flag.StringVar(&password, "password", "root", "--password=root")
	flag.Parse()
	config := &client.PoolConfig{
		Host:     host,
		Port:     port,
		UserName: user,
		Password: password,
	}
	sessionPool = client.NewSessionPool(config, 3, 60000, 60000, false)

	defer sessionPool.Close()
	//useNodeUrls()
	setStorageGroup("root.ln1")
	setStorageGroup("root.ln2")
	deleteStorageGroups("root.ln1", "root.ln2")

	createTimeseries("root.sg1.dev1.status")
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
	executeQueryStatement("show timeseries root.**")

}

// If your IoTDB is a cluster version, you can use the following code for session pool connection
func useNodeUrls() {

	config := &client.PoolConfig{
		UserName: user,
		Password: password,
		NodeUrls: strings.Split("127.0.0.1:6667,127.0.0.1:6668", ","),
	}
	sessionPool = client.NewSessionPool(config, 3, 60000, 60000, false)
	defer sessionPool.Close()
	session, err := sessionPool.GetSession()
	defer sessionPool.PutBack(session)
	if err != nil {
		log.Print(err)
		return
	}

}

func setStorageGroup(sg string) {
	session, err := sessionPool.GetSession()
	defer sessionPool.PutBack(session)
	if err == nil {
		session.SetStorageGroup(sg)
	}
}

func deleteStorageGroup(sg string) {
	session, err := sessionPool.GetSession()
	defer sessionPool.PutBack(session)
	if err == nil {
		checkError(session.DeleteStorageGroup(sg))
	}
}

func deleteStorageGroups(sgs ...string) {
	session, err := sessionPool.GetSession()
	defer sessionPool.PutBack(session)
	if err == nil {
		checkError(session.DeleteStorageGroups(sgs...))
	}
}

func createTimeseries(path string) {
	var (
		dataType   = client.FLOAT
		encoding   = client.PLAIN
		compressor = client.SNAPPY
	)
	session, err := sessionPool.GetSession()
	defer sessionPool.PutBack(session)
	if err == nil {
		checkError(session.CreateTimeseries(path, dataType, encoding, compressor, nil, nil))
	}
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
	session, err := sessionPool.GetSession()
	defer sessionPool.PutBack(session)
	if err == nil {
		checkError(session.CreateAlignedTimeseries(prefixPath, measurements, dataTypes, encodings, compressors, measurementAlias))
	}

}

func createMultiTimeseries() {
	var (
		paths       = []string{"root.sg1.dev1.temperature"}
		dataTypes   = []client.TSDataType{client.TEXT}
		encodings   = []client.TSEncoding{client.PLAIN}
		compressors = []client.TSCompressionType{client.SNAPPY}
	)
	session, err := sessionPool.GetSession()
	defer sessionPool.PutBack(session)
	if err == nil {
		checkError(session.CreateMultiTimeseries(paths, dataTypes, encodings, compressors))
	}

}

func deleteTimeseries(paths ...string) {
	session, err := sessionPool.GetSession()
	defer sessionPool.PutBack(session)
	if err == nil {
		checkError(session.DeleteTimeseries(paths))
	}

}

func insertStringRecord() {
	var (
		deviceId           = "root.ln.wf02.wt02"
		measurements       = []string{"hardware"}
		values             = []string{"123"}
		timestamp    int64 = 12
	)
	session, err := sessionPool.GetSession()
	defer sessionPool.PutBack(session)
	if err == nil {
		checkError(session.InsertStringRecord(deviceId, measurements, values, timestamp))
	}

}

func insertRecord() {
	var (
		deviceId           = "root.sg1.dev1"
		measurements       = []string{"status"}
		values             = []interface{}{"123"}
		dataTypes          = []client.TSDataType{client.TEXT}
		timestamp    int64 = 12
	)
	session, err := sessionPool.GetSession()
	defer sessionPool.PutBack(session)
	if err == nil {
		checkError(session.InsertRecord(deviceId, measurements, dataTypes, values, timestamp))
	}

}

func insertAlignedRecord() {
	var (
		deviceId           = "root.al1.dev1"
		measurements       = []string{"status"}
		values             = []interface{}{"123"}
		dataTypes          = []client.TSDataType{client.TEXT}
		timestamp    int64 = 12
	)
	session, err := sessionPool.GetSession()
	defer sessionPool.PutBack(session)
	if err == nil {
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

}

func insertRecords() {
	var (
		deviceId     = []string{"root.sg1.dev1"}
		measurements = [][]string{{"status"}}
		dataTypes    = [][]client.TSDataType{{client.TEXT}}
		values       = [][]interface{}{{"123"}}
		timestamp    = []int64{12}
	)
	session, err := sessionPool.GetSession()
	defer sessionPool.PutBack(session)
	if err == nil {
		checkError(session.InsertRecords(deviceId, measurements, dataTypes, values, timestamp))
	}

}

func insertAlignedRecords() {
	var (
		deviceIds    = []string{"root.al1.dev2", "root.al1.dev3"}
		measurements = [][]string{{"status"}, {"temperature"}}
		dataTypes    = [][]client.TSDataType{{client.TEXT}, {client.TEXT}}
		values       = [][]interface{}{{"33"}, {"44"}}
		timestamps   = []int64{12, 13}
	)
	session, err := sessionPool.GetSession()
	defer sessionPool.PutBack(session)
	if err == nil {
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
	session, err := sessionPool.GetSession()
	defer sessionPool.PutBack(session)
	if err == nil {
		checkError(session.InsertRecordsOfOneDevice(deviceId, timestamps, measurementsSlice, dataTypes, values, false))
	}

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
	session, err := sessionPool.GetSession()
	defer sessionPool.PutBack(session)
	if err == nil {
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

}

func deleteData() {
	var (
		paths           = []string{"root.sg1.dev1.status"}
		startTime int64 = 0
		endTime   int64 = 12
	)
	session, err := sessionPool.GetSession()
	defer sessionPool.PutBack(session)
	if err == nil {
		checkError(session.DeleteData(paths, startTime, endTime))
	}

}

func insertTablet() {
	session, err := sessionPool.GetSession()
	defer sessionPool.PutBack(session)
	if err == nil {
		if tablet, err := createTablet(12); err == nil {
			err := session.InsertTablet(tablet, false)
			tablet.Reset()
			checkError(err)
		} else {
			log.Fatal(err)
		}
	}

}

func insertAlignedTablet() {
	session, err := sessionPool.GetSession()
	defer sessionPool.PutBack(session)
	if err == nil {
		if tablet, err := createTablet(12); err == nil {
			err := session.InsertAlignedTablet(tablet, false)
			tablet.Reset()
			checkError(err)
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
	tablet1.Reset()
	tablet2.Reset()
	session, err := sessionPool.GetSession()
	defer sessionPool.PutBack(session)
	if err == nil {
		checkError(session.InsertTablets(tablets, false))
	}
	for _, tablet := range tablets {
		tablet.Reset()
	}

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
	session, err := sessionPool.GetSession()
	defer sessionPool.PutBack(session)
	if err == nil {
		checkError(session.InsertAlignedTablets(tablets, false))
	}
	for _, tablet := range tablets {
		tablet.Reset()
	}

}

func setTimeZone() {
	var timeZone = "GMT"
	session, err := sessionPool.GetSession()
	defer sessionPool.PutBack(session)
	if err == nil {
		session.SetTimeZone(timeZone)
	}

}

func getTimeZone() (string, error) {
	session, err := sessionPool.GetSession()
	defer sessionPool.PutBack(session)
	if err == nil {
		return session.GetTimeZone()
	}
	return "", err
}

func executeQueryStatement(sql string) {
	var timeout int64 = 1000
	session, err := sessionPool.GetSession()
	defer sessionPool.PutBack(session)
	if err != nil {
		log.Print(err)
		return
	}
	sessionDataSet, err := session.ExecuteQueryStatement(sql, &timeout)
	if err == nil {
		defer sessionDataSet.Close()
		printDataSet1(sessionDataSet)
	} else {
		log.Println(err)
	}
}

func executeStatement() {
	var sql = "show storage group"
	session, err := sessionPool.GetSession()
	defer sessionPool.PutBack(session)
	if err != nil {
		log.Print(err)
		return
	}
	sessionDataSet, err := session.ExecuteStatement(sql)
	if err == nil {
		printDataSet0(sessionDataSet)
		sessionDataSet.Close()
	} else {
		log.Println(err)
	}
}

func executeRawDataQuery() {
	session, err := sessionPool.GetSession()
	defer sessionPool.PutBack(session)
	if err != nil {
		log.Print(err)
		return
	}
	session.ExecuteNonQueryStatement("insert into root.ln.wf02.wt02(time,s5) values(1,true)")
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

func executeBatchStatement() {
	var sqls = []string{"insert into root.ln.wf02.wt02(time,s5) values(1,true)",
		"insert into root.ln.wf02.wt02(time,s5) values(2,true)"}
	session, err := sessionPool.GetSession()
	defer sessionPool.PutBack(session)
	if err == nil {
		checkError(session.ExecuteBatchStatement(sqls))
	}

}

func printDevice1(sds *client.SessionDataSet) {
	for _, columnName := range sds.GetColumnNames() {
		fmt.Printf("%s\t", columnName)
	}
	fmt.Println()

	for next, err := sds.Next(); err == nil && next; next, err = sds.Next() {
		timestamp, _ := sds.GetStringByIndex(1)
		restartCount, _ := sds.GetIntByIndex(2)
		tickCount, _ := sds.GetLongByIndex(3)
		price, _ := sds.GetDoubleByIndex(4)
		temperature, _ := sds.GetFloatByIndex(5)
		description, _ := sds.GetStringByIndex(6)
		status, _ := sds.GetBooleanByIndex(7)

		whitespace := "\t\t"
		fmt.Printf("%s\t", timestamp)
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
	columns := sessionDataSet.GetColumnNames()
	for _, columnName := range columns {
		fmt.Printf("%s\t", columnName)
	}
	fmt.Println()

	for next, err := sessionDataSet.Next(); err == nil && next; next, err = sessionDataSet.Next() {
		for i, columnName := range columns {
			dataType, _ := client.GetDataTypeByStr(sessionDataSet.GetColumnTypes()[i])
			switch dataType {
			case client.BOOLEAN:
				value, _ := sessionDataSet.GetBoolean(columnName)
				fmt.Print(value)
			case client.INT32:
				value, _ := sessionDataSet.GetInt(columnName)
				fmt.Print(value)
			case client.INT64, client.TIMESTAMP:
				value, _ := sessionDataSet.GetLong(columnName)
				fmt.Print(value)
			case client.FLOAT:
				value, _ := sessionDataSet.GetFloat(columnName)
				fmt.Print(value)
			case client.DOUBLE:
				value, _ := sessionDataSet.GetDouble(columnName)
				fmt.Print(value)
			case client.TEXT, client.STRING, client.BLOB, client.DATE:
				value, _ := sessionDataSet.GetString(columnName)
				fmt.Print(value)
			default:
			}
			fmt.Print("\t\t")
		}
		fmt.Println()
	}
}

func printDataSet1(sds *client.SessionDataSet) {
	columnNames := sds.GetColumnNames()
	for _, value := range columnNames {
		fmt.Printf("%s\t", value)
	}
	fmt.Println()

	for next, err := sds.Next(); err == nil && next; next, err = sds.Next() {
		for _, columnName := range columnNames {
			isNull, _ := sds.IsNull(columnName)

			if isNull {
				fmt.Printf("%v\t\t", "null")
			} else {
				v, _ := sds.GetString(columnName)
				fmt.Printf("%v\t\t", v)
			}
		}
		fmt.Println()
	}
}

func printDataSet2(sds *client.SessionDataSet) {
	columnNames := sds.GetColumnNames()
	for _, value := range columnNames {
		fmt.Printf("%s\t", value)
	}
	fmt.Println()

	for next, err := sds.Next(); err == nil && next; next, err = sds.Next() {
		for i := int32(0); i < int32(len(columnNames)); i++ {
			isNull, _ := sds.IsNullByIndex(i)

			if isNull {
				fmt.Printf("%v\t\t", "null")
			} else {
				v, _ := sds.GetStringByIndex(i)
				fmt.Printf("%v\t\t", v)
			}
		}
		fmt.Println()
	}
}

func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
