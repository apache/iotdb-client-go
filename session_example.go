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
	"fmt"
	"log"
	"os"
	"time"

	"github.com/apache/iotdb-client-go/client"
	"github.com/apache/iotdb-client-go/rpc"
)

var session *client.Session

func main() {
	config := &client.Config{
		UserName: "root",
		Password: "root",
		TimeZone: client.DEFAULT_TIME_ZONE,
		Port:     "6667",
	}
	session = client.NewSession(config)
	err := session.Open(false, 0)
	if err != nil {
		log.Fatal(err)
	}

	defer session.Close()
	setStorageGroup()
	deleteStorageGroup()

	setStorageGroup()
	deleteStorageGroups()
	createTimeseries()
	createMultiTimeseries()
	deleteData()
	deleteTimeseries()
	insertStringRecord()
	setTimeZone()
	if tz, err := getTimeZone(); err == nil {
		fmt.Printf("TimeZone: %s\n", tz)
	}
	ts := time.Now().UTC().UnixNano() / 1000000
	status, err := session.InsertRecord("root.ln.device1", []string{"description", "price", "tick_count", "status", "restart_count", "temperature"}, []int32{client.TEXT, client.DOUBLE, client.INT64, client.BOOLEAN, client.INT32, client.FLOAT},
		[]interface{}{string("Test Device 1"), float64(1988.20), int64(3333333), true, int32(1), float32(12.10)}, ts)
	if err != nil {
		if status != nil {
			log.Printf("Code: %d, Message: %d", status.Code, status.Message)
		}
		log.Fatal(err)
	}

	sessionDataSet, err := session.ExecuteQueryStatement("SHOW TIMESERIES")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	printDataSet0(sessionDataSet)

	ds, err := session.ExecuteQueryStatement("select * from root.ln.device1")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	printDevice1(ds)
	session.ExecuteStatement(fmt.Sprintf("delete from root.ln.device1 where time=%v", ts))
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
			fmt.Printf("%s\t", sessionDataSet.GetText(client.TIMESTAMP_STR))
		}
		for i := 0; i < sessionDataSet.GetColumnCount(); i++ {
			columnName := sessionDataSet.GetColumnName(i)
			switch sessionDataSet.GetColumnDataType(i) {
			case client.BOOLEAN:
				fmt.Print(sessionDataSet.GetBool(columnName))
				break
			case client.INT32:
				fmt.Print(sessionDataSet.GetInt32(columnName))
				break
			case client.INT64:
				fmt.Print(sessionDataSet.GetInt64(columnName))
				break
			case client.FLOAT:
				fmt.Print(sessionDataSet.GetFloat(columnName))
				break
			case client.DOUBLE:
				fmt.Print(sessionDataSet.GetDouble(columnName))
				break
			case client.TEXT:
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
			fmt.Printf("%s\t", sds.GetText(client.TIMESTAMP_STR))
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
			fmt.Printf("%s\t", sds.GetText(client.TIMESTAMP_STR))
		}

		for _, field := range sds.GetRowRecord().GetFields() {
			v := field.GetValue()
			if field.IsNull() {
				v = "null"
			}
			fmt.Printf("%v\t\t", v)
		}
		fmt.Println()
	}
}

func printDevice1(sds *client.SessionDataSet) {
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
			fmt.Printf("%s\t", sds.GetText(client.TIMESTAMP_STR))
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

		if err := sds.Scan(&restartCount, &price, &tickCount, &temperature, &description, &status); err != nil {
			log.Fatal(err)
		}

		whitespace := "\t\t"
		fmt.Printf("%v%s", restartCount, whitespace)
		fmt.Printf("%v%s", price, whitespace)
		fmt.Printf("%v%s", restartCount, whitespace)
		fmt.Printf("%v%s", temperature, whitespace)
		fmt.Printf("%v%s", description, whitespace)
		fmt.Printf("%v%s", status, whitespace)

		fmt.Println()
	}
}

func setStorageGroup() {
	var storageGroupId = "root.ln1"
	status, err := session.SetStorageGroup(storageGroupId)
	checkError(status, err)
}

func deleteStorageGroup() {
	var storageGroupId = "root.ln1"
	status, err := session.DeleteStorageGroup(storageGroupId)
	checkError(status, err)
}

func deleteStorageGroups() {
	var storageGroupId = []string{"root.ln1"}
	status, err := session.DeleteStorageGroups(storageGroupId)
	checkError(status, err)
}

func createTimeseries() {
	var path = "root.sg1.dev1.status"
	var dataType = client.FLOAT
	var encoding = client.PLAIN
	var compressor = client.SNAPPY
	status, err := session.CreateTimeseries(path, dataType, encoding, compressor, nil, nil)
	checkError(status, err)
}

func createMultiTimeseries() {
	var paths = []string{"root.sg1.dev1.temperature"}
	var dataTypes = []int32{client.TEXT}
	var encodings = []int32{client.PLAIN}
	var compressors = []int32{client.SNAPPY}
	status, err := session.CreateMultiTimeseries(paths, dataTypes, encodings, compressors)
	checkError(status, err)
}

func deleteTimeseries() {
	var paths = []string{"root.sg1.dev1.status"}
	status, err := session.DeleteTimeseries(paths)
	checkError(status, err)
}

func insertStringRecord() {
	var deviceId = "root.ln.wf02.wt02"
	var measurements = []string{"hardware"}
	var values = []string{"123"}
	var timestamp int64 = 12
	status, err := session.InsertStringRecord(deviceId, measurements, values, timestamp)
	checkError(status, err)
}

func checkError(status *rpc.TSStatus, err error) {
	if err != nil {
		if status != nil {
			log.Printf("status: %d, msg: %v", status.Code, status.Message)
		}
		log.Fatal(err)
	}
}

func deleteData() {
	var paths = []string{"root.sg1.dev1.status"}
	var startTime int64 = 0
	var endTime int64 = 12
	status, err := session.DeleteData(paths, startTime, endTime)
	checkError(status, err)
}

func setTimeZone() {
	var timeZone = "GMT"
	status, err := session.SetTimeZone(timeZone)
	checkError(status, err)
}

func getTimeZone() (string, error) {
	return session.GetTimeZone()
}
