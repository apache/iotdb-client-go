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

package main

import (
	"flag"
	"github.com/apache/iotdb-client-go/client"
	"github.com/apache/iotdb-client-go/common"
	"log"
	"math/rand"
	"strconv"
	"time"
)

func main() {
	flag.Parse()
	config := &client.Config{
		Host:     "127.0.0.1",
		Port:     "6667",
		UserName: "root",
		Password: "root",
		Database: "test_session",
	}
	session, err := client.NewTableSession(config, false, 0)
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	checkError(session.ExecuteNonQueryStatement("create database test_db"))
	checkError(session.ExecuteNonQueryStatement("use test_db"))
	checkError(session.ExecuteNonQueryStatement("create table t1 (id1 string id, id2 string id, s1 text measurement, s2 text measurement)"))
	insertRelationalTablet(session)
	showTables(session)
	query(session)
}

func getTextValueFromDataSet(dataSet *client.SessionDataSet, columnName string) string {
	if dataSet.IsNull(columnName) {
		return "null"
	} else {
		return dataSet.GetText(columnName)
	}
}

func insertRelationalTablet(session client.ITableSession) {
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
	if err != nil {
		log.Fatal("Failed to create relational tablet {}", err)
	}
	ts := time.Now().UTC().UnixNano() / 1000000
	for row := 0; row < 16; row++ {
		ts++
		tablet.SetTimestamp(ts, row)
		tablet.SetValueAt("id1_field_"+strconv.Itoa(row), 0, row)
		tablet.SetValueAt("id2_field_"+strconv.Itoa(row), 1, row)
		tablet.SetValueAt("s1_value_"+strconv.Itoa(row), 2, row)
		tablet.SetValueAt("s2_value_"+strconv.Itoa(row), 3, row)
		tablet.RowSize++
	}
	checkError(session.Insert(tablet))

	tablet.Reset()

	for row := 0; row < 16; row++ {
		ts++
		tablet.SetTimestamp(ts, row)
		tablet.SetValueAt("id1_field_1", 0, row)
		tablet.SetValueAt("id2_field_1", 1, row)
		tablet.SetValueAt("s1_value_"+strconv.Itoa(row), 2, row)
		tablet.SetValueAt("s2_value_"+strconv.Itoa(row), 3, row)

		nullValueColumn := rand.Intn(4)
		tablet.SetValueAt(nil, nullValueColumn, row)
		tablet.RowSize++
	}
	checkError(session.Insert(tablet))
}

func showTables(session client.ITableSession) {
	timeout := int64(2000)
	dataSet, err := session.ExecuteQueryStatement("show tables", &timeout)
	if err != nil {
		log.Fatal(err)
	}
	for {
		hasNext, err := dataSet.Next()
		if err != nil {
			log.Fatal(err)
		}
		if !hasNext {
			break
		}
		log.Printf("tableName is", dataSet.GetText("TableName"))
	}
}

func query(session client.ITableSession) {
	timeout := int64(2000)
	dataSet, err := session.ExecuteQueryStatement("select * from t1", &timeout)
	if err != nil {
		log.Fatal(err)
	}
	for {
		hasNext, err := dataSet.Next()
		if err != nil {
			log.Fatal(err)
		}
		if !hasNext {
			break
		}
		log.Printf("%v %v %v %v", getTextValueFromDataSet(dataSet, "id1"), getTextValueFromDataSet(dataSet, "id2"), getTextValueFromDataSet(dataSet, "s1"), getTextValueFromDataSet(dataSet, "s2"))
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
