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
	"github.com/apache/iotdb-client-go/client"
	"github.com/apache/iotdb-client-go/common"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	sessionPoolWithSpecificDatabaseExample()
	sessionPoolWithoutSpecificDatabaseExample()
	putBackToSessionPoolExample()
}

func putBackToSessionPoolExample() {
	// should create database test_db before executing
	config := &client.PoolConfig{
		Host:     "127.0.0.1",
		Port:     "6667",
		UserName: "root",
		Password: "root",
		Database: "test_db",
	}
	sessionPool := client.NewTableSessionPool(config, 3, 60000, 4000, false)
	defer sessionPool.Close()

	num := 4
	successGetSessionNum := int32(0)
	var wg sync.WaitGroup
	wg.Add(num)
	for i := 0; i < num; i++ {
		dbName := "db" + strconv.Itoa(i)
		go func() {
			defer wg.Done()
			session, err := sessionPool.GetSession()
			if err != nil {
				log.Println("Failed to create database "+dbName+"because ", err)
				return
			}
			atomic.AddInt32(&successGetSessionNum, 1)
			defer func() {
				time.Sleep(6 * time.Second)
				// put back to session pool
				session.Close()
			}()
			checkError(session.ExecuteNonQueryStatement("create database " + dbName))
			checkError(session.ExecuteNonQueryStatement("use " + dbName))
			checkError(session.ExecuteNonQueryStatement("create table table_of_" + dbName + " (tag1 string tag, tag2 string tag, s1 text field, s2 text field)"))
		}()
	}
	wg.Wait()
	log.Println("success num is", successGetSessionNum)

	log.Println("All session's database have been reset.")
	// the using database will automatically reset to session pool's database after the session closed
	wg.Add(5)
	for i := 0; i < 5; i++ {
		go func() {
			defer wg.Done()
			session, err := sessionPool.GetSession()
			if err != nil {
				log.Println("Failed to get session because ", err)
			}
			defer session.Close()
			timeout := int64(3000)
			dataSet, err := session.ExecuteQueryStatement("show tables", &timeout)
			for {
				hasNext, err := dataSet.Next()
				if err != nil {
					log.Fatal(err)
				}
				if !hasNext {
					break
				}
				log.Println("table is", dataSet.GetText("TableName"))
			}
		}()
	}
	wg.Wait()
}

func sessionPoolWithSpecificDatabaseExample() {
	// should create database test_db before executing
	config := &client.PoolConfig{
		Host:     "127.0.0.1",
		Port:     "6667",
		UserName: "root",
		Password: "root",
		Database: "test_db",
	}
	sessionPool := client.NewTableSessionPool(config, 3, 60000, 8000, false)
	defer sessionPool.Close()
	num := 10
	var wg sync.WaitGroup
	wg.Add(num)
	for i := 0; i < num; i++ {
		tableName := "t" + strconv.Itoa(i)
		go func() {
			defer wg.Done()
			session, err := sessionPool.GetSession()
			if err != nil {
				log.Println("Failed to create table "+tableName+"because ", err)
				return
			}
			defer session.Close()
			checkError(session.ExecuteNonQueryStatement("create table " + tableName + " (tag1 string tag, tag2 string tag, s1 text field, s2 text field)"))
		}()
	}
	wg.Wait()
}

func sessionPoolWithoutSpecificDatabaseExample() {
	config := &client.PoolConfig{
		Host:     "127.0.0.1",
		Port:     "6667",
		UserName: "root",
		Password: "root",
	}
	sessionPool := client.NewTableSessionPool(config, 3, 60000, 8000, false)
	defer sessionPool.Close()
	num := 10
	var wg sync.WaitGroup
	wg.Add(num)
	for i := 0; i < num; i++ {
		dbName := "db" + strconv.Itoa(i)
		go func() {
			defer wg.Done()
			session, err := sessionPool.GetSession()
			if err != nil {
				log.Println("Failed to create database ", dbName, err)
				return
			}
			defer session.Close()
			checkError(session.ExecuteNonQueryStatement("create database " + dbName))
			checkError(session.ExecuteNonQueryStatement("use " + dbName))
			checkError(session.ExecuteNonQueryStatement("create table t1 (tag1 string tag, tag2 string tag, s1 text field, s2 text field)"))
		}()
	}
	wg.Wait()
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
