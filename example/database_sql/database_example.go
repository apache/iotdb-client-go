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

// Package iotdb_go_test demonstrates using the database/sql driver for Apache IoTDB.
//
// The driver currently supports only IoTDB's tree model (TreeSqlDialect).
// It constructs a session pool via client.NewSessionPool which uses the tree
// SQL dialect. Table model (TableSqlDialect / TableSessionPool) is not yet
// supported through the database/sql interface.
//
// The DSN format is:
//
//	iotdb://user:password@host:port?fetch_size=1024&time_zone=UTC&connect_retry_max=3
//
// Supported query parameters: fetch_size, time_zone, connect_retry_max.
// Multiple hosts can be specified as comma-separated addresses in the DSN.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	// Register the iotdb driver with database/sql.
	_ "github.com/apache/iotdb-client-go/v2/database"
)

// Example demonstrates how to use the database/sql driver for Apache IoTDB.
// It shows opening a connection, inserting data with bound parameters,
// and querying results with Scan.
func main() {
	// Open a connection. The DSN supports query parameters for fetch_size,
	// time_zone, and connect_retry_max.
	db, err := sql.Open("iotdb", "iotdb://root:root@127.0.0.1:6667?fetch_size=1024&time_zone=UTC&connect_retry_max=5")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create a database using IoTDB tree-syntax paths.
	_, err = db.ExecContext(ctx, "CREATE DATABASE root.example")
	if err != nil {
		log.Fatal(err)
	}

	// Create a timeseries.
	_, err = db.ExecContext(ctx,
		"CREATE TIMESERIES root.example.device1.temperature WITH DATATYPE=FLOAT, ENCODING=PLAIN")
	if err != nil {
		log.Fatal(err)
	}

	// Insert data using ? placeholders for parameter binding.
	// Supports positional (?), numeric ($1, $2), and named (@name) binding styles.
	_, err = db.ExecContext(ctx,
		"INSERT INTO root.example.device1(timestamp, temperature) VALUES (?, ?)",
		1704067200000, 25.5)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.ExecContext(ctx,
		"INSERT INTO root.example.device1(timestamp, temperature) VALUES (?, ?)",
		1704067201000, 26.0)
	if err != nil {
		log.Fatal(err)
	}

	// Query data and scan results.
	rows, err := db.QueryContext(ctx,
		"SELECT temperature FROM root.example.device1 ORDER BY TIME ASC")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var ts int64
		var temp float64
		if err := rows.Scan(&ts, &temp); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("timestamp=%d, temperature=%.1f\n", ts, temp)
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	// Clean up.
	_, _ = db.ExecContext(ctx, "DELETE STORAGE GROUP root.example")
}
