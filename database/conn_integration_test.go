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

package iotdb_go

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ==================== Connection Lifecycle ====================

// TestConnect_Ping verifies a basic ping to the server succeeds.
func TestConnect_Ping(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	db, err := sql.Open("iotdb", "iotdb://root:root@127.0.0.1:6667")
	require.NoError(t, err)
	defer db.Close()

	err = db.PingContext(context.Background())
	require.NoError(t, err)
}

// TestConnect_PingTimeout verifies ping succeeds within a context deadline.
func TestConnect_PingTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	db, err := sql.Open("iotdb", "iotdb://root:root@127.0.0.1:6667")
	require.NoError(t, err)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = db.PingContext(ctx)
	require.NoError(t, err)
}

// TestConnect_PingUnreachable verifies that ping to an unreachable host returns an error.
func TestConnect_PingUnreachable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	db, err := sql.Open("iotdb", "iotdb://root:root@127.0.0.1:19999")
	require.NoError(t, err)
	defer db.Close()

	err = db.PingContext(context.Background())
	require.Error(t, err)
}

// ==================== Connection Pool ====================

// TestConnect_ConnectionPool verifies pool settings are applied and connections are created.
func TestConnect_ConnectionPool(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	db, err := sql.Open("iotdb", "iotdb://root:root@127.0.0.1:6667")
	require.NoError(t, err)
	defer db.Close()

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(10 * time.Minute)

	assert.Equal(t, 10, db.Stats().MaxOpenConnections)

	// Acquire a connection to force pool creation
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	assert.GreaterOrEqual(t, db.Stats().OpenConnections, 1)
}

// ==================== Close ====================

// TestConnect_Close verifies that closing a db handle succeeds cleanly.
func TestConnect_Close(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	db, err := sql.Open("iotdb", "iotdb://root:root@127.0.0.1:6667")
	require.NoError(t, err)

	// Ping first to establish a real connection
	err = db.PingContext(context.Background())
	require.NoError(t, err)

	err = db.Close()
	require.NoError(t, err)
}

// ==================== Exec + Query Round Trip ====================

// TestConnect_QueryBasic verifies a full write-then-read cycle through the driver.
func TestConnect_QueryBasic(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	db, err := sql.Open("iotdb", "iotdb://root:root@127.0.0.1:6667")
	require.NoError(t, err)
	defer db.Close()

	sg := "root.conn_test"
	_, err = db.ExecContext(context.Background(), "CREATE DATABASE "+sg)
	require.NoError(t, err)
	defer func() { _, _ = db.ExecContext(context.Background(), "DELETE DATABASE "+sg) }()

	_, err = db.ExecContext(context.Background(),
		"CREATE TIMESERIES "+sg+".d1.value WITH DATATYPE=FLOAT, ENCODING=PLAIN")
	require.NoError(t, err)

	_, err = db.ExecContext(context.Background(),
		"INSERT INTO "+sg+".d1(timestamp, value) VALUES (?, ?)", 1, 123.45)
	require.NoError(t, err)

	rows, err := db.QueryContext(context.Background(), "SELECT value FROM "+sg+".d1")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var ts int64
	var value float64
	err = rows.Scan(&ts, &value)
	require.NoError(t, err)
	assert.InDelta(t, 123.45, value, 0.01)
	require.NoError(t, rows.Err())
}

// TestConnect_ShowDatabases verifies metadata queries work through a fresh connection.
func TestConnect_ShowDatabases(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	db, err := sql.Open("iotdb", "iotdb://root:root@127.0.0.1:6667")
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.QueryContext(context.Background(), "SHOW DATABASES")
	require.NoError(t, err)
	defer rows.Close()

	var count int
	for rows.Next() {
		count++
	}
	assert.GreaterOrEqual(t, count, 1)
	require.NoError(t, rows.Err())
}
