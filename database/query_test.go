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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var queryConn *sql.DB

func init() {
	var err error
	queryConn, err = sql.Open("iotdb", "iotdb://root:root@127.0.0.1:6667")
	if err != nil {
		panic(err)
	}
}

// setupQueryTestData creates a storage group with two devices and sample data.
// Returns a cleanup function that deletes the storage group.
func setupQueryTestData(t *testing.T, db *sql.DB, sg string) func() {
	_, err := db.ExecContext(context.Background(), "CREATE DATABASE "+sg)
	require.NoError(t, err)

	_, err = db.ExecContext(context.Background(),
		"CREATE TIMESERIES "+sg+".d1.temp WITH DATATYPE=FLOAT, ENCODING=PLAIN")
	require.NoError(t, err)

	_, err = db.ExecContext(context.Background(),
		"CREATE TIMESERIES "+sg+".d1.hum WITH DATATYPE=DOUBLE, ENCODING=PLAIN")
	require.NoError(t, err)

	_, err = db.ExecContext(context.Background(),
		"CREATE TIMESERIES "+sg+".d1.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN")
	require.NoError(t, err)

	_, err = db.ExecContext(context.Background(),
		"CREATE TIMESERIES "+sg+".d2.temp WITH DATATYPE=FLOAT, ENCODING=PLAIN")
	require.NoError(t, err)

	_, err = db.ExecContext(context.Background(),
		"INSERT INTO "+sg+".d1(timestamp, temp, hum, status) VALUES (?, ?, ?, ?), (?, ?, ?, ?), (?, ?, ?, ?), (?, ?, ?, ?), (?, ?, ?, ?)",
		1704067200000, 20.0, 60.0, true,
		1704067260000, 21.0, 61.0, false,
		1704067320000, 22.0, 62.0, true,
		1704067380000, 23.0, 63.0, false,
		1704067440000, 24.0, 64.0, true)
	require.NoError(t, err)

	_, err = db.ExecContext(context.Background(),
		"INSERT INTO "+sg+".d2(timestamp, temp) VALUES (?, ?), (?, ?), (?, ?)",
		1704067200000, 30.0,
		1704067260000, 31.0,
		1704067320000, 32.0)
	require.NoError(t, err)

	return func() {
		_, _ = db.ExecContext(context.Background(), "DELETE DATABASE "+sg)
	}
}

// countRows iterates all rows and returns the count, asserting no iteration error.
func countRows(t *testing.T, rows *sql.Rows) int {
	var count int
	for rows.Next() {
		count++
	}
	require.NoError(t, rows.Err())
	return count
}

// ==================== SELECT Basics ====================

// TestQuery_SelectSingleColumn verifies selecting one column returns all rows.
func TestQuery_SelectSingleColumn(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	cleanup := setupQueryTestData(t, queryConn, "root.select_single")
	defer cleanup()

	rows, err := queryConn.QueryContext(context.Background(),
		"SELECT temp FROM root.select_single.d1")
	require.NoError(t, err)
	defer rows.Close()

	assert.Equal(t, 5, countRows(t, rows))
}

// TestQuery_SelectMultipleColumns verifies column metadata for multi-column select.
func TestQuery_SelectMultipleColumns(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	cleanup := setupQueryTestData(t, queryConn, "root.select_multi")
	defer cleanup()

	rows, err := queryConn.QueryContext(context.Background(),
		"SELECT temp, hum, status FROM root.select_multi.d1")
	require.NoError(t, err)
	defer rows.Close()

	cols, err := rows.Columns()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(cols), 3)
}

// TestQuery_SelectAll verifies SELECT * returns all rows.
func TestQuery_SelectAll(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	cleanup := setupQueryTestData(t, queryConn, "root.select_all")
	defer cleanup()

	rows, err := queryConn.QueryContext(context.Background(),
		"SELECT * FROM root.select_all.d1")
	require.NoError(t, err)
	defer rows.Close()

	assert.Equal(t, 5, countRows(t, rows))
}

// TestQuery_SelectWithAlias verifies column aliasing works.
func TestQuery_SelectWithAlias(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	cleanup := setupQueryTestData(t, queryConn, "root.select_alias")
	defer cleanup()

	rows, err := queryConn.QueryContext(context.Background(),
		"SELECT temp AS temperature, hum AS humidity FROM root.select_alias.d1")
	require.NoError(t, err)
	defer rows.Close()

	cols, err := rows.Columns()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(cols), 2)
}

// ==================== WHERE Clause ====================

// TestQuery_WhereTimeFilter verifies time-based filtering.
func TestQuery_WhereTimeFilter(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	cleanup := setupQueryTestData(t, queryConn, "root.where_time")
	defer cleanup()

	rows, err := queryConn.QueryContext(context.Background(),
		"SELECT temp FROM root.where_time.d1 WHERE time >= 1704067260000")
	require.NoError(t, err)
	defer rows.Close()

	assert.Equal(t, 4, countRows(t, rows))
}

// TestQuery_WhereTimeRange verifies time range filtering with AND.
func TestQuery_WhereTimeRange(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	cleanup := setupQueryTestData(t, queryConn, "root.where_range")
	defer cleanup()

	rows, err := queryConn.QueryContext(context.Background(),
		"SELECT temp FROM root.where_range.d1 WHERE time >= 1704067260000 AND time < 1704067380000")
	require.NoError(t, err)
	defer rows.Close()

	assert.Equal(t, 2, countRows(t, rows))
}

// TestQuery_WhereValueFilter verifies value-based filtering.
func TestQuery_WhereValueFilter(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	cleanup := setupQueryTestData(t, queryConn, "root.where_value")
	defer cleanup()

	rows, err := queryConn.QueryContext(context.Background(),
		"SELECT temp FROM root.where_value.d1 WHERE temp > 22.0")
	require.NoError(t, err)
	defer rows.Close()

	assert.Equal(t, 2, countRows(t, rows))
}

// TestQuery_WhereBooleanFilter verifies boolean value filtering.
func TestQuery_WhereBooleanFilter(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	cleanup := setupQueryTestData(t, queryConn, "root.where_bool")
	defer cleanup()

	rows, err := queryConn.QueryContext(context.Background(),
		"SELECT temp FROM root.where_bool.d1 WHERE status = true")
	require.NoError(t, err)
	defer rows.Close()

	assert.Equal(t, 3, countRows(t, rows))
}

// TestQuery_WhereCombinedFilter verifies compound WHERE with time, value, and boolean.
func TestQuery_WhereCombinedFilter(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	cleanup := setupQueryTestData(t, queryConn, "root.where_combined")
	defer cleanup()

	rows, err := queryConn.QueryContext(context.Background(),
		"SELECT temp FROM root.where_combined.d1 WHERE time > 1704067260000 AND temp > 22.0 AND status = true")
	require.NoError(t, err)
	defer rows.Close()

	assert.Equal(t, 1, countRows(t, rows))
}
