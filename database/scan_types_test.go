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

var scanConn *sql.DB

func init() {
	var err error
	scanConn, err = sql.Open("iotdb", defaultAddress)
	if err != nil {
		panic(err)
	}
}

// setupScanTestData creates a database with all supported data types and inserts one row.
func setupScanTestData(t *testing.T, db *sql.DB, sg string) func() {
	_, err := db.ExecContext(context.Background(), "CREATE DATABASE "+sg)
	require.NoError(t, err)

	timeseries := []struct {
		name     string
		dataType string
	}{
		{"int32_val", "INT32"},
		{"int64_val", "INT64"},
		{"float_val", "FLOAT"},
		{"double_val", "DOUBLE"},
		{"bool_val", "BOOLEAN"},
		{"text_val", "TEXT"},
	}
	for _, ts := range timeseries {
		_, err = db.ExecContext(context.Background(),
			"CREATE TIMESERIES "+sg+".d1."+ts.name+" WITH DATATYPE="+ts.dataType+", ENCODING=PLAIN")
		require.NoError(t, err)
	}

	_, err = db.ExecContext(context.Background(),
		"INSERT INTO "+sg+".d1(timestamp, int32_val, int64_val, float_val, double_val, bool_val, text_val) "+
			"VALUES (?, ?, ?, ?, ?, ?, ?)",
		1704067200000, 42, 1718193600000, 3.14, 2.718281828, true, "hello world")
	require.NoError(t, err)

	return func() {
		_, _ = db.ExecContext(context.Background(), "DELETE DATABASE "+sg)
	}
}

// ==================== Individual Type Scan Tests ====================

// TestScan_Int32 verifies INT32 column scans correctly as int32.
func TestScan_Int32(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	cleanup := setupScanTestData(t, scanConn, "root.scan_int32")
	defer cleanup()

	rows, err := scanConn.QueryContext(context.Background(),
		"SELECT int32_val FROM root.scan_int32.d1")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var ts int64
	var val int32
	err = rows.Scan(&ts, &val)
	require.NoError(t, err)
	assert.Equal(t, int32(42), val)
	require.NoError(t, rows.Err())
}

// TestScan_Int64 verifies INT64 column handles large values without truncation.
func TestScan_Int64(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	cleanup := setupScanTestData(t, scanConn, "root.scan_int64")
	defer cleanup()

	rows, err := scanConn.QueryContext(context.Background(),
		"SELECT int64_val FROM root.scan_int64.d1")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var ts int64
	var val int64
	err = rows.Scan(&ts, &val)
	require.NoError(t, err)
	assert.Equal(t, int64(1718193600000), val)
	require.NoError(t, rows.Err())
}

// TestScan_Float verifies FLOAT column scans correctly.
func TestScan_Float(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	cleanup := setupScanTestData(t, scanConn, "root.scan_float")
	defer cleanup()

	rows, err := scanConn.QueryContext(context.Background(),
		"SELECT float_val FROM root.scan_float.d1")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var ts int64
	var val float64
	err = rows.Scan(&ts, &val)
	require.NoError(t, err)
	assert.InDelta(t, 3.14, val, 0.01)
	require.NoError(t, rows.Err())
}

// TestScan_Double verifies DOUBLE column scans with full precision.
func TestScan_Double(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	cleanup := setupScanTestData(t, scanConn, "root.scan_double")
	defer cleanup()

	rows, err := scanConn.QueryContext(context.Background(),
		"SELECT double_val FROM root.scan_double.d1")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var ts int64
	var val float64
	err = rows.Scan(&ts, &val)
	require.NoError(t, err)
	assert.InDelta(t, 2.718281828, val, 0.000001)
	require.NoError(t, rows.Err())
}

// TestScan_Boolean verifies BOOLEAN column scans as bool.
func TestScan_Boolean(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	cleanup := setupScanTestData(t, scanConn, "root.scan_bool")
	defer cleanup()

	rows, err := scanConn.QueryContext(context.Background(),
		"SELECT bool_val FROM root.scan_bool.d1")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var ts int64
	var val bool
	err = rows.Scan(&ts, &val)
	require.NoError(t, err)
	assert.True(t, val)
	require.NoError(t, rows.Err())
}

// TestScan_Text verifies TEXT/STRING column scans as string.
func TestScan_Text(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	cleanup := setupScanTestData(t, scanConn, "root.scan_text")
	defer cleanup()

	rows, err := scanConn.QueryContext(context.Background(),
		"SELECT text_val FROM root.scan_text.d1")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var ts int64
	var val string
	err = rows.Scan(&ts, &val)
	require.NoError(t, err)
	assert.Equal(t, "hello world", val)
	require.NoError(t, rows.Err())
}

// ==================== Special String Values ====================

// TestScan_TextSpecialChars verifies TEXT handles quotes, backslashes, and unicode.
func TestScan_TextSpecialChars(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sg := "root.scan_text_special"
	_, err := scanConn.ExecContext(context.Background(), "CREATE DATABASE "+sg)
	require.NoError(t, err)
	defer func() { _, _ = scanConn.ExecContext(context.Background(), "DELETE DATABASE "+sg) }()

	_, err = scanConn.ExecContext(context.Background(),
		"CREATE TIMESERIES "+sg+".d1.msg WITH DATATYPE=TEXT, ENCODING=PLAIN")
	require.NoError(t, err)

	testCases := []struct {
		name  string
		ts    int
		value string
	}{
		{"chinese", 1, "你好世界"},
		{"emoji", 2, "hello 🌍"},
	}

	for _, tc := range testCases {
		_, err = scanConn.ExecContext(context.Background(),
			"INSERT INTO "+sg+".d1(timestamp, msg) VALUES (?, ?)", tc.ts, tc.value)
		require.NoError(t, err, "insert failed for %s", tc.name)
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rows, err := scanConn.QueryContext(context.Background(),
				"SELECT msg FROM "+sg+".d1 WHERE time = ?", tc.ts)
			require.NoError(t, err)
			defer rows.Close()

			require.True(t, rows.Next())
			var ts int64
			var val string
			err = rows.Scan(&ts, &val)
			require.NoError(t, err)
			assert.Equal(t, tc.value, val)
			require.NoError(t, rows.Err())
		})
	}
}

// ==================== Empty Result Set ====================

// TestScan_EmptyResult verifies rows.Next() returns false for empty result.
func TestScan_EmptyResult(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	cleanup := setupScanTestData(t, scanConn, "root.scan_empty")
	defer cleanup()

	rows, err := scanConn.QueryContext(context.Background(),
		"SELECT int32_val FROM root.scan_empty.d1 WHERE time = 999999999999999")
	require.NoError(t, err)
	defer rows.Close()

	assert.False(t, rows.Next())
	require.NoError(t, rows.Err())
}

// ==================== Multiple Columns In Single Scan ====================

// TestScan_AllTypesInOneRow verifies scanning all supported types in a single row.
func TestScan_AllTypesInOneRow(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	cleanup := setupScanTestData(t, scanConn, "root.scan_all")
	defer cleanup()

	rows, err := scanConn.QueryContext(context.Background(),
		"SELECT int32_val, int64_val, float_val, double_val, bool_val, text_val FROM root.scan_all.d1")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var (
		ts        int64
		int32Val  int32
		int64Val  int64
		floatVal  float64
		doubleVal float64
		boolVal   bool
		textVal   string
	)
	err = rows.Scan(&ts, &int32Val, &int64Val, &floatVal, &doubleVal, &boolVal, &textVal)
	require.NoError(t, err)

	assert.Equal(t, int32(42), int32Val)
	assert.Equal(t, int64(1718193600000), int64Val)
	assert.InDelta(t, 3.14, floatVal, 0.01)
	assert.InDelta(t, 2.718281828, doubleVal, 0.000001)
	assert.True(t, boolVal)
	assert.Equal(t, "hello world", textVal)
	require.NoError(t, rows.Err())
}

// TestScan_Null verifies that SQL NULLs surface as nil to database/sql, so
// sql.Null* and pointer scans report absence instead of typed zero values.
func TestScan_Null(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sg := "root.scan_null"
	_, err := scanConn.ExecContext(context.Background(), "CREATE DATABASE "+sg)
	require.NoError(t, err)
	defer func() { _, _ = scanConn.ExecContext(context.Background(), "DELETE DATABASE "+sg) }()

	for _, ts := range []struct{ name, dataType string }{
		{"present", "INT64"},
		{"missing_text", "TEXT"},
		{"missing_double", "DOUBLE"},
		{"missing_int", "INT64"},
	} {
		_, err = scanConn.ExecContext(context.Background(),
			"CREATE TIMESERIES "+sg+".d1."+ts.name+" WITH DATATYPE="+ts.dataType+", ENCODING=PLAIN")
		require.NoError(t, err)
	}

	// Insert only the "present" column, leaving the other three NULL for this row.
	_, err = scanConn.ExecContext(context.Background(),
		"INSERT INTO "+sg+".d1(timestamp, present) VALUES (?, ?)", 1704067200000, 7)
	require.NoError(t, err)

	rows, err := scanConn.QueryContext(context.Background(),
		"SELECT present, missing_text, missing_double, missing_int FROM "+sg+".d1")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var (
		ts      int64
		present sql.NullInt64
		text    sql.NullString
		double  sql.NullFloat64
		intPtr  *int64
	)
	err = rows.Scan(&ts, &present, &text, &double, &intPtr)
	require.NoError(t, err)

	assert.True(t, present.Valid)
	assert.Equal(t, int64(7), present.Int64)
	assert.False(t, text.Valid, "NULL TEXT should scan as invalid, not empty string")
	assert.False(t, double.Valid, "NULL DOUBLE should scan as invalid, not 0")
	assert.Nil(t, intPtr, "NULL INT64 should scan as nil pointer, not 0")
	require.NoError(t, rows.Err())
}
