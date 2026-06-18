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
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var stdConn *sql.DB

func init() {
	var err error
	stdConn, err = sql.Open("iotdb", defaultAddress)
	if err != nil {
		panic(err)
	}
}

// ==================== INSERT Operations ====================

// TestStd_BasicInsert verifies a single-row INSERT with mixed types.
func TestStd_BasicInsert(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sg := "root.std_test"
	_, err := stdConn.ExecContext(context.Background(), "CREATE DATABASE "+sg)
	require.NoError(t, err)
	defer func() { _, _ = stdConn.ExecContext(context.Background(), "DELETE DATABASE "+sg) }()

	_, err = stdConn.ExecContext(context.Background(),
		"CREATE TIMESERIES "+sg+".d1.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN")
	require.NoError(t, err)

	_, err = stdConn.ExecContext(context.Background(),
		"CREATE TIMESERIES "+sg+".d1.temp WITH DATATYPE=FLOAT, ENCODING=PLAIN")
	require.NoError(t, err)

	_, err = stdConn.ExecContext(context.Background(),
		"INSERT INTO "+sg+".d1(timestamp, status, temp) VALUES (?, ?, ?)",
		1, true, 25.5)
	require.NoError(t, err)

	// Verify inserted data can be read back
	rows, err := stdConn.QueryContext(context.Background(), "SELECT status, temp FROM "+sg+".d1")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var ts int64
	var status bool
	var temp float64
	err = rows.Scan(&ts, &status, &temp)
	require.NoError(t, err)
	assert.True(t, status)
	assert.InDelta(t, 25.5, temp, 0.1)
	require.NoError(t, rows.Err())
}

// TestStd_MultiValueInsert verifies inserting multiple rows in a single statement.
func TestStd_MultiValueInsert(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sg := "root.std_multi"
	_, err := stdConn.ExecContext(context.Background(), "CREATE DATABASE "+sg)
	require.NoError(t, err)
	defer func() { _, _ = stdConn.ExecContext(context.Background(), "DELETE DATABASE "+sg) }()

	_, err = stdConn.ExecContext(context.Background(),
		"CREATE TIMESERIES "+sg+".d1.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN")
	require.NoError(t, err)

	_, err = stdConn.ExecContext(context.Background(),
		"CREATE TIMESERIES "+sg+".d1.temp WITH DATATYPE=FLOAT, ENCODING=PLAIN")
	require.NoError(t, err)

	_, err = stdConn.ExecContext(context.Background(),
		"INSERT INTO "+sg+".d1(timestamp, status, temp) VALUES (?, ?, ?), (?, ?, ?)",
		1, true, 25.0,
		2, false, 26.0)
	require.NoError(t, err)

	// Verify both rows exist
	rows, err := stdConn.QueryContext(context.Background(), "SELECT temp FROM "+sg+".d1 ORDER BY time ASC")
	require.NoError(t, err)
	defer rows.Close()

	var count int
	for rows.Next() {
		count++
	}
	assert.Equal(t, 2, count)
	require.NoError(t, rows.Err())
}

// TestStd_InsertTypes verifies INSERT with INT64 and FLOAT types.
func TestStd_InsertTypes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sg := "root.std_types"
	_, err := stdConn.ExecContext(context.Background(), "CREATE DATABASE "+sg)
	require.NoError(t, err)
	defer func() { _, _ = stdConn.ExecContext(context.Background(), "DELETE DATABASE "+sg) }()

	_, err = stdConn.ExecContext(context.Background(),
		"CREATE TIMESERIES "+sg+".d1.int_val WITH DATATYPE=INT64, ENCODING=PLAIN")
	require.NoError(t, err)

	_, err = stdConn.ExecContext(context.Background(),
		"CREATE TIMESERIES "+sg+".d1.float_val WITH DATATYPE=FLOAT, ENCODING=PLAIN")
	require.NoError(t, err)

	_, err = stdConn.ExecContext(context.Background(),
		"INSERT INTO "+sg+".d1(timestamp, int_val, float_val) VALUES (?, ?, ?)",
		1, 1718193600000, 99.9)
	require.NoError(t, err)

	// Verify values
	row := stdConn.QueryRowContext(context.Background(),
		"SELECT int_val, float_val FROM "+sg+".d1")
	var ts int64
	var intVal int64
	var floatVal float64
	err = row.Scan(&ts, &intVal, &floatVal)
	require.NoError(t, err)
	assert.Equal(t, int64(1718193600000), intVal)
	assert.InDelta(t, 99.9, floatVal, 0.1)
}

// ==================== Basic Query ====================

// TestStd_QueryBasic verifies a round-trip INSERT then SELECT.
func TestStd_QueryBasic(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sg := "root.std_query"
	_, err := stdConn.ExecContext(context.Background(), "CREATE DATABASE "+sg)
	require.NoError(t, err)
	defer func() { _, _ = stdConn.ExecContext(context.Background(), "DELETE DATABASE "+sg) }()

	_, err = stdConn.ExecContext(context.Background(),
		"CREATE TIMESERIES "+sg+".d1.temp WITH DATATYPE=FLOAT, ENCODING=PLAIN")
	require.NoError(t, err)

	_, err = stdConn.ExecContext(context.Background(),
		"INSERT INTO "+sg+".d1(timestamp, temp) VALUES (?, ?)",
		1704067200000, 25.5)
	require.NoError(t, err)

	rows, err := stdConn.QueryContext(context.Background(), "SELECT temp FROM "+sg+".d1")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var ts int64
	var temp float64
	err = rows.Scan(&ts, &temp)
	require.NoError(t, err)
	assert.InDelta(t, 25.5, temp, 0.1)
	require.NoError(t, rows.Err())
}

// ==================== Struct Scanning ====================

// TestStd_ScanStruct verifies scanning multiple columns into struct fields.
func TestStd_ScanStruct(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sg := "root.std_scan"
	_, err := stdConn.ExecContext(context.Background(), "CREATE DATABASE "+sg)
	require.NoError(t, err)
	defer func() { _, _ = stdConn.ExecContext(context.Background(), "DELETE DATABASE "+sg) }()

	_, err = stdConn.ExecContext(context.Background(),
		"CREATE TIMESERIES "+sg+".d1.temp WITH DATATYPE=FLOAT, ENCODING=PLAIN")
	require.NoError(t, err)

	_, err = stdConn.ExecContext(context.Background(),
		"CREATE TIMESERIES "+sg+".d1.hum WITH DATATYPE=DOUBLE, ENCODING=PLAIN")
	require.NoError(t, err)

	_, err = stdConn.ExecContext(context.Background(),
		"INSERT INTO "+sg+".d1(timestamp, temp, hum) VALUES (?, ?, ?)",
		1704067200000, 25.5, 60.5)
	require.NoError(t, err)

	rows, err := stdConn.QueryContext(context.Background(), "SELECT temp, hum FROM "+sg+".d1")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())

	type Reading struct {
		Time int64
		Temp float64
		Hum  float64
	}
	var r Reading
	err = rows.Scan(&r.Time, &r.Temp, &r.Hum)
	require.NoError(t, err)
	assert.InDelta(t, 25.5, r.Temp, 0.1)
	assert.InDelta(t, 60.5, r.Hum, 0.1)
	require.NoError(t, rows.Err())
}

// ==================== Metadata Queries ====================

// TestStd_ShowDatabases verifies SHOW DATABASES returns at least one result.
func TestStd_ShowDatabases(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	rows, err := stdConn.QueryContext(context.Background(), "SHOW DATABASES")
	require.NoError(t, err)
	defer rows.Close()

	var count int
	for rows.Next() {
		count++
	}
	assert.GreaterOrEqual(t, count, 1)
	require.NoError(t, rows.Err())
}

// ==================== Transactions ====================

// TestStd_BeginUnsupported verifies that Begin reports transactions as
// unsupported rather than returning a fake no-op transaction.
func TestStd_BeginUnsupported(t *testing.T) {
	tx, err := (&stdDriver{}).Begin()
	assert.Nil(t, tx)
	assert.ErrorIs(t, err, ErrTransactionsUnsupported)
}

// TestStd_BeginTxUnsupported verifies the same for the context-aware BeginTx.
func TestStd_BeginTxUnsupported(t *testing.T) {
	tx, err := (&stdDriver{}).BeginTx(context.Background(), driver.TxOptions{})
	assert.Nil(t, tx)
	assert.ErrorIs(t, err, ErrTransactionsUnsupported)
}

// TestStd_DBBeginUnsupported verifies the error surfaces through database/sql.
func TestStd_DBBeginUnsupported(t *testing.T) {
	_, err := stdConn.Begin()
	assert.True(t, errors.Is(err, ErrTransactionsUnsupported))
}
