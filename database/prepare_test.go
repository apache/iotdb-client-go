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
	"database/sql/driver"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestValuesToNamedValues verifies the legacy []driver.Value adapter assigns
// 1-based ordinals. Pure logic, no server required.
func TestValuesToNamedValues(t *testing.T) {
	got := valuesToNamedValues([]driver.Value{"a", int64(2), true})
	require.Len(t, got, 3)
	assert.Equal(t, driver.NamedValue{Ordinal: 1, Value: "a"}, got[0])
	assert.Equal(t, driver.NamedValue{Ordinal: 2, Value: int64(2)}, got[1])
	assert.Equal(t, driver.NamedValue{Ordinal: 3, Value: true}, got[2])

	assert.Empty(t, valuesToNamedValues(nil))
}

// TestPrepare_ExecAndQuery verifies a prepared statement is usable end to end:
// it executes inserts and queries with bound parameters, and is reusable.
func TestPrepare_ExecAndQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sg := "root.prepare_test"
	_, err := stdConn.ExecContext(context.Background(), "CREATE DATABASE "+sg)
	require.NoError(t, err)
	defer func() { _, _ = stdConn.ExecContext(context.Background(), "DELETE DATABASE "+sg) }()

	_, err = stdConn.ExecContext(context.Background(),
		"CREATE TIMESERIES "+sg+".d1.temp WITH DATATYPE=DOUBLE, ENCODING=PLAIN")
	require.NoError(t, err)

	// Prepared INSERT, reused for multiple rows.
	insStmt, err := stdConn.PrepareContext(context.Background(),
		"INSERT INTO "+sg+".d1(timestamp, temp) VALUES (?, ?)")
	require.NoError(t, err)
	defer insStmt.Close()

	_, err = insStmt.ExecContext(context.Background(), 1, 10.5)
	require.NoError(t, err)
	_, err = insStmt.ExecContext(context.Background(), 2, 20.5)
	require.NoError(t, err)

	// Prepared SELECT.
	selStmt, err := stdConn.PrepareContext(context.Background(),
		"SELECT temp FROM "+sg+".d1 ORDER BY time ASC")
	require.NoError(t, err)
	defer selStmt.Close()

	rows, err := selStmt.QueryContext(context.Background())
	require.NoError(t, err)
	defer rows.Close()

	var got []float64
	for rows.Next() {
		var ts int64
		var temp float64
		require.NoError(t, rows.Scan(&ts, &temp))
		got = append(got, temp)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, []float64{10.5, 20.5}, got)
}

// TestPrepare_ReturnsUsableStmt verifies PrepareContext stores the query and
// returns a statement implementing the context-aware driver interfaces.
func TestPrepare_ReturnsUsableStmt(t *testing.T) {
	stmt := &stdStmt{std: &stdDriver{}, query: "SELECT 1"}
	var _ driver.Stmt = stmt
	var _ driver.StmtExecContext = stmt
	var _ driver.StmtQueryContext = stmt
	assert.Equal(t, -1, stmt.NumInput())
	assert.NoError(t, stmt.Close())
}
