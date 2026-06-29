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
	"testing"
	"time"

	"github.com/apache/iotdb-client-go/v2/database/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBind_Positional_Basic(t *testing.T) {
	result, err := bind(nil, "INSERT INTO t(a, b) VALUES (?, ?)", "hello", 42)
	require.NoError(t, err)
	assert.Equal(t, "INSERT INTO t(a, b) VALUES ('hello', 42)", result)
}

func TestBind_Positional_MultipleValues(t *testing.T) {
	result, err := bind(nil, "SELECT * FROM t WHERE a = ? AND b = ? AND c = ?", "x", 1, true)
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE a = 'x' AND b = 1 AND c = 1", result)
}

func TestBind_Positional_EscapedQuestionMark(t *testing.T) {
	result, err := bind(nil, `SELECT * FROM t WHERE a = \? AND b = ?`, "val")
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE a = ? AND b = 'val'", result)
}

func TestBind_Positional_TooFewArgs(t *testing.T) {
	_, err := bind(nil, "INSERT INTO t VALUES (?, ?, ?)", "a")
	assert.Error(t, err)
}

func TestBind_Positional_NoArgs(t *testing.T) {
	result, err := bind(nil, "SELECT * FROM t")
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t", result)
}

func TestBind_Numeric_Basic(t *testing.T) {
	result, err := bind(nil, "SELECT * FROM t WHERE a = $1 AND b = $2", "foo", 100)
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE a = 'foo' AND b = 100", result)
}

func TestBind_Numeric_OutOfOrder(t *testing.T) {
	result, err := bind(nil, "SELECT $2, $1 FROM t", "first", "second")
	require.NoError(t, err)
	assert.Equal(t, "SELECT 'second', 'first' FROM t", result)
}

func TestBind_Numeric_Reuse(t *testing.T) {
	result, err := bind(nil, "SELECT * FROM t WHERE a = $1 OR b = $1", "val")
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE a = 'val' OR b = 'val'", result)
}

func TestBind_Numeric_MissingArg(t *testing.T) {
	_, err := bind(nil, "SELECT * FROM t WHERE a = $1 AND b = $3", "only_one")
	assert.Error(t, err)
}

func TestBind_Named_Basic(t *testing.T) {
	result, err := bind(nil, "SELECT * FROM t WHERE name = @name AND age = @age",
		driver.NamedValue{Name: "name", Value: "Alice"},
		driver.NamedValue{Name: "age", Value: 30},
	)
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE name = 'Alice' AND age = 30", result)
}

func TestBind_Named_Reuse(t *testing.T) {
	result, err := bind(nil, "SELECT * FROM t WHERE a = @x OR b = @x",
		driver.NamedValue{Name: "x", Value: "val"},
	)
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE a = 'val' OR b = 'val'", result)
}

func TestBind_Named_MissingParam(t *testing.T) {
	_, err := bind(nil, "SELECT * FROM t WHERE a = @missing",
		driver.NamedValue{Name: "other", Value: "val"},
	)
	assert.Error(t, err)
}

func TestBind_Named_DateValue(t *testing.T) {
	ts := time.Date(2024, 6, 12, 15, 30, 45, 0, time.UTC)
	result, err := bind(nil, "SELECT * FROM t WHERE ts = @ts",
		driver.NamedDateValue{Name: "ts", Value: ts, Scale: uint8(Seconds)},
	)
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE ts = 2024-06-12 15:30:45", result)
}

func TestBind_Mixed_NamedAndPositional(t *testing.T) {
	_, err := bind(nil, "SELECT * FROM t WHERE a = ? AND b = ?",
		driver.NamedValue{Name: "a", Value: "x"},
		"anonymous",
	)
	assert.ErrorIs(t, err, ErrBindMixedParamsFormats)
}

func TestBind_Positional_StringLiteral(t *testing.T) {
	result, err := bind(nil, "SELECT '?' ")
	require.NoError(t, err)
	assert.Equal(t, "SELECT '?' ", result)
}

func TestBind_Positional_StringLiteralAndArg(t *testing.T) {
	result, err := bind(nil, "SELECT '?' FROM t WHERE a = ?", "val")
	require.NoError(t, err)
	assert.Equal(t, "SELECT '?' FROM t WHERE a = 'val'", result)
}

func TestBind_Positional_EscapedInStringLiteral(t *testing.T) {
	result, err := bind(nil, `SELECT 'it\'s ?' AND b = ?`, "x")
	require.NoError(t, err)
	assert.Equal(t, `SELECT 'it\'s ?' AND b = 'x'`, result)
}

func TestBind_Positional_LineComment(t *testing.T) {
	result, err := bind(nil, "-- what is ?\nSELECT ?", 1)
	require.NoError(t, err)
	assert.Equal(t, "-- what is ?\nSELECT 1", result)
}

func TestBind_Positional_BlockComment(t *testing.T) {
	result, err := bind(nil, "/* ? */ SELECT ?", "x")
	require.NoError(t, err)
	assert.Equal(t, "/* ? */ SELECT 'x'", result)
}

func TestBind_Numeric_StringLiteral(t *testing.T) {
	result, err := bind(nil, "SELECT '$1'")
	require.NoError(t, err)
	assert.Equal(t, "SELECT '$1'", result)
}

func TestBind_Numeric_StringLiteralAndArg(t *testing.T) {
	result, err := bind(nil, "SELECT '$1' FROM t WHERE a = $1", "val")
	require.NoError(t, err)
	assert.Equal(t, "SELECT '$1' FROM t WHERE a = 'val'", result)
}

func TestBind_Numeric_LineComment(t *testing.T) {
	result, err := bind(nil, "-- $1")
	require.NoError(t, err)
	assert.Equal(t, "-- $1", result)
}

func TestBind_Numeric_BlockComment(t *testing.T) {
	result, err := bind(nil, "/* $1 */")
	require.NoError(t, err)
	assert.Equal(t, "/* $1 */", result)
}

func TestBind_Named_StringLiteral(t *testing.T) {
	result, err := bind(nil, "SELECT '@name'")
	require.NoError(t, err)
	assert.Equal(t, "SELECT '@name'", result)
}

func TestBind_Named_StringLiteralAndArg(t *testing.T) {
	result, err := bind(nil, "SELECT '@name' FROM t WHERE a = @name",
		driver.NamedValue{Name: "name", Value: "val"},
	)
	require.NoError(t, err)
	assert.Equal(t, "SELECT '@name' FROM t WHERE a = 'val'", result)
}

func TestBind_Named_LineComment(t *testing.T) {
	result, err := bind(nil, "-- @name")
	require.NoError(t, err)
	assert.Equal(t, "-- @name", result)
}

func TestBind_Named_BlockComment(t *testing.T) {
	result, err := bind(nil, "/* @name */")
	require.NoError(t, err)
	assert.Equal(t, "/* @name */", result)
}
