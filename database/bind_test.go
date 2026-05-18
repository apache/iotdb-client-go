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

// ==================== Positional Binding (?) ====================

// TestBind_Positional_Basic verifies basic positional placeholder replacement.
func TestBind_Positional_Basic(t *testing.T) {
	result, err := bind(nil, "INSERT INTO t(a, b) VALUES (?, ?)", "hello", 42)
	require.NoError(t, err)
	assert.Equal(t, "INSERT INTO t(a, b) VALUES ('hello', 42)", result)
}

// TestBind_Positional_MultipleValues verifies multiple positional args in one query.
func TestBind_Positional_MultipleValues(t *testing.T) {
	result, err := bind(nil, "SELECT * FROM t WHERE a = ? AND b = ? AND c = ?", "x", 1, true)
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE a = 'x' AND b = 1 AND c = 1", result)
}

// TestBind_Positional_EscapedQuestionMark verifies that \? is not treated as a placeholder.
func TestBind_Positional_EscapedQuestionMark(t *testing.T) {
	result, err := bind(nil, `SELECT * FROM t WHERE a = \? AND b = ?`, "val")
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE a = ? AND b = 'val'", result)
}

// TestBind_Positional_TooFewArgs returns an error when args are fewer than placeholders.
func TestBind_Positional_TooFewArgs(t *testing.T) {
	_, err := bind(nil, "INSERT INTO t VALUES (?, ?, ?)", "a")
	assert.Error(t, err)
}

// TestBind_Positional_NoArgs returns the query unchanged when no args are provided.
func TestBind_Positional_NoArgs(t *testing.T) {
	result, err := bind(nil, "SELECT * FROM t")
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t", result)
}

// ==================== Numeric Binding ($N) ====================

// TestBind_Numeric_Basic verifies $1, $2 style placeholder replacement.
func TestBind_Numeric_Basic(t *testing.T) {
	result, err := bind(nil, "SELECT * FROM t WHERE a = $1 AND b = $2", "foo", 100)
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE a = 'foo' AND b = 100", result)
}

// TestBind_Numeric_OutOfOrder verifies that $N params can be used in any order.
func TestBind_Numeric_OutOfOrder(t *testing.T) {
	result, err := bind(nil, "SELECT $2, $1 FROM t", "first", "second")
	require.NoError(t, err)
	assert.Equal(t, "SELECT 'second', 'first' FROM t", result)
}

// TestBind_Numeric_Reuse verifies that the same $N can be used multiple times.
func TestBind_Numeric_Reuse(t *testing.T) {
	result, err := bind(nil, "SELECT * FROM t WHERE a = $1 OR b = $1", "val")
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE a = 'val' OR b = 'val'", result)
}

// TestBind_Numeric_MissingArg returns an error when a referenced $N has no corresponding arg.
func TestBind_Numeric_MissingArg(t *testing.T) {
	_, err := bind(nil, "SELECT * FROM t WHERE a = $1 AND b = $3", "only_one")
	assert.Error(t, err)
}

// ==================== Named Binding (@name) ====================

// TestBind_Named_Basic verifies @name style placeholder replacement.
func TestBind_Named_Basic(t *testing.T) {
	result, err := bind(nil, "SELECT * FROM t WHERE name = @name AND age = @age",
		driver.NamedValue{Name: "name", Value: "Alice"},
		driver.NamedValue{Name: "age", Value: 30},
	)
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE name = 'Alice' AND age = 30", result)
}

// TestBind_Named_Reuse verifies the same @name can appear multiple times.
func TestBind_Named_Reuse(t *testing.T) {
	result, err := bind(nil, "SELECT * FROM t WHERE a = @x OR b = @x",
		driver.NamedValue{Name: "x", Value: "val"},
	)
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE a = 'val' OR b = 'val'", result)
}

// TestBind_Named_MissingParam returns an error when a @param in query has no matching arg.
func TestBind_Named_MissingParam(t *testing.T) {
	_, err := bind(nil, "SELECT * FROM t WHERE a = @missing",
		driver.NamedValue{Name: "other", Value: "val"},
	)
	assert.Error(t, err)
}

// TestBind_Named_DateValue verifies NamedDateValue binds time correctly.
func TestBind_Named_DateValue(t *testing.T) {
	ts := time.Date(2024, 6, 12, 15, 30, 45, 0, time.UTC)
	result, err := bind(nil, "SELECT * FROM t WHERE ts = @ts",
		driver.NamedDateValue{Name: "ts", Value: ts, Scale: uint8(Seconds)},
	)
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE ts = 2024-06-12 15:30:45", result)
}

// ==================== Mixed Format Detection ====================

// TestBind_Mixed_NamedAndPositional returns an error when mixing named and anonymous args.
func TestBind_Mixed_NamedAndPositional(t *testing.T) {
	_, err := bind(nil, "SELECT * FROM t WHERE a = ? AND b = ?",
		driver.NamedValue{Name: "a", Value: "x"},
		"anonymous",
	)
	assert.ErrorIs(t, err, ErrBindMixedParamsFormats)
}

// ==================== Value Formatting ====================

// TestFormat_Nil formats nil as NULL.
func TestFormat_Nil(t *testing.T) {
	result, err := format(nil, Seconds, nil)
	require.NoError(t, err)
	assert.Equal(t, "NULL", result)
}

// TestFormat_String wraps string in single quotes and escapes special chars.
func TestFormat_String(t *testing.T) {
	result, err := format(nil, Seconds, "it's a \"test\"")
	require.NoError(t, err)
	assert.Equal(t, `'it\'s a "test"'`, result)
}

// TestFormat_StringWithBackslash escapes backslashes.
func TestFormat_StringWithBackslash(t *testing.T) {
	result, err := format(nil, Seconds, `path\to\file`)
	require.NoError(t, err)
	assert.Equal(t, `'path\\to\\file'`, result)
}

// TestFormat_Bool formats true as 1 and false as 0.
func TestFormat_Bool(t *testing.T) {
	trueResult, err := format(nil, Seconds, true)
	require.NoError(t, err)
	assert.Equal(t, "1", trueResult)

	falseResult, err := format(nil, Seconds, false)
	require.NoError(t, err)
	assert.Equal(t, "0", falseResult)
}

// TestFormat_Time formats time.Time as "2006-01-02 15:04:05".
func TestFormat_Time(t *testing.T) {
	ts := time.Date(2024, 6, 12, 15, 30, 45, 0, time.UTC)
	result, err := format(nil, Seconds, ts)
	require.NoError(t, err)
	assert.Equal(t, "2024-06-12 15:30:45", result)
}

// TestFormat_NilPointerTime formats nil *time.Time as NULL.
func TestFormat_NilPointerTime(t *testing.T) {
	var ts *time.Time
	result, err := format(nil, Seconds, ts)
	require.NoError(t, err)
	assert.Equal(t, "NULL", result)
}

// TestFormat_Integer formats integers without quotes.
func TestFormat_Integer(t *testing.T) {
	result, err := format(nil, Seconds, 42)
	require.NoError(t, err)
	assert.Equal(t, "42", result)
}

// TestFormat_Float formats floats without quotes.
func TestFormat_Float(t *testing.T) {
	result, err := format(nil, Seconds, 3.14)
	require.NoError(t, err)
	assert.Contains(t, result, "3.14")
}

// TestFormat_GroupSet formats GroupSet as parenthesized comma-separated values.
func TestFormat_GroupSet(t *testing.T) {
	result, err := format(nil, Seconds, GroupSet{Value: []any{"a", "b", "c"}})
	require.NoError(t, err)
	assert.Equal(t, "('a', 'b', 'c')", result)
}

// TestFormat_ArraySet formats ArraySet as bracketed comma-separated values.
func TestFormat_ArraySet(t *testing.T) {
	result, err := format(nil, Seconds, ArraySet{"x", "y"})
	require.NoError(t, err)
	assert.Equal(t, "['x', 'y']", result)
}

// TestFormat_NilPointer formats a nil pointer as NULL.
func TestFormat_NilPointer(t *testing.T) {
	var p *int
	result, err := format(nil, Seconds, p)
	require.NoError(t, err)
	assert.Equal(t, "NULL", result)
}

// TestFormat_Slice formats a slice as bracketed comma-separated values.
func TestFormat_Slice(t *testing.T) {
	result, err := format(nil, Seconds, []int{1, 2, 3})
	require.NoError(t, err)
	assert.Equal(t, "[1, 2, 3]", result)
}
