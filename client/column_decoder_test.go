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

package client

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func buildNullIndicatorBytes(nulls []bool) []byte {
	var buf bytes.Buffer
	hasNull := false
	for _, n := range nulls {
		if n {
			hasNull = true
			break
		}
	}
	if !hasNull {
		buf.WriteByte(0)
		return buf.Bytes()
	}
	buf.WriteByte(1)
	packed := make([]byte, (len(nulls)+7)/8)
	for i, n := range nulls {
		if n {
			packed[i/8] |= 0b10000000 >> (uint(i) % 8)
		}
	}
	buf.Write(packed)
	return buf.Bytes()
}

func TestBinaryArrayColumnDecoder_EmptyString(t *testing.T) {
	var buf bytes.Buffer
	buf.Write(buildNullIndicatorBytes([]bool{false}))
	_ = binary.Write(&buf, binary.BigEndian, int32(0))

	col, err := (&BinaryArrayColumnDecoder{}).ReadColumn(bytes.NewReader(buf.Bytes()), TEXT, 1)
	if err != nil {
		t.Fatalf("ReadColumn failed: %v", err)
	}
	if col.GetPositionCount() != 1 {
		t.Fatalf("expected positionCount=1, got %d", col.GetPositionCount())
	}
	if col.IsNull(0) {
		t.Fatal("row 0 should not be null")
	}
	val, err := col.GetBinary(0)
	if err != nil {
		t.Fatalf("GetBinary(0) failed: %v", err)
	}
	if len(val.values) != 0 {
		t.Fatalf("expected empty string, got %q", string(val.values))
	}
}

func TestBinaryArrayColumnDecoder_NullThenEmptyString(t *testing.T) {
	var buf bytes.Buffer
	buf.Write(buildNullIndicatorBytes([]bool{true, false}))
	_ = binary.Write(&buf, binary.BigEndian, int32(0))

	col, err := (&BinaryArrayColumnDecoder{}).ReadColumn(bytes.NewReader(buf.Bytes()), TEXT, 2)
	if err != nil {
		t.Fatalf("ReadColumn failed: %v", err)
	}
	if !col.IsNull(0) {
		t.Error("row 0 should be null")
	}
	if col.IsNull(1) {
		t.Error("row 1 should not be null")
	}
	val, err := col.GetBinary(1)
	if err != nil {
		t.Fatalf("GetBinary(1) failed: %v", err)
	}
	if len(val.values) != 0 {
		t.Fatalf("expected empty string, got %q", string(val.values))
	}
}

func TestBinaryArrayColumnDecoder_WithNull(t *testing.T) {
	var buf bytes.Buffer
	buf.Write(buildNullIndicatorBytes([]bool{false, true, false}))
	writeText := func(s string) {
		_ = binary.Write(&buf, binary.BigEndian, int32(len(s)))
		buf.WriteString(s)
	}
	writeText("hello")
	writeText("world")

	col, err := (&BinaryArrayColumnDecoder{}).ReadColumn(bytes.NewReader(buf.Bytes()), TEXT, 3)
	if err != nil {
		t.Fatalf("ReadColumn failed: %v", err)
	}
	if col.IsNull(0) {
		t.Error("row 0 should not be null")
	}
	if v, _ := col.GetBinary(0); string(v.values) != "hello" {
		t.Errorf("row 0: expected \"hello\", got %q", string(v.values))
	}
	if !col.IsNull(1) {
		t.Error("row 1 should be null")
	}
	if col.IsNull(2) {
		t.Error("row 2 should not be null")
	}
	if v, _ := col.GetBinary(2); string(v.values) != "world" {
		t.Errorf("row 2: expected \"world\", got %q", string(v.values))
	}
}

func TestInt64ArrayColumnDecoder_WithNull(t *testing.T) {
	var buf bytes.Buffer
	buf.Write(buildNullIndicatorBytes([]bool{false, true, false}))
	_ = binary.Write(&buf, binary.BigEndian, int64(100))
	_ = binary.Write(&buf, binary.BigEndian, int64(200))

	col, err := (&Int64ArrayColumnDecoder{}).ReadColumn(bytes.NewReader(buf.Bytes()), INT64, 3)
	if err != nil {
		t.Fatalf("ReadColumn failed: %v", err)
	}
	if col.IsNull(0) {
		t.Error("row 0 should not be null")
	}
	if v, _ := col.GetLong(0); v != 100 {
		t.Errorf("row 0: expected 100, got %d", v)
	}
	if !col.IsNull(1) {
		t.Error("row 1 should be null")
	}
	if col.IsNull(2) {
		t.Error("row 2 should not be null")
	}
	if v, _ := col.GetLong(2); v != 200 {
		t.Errorf("row 2: expected 200, got %d", v)
	}
}

func TestColumnDecoder_ZeroPositionCount(t *testing.T) {
	empty := func() *bytes.Reader { return bytes.NewReader([]byte{}) }

	t.Run("Int32ArrayColumnDecoder", func(t *testing.T) {
		col, err := (&Int32ArrayColumnDecoder{}).ReadColumn(empty(), INT32, 0)
		if err != nil {
			t.Fatalf("ReadColumn failed: %v", err)
		}
		if col.GetPositionCount() != 0 {
			t.Errorf("expected positionCount=0, got %d", col.GetPositionCount())
		}
	})

	t.Run("Int64ArrayColumnDecoder", func(t *testing.T) {
		col, err := (&Int64ArrayColumnDecoder{}).ReadColumn(empty(), INT64, 0)
		if err != nil {
			t.Fatalf("ReadColumn failed: %v", err)
		}
		if col.GetPositionCount() != 0 {
			t.Errorf("expected positionCount=0, got %d", col.GetPositionCount())
		}
	})

	t.Run("ByteArrayColumnDecoder", func(t *testing.T) {
		col, err := (&ByteArrayColumnDecoder{}).ReadColumn(empty(), BOOLEAN, 0)
		if err != nil {
			t.Fatalf("ReadColumn failed: %v", err)
		}
		if col.GetPositionCount() != 0 {
			t.Errorf("expected positionCount=0, got %d", col.GetPositionCount())
		}
	})

	t.Run("BinaryArrayColumnDecoder", func(t *testing.T) {
		col, err := (&BinaryArrayColumnDecoder{}).ReadColumn(empty(), TEXT, 0)
		if err != nil {
			t.Fatalf("ReadColumn failed: %v", err)
		}
		if col.GetPositionCount() != 0 {
			t.Errorf("expected positionCount=0, got %d", col.GetPositionCount())
		}
	})
}
