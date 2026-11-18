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
	"reflect"
	"testing"
)

func TestNewDecodedTabletUsesProvidedStorage(t *testing.T) {
	timestamps := []int64{10, 20}
	booleans := []bool{true, false}
	int32s := []int32{1, 2}
	int64s := []int64{3, 4}
	float32s := []float32{5, 6}
	float64s := []float64{7, 8}
	dates := []int32{20240101, 20240102}
	texts := [][]byte{[]byte("left"), []byte("right")}
	blobs := [][]byte{{1}, {2}}
	objects := [][]byte{{3}, {4}}
	schemas := []*MeasurementSchema{
		{Measurement: "bool", DataType: BOOLEAN},
		{Measurement: "i32", DataType: INT32},
		{Measurement: "i64", DataType: INT64},
		{Measurement: "f32", DataType: FLOAT},
		{Measurement: "f64", DataType: DOUBLE},
		{Measurement: "date", DataType: DATE},
		{Measurement: "text", DataType: TEXT},
		{Measurement: "blob", DataType: BLOB},
		{Measurement: "object", DataType: OBJECT},
	}
	categories := []ColumnCategory{FIELD, FIELD, FIELD, FIELD, FIELD, FIELD, FIELD, FIELD, FIELD}
	values := []interface{}{booleans, int32s, int64s, float32s, float64s, dates, texts, blobs, objects}
	nulls := make([][]byte, len(schemas))
	nulls[2] = []byte{2}
	tablet := NewDecodedTablet("events", schemas, categories, 2, timestamps, values, nulls)

	timestamps[0] = 11
	booleans[0] = false
	if got := tablet.GetTimestampAt(0); got != 11 {
		t.Fatalf("GetTimestampAt(0) = %d, want 11", got)
	}
	if got, err := tablet.GetValueAt(0, 0); err != nil || got != false {
		t.Fatalf("shared BOOLEAN value = %v, %v", got, err)
	}
	if got, err := tablet.GetValueAt(2, 1); err != nil || got != nil {
		t.Fatalf("NULL INT64 value = %v, %v", got, err)
	}
	if got, err := tablet.GetValueAt(6, 0); err != nil || got != "left" {
		t.Fatalf("TEXT value = %v, %v", got, err)
	}
	if err := tablet.SetValueAt([]byte{9}, 8, 0); err != nil {
		t.Fatalf("SetValueAt raw OBJECT bytes: %v", err)
	}
	if got, err := tablet.GetValueAt(8, 0); err != nil || !reflect.DeepEqual(got, []byte{9}) {
		t.Fatalf("raw OBJECT bytes = %v, %v", got, err)
	}
}

func TestDecodedTabletNormalizesNilTimestamps(t *testing.T) {
	tablet := NewDecodedTablet(
		"events",
		[]*MeasurementSchema{{Measurement: "value", DataType: INT64}},
		[]ColumnCategory{FIELD},
		2,
		nil,
		[]interface{}{[]int64{1, 2}},
		nil,
	)
	for row := 0; row < tablet.Len(); row++ {
		if got := tablet.GetTimestampAt(row); got != 0 {
			t.Fatalf("GetTimestampAt(%d) = %d, want 0", row, got)
		}
	}
	if err := tablet.SetTimestampAt(7, 1); err != nil {
		t.Fatal(err)
	}
	if got := tablet.GetTimestampAt(1); got != 7 {
		t.Fatalf("GetTimestampAt(1) = %d, want 7", got)
	}
}

func TestTabletSetValueAtPreservesOtherNullRows(t *testing.T) {
	tablet := NewDecodedTablet(
		"events",
		[]*MeasurementSchema{{Measurement: "value", DataType: INT64}},
		[]ColumnCategory{FIELD},
		3,
		[]int64{1, 2, 3},
		[]interface{}{make([]int64, 3)},
		[][]byte{{7}},
	)
	if err := tablet.SetValueAt(int64(101), 0, 0); err != nil {
		t.Fatal(err)
	}
	if got, err := tablet.GetValueAt(0, 0); err != nil || got != int64(101) {
		t.Fatalf("filled value = %v, %v", got, err)
	}
	for row := 1; row < tablet.Len(); row++ {
		if got, err := tablet.GetValueAt(0, row); err != nil || got != nil {
			t.Fatalf("row %d after filling nullable column = %v, %v", row, got, err)
		}
	}
}

func TestDecodedTabletDistinguishesEmptyBinaryFromNull(t *testing.T) {
	tablet := NewDecodedTablet(
		"events",
		[]*MeasurementSchema{{Measurement: "value", DataType: BLOB}},
		[]ColumnCategory{FIELD},
		2,
		[]int64{1, 2},
		[]interface{}{[][]byte{{}, nil}},
		[][]byte{{2}},
	)
	empty, err := tablet.GetValueAt(0, 0)
	if err != nil || empty == nil {
		t.Fatalf("empty binary = %v, %v", empty, err)
	}
	if value, ok := empty.([]byte); !ok || len(value) != 0 {
		t.Fatalf("empty binary value = %#v", empty)
	}
	if value, err := tablet.GetValueAt(0, 1); err != nil || value != nil {
		t.Fatalf("NULL binary = %v, %v", value, err)
	}
}

func BenchmarkDecodedTabletNumericValue(b *testing.B) {
	tablet := NewDecodedTablet(
		"events",
		[]*MeasurementSchema{{Measurement: "value", DataType: INT64}},
		[]ColumnCategory{FIELD},
		1024,
		make([]int64, 1024),
		[]interface{}{make([]int64, 1024)},
		nil,
	)
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		_, _ = tablet.GetValueAt(0, index&1023)
	}
}

func BenchmarkDecodedTabletTextValue(b *testing.B) {
	values := make([][]byte, 1024)
	for index := range values {
		values[index] = []byte("value")
	}
	tablet := NewDecodedTablet(
		"events",
		[]*MeasurementSchema{{Measurement: "value", DataType: TEXT}},
		[]ColumnCategory{FIELD},
		1024,
		make([]int64, 1024),
		[]interface{}{values},
		nil,
	)
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		_, _ = tablet.GetValueAt(0, index&1023)
	}
}
