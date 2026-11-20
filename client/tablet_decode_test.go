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
	"encoding/binary"
	"testing"
)

func decodedTestTabletFrame(timestampsPresent, valuesPresent bool) []byte {
	var data []byte
	data = appendTabletInt32(data, int32(len("events")))
	data = append(data, "events"...)
	data = appendTabletInt32(data, 2)
	data = append(data, 1)
	data = appendTabletInt32(data, 1)
	data = append(data, 1)
	data = appendTabletInt32(data, int32(len("s1")))
	data = append(data, "s1"...)
	data = append(data, byte(INT64), 0, 0)
	data = appendTabletInt32(data, 0)
	data = append(data, byte(FIELD))
	if timestampsPresent {
		data = append(data, 1)
		data = appendTabletInt64(data, 10)
		data = appendTabletInt64(data, 20)
	} else {
		data = append(data, 0)
	}
	data = append(data, 0)
	if valuesPresent {
		data = append(data, 1, 1)
		data = appendTabletInt64(data, 100)
		data = appendTabletInt64(data, 200)
	} else {
		data = append(data, 0)
	}
	return data
}

func appendTabletInt32(data []byte, value int32) []byte {
	return binary.BigEndian.AppendUint32(data, uint32(value))
}

func appendTabletInt64(data []byte, value int64) []byte {
	return binary.BigEndian.AppendUint64(data, uint64(value))
}

func TestDecodeTabletReturnsTabletAndBytesRead(t *testing.T) {
	frame := decodedTestTabletFrame(true, true)
	data := append(append([]byte{}, frame...), 0xff)
	tablet, bytesRead, err := DecodeTablet(data)
	if err != nil {
		t.Fatal(err)
	}
	if bytesRead != len(frame) {
		t.Fatalf("BytesRead = %d, want %d", bytesRead, len(frame))
	}
	if tablet.GetTargetName() != "events" || tablet.Len() != 2 {
		t.Fatalf("decoded Tablet metadata is inconsistent")
	}
	if tablet.ColumnCount() != 1 || tablet.ColumnName(0) != "s1" || tablet.ColumnDataType(0) != INT64 || tablet.ColumnCategory(0) != FIELD {
		t.Fatalf("decoded column metadata is inconsistent")
	}
	for row, want := range []int64{100, 200} {
		value, err := tablet.GetValueAt(0, row)
		if err != nil || value != want {
			t.Fatalf("row %d value = %v, %v", row, value, err)
		}
	}
	if tablet.EstimatedSize() == 0 {
		t.Fatal("decoded Tablet size estimate is zero")
	}
}

func TestDecodeTabletReturnsAbsentColumnAsNull(t *testing.T) {
	tablet, _, err := DecodeTablet(decodedTestTabletFrame(true, false))
	if err != nil {
		t.Fatal(err)
	}
	for row := 0; row < tablet.Len(); row++ {
		value, err := tablet.GetValueAt(0, row)
		if err != nil || value != nil {
			t.Fatalf("row %d absent value = %v, %v", row, value, err)
		}
	}
}

func TestDecodedTabletKeepsAbsentColumnOnWrite(t *testing.T) {
	tablet, _, err := DecodeTablet(decodedTestTabletFrame(true, false))
	if err != nil {
		t.Fatal(err)
	}
	if err := tablet.SetValueAt(int64(101), 0, 0); err == nil {
		t.Fatal("SetValueAt accepted a missing decoded column")
	}
	if tablet.values[0] != nil {
		t.Fatal("failed SetValueAt materialized a missing decoded column")
	}
	if err := tablet.SetValueAt(nil, 0, 0); err != nil {
		t.Fatalf("SetValueAt(NULL) on an absent decoded column: %v", err)
	}
	if tablet.values[0] != nil {
		t.Fatal("SetValueAt(NULL) materialized a missing decoded column")
	}
	for row := 0; row < tablet.Len(); row++ {
		value, err := tablet.GetValueAt(0, row)
		if err != nil || value != nil {
			t.Fatalf("row %d absent value after write = %v, %v", row, value, err)
		}
	}
}

func TestDecodedTabletRejectsSerializationWithAbsentColumn(t *testing.T) {
	tablet, _, err := DecodeTablet(decodedTestTabletFrame(true, false))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tablet.getValuesBytes(); err == nil {
		t.Fatal("serializing a Tablet with an absent value column succeeded")
	}
}

func TestDecodeTabletWithoutTimestampsUsesZeroTimestampArray(t *testing.T) {
	tablet, _, err := DecodeTablet(decodedTestTabletFrame(false, true))
	if err != nil {
		t.Fatal(err)
	}
	for row := 0; row < tablet.Len(); row++ {
		if got := tablet.GetTimestampAt(row); got != 0 {
			t.Fatalf("GetTimestampAt(%d) = %d, want 0", row, got)
		}
	}
	if err := tablet.SetTimestampAt(3, 0); err != nil {
		t.Fatal(err)
	}
	if err := tablet.SetTimestampAt(2, 1); err != nil {
		t.Fatal(err)
	}
	if err := tablet.Sort(); err != nil {
		t.Fatal(err)
	}
	for row, want := range []int64{2, 3} {
		if got := tablet.GetTimestampAt(row); got != want {
			t.Fatalf("sorted timestamp %d = %d, want %d", row, got, want)
		}
		value, err := tablet.GetValueAt(0, row)
		if err != nil || value != []int64{200, 100}[row] {
			t.Fatalf("sorted value %d = %v, %v", row, value, err)
		}
	}
}

func TestDecodeTabletPreservesRawObjectBytes(t *testing.T) {
	var data []byte
	data = appendTabletInt32(data, int32(len("events")))
	data = append(data, "events"...)
	data = appendTabletInt32(data, 1)
	data = append(data, 1)
	data = appendTabletInt32(data, 1)
	data = append(data, 1)
	data = appendTabletInt32(data, int32(len("object")))
	data = append(data, "object"...)
	data = append(data, byte(OBJECT), 0, 0)
	data = appendTabletInt32(data, 0)
	data = append(data, byte(FIELD))
	data = append(data, 0, 0, 1, 1, 1)
	data = appendTabletInt32(data, 3)
	data = append(data, 1, 2, 3)

	tablet, _, err := DecodeTablet(data)
	if err != nil {
		t.Fatal(err)
	}
	value, err := tablet.GetValueAt(0, 0)
	if err != nil || string(value.([]byte)) != string([]byte{1, 2, 3}) {
		t.Fatalf("raw OBJECT value = %v, %v", value, err)
	}
}

func TestDecodedTabletSetValueAtStoresRawObjectValues(t *testing.T) {
	tablet := NewDecodedTablet(
		"events",
		[]*MeasurementSchema{{Measurement: "object", DataType: OBJECT}},
		[]ColumnCategory{FIELD},
		2,
		[]int64{1, 2},
		[]interface{}{make([][]byte, 2)},
		nil,
	)
	raw := []byte{1, 2, 3}
	if err := tablet.SetValueAt(raw, 0, 0); err != nil {
		t.Fatal(err)
	}
	if err := tablet.SetValueAt("hello", 0, 1); err != nil {
		t.Fatal(err)
	}
	value, err := tablet.GetValueAt(0, 0)
	if err != nil || string(value.([]byte)) != string(raw) {
		t.Fatalf("raw OBJECT bytes = %v, %v", value, err)
	}
	value, err = tablet.GetValueAt(0, 1)
	if err != nil || string(value.([]byte)) != "hello" {
		t.Fatalf("raw OBJECT string = %v, %v", value, err)
	}
	values, err := tablet.getValuesBytes()
	if err != nil {
		t.Fatal(err)
	}
	wantValues := []byte{0, 0, 0, byte(len(raw))}
	wantValues = append(wantValues, raw...)
	wantValues = append(wantValues, 0, 0, 0, byte(len("hello")))
	wantValues = append(wantValues, "hello"...)
	if string(values) != string(wantValues) {
		t.Fatalf("raw OBJECT serialization = %v, want %v", values, wantValues)
	}
}
