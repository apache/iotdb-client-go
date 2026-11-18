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

func decodedTestTabletFrame(valuesPresent bool) []byte {
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
	data = append(data, 1)
	data = appendTabletInt64(data, 10)
	data = appendTabletInt64(data, 20)
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
	frame := decodedTestTabletFrame(true)
	data := append(append([]byte{}, frame...), 0xff)
	tablet, bytesRead, err := DecodeTablet(data)
	if err != nil {
		t.Fatal(err)
	}
	if bytesRead != len(frame) {
		t.Fatalf("BytesRead = %d, want %d", bytesRead, len(frame))
	}
	if tablet.GetTargetName() != "events" || !tablet.HasTimestamps() || tablet.Len() != 2 {
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
	tablet, _, err := DecodeTablet(decodedTestTabletFrame(false))
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

func TestDecodedTabletMaterializesSparseColumnOnFirstWrite(t *testing.T) {
	tablet, _, err := DecodeTablet(decodedTestTabletFrame(false))
	if err != nil {
		t.Fatal(err)
	}
	if err := tablet.SetValueAt(int64(101), 0, 0); err != nil {
		t.Fatal(err)
	}
	if value, err := tablet.GetValueAt(0, 0); err != nil || value != int64(101) {
		t.Fatalf("filled value = %v, %v", value, err)
	}
	if value, err := tablet.GetValueAt(0, 1); err != nil || value != nil {
		t.Fatalf("other sparse value = %v, %v", value, err)
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

func TestTabletSetObjectBytesAtStoresRawBytes(t *testing.T) {
	tablet, err := NewRelationalTablet(
		"events",
		[]*MeasurementSchema{{Measurement: "object", DataType: OBJECT}},
		[]ColumnCategory{FIELD},
		1,
	)
	if err != nil {
		t.Fatal(err)
	}
	raw := []byte{1, 2, 3}
	if err := tablet.SetObjectBytesAt(raw, 0, 0); err != nil {
		t.Fatal(err)
	}
	value, err := tablet.GetValueAt(0, 0)
	if err != nil || string(value.([]byte)) != string(raw) {
		t.Fatalf("raw OBJECT value = %v, %v", value, err)
	}
}
