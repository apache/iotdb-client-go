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
	"reflect"
	"testing"
	"time"

	"github.com/apache/iotdb-client-go/rpc"
)

func createIoTDBRpcDataSet() *IoTDBRpcDataSet {
	columns := []string{
		"root.ln.device1.restart_count",
		"root.ln.device1.price",
		"root.ln.device1.tick_count",
		"root.ln.device1.temperature",
		"root.ln.device1.description",
		"root.ln.device1.status",
		"root.ln.device1.description_string",
		"root.ln.device1.description_blob",
		"root.ln.device1.date",
		"root.ln.device1.timestamp",
	}
	dataTypes := []string{"INT32", "DOUBLE", "INT64", "FLOAT", "TEXT", "BOOLEAN", "STRING", "BLOB", "DATE", "TIMESTAMP"}
	columnNameIndex := map[string]int32{
		"root.ln.device1.restart_count":      2,
		"root.ln.device1.price":              1,
		"root.ln.device1.tick_count":         5,
		"root.ln.device1.temperature":        4,
		"root.ln.device1.description":        0,
		"root.ln.device1.status":             3,
		"root.ln.device1.description_string": 6,
		"root.ln.device1.description_blob":   7,
		"root.ln.device1.date":               8,
		"root.ln.device1.timestamp":          9,
	}
	var queyrId int64 = 1
	var sessionId int64 = 1
	var client *rpc.IClientRPCServiceClient = nil
	queryDataSet := rpc.TSQueryDataSet{
		Time: []byte{0, 0, 1, 118, 76, 52, 0, 236, 0, 0, 1, 118, 76, 52, 25, 228, 0, 0, 1, 118, 76, 52, 41, 42, 0, 0, 1, 118, 76, 52, 243, 148, 0, 0, 1, 118, 76, 95, 98, 255},
		ValueList: [][]byte{
			{0, 0, 0, 13, 84, 101, 115, 116, 32, 68, 101, 118, 105, 99, 101, 32, 49, 0, 0, 0, 13, 84, 101, 115, 116, 32, 68, 101, 118, 105, 99, 101, 32, 49, 0, 0, 0, 13, 84, 101, 115, 116, 32, 68, 101, 118, 105, 99, 101, 32, 49, 0, 0, 0, 13, 84, 101, 115, 116, 32, 68, 101, 118, 105, 99, 101, 32, 49, 0, 0, 0, 13, 84, 101, 115, 116, 32, 68, 101, 118, 105, 99, 101, 32, 49},
			{64, 159, 16, 204, 204, 204, 204, 205, 64, 159, 16, 204, 204, 204, 204, 205, 64, 159, 16, 204, 204, 204, 204, 205, 64, 159, 16, 204, 204, 204, 204, 205, 64, 159, 16, 204, 204, 204, 204, 205},
			{0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1},
			{1, 1, 1, 1, 1},
			{65, 65, 153, 154, 65, 65, 153, 154, 65, 65, 153, 154, 65, 65, 153, 154, 65, 65, 153, 154},
			{0, 0, 0, 0, 0, 50, 220, 213, 0, 0, 0, 0, 0, 50, 220, 213, 0, 0, 0, 0, 0, 50, 220, 213, 0, 0, 0, 0, 0, 50, 220, 213, 0, 0, 0, 0, 0, 50, 220, 213},
			{0, 0, 0, 13, 84, 101, 115, 116, 32, 68, 101, 118, 105, 99, 101, 32, 49, 0, 0, 0, 13, 84, 101, 115, 116, 32, 68, 101, 118, 105, 99, 101, 32, 49, 0, 0, 0, 13, 84, 101, 115, 116, 32, 68, 101, 118, 105, 99, 101, 32, 49, 0, 0, 0, 13, 84, 101, 115, 116, 32, 68, 101, 118, 105, 99, 101, 32, 49, 0, 0, 0, 13, 84, 101, 115, 116, 32, 68, 101, 118, 105, 99, 101, 32, 49},
			{0, 0, 0, 13, 84, 101, 115, 116, 32, 68, 101, 118, 105, 99, 101, 32, 49, 0, 0, 0, 13, 84, 101, 115, 116, 32, 68, 101, 118, 105, 99, 101, 32, 49, 0, 0, 0, 13, 84, 101, 115, 116, 32, 68, 101, 118, 105, 99, 101, 32, 49, 0, 0, 0, 13, 84, 101, 115, 116, 32, 68, 101, 118, 105, 99, 101, 32, 49, 0, 0, 0, 13, 84, 101, 115, 116, 32, 68, 101, 118, 105, 99, 101, 32, 49},
			{1, 52, 216, 17, 1, 52, 216, 17, 1, 52, 216, 17, 1, 52, 216, 17, 1, 52, 216, 17},
			{0, 0, 0, 0, 0, 50, 220, 213, 0, 0, 0, 0, 0, 50, 220, 213, 0, 0, 0, 0, 0, 50, 220, 213, 0, 0, 0, 0, 0, 50, 220, 213, 0, 0, 0, 0, 0, 50, 220, 213},
		},
		BitmapList: [][]byte{{248}, {248}, {248}, {248}, {248}, {248}, {248}, {248}, {248}, {248}},
	}
	return NewIoTDBRpcDataSet("select * from root.ln.device1", columns, dataTypes, columnNameIndex, queyrId, client, sessionId, &queryDataSet, false, DefaultFetchSize, nil)
}

func TestIoTDBRpcDataSet_getColumnType(t *testing.T) {
	type args struct {
		columnName string
	}

	ds := createIoTDBRpcDataSet()
	closedDataSet := createIoTDBRpcDataSet()
	closedDataSet.Close()
	tests := []struct {
		name    string
		dataSet *IoTDBRpcDataSet
		args    args
		want    TSDataType
	}{
		{
			name:    "Normal",
			dataSet: ds,
			args: args{
				columnName: "root.ln.device1.tick_count",
			},
			want: INT64,
		}, {
			name:    "Closed",
			dataSet: closedDataSet,
			args: args{
				columnName: "root.ln.device1.tick_count",
			},
			want: UNKNOWN,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := tt.dataSet
			if got := s.getColumnType(tt.args.columnName); got != tt.want {
				t.Errorf("IoTDBRpcDataSet.getColumnType() = %v, want %v", got, tt.want)
			}
			s.Close()
		})
	}
}

func TestIoTDBRpcDataSet_getColumnIndex(t *testing.T) {
	type args struct {
		columnName string
	}
	closedDataSet := createIoTDBRpcDataSet()
	closedDataSet.Close()
	tests := []struct {
		name    string
		dataset *IoTDBRpcDataSet
		args    args
		want    int32
	}{
		{
			name:    "Normal",
			dataset: createIoTDBRpcDataSet(),
			args: args{
				columnName: "root.ln.device1.tick_count",
			},
			want: 5,
		}, {
			name:    "Closed",
			dataset: closedDataSet,
			args: args{
				columnName: "root.ln.device1.tick_count",
			},
			want: -1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := tt.dataset
			if got := s.getColumnIndex(tt.args.columnName); got != tt.want {
				t.Errorf("IoTDBRpcDataSet.getColumnIndex() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIoTDBRpcDataSet_isNull(t *testing.T) {
	type args struct {
		columnIndex int
		rowIndex    int
	}
	ds := createIoTDBRpcDataSet()
	ds.next()

	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "Normal",
			args: args{
				columnIndex: 0,
				rowIndex:    0,
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := createIoTDBRpcDataSet()
			s.next()
			if got := s.isNull(tt.args.columnIndex, tt.args.rowIndex); got != tt.want {
				t.Errorf("IoTDBRpcDataSet.isNull() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIoTDBRpcDataSet_getValue(t *testing.T) {

	type args struct {
		columnName string
	}
	tests := []struct {
		name string
		args args
		want interface{}
	}{
		{
			name: "restart_count",
			args: args{
				columnName: "root.ln.device1.restart_count",
			},
			want: int32(1),
		}, {
			name: "tick_count",
			args: args{
				columnName: "root.ln.device1.tick_count",
			},
			want: int64(3333333),
		}, {
			name: "price",
			args: args{
				columnName: "root.ln.device1.price",
			},
			want: float64(1988.2),
		}, {
			name: "temperature",
			args: args{
				columnName: "root.ln.device1.temperature",
			},
			want: float32(12.1),
		}, {
			name: "description",
			args: args{
				columnName: "root.ln.device1.description",
			},
			want: "Test Device 1",
		}, {
			name: "status",
			args: args{
				columnName: "root.ln.device1.status",
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := createIoTDBRpcDataSet()
			s.next()
			if got := s.getValue(tt.args.columnName); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("IoTDBRpcDataSet.getValue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIoTDBRpcDataSet_scan(t *testing.T) {
	type args struct {
		dest []interface{}
	}

	type want struct {
		err    error
		values []interface{}
	}

	var restartCount int32
	var price float64
	var tickCount int64
	var temperature float32
	var description string
	var status bool

	var restartCountStr string
	var priceStr string
	var tickCountStr string
	var temperatureStr string
	var descriptionStr string
	var statusStr string

	var wantRestartCount int32 = 1
	var wantPrice float64 = 1988.2
	var wantTickCount int64 = 3333333
	var wantTemperature float32 = 12.1
	var wantDescription string = "Test Device 1"
	var wantStatus bool = true

	var wantRestartCountStr string = "1"
	var wantPriceStr string = "1988.2"
	var wantTickCountStr string = "3333333"
	var wantTemperatureStr string = "12.1"
	var wantDescriptionStr string = "Test Device 1"
	var wantStatusStr string = "true"

	tests := []struct {
		name string
		args args
		want want
	}{
		{
			name: "Normal",
			args: args{
				dest: []interface{}{&restartCount, &price, &tickCount, &temperature, &description, &status},
			},
			want: want{
				err:    nil,
				values: []interface{}{&wantRestartCount, &wantPrice, &wantTickCount, &wantTemperature, &wantDescription, &wantStatus},
			},
		}, {
			name: "String",
			args: args{
				dest: []interface{}{&restartCountStr, &priceStr, &tickCountStr, &temperatureStr, &descriptionStr, &statusStr},
			},
			want: want{
				err:    nil,
				values: []interface{}{&wantRestartCountStr, &wantPriceStr, &wantTickCountStr, &wantTemperatureStr, &wantDescriptionStr, &wantStatusStr},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := createIoTDBRpcDataSet()
			s.next()
			if err := s.scan(tt.args.dest...); err != tt.want.err {
				t.Errorf("IoTDBRpcDataSet.scan() error = %v, wantErr %v", err, tt.want.err)
			}
			if got := tt.args.dest; !reflect.DeepEqual(got, tt.want.values) {
				t.Errorf("IoTDBRpcDataSet.scan(), dest = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIoTDBRpcDataSet_GetTimestamp(t *testing.T) {
	tests := []struct {
		name string
		want int64
	}{
		{
			name: "GetTimestamp",
			want: 1607596245228,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := createIoTDBRpcDataSet()
			s.next()
			if got := s.GetTimestamp(); got != tt.want {
				t.Errorf("IoTDBRpcDataSet.GetTimestamp() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIoTDBRpcDataSet_getText(t *testing.T) {
	type args struct {
		columnName string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "restart_count",
			args: args{
				columnName: "root.ln.device1.restart_count",
			},
			want: "1",
		}, {
			name: "price",
			args: args{
				columnName: "root.ln.device1.price",
			},
			want: "1988.2",
		}, {
			name: "tick_count",
			args: args{
				columnName: "root.ln.device1.tick_count",
			},
			want: "3333333",
		}, {
			name: "temperature",
			args: args{
				columnName: "root.ln.device1.temperature",
			},
			want: "12.1",
		}, {
			name: "description",
			args: args{
				columnName: "root.ln.device1.description",
			},
			want: "Test Device 1",
		}, {
			name: "status",
			args: args{
				columnName: "root.ln.device1.status",
			},
			want: "true",
		}, {
			name: TimestampColumnName,
			args: args{
				columnName: TimestampColumnName,
			},
			want: time.Unix(0, 1607596245228000000).Format(time.RFC3339),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := createIoTDBRpcDataSet()
			s.next()
			if got := s.getText(tt.args.columnName); got != tt.want {
				t.Errorf("IoTDBRpcDataSet.getText() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIoTDBRpcDataSet_getBool(t *testing.T) {
	type args struct {
		columnName string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "status",
			args: args{
				columnName: "root.ln.device1.status",
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := createIoTDBRpcDataSet()
			s.next()
			if got := s.getBool(tt.args.columnName); got != tt.want {
				t.Errorf("IoTDBRpcDataSet.getBool() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIoTDBRpcDataSet_getFloat(t *testing.T) {
	type args struct {
		columnName string
	}
	tests := []struct {
		name string
		args args
		want float32
	}{
		{
			name: "temperature",
			args: args{
				columnName: "root.ln.device1.temperature",
			},
			want: 12.1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := createIoTDBRpcDataSet()
			s.next()
			if got := s.getFloat(tt.args.columnName); got != tt.want {
				t.Errorf("IoTDBRpcDataSet.getFloat() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIoTDBRpcDataSet_getDouble(t *testing.T) {
	type args struct {
		columnName string
	}
	tests := []struct {
		name string
		args args
		want float64
	}{
		{
			name: "price",
			args: args{
				columnName: "root.ln.device1.price",
			},
			want: 1988.2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := createIoTDBRpcDataSet()
			s.next()
			if got := s.getDouble(tt.args.columnName); got != tt.want {
				t.Errorf("IoTDBRpcDataSet.getDouble() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIoTDBRpcDataSet_getInt32(t *testing.T) {
	type args struct {
		columnName string
	}
	tests := []struct {
		name string
		args args
		want int32
	}{
		{
			name: "restart_count",
			args: args{
				columnName: "root.ln.device1.restart_count",
			},
			want: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := createIoTDBRpcDataSet()
			s.next()
			if got := s.getInt32(tt.args.columnName); got != tt.want {
				t.Errorf("IoTDBRpcDataSet.getInt32() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIoTDBRpcDataSet_getInt64(t *testing.T) {
	type args struct {
		columnName string
	}
	tests := []struct {
		name string
		args args
		want int64
	}{
		{
			name: "tick_count",
			args: args{
				columnName: "root.ln.device1.tick_count",
			},
			want: 3333333,
		}, {
			name: TimestampColumnName,
			args: args{
				columnName: TimestampColumnName,
			},
			want: 1607596245228,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := createIoTDBRpcDataSet()
			s.next()
			if got := s.getInt64(tt.args.columnName); got != tt.want {
				t.Errorf("IoTDBRpcDataSet.getInt64() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIoTDBRpcDataSet_getRowRecord(t *testing.T) {
	tests := []struct {
		name    string
		want    *RowRecord
		wantErr bool
	}{
		{
			name: "",
			want: &RowRecord{
				timestamp: 0,
				fields: []*Field{
					{
						name:     "root.ln.device1.restart_count",
						dataType: INT32,
						value:    int32(1),
					}, {
						name:     "root.ln.device1.price",
						dataType: DOUBLE,
						value:    float64(1988.2),
					}, {
						name:     "root.ln.device1.tick_count",
						dataType: INT64,
						value:    int64(3333333),
					}, {
						name:     "root.ln.device1.temperature",
						dataType: FLOAT,
						value:    float32(12.1),
					}, {
						name:     "root.ln.device1.description",
						dataType: TEXT,
						value:    "Test Device 1",
					}, {
						name:     "root.ln.device1.status",
						dataType: BOOLEAN,
						value:    true,
					}, {
						name:     "root.ln.device1.description_string",
						dataType: STRING,
						value:    "Test Device 1",
					}, {
						name:     "root.ln.device1.description_blob",
						dataType: BLOB,
						value:    []byte("Test Device 1"),
					}, {
						name:     "root.ln.device1.date",
						dataType: DATE,
						value:    time.Date(2024, time.April, 1, 0, 0, 0, 0, time.UTC),
					}, {
						name:     "root.ln.device1.timestamp",
						dataType: TIMESTAMP,
						value:    int64(3333333),
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := createIoTDBRpcDataSet()
			s.next()
			got, err := s.getRowRecord()
			if (err != nil) != tt.wantErr {
				t.Errorf("IoTDBRpcDataSet.getRowRecord() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			match := true
			for i := 0; i < len(got.fields); i++ {
				gotField := got.fields[i]
				wantField := tt.want.fields[i]
				if gotField.dataType != BLOB {
					if gotField.dataType != wantField.dataType || gotField.name != wantField.name || gotField.value != wantField.value {
						match = false
					}
				} else {
					if gotField.dataType != wantField.dataType || gotField.name != wantField.name || !bytes.Equal(gotField.value.([]byte), wantField.value.([]byte)) {
						match = false
					}
				}
			}
			if !match {
				t.Errorf("IoTDBRpcDataSet.getRowRecord() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIoTDBRpcDataSet_Close(t *testing.T) {

	tests := []struct {
		name    string
		wantErr bool
	}{
		{
			name:    "",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := createIoTDBRpcDataSet()
			s.next()
			if err := s.Close(); (err != nil) != tt.wantErr {
				t.Errorf("IoTDBRpcDataSet.Close() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
