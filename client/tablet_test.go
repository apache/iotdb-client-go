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
	"time"
)

func createTablet(size int) (*Tablet, error) {
	tablet, err := NewTablet("root.ln.TestDevice", []*MeasurementSchema{
		{
			Measurement: "restart_count",
			DataType:    INT32,
		}, {
			Measurement: "price",
			DataType:    DOUBLE,
		}, {
			Measurement: "tick_count",
			DataType:    INT64,
		}, {
			Measurement: "temperature",
			DataType:    FLOAT,
		}, {
			Measurement: "description",
			DataType:    TEXT,
		},
		{
			Measurement: "status",
			DataType:    BOOLEAN,
		},
		{
			Measurement: "description_string",
			DataType:    STRING,
		},
		{
			Measurement: "description_blob",
			DataType:    BLOB,
		},
		{
			Measurement: "date",
			DataType:    DATE,
		},
		{
			Measurement: "ts",
			DataType:    TIMESTAMP,
		},
	}, size)
	return tablet, err
}

func TestTablet_getDataTypes(t *testing.T) {
	type fields struct {
		deviceId           string
		measurementSchemas []*MeasurementSchema
		timestamps         []int64
		values             []interface{}
		rowCount           int
	}
	tests := []struct {
		name   string
		fields fields
		want   []int32
	}{
		{
			name: "",
			fields: fields{
				deviceId: "root.ln.device5",
				measurementSchemas: []*MeasurementSchema{
					{
						Measurement: "restart_count",
						DataType:    INT32,
					}, {
						Measurement: "price",
						DataType:    DOUBLE,
					}, {
						Measurement: "tick_count",
						DataType:    INT64,
					}, {
						Measurement: "temperature",
						DataType:    FLOAT,
					}, {
						Measurement: "description",
						DataType:    TEXT,
					}, {
						Measurement: "status",
						DataType:    BOOLEAN,
					}, {
						Measurement: "description_string",
						DataType:    STRING,
					}, {
						Measurement: "description_blob",
						DataType:    BLOB,
					}, {
						Measurement: "date",
						DataType:    DATE,
					}, {
						Measurement: "ts",
						DataType:    TIMESTAMP,
					},
				},
				timestamps: []int64{},
				values:     []interface{}{},
				rowCount:   0,
			},
			want: []int32{int32(INT32), int32(DOUBLE), int32(INT64), int32(FLOAT), int32(TEXT), int32(BOOLEAN), int32(STRING), int32(BLOB), int32(DATE), int32(TIMESTAMP)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tablet := &Tablet{
				insertTargetName:   tt.fields.deviceId,
				measurementSchemas: tt.fields.measurementSchemas,
				timestamps:         tt.fields.timestamps,
				values:             tt.fields.values,
				maxRowNumber:       tt.fields.rowCount,
			}
			if got := tablet.getDataTypes(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Tablet.getDataTypes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTablet_SetTimestamp(t *testing.T) {
	type args struct {
		timestamp int64
		rowIndex  int
	}
	tests := []struct {
		name string
		args args
		want int64
	}{
		{
			name: "",
			args: args{
				timestamp: 1608268702769,
				rowIndex:  0,
			},
			want: 1608268702769,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tablet, err := createTablet(1)
			if err != nil {
				t.Error(err)
			}
			tablet.SetTimestamp(tt.args.timestamp, tt.args.rowIndex)
			if got := tablet.timestamps[0]; got != tt.want {
				t.Errorf("Tablet.SetTimestamp(int64, int32) = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTablet_SetTimestampAtRowIndexBounds(t *testing.T) {
	tablet, err := createTablet(1) // maxRowNumber == 1, so only row index 0 is valid
	if err != nil {
		t.Fatal(err)
	}
	// A valid row index returns no error.
	if err := tablet.SetTimestampAt(1608268702769, 0); err != nil {
		t.Errorf("SetTimestampAt(_, 0) on a 1-row tablet returned error: %v", err)
	}
	// Out-of-range row indices return an error instead of panicking with an
	// index-out-of-range, matching SetValueAt/GetValueAt.
	for _, rowIndex := range []int{1, -1} {
		if err := tablet.SetTimestampAt(1, rowIndex); err == nil {
			t.Errorf("SetTimestampAt(_, %d) on a 1-row tablet should return an error, got nil", rowIndex)
		}
	}
}

func TestTablet_SetValueAt(t *testing.T) {
	type args struct {
		value       interface{}
		columnIndex int
		rowIndex    int
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "nil",
			args: args{
				value:       nil,
				columnIndex: 0,
				rowIndex:    0,
			},
			wantErr: false,
		}, {
			name: "columnIndex-1",
			args: args{
				value:       0,
				columnIndex: -1,
				rowIndex:    0,
			},
			wantErr: true,
		}, {
			name: "rowIndex-1",
			args: args{
				value:       0,
				columnIndex: 0,
				rowIndex:    -1,
			},
			wantErr: true,
		}, {
			name: "rowIndex-65535",
			args: args{
				value:       0,
				columnIndex: 0,
				rowIndex:    65535,
			},
			wantErr: true,
		}, {
			name: "columnIndex-65535",
			args: args{
				value:       0,
				columnIndex: 65535,
				rowIndex:    0,
			},
			wantErr: true,
		}, {
			name: "columnIndex-out-of-range-boundary",
			args: args{
				value:       0,
				columnIndex: 10,
				rowIndex:    0,
			},
			wantErr: true,
		}, {
			name: "rowIndex-out-of-range-boundary",
			args: args{
				value:       0,
				columnIndex: 0,
				rowIndex:    1,
			},
			wantErr: true,
		}, {
			name: "restart_count",
			args: args{
				value:       int32(0),
				columnIndex: 0,
				rowIndex:    0,
			},
			wantErr: false,
		}, {
			name: "price",
			args: args{
				value:       float64(32.768),
				columnIndex: 1,
				rowIndex:    0,
			},
			wantErr: false,
		}, {
			name: "tick_count",
			args: args{
				value:       int64(1608268702780),
				columnIndex: 2,
				rowIndex:    0,
			},
			wantErr: false,
		}, {
			name: "temperature",
			args: args{
				value:       float32(36.5),
				columnIndex: 3,
				rowIndex:    0,
			},
			wantErr: false,
		}, {
			name: "description",
			args: args{
				value:       "Hello world!",
				columnIndex: 4,
				rowIndex:    0,
			},
			wantErr: false,
		}, {
			name: "status",
			args: args{
				value:       true,
				columnIndex: 5,
				rowIndex:    0,
			},
			wantErr: false,
		}, {
			name: "description_string",
			args: args{
				value:       "Hello world!",
				columnIndex: 6,
				rowIndex:    0,
			},
			wantErr: false,
		}, {
			name: "description_blob",
			args: args{
				value:       []byte("Hello world!"),
				columnIndex: 7,
				rowIndex:    0,
			},
			wantErr: false,
		}, {
			name: "date",
			args: args{
				value:       time.Date(2024, time.April, 1, 0, 0, 0, 0, time.UTC),
				columnIndex: 8,
				rowIndex:    0,
			},
			wantErr: false,
		}, {
			name: "ts",
			args: args{
				value:       int64(1608268702780),
				columnIndex: 9,
				rowIndex:    0,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tablet, err := createTablet(1)
			if err != nil {
				t.Error(err)
			}
			if err := tablet.SetValueAt(tt.args.value, tt.args.columnIndex, tt.args.rowIndex); (err != nil) != tt.wantErr {
				t.Errorf("Tablet.SetValueAt() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestTablet_GetValueAt(t *testing.T) {
	type args struct {
		columnIndex int
		rowIndex    int
	}
	tests := []struct {
		name    string
		args    args
		want    interface{}
		wantErr bool
	}{
		{
			name: "INT32",
			args: args{
				columnIndex: 0,
				rowIndex:    0,
			},
			want:    int32(256),
			wantErr: false,
		}, {
			name: "FLOAT64",
			args: args{
				columnIndex: 1,
				rowIndex:    0,
			},
			want:    float64(32.768),
			wantErr: false,
		}, {
			name: "INT64",
			args: args{
				columnIndex: 2,
				rowIndex:    0,
			},
			want:    int64(65535),
			wantErr: false,
		}, {
			name: "FLOAT32",
			args: args{
				columnIndex: 3,
				rowIndex:    0,
			},
			want:    float32(36.5),
			wantErr: false,
		}, {
			name: "TEXT",
			args: args{
				columnIndex: 4,
				rowIndex:    0,
			},
			want:    "Hello World!",
			wantErr: false,
		}, {
			name: "BOOLEAN",
			args: args{
				columnIndex: 5,
				rowIndex:    0,
			},
			want:    true,
			wantErr: false,
		}, {
			name: "TEXT",
			args: args{
				columnIndex: 6,
				rowIndex:    0,
			},
			want:    "Hello World!",
			wantErr: false,
		}, {
			name: "BLOB",
			args: args{
				columnIndex: 7,
				rowIndex:    0,
			},
			want:    []byte("Hello World!"),
			wantErr: false,
		}, {
			name: "DATE",
			args: args{
				columnIndex: 8,
				rowIndex:    0,
			},
			want:    time.Date(2024, time.April, 1, 0, 0, 0, 0, time.UTC),
			wantErr: false,
		}, {
			name: "TIMESTAMP",
			args: args{
				columnIndex: 9,
				rowIndex:    0,
			},
			want:    int64(1608268702780),
			wantErr: false,
		}, {
			name: "columnIndex-out-of-range-boundary",
			args: args{
				columnIndex: 10,
				rowIndex:    0,
			},
			wantErr: true,
		}, {
			name: "rowIndex-out-of-range-boundary",
			args: args{
				columnIndex: 0,
				rowIndex:    1,
			},
			wantErr: true,
		},
	}
	if tablet, err := createTablet(1); err == nil {
		tablet.SetValueAt(int32(256), 0, 0)
		tablet.SetValueAt(32.768, 1, 0)
		tablet.SetValueAt(int64(65535), 2, 0)
		tablet.SetValueAt(float32(36.5), 3, 0)
		tablet.SetValueAt("Hello World!", 4, 0)
		tablet.SetValueAt(true, 5, 0)
		tablet.SetValueAt("Hello World!", 6, 0)
		tablet.SetValueAt([]byte("Hello World!"), 7, 0)
		tablet.SetValueAt(time.Date(2024, time.April, 1, 0, 0, 0, 0, time.UTC), 8, 0)
		tablet.SetValueAt(int64(1608268702780), 9, 0)
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got, err := tablet.GetValueAt(tt.args.columnIndex, tt.args.rowIndex)
				if (err != nil) != tt.wantErr {
					t.Errorf("Tablet.GetValueAt() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("Tablet.GetValueAt() = %v, want %v", got, tt.want)
				}
			})
		}
	}
}

func TestTablet_GetNilValueAt(t *testing.T) {
	type args struct {
		columnIndex int
		rowIndex    int
	}
	tests := []struct {
		name    string
		args    args
		want    interface{}
		wantErr bool
	}{
		{
			name: "INT32",
			args: args{
				columnIndex: 0,
				rowIndex:    0,
			},
			want:    int32(256),
			wantErr: false,
		}, {
			name: "FLOAT64",
			args: args{
				columnIndex: 1,
				rowIndex:    0,
			},
			want:    nil,
			wantErr: false,
		}, {
			name: "INT64",
			args: args{
				columnIndex: 2,
				rowIndex:    0,
			},
			want:    int64(65535),
			wantErr: false,
		}, {
			name: "FLOAT32",
			args: args{
				columnIndex: 3,
				rowIndex:    0,
			},
			want:    float32(36.5),
			wantErr: false,
		}, {
			name: "STRING",
			args: args{
				columnIndex: 4,
				rowIndex:    0,
			},
			want:    "Hello World!",
			wantErr: false,
		}, {
			name: "BOOLEAN",
			args: args{
				columnIndex: 5,
				rowIndex:    0,
			},
			want:    true,
			wantErr: false,
		}, {
			name: "STRING",
			args: args{
				columnIndex: 6,
				rowIndex:    0,
			},
			want:    nil,
			wantErr: false,
		}, {
			name: "BLOB",
			args: args{
				columnIndex: 7,
				rowIndex:    0,
			},
			want:    nil,
			wantErr: false,
		}, {
			name: "DATE",
			args: args{
				columnIndex: 8,
				rowIndex:    0,
			},
			want:    nil,
			wantErr: false,
		}, {
			name: "TIMESTAMP",
			args: args{
				columnIndex: 9,
				rowIndex:    0,
			},
			want:    nil,
			wantErr: false,
		},
	}
	if tablet, err := createTablet(1); err == nil {
		tablet.SetValueAt(int32(256), 0, 0)
		tablet.SetValueAt(nil, 1, 0)
		tablet.SetValueAt(int64(65535), 2, 0)
		tablet.SetValueAt(float32(36.5), 3, 0)
		tablet.SetValueAt("Hello World!", 4, 0)
		tablet.SetValueAt(true, 5, 0)
		tablet.SetValueAt(nil, 6, 0)
		tablet.SetValueAt(nil, 7, 0)
		tablet.SetValueAt(nil, 8, 0)
		tablet.SetValueAt(nil, 9, 0)
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got, err := tablet.GetValueAt(tt.args.columnIndex, tt.args.rowIndex)
				if (err != nil) != tt.wantErr {
					t.Errorf("Tablet.GetValueAt() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("Tablet.GetValueAt() = %v, want %v", got, tt.want)
				}
			})
		}
	}
}

func TestTablet_Sort(t *testing.T) {

	tests := []struct {
		name    string
		want    [][]interface{}
		wantErr bool
	}{
		{
			name: "item-1",
			want: [][]interface{}{
				{int32(3), float64(3.0), int64(3), float32(3.0), "3", true},
				{int32(4), float64(4.0), int64(4), float32(4.0), "4", true},
				{int32(1), float64(1.0), int64(1), float32(1.0), "1", true},
				{int32(2), float64(2.0), int64(2), float32(2.0), "2", true},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tablet, _ := createTablet(4)
			tablet.SetValueAt(int32(1), 0, 0)
			tablet.SetValueAt(float64(1.0), 1, 0)
			tablet.SetValueAt(int64(1), 2, 0)
			tablet.SetValueAt(float32(1.0), 3, 0)
			tablet.SetValueAt("1", 4, 0)
			tablet.SetValueAt(true, 5, 0)
			tablet.SetTimestamp(3, 0)
			tablet.RowSize++

			tablet.SetValueAt(int32(2), 0, 1)
			tablet.SetValueAt(float64(2.0), 1, 1)
			tablet.SetValueAt(int64(2), 2, 1)
			tablet.SetValueAt(float32(2.0), 3, 1)
			tablet.SetValueAt("2", 4, 1)
			tablet.SetValueAt(true, 5, 1)
			tablet.SetTimestamp(4, 1)
			tablet.RowSize++

			tablet.SetValueAt(int32(3), 0, 2)
			tablet.SetValueAt(float64(3.0), 1, 2)
			tablet.SetValueAt(int64(3), 2, 2)
			tablet.SetValueAt(float32(3.0), 3, 2)
			tablet.SetValueAt("3", 4, 2)
			tablet.SetValueAt(true, 5, 2)
			tablet.SetTimestamp(1, 2)
			tablet.RowSize++

			tablet.SetValueAt(int32(4), 0, 3)
			tablet.SetValueAt(float64(4.0), 1, 3)
			tablet.SetValueAt(int64(4), 2, 3)
			tablet.SetValueAt(float32(4.0), 3, 3)
			tablet.SetValueAt("4", 4, 3)
			tablet.SetValueAt(true, 5, 3)
			tablet.SetTimestamp(2, 3)
			tablet.RowSize++

			if err := tablet.Sort(); (err != nil) != tt.wantErr {
				t.Errorf("Tablet.Sort() error = %v, wantErr %v", err, tt.wantErr)
			}
			for rowIndex, row := range tt.want {
				for columnIndex, wantValue := range row {
					value, _ := tablet.GetValueAt(columnIndex, rowIndex)
					if !reflect.DeepEqual(value, wantValue) {
						t.Errorf("Tablet.Sort() colum: %d, row: %d, value: %v != %v", columnIndex, rowIndex, value, wantValue)
					}
				}
			}
		})
	}
}

func TestTablet_OBJECT(t *testing.T) {
	tablet, err := NewRelationalTablet("t1", []*MeasurementSchema{
		{Measurement: "tag1", DataType: STRING},
		{Measurement: "obj", DataType: OBJECT},
	}, []ColumnCategory{TAG, FIELD}, 4)
	if err != nil {
		t.Fatal(err)
	}

	if got := tablet.getColumnCategories(); !reflect.DeepEqual(got, []int8{0, 1}) {
		t.Errorf("getColumnCategories() = %v, want [0 1]", got)
	}

	if got := tablet.getDataTypes(); !reflect.DeepEqual(got, []int32{11, 12}) {
		t.Errorf("getDataTypes() = %v, want [11 12]", got)
	}

	objVal := []byte{0x01, 0x02, 0x03}
	if err := tablet.SetValueAt(objVal, 1, 0); err == nil {
		t.Fatal("SetValueAt([]byte) for OBJECT: want error, got nil")
	}
	if err := tablet.SetObjectValueAt(true, 0, objVal, 1, 0); err != nil {
		t.Fatalf("SetObjectValueAt([]byte) error = %v", err)
	}
	tablet.SetTimestamp(1608268702780, 0)
	tablet.RowSize++

	if err := tablet.SetValueAt("hello", 1, 1); err == nil {
		t.Fatal("SetValueAt(string) for OBJECT: want error, got nil")
	}
	if err := tablet.SetObjectValueAt(true, 0, []byte("hello"), 1, 1); err != nil {
		t.Fatalf("SetObjectValueAt(string bytes) error = %v", err)
	}
	tablet.SetTimestamp(1608268702781, 1)
	tablet.RowSize++

	wantObject1 := append([]byte{1, 0, 0, 0, 0, 0, 0, 0, 0}, objVal...)
	wantObject2 := append([]byte{1, 0, 0, 0, 0, 0, 0, 0, 0}, []byte("hello")...)
	if got, err := tablet.GetValueAt(1, 0); err != nil || !reflect.DeepEqual(got, wantObject1) {
		t.Errorf("GetValueAt(1,0) = %v, %v; want %v, nil", got, err, wantObject1)
	}
	if got, err := tablet.GetValueAt(1, 1); err != nil || !reflect.DeepEqual(got, wantObject2) {
		t.Errorf("GetValueAt(1,1) = %v, %v; want %v, nil", got, err, wantObject2)
	}

	valuesBytes, err := tablet.getValuesBytes()
	if err != nil {
		t.Fatal(err)
	}
	wantValues := []byte{
		0, 0, 0, 0,
		0, 0, 0, 0,
		0, 0, 0, byte(len(wantObject1)),
	}
	wantValues = append(wantValues, wantObject1...)
	wantValues = append(wantValues, 0, 0, 0, byte(len(wantObject2)))
	wantValues = append(wantValues, wantObject2...)
	if !reflect.DeepEqual(valuesBytes, wantValues) {
		t.Errorf("getValuesBytes() = %v, want %v", valuesBytes, wantValues)
	}
}

func TestTablet_SetObjectValueAt(t *testing.T) {
	tablet, err := NewRelationalTablet("t1", []*MeasurementSchema{
		{Measurement: "tag1", DataType: STRING},
		{Measurement: "obj", DataType: OBJECT},
	}, []ColumnCategory{TAG, FIELD}, 4)
	if err != nil {
		t.Fatal(err)
	}

	tablet.SetTimestamp(1, 0)
	if err := tablet.SetObjectValueAt(false, 0, []byte{0x11, 0x22}, 1, 0); err != nil {
		t.Fatalf("SetObjectValueAt(segment) error = %v", err)
	}
	tablet.RowSize++

	tablet.SetTimestamp(1, 1)
	if err := tablet.SetObjectValueAt(true, 512, []byte{0x33}, 1, 1); err != nil {
		t.Fatalf("SetObjectValueAt(last segment) error = %v", err)
	}
	tablet.RowSize++

	wantSegment1 := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0x11, 0x22}
	wantSegment2 := []byte{1, 0, 0, 0, 0, 0, 0, 2, 0, 0x33}
	if got, err := tablet.GetValueAt(1, 0); err != nil || !reflect.DeepEqual(got, wantSegment1) {
		t.Errorf("GetValueAt(1,0) = %v, %v; want %v, nil", got, err, wantSegment1)
	}
	if got, err := tablet.GetValueAt(1, 1); err != nil || !reflect.DeepEqual(got, wantSegment2) {
		t.Errorf("GetValueAt(1,1) = %v, %v; want %v, nil", got, err, wantSegment2)
	}

	if err := tablet.SetValueAt(nil, 1, 2); err != nil {
		t.Fatalf("SetValueAt(nil) error = %v", err)
	}
	if tablet.bitMaps == nil || tablet.bitMaps[1] == nil || !tablet.bitMaps[1].IsMarked(2) {
		t.Fatal("SetValueAt(nil) did not mark the OBJECT cell as null")
	}
	if err := tablet.SetObjectValueAt(true, 0, []byte{0x44}, 1, 2); err != nil {
		t.Fatalf("SetObjectValueAt() after null error = %v", err)
	}
	if tablet.bitMaps[1].IsMarked(2) {
		t.Error("SetObjectValueAt() did not clear the previously marked null bit")
	}

	if err := tablet.SetObjectValueAt(false, 0, []byte{0x01}, 0, 0); err == nil {
		t.Error("SetObjectValueAt() on non-OBJECT column: want error, got nil")
	}
	if err := tablet.SetObjectValueAt(false, 0, []byte{0x01}, 1, -1); err == nil {
		t.Error("SetObjectValueAt() with invalid rowIndex: want error, got nil")
	}
	if err := tablet.SetObjectValueAt(false, 0, []byte{0x01}, -1, 0); err == nil {
		t.Error("SetObjectValueAt() with invalid columnIndex: want error, got nil")
	}
}
