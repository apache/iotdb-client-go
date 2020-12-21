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

func createTablet(size int) (*Tablet, error) {
	tablet, err := NewTablet("root.ln.TestDevice", []*MeasurementSchema{
		&MeasurementSchema{
			Measurement: "restart_count",
			DataType:    INT32,
			Encoding:    RLE,
			Compressor:  SNAPPY,
			Properties: map[string]string{
				"owner": "Mark Liu",
			},
		}, &MeasurementSchema{
			Measurement: "price",
			DataType:    DOUBLE,
			Encoding:    GORILLA,
			Compressor:  SNAPPY,
		}, &MeasurementSchema{
			Measurement: "tick_count",
			DataType:    INT64,
			Encoding:    RLE,
			Compressor:  SNAPPY,
		}, &MeasurementSchema{
			Measurement: "temperature",
			DataType:    FLOAT,
			Encoding:    GORILLA,
			Compressor:  SNAPPY,
			Properties: map[string]string{
				"owner": "Mark Liu",
			},
		}, &MeasurementSchema{
			Measurement: "description",
			DataType:    TEXT,
			Encoding:    PLAIN,
			Compressor:  SNAPPY,
			Properties: map[string]string{
				"owner": "Mark Liu",
			},
		},
		&MeasurementSchema{
			Measurement: "status",
			DataType:    BOOLEAN,
			Encoding:    RLE,
			Compressor:  SNAPPY,
			Properties: map[string]string{
				"owner": "Mark Liu",
			},
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
					&MeasurementSchema{
						Measurement: "restart_count",
						DataType:    INT32,
						Encoding:    RLE,
						Compressor:  SNAPPY,
						Properties: map[string]string{
							"owner": "Mark Liu",
						},
					}, &MeasurementSchema{
						Measurement: "price",
						DataType:    DOUBLE,
						Encoding:    GORILLA,
						Compressor:  SNAPPY,
					}, &MeasurementSchema{
						Measurement: "tick_count",
						DataType:    INT64,
						Encoding:    RLE,
						Compressor:  SNAPPY,
					}, &MeasurementSchema{
						Measurement: "temperature",
						DataType:    FLOAT,
						Encoding:    GORILLA,
						Compressor:  SNAPPY,
						Properties: map[string]string{
							"owner": "Mark Liu",
						},
					}, &MeasurementSchema{
						Measurement: "description",
						DataType:    TEXT,
						Encoding:    PLAIN,
						Compressor:  SNAPPY,
						Properties: map[string]string{
							"owner": "Mark Liu",
						},
					},
					&MeasurementSchema{
						Measurement: "status",
						DataType:    BOOLEAN,
						Encoding:    RLE,
						Compressor:  SNAPPY,
						Properties: map[string]string{
							"owner": "Mark Liu",
						},
					},
				},
				timestamps: []int64{},
				values:     []interface{}{},
				rowCount:   0,
			},
			want: []int32{int32(INT32), int32(DOUBLE), int32(INT64), int32(FLOAT), int32(TEXT), int32(BOOLEAN)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tablet := &Tablet{
				deviceId:           tt.fields.deviceId,
				measurementSchemas: tt.fields.measurementSchemas,
				timestamps:         tt.fields.timestamps,
				values:             tt.fields.values,
				rowCount:           tt.fields.rowCount,
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
			wantErr: true,
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
