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

func TestField_IsNull(t *testing.T) {
	type fields struct {
		dataType TSDataType
		name     string
		value    interface{}
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name: "IsNull-1",
			fields: fields{
				dataType: 0,
				name:     "",
				value:    nil,
			},
			want: true,
		}, {
			name: "IsNull-2",
			fields: fields{
				dataType: 0,
				name:     "",
				value:    1,
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &Field{
				dataType: tt.fields.dataType,
				name:     tt.fields.name,
				value:    tt.fields.value,
			}
			if got := f.IsNull(); got != tt.want {
				t.Errorf("Field.IsNull() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestField_GetDataType(t *testing.T) {
	type fields struct {
		dataType TSDataType
		name     string
		value    interface{}
	}
	tests := []struct {
		name   string
		fields fields
		want   TSDataType
	}{
		{
			name: "GetDataType-BOOLEAN",
			fields: fields{
				dataType: BOOLEAN,
				name:     "",
				value:    nil,
			},
			want: BOOLEAN,
		}, {
			name: "GetDataType-INT32",
			fields: fields{
				dataType: INT32,
				name:     "",
				value:    nil,
			},
			want: INT32,
		}, {
			name: "GetDataType-INT64",
			fields: fields{
				dataType: INT64,
				name:     "",
				value:    nil,
			},
			want: INT64,
		}, {
			name: "GetDataType-FLOAT",
			fields: fields{
				dataType: FLOAT,
				name:     "",
				value:    nil,
			},
			want: FLOAT,
		}, {
			name: "GetDataType-DOUBLE",
			fields: fields{
				dataType: DOUBLE,
				name:     "",
				value:    nil,
			},
			want: DOUBLE,
		}, {
			name: "GetDataType-TEXT",
			fields: fields{
				dataType: TEXT,
				name:     "",
				value:    nil,
			},
			want: TEXT,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &Field{
				dataType: tt.fields.dataType,
				name:     tt.fields.name,
				value:    tt.fields.value,
			}
			if got := f.GetDataType(); got != tt.want {
				t.Errorf("Field.GetDataType() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestField_GetValue(t *testing.T) {
	type fields struct {
		dataType TSDataType
		name     string
		value    interface{}
	}
	tests := []struct {
		name   string
		fields fields
		want   interface{}
	}{
		{
			name: "GetValue-BOOLEAN",
			fields: fields{
				dataType: BOOLEAN,
				name:     "",
				value:    true,
			},
			want: true,
		}, {
			name: "GetValue-INT32",
			fields: fields{
				dataType: INT32,
				name:     "",
				value:    int32(65535),
			},
			want: int32(65535),
		}, {
			name: "GetValue-INT64",
			fields: fields{
				dataType: INT64,
				name:     "",
				value:    int64(65535),
			},
			want: int64(65535),
		}, {
			name: "GetValue-FLOAT",
			fields: fields{
				dataType: FLOAT,
				name:     "",
				value:    float32(32.768),
			},
			want: float32(32.768),
		}, {
			name: "GetValue-DOUBLE",
			fields: fields{
				dataType: DOUBLE,
				name:     "",
				value:    float64(32.768),
			},
			want: float64(32.768),
		}, {
			name: "GetValue-TEXT",
			fields: fields{
				dataType: TEXT,
				name:     "",
				value:    "TEXT",
			},
			want: "TEXT",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &Field{
				dataType: tt.fields.dataType,
				name:     tt.fields.name,
				value:    tt.fields.value,
			}
			if got := f.GetValue(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Field.GetValue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestField_GetInt32(t *testing.T) {
	type fields struct {
		dataType TSDataType
		name     string
		value    interface{}
	}
	tests := []struct {
		name   string
		fields fields
		want   int32
	}{
		{
			name: "GetInt32-01",
			fields: fields{
				dataType: INT32,
				name:     "",
				value:    int32(65535),
			},
			want: 65535,
		}, {
			name: "GetInt32-02",
			fields: fields{
				dataType: INT32,
				name:     "restart_count",
				value:    nil,
			},
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &Field{
				dataType: tt.fields.dataType,
				name:     tt.fields.name,
				value:    tt.fields.value,
			}
			if got := f.GetInt32(); got != tt.want {
				t.Errorf("Field.GetInt32() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestField_GetInt64(t *testing.T) {
	type fields struct {
		dataType TSDataType
		name     string
		value    interface{}
	}
	tests := []struct {
		name   string
		fields fields
		want   int64
	}{
		{
			name: "GetInt64-01",
			fields: fields{
				dataType: INT64,
				name:     "",
				value:    int64(65535),
			},
			want: 65535,
		}, {
			name: "GetInt64-02",
			fields: fields{
				dataType: INT64,
				name:     "tickCount",
				value:    nil,
			},
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &Field{
				dataType: tt.fields.dataType,
				name:     tt.fields.name,
				value:    tt.fields.value,
			}
			if got := f.GetInt64(); got != tt.want {
				t.Errorf("Field.GetInt64() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestField_GetFloat32(t *testing.T) {
	type fields struct {
		dataType TSDataType
		name     string
		value    interface{}
	}
	tests := []struct {
		name   string
		fields fields
		want   float32
	}{
		{
			name: "GetFloat32",
			fields: fields{
				dataType: FLOAT,
				name:     "",
				value:    float32(32.768),
			},
			want: 32.768,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &Field{
				dataType: tt.fields.dataType,
				name:     tt.fields.name,
				value:    tt.fields.value,
			}
			if got := f.GetFloat32(); got != tt.want {
				t.Errorf("Field.GetFloat32() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestField_GetFloat64(t *testing.T) {
	type fields struct {
		dataType TSDataType
		name     string
		value    interface{}
	}
	tests := []struct {
		name   string
		fields fields
		want   float64
	}{
		{
			name: "GetFloat64",
			fields: fields{
				dataType: DOUBLE,
				name:     "",
				value:    float64(32.768),
			},
			want: 32.768,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &Field{
				dataType: tt.fields.dataType,
				name:     tt.fields.name,
				value:    tt.fields.value,
			}
			if got := f.GetFloat64(); got != tt.want {
				t.Errorf("Field.GetFloat64() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestField_GetText(t *testing.T) {
	type fields struct {
		dataType TSDataType
		name     string
		value    interface{}
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "GetText-01",
			fields: fields{
				dataType: TEXT,
				name:     "",
				value:    "32.768",
			},
			want: "32.768",
		}, {
			name: "GetText-02",
			fields: fields{
				dataType: TEXT,
				name:     "",
				value:    nil,
			},
			want: "",
		}, {
			name: "GetText-03",
			fields: fields{
				dataType: INT32,
				name:     "",
				value:    int32(1),
			},
			want: "1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &Field{
				dataType: tt.fields.dataType,
				name:     tt.fields.name,
				value:    tt.fields.value,
			}
			if got := f.GetText(); got != tt.want {
				t.Errorf("Field.GetText() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestField_getName(t *testing.T) {
	type fields struct {
		dataType TSDataType
		name     string
		value    interface{}
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "GetName",
			fields: fields{
				dataType: TEXT,
				name:     "temperature",
				value:    float32(32),
			},
			want: "temperature",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &Field{
				dataType: tt.fields.dataType,
				name:     tt.fields.name,
				value:    tt.fields.value,
			}
			if got := f.GetName(); got != tt.want {
				t.Errorf("Field.GetName() = %v, want %v", got, tt.want)
			}
		})
	}
}
