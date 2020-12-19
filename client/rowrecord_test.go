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

func TestRowRecord_GetFields(t *testing.T) {
	type fields struct {
		timestamp int64
		fields    []*Field
	}
	tests := []struct {
		name   string
		fields fields
		want   []*Field
	}{
		{
			name: "GetFields",
			fields: fields{
				timestamp: 0,
				fields: []*Field{&Field{
					dataType: FLOAT,
					name:     "temperature",
					value:    0.1,
				}},
			},
			want: []*Field{&Field{
				dataType: FLOAT,
				name:     "temperature",
				value:    0.1,
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &RowRecord{
				timestamp: tt.fields.timestamp,
				fields:    tt.fields.fields,
			}
			if got := r.GetFields(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RowRecord.GetFields() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRowRecord_GetTimestamp(t *testing.T) {
	type fields struct {
		timestamp int64
		fields    []*Field
	}
	tests := []struct {
		name   string
		fields fields
		want   int64
	}{
		{
			name: "GetTimestamp",
			fields: fields{
				timestamp: 1024,
			},
			want: 1024,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &RowRecord{
				timestamp: tt.fields.timestamp,
				fields:    tt.fields.fields,
			}
			if got := r.GetTimestamp(); got != tt.want {
				t.Errorf("RowRecord.GetTimestamp() = %v, want %v", got, tt.want)
			}
		})
	}
}
