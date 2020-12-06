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
	"testing"
)

func Test_bytesToInt32(t *testing.T) {
	type args struct {
		bys []byte
	}
	tests := []struct {
		name string
		args args
		want int32
	}{
		{
			name: "",
			args: args{
				bys: int32ToBytes(65535),
			},
			want: 65535,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := bytesToInt32(tt.args.bys); got != tt.want {
				t.Errorf("bytesToInt32() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_bytesToInt64(t *testing.T) {
	type args struct {
		bys []byte
	}
	tests := []struct {
		name string
		args args
		want int64
	}{
		{
			name: "",
			args: args{
				bys: int64ToBytes(1607237683018),
			},
			want: 1607237683018,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := bytesToInt64(tt.args.bys); got != tt.want {
				t.Errorf("bytesToInt64() = %v, want %v", got, tt.want)
			}
		})
	}
}
