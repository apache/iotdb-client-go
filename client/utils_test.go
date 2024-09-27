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
	"github.com/apache/iotdb-client-go/common"
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

func Test_int32ToString(t *testing.T) {
	type args struct {
		n int32
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "Test Int32",
			args: args{
				n: 65535,
			},
			want: "65535",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := int32ToString(tt.args.n); got != tt.want {
				t.Errorf("int32ToString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_int64ToString(t *testing.T) {
	type args struct {
		n int64
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "Test Int64",
			args: args{
				n: 7684873721715404507,
			},
			want: "7684873721715404507",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := int64ToString(tt.args.n); got != tt.want {
				t.Errorf("int64ToString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_float32ToString(t *testing.T) {
	type args struct {
		val float32
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "Test Float32",
			args: args{
				val: 0.97800666,
			},
			want: "0.97800666",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := float32ToString(tt.args.val); got != tt.want {
				t.Errorf("float32ToString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_float64ToString(t *testing.T) {
	type args struct {
		val float64
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "Test Float64",
			args: args{
				val: 0.39751212862981283,
			},
			want: "0.39751212862981283",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := float64ToString(tt.args.val); got != tt.want {
				t.Errorf("float64ToString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_bytesToHexString(t *testing.T) {
	type args struct {
		val []byte
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "Test bytes",
			args: args{
				val: []byte("bytes"),
			},
			want: "0x6279746573",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := bytesToHexString(tt.args.val); got != tt.want {
				t.Errorf("bytesToHexString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_verifySuccess(t *testing.T) {
	type args struct {
		status *common.TSStatus
	}
	var errMsg = "error occurred"
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "RedirectionRecommend",
			args: args{
				status: &common.TSStatus{
					Code:      RedirectionRecommend,
					Message:   &errMsg,
					SubStatus: []*common.TSStatus{},
				},
			},
			wantErr: false,
		}, {
			name: "SuccessStatus",
			args: args{
				status: &common.TSStatus{
					Code:      SuccessStatus,
					Message:   &errMsg,
					SubStatus: []*common.TSStatus{},
				},
			},
			wantErr: false,
		}, {
			name: "MultipleError",
			args: args{
				status: &common.TSStatus{
					Code:    MultipleError,
					Message: &errMsg,
					SubStatus: []*common.TSStatus{
						{
							Code:    ShutDownError,
							Message: &errMsg,
						},
					},
				},
			},
			wantErr: true,
		}, {
			name: "CloseOperationError",
			args: args{
				status: &common.TSStatus{
					Code:      CloseOperationError,
					Message:   &errMsg,
					SubStatus: []*common.TSStatus{},
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := VerifySuccess(tt.args.status); (err != nil) != tt.wantErr {
				t.Errorf("VerifySuccess() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_verifySuccesses(t *testing.T) {
	type args struct {
		statuses []*common.TSStatus
	}
	var internalServerError = "InternalServerError"
	var success = "Success"
	var redirectionRecommend = "RedirectionRecommend"
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "InternalServerError",
			args: args{
				statuses: []*common.TSStatus{
					{
						Code:      InternalServerError,
						Message:   &internalServerError,
						SubStatus: []*common.TSStatus{},
					},
				},
			},
			wantErr: true,
		}, {
			name: "SuccessStatus",
			args: args{
				statuses: []*common.TSStatus{
					{
						Code:      SuccessStatus,
						Message:   &success,
						SubStatus: []*common.TSStatus{},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "RedirectionRecommend",
			args: args{
				statuses: []*common.TSStatus{
					{
						Code:      RedirectionRecommend,
						Message:   &redirectionRecommend,
						SubStatus: []*common.TSStatus{},
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := verifySuccesses(tt.args.statuses); (err != nil) != tt.wantErr {
				t.Errorf("verifySuccesses() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
