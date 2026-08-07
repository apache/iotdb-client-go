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
	"context"
	"testing"

	"github.com/apache/iotdb-client-go/v2/common"
	"github.com/apache/iotdb-client-go/v2/rpc"
	"github.com/apache/thrift/lib/go/thrift"
)

// fakeThriftClient implements thrift.TClient to intercept RPC calls made by
// rpc.IClientRPCServiceClient without requiring a real IoTDB server.
type fakeThriftClient struct {
	thrift.TClient
	fetchResultsResp *rpc.TSFetchResultsResp
	closeStatus      *common.TSStatus
}

func (f *fakeThriftClient) Call(ctx context.Context, method string, args, result thrift.TStruct) (thrift.ResponseMeta, error) {
	switch method {
	case "fetchResultsV2":
		fetchResultsResult, ok := result.(*rpc.IClientRPCServiceFetchResultsV2Result)
		if !ok {
			return thrift.ResponseMeta{}, thrift.NewTApplicationException(thrift.INTERNAL_ERROR, "unexpected result type")
		}
		fetchResultsResult.Success = f.fetchResultsResp
	case "closeOperation":
		closeResult, ok := result.(*rpc.IClientRPCServiceCloseOperationResult)
		if !ok {
			return thrift.ResponseMeta{}, thrift.NewTApplicationException(thrift.INTERNAL_ERROR, "unexpected result type")
		}
		closeResult.Success = f.closeStatus
	default:
		return thrift.ResponseMeta{}, thrift.NewTApplicationException(thrift.UNKNOWN_METHOD, "unexpected method "+method)
	}
	return thrift.ResponseMeta{}, nil
}

func boolPtr(b bool) *bool {
	return &b
}

func newTestRPCDataSet(t *testing.T, fake *fakeThriftClient, moreData bool) *IoTDBRpcDataSet {
	t.Helper()
	client := rpc.NewIClientRPCServiceClient(fake)
	ds, err := NewIoTDBRpcDataSet(
		"select s1 from root.test",
		[]string{"s1"},
		[]string{"INT64"},
		map[string]int32{"s1": 1},
		false,
		moreData,
		1,
		2,
		client,
		3,
		nil,
		1024,
		nil,
		"UTC",
		DEFAULT_TIME_FORMAT,
		1_000,
		[]int32{0},
	)
	if err != nil {
		t.Fatalf("NewIoTDBRpcDataSet error: %v", err)
	}
	return ds
}

func TestIoTDBRpcDataSet_fetchResults_updatesMoreData(t *testing.T) {
	tests := []struct {
		name            string
		initialMoreData bool
		respMoreData    bool
		wantMoreData    bool
	}{
		{name: "more data available", initialMoreData: false, respMoreData: true, wantMoreData: true},
		{name: "no more data", initialMoreData: true, respMoreData: false, wantMoreData: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &fakeThriftClient{
				fetchResultsResp: &rpc.TSFetchResultsResp{
					Status:       &common.TSStatus{Code: SuccessStatus},
					HasResultSet: true,
					MoreData:     boolPtr(tt.respMoreData),
				},
			}
			ds := newTestRPCDataSet(t, fake, tt.initialMoreData)
			ok, err := ds.fetchResults()
			if err != nil {
				t.Fatalf("fetchResults() error = %v", err)
			}
			if !ok {
				t.Fatalf("fetchResults() = false, want true")
			}
			if ds.moreData != tt.wantMoreData {
				t.Errorf("moreData = %v, want %v", ds.moreData, tt.wantMoreData)
			}
		})
	}
}

func TestIoTDBRpcDataSet_fetchResults_noMoreDataFieldDefaultsToFalse(t *testing.T) {
	fake := &fakeThriftClient{
		fetchResultsResp: &rpc.TSFetchResultsResp{
			Status:       &common.TSStatus{Code: SuccessStatus},
			HasResultSet: true,
		},
	}
	ds := newTestRPCDataSet(t, fake, true)
	if _, err := ds.fetchResults(); err != nil {
		t.Fatalf("fetchResults() error = %v", err)
	}
	if ds.moreData {
		t.Errorf("moreData = true, want false when MoreData field is unset")
	}
}

func TestIoTDBRpcDataSet_fetchResults_closesWhenNoResultSet(t *testing.T) {
	fake := &fakeThriftClient{
		fetchResultsResp: &rpc.TSFetchResultsResp{
			Status:       &common.TSStatus{Code: SuccessStatus},
			HasResultSet: false,
			MoreData:     boolPtr(false),
		},
		closeStatus: &common.TSStatus{Code: SuccessStatus},
	}
	ds := newTestRPCDataSet(t, fake, true)
	ok, err := ds.fetchResults()
	if err != nil {
		t.Fatalf("fetchResults() error = %v", err)
	}
	if ok {
		t.Fatalf("fetchResults() = true, want false")
	}
	if !ds.IsClosed() {
		t.Errorf("data set is not closed after fetching empty result")
	}
	if ds.moreData {
		t.Errorf("moreData = true, want false")
	}
}
