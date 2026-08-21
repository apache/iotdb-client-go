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
	"strings"
	"testing"
)

func newObjectSessionDataSet(t *testing.T, value []byte) *SessionDataSet {
	t.Helper()

	column, err := NewBinaryColumn(0, 1, nil, []*Binary{NewBinary(value)})
	if err != nil {
		t.Fatalf("NewBinaryColumn() error = %v", err)
	}
	block, err := NewTsBlock(1, nil, column)
	if err != nil {
		t.Fatalf("NewTsBlock() error = %v", err)
	}

	return &SessionDataSet{ioTDBRpcDataSet: &IoTDBRpcDataSet{
		columnNameList:                     []string{"file"},
		columnTypeList:                     []string{"OBJECT"},
		columnName2TsBlockColumnIndexMap:   map[string]int32{"file": 0},
		columnIndex2TsBlockColumnIndexList: []int32{0},
		dataTypeForTsBlockColumn:           []TSDataType{OBJECT},
		queryResult:                        [][]byte{{1}},
		queryResultSize:                    1,
		curTsBlock:                         block,
		tsBlockSize:                        1,
		tsBlockIndex:                       0,
	}}
}

func TestSessionDataSet_OBJECTGetters(t *testing.T) {
	value := make([]byte, 8+len("internal/path/1.bin"))
	binary.BigEndian.PutUint64(value[:8], 1024)
	copy(value[8:], "internal/path/1.bin")
	dataSet := newObjectSessionDataSet(t, value)

	object, err := dataSet.GetObject("file")
	if err != nil {
		t.Fatalf("GetObject() error = %v", err)
	}
	if object != "(Object) 1.00 KB" {
		t.Errorf("GetObject() = %#v, want %q", object, "(Object) 1.00 KB")
	}

	object, err = dataSet.GetObjectByIndex(1)
	if err != nil {
		t.Fatalf("GetObjectByIndex() error = %v", err)
	}
	if object != "(Object) 1.00 KB" {
		t.Errorf("GetObjectByIndex() = %#v, want %q", object, "(Object) 1.00 KB")
	}

	stringValue, err := dataSet.GetString("file")
	if err != nil {
		t.Fatalf("GetString() error = %v", err)
	}
	if stringValue != "(Object) 1.00 KB" {
		t.Errorf("GetString() = %q, want %q", stringValue, "(Object) 1.00 KB")
	}

	if _, err := dataSet.GetBlob("file"); err == nil || !strings.Contains(err.Error(), "OBJECT") {
		t.Fatalf("GetBlob() error = %v, want an OBJECT type error", err)
	}
	if _, err := dataSet.GetBlobByIndex(1); err == nil || !strings.Contains(err.Error(), "OBJECT") {
		t.Fatalf("GetBlobByIndex() error = %v, want an OBJECT type error", err)
	}
}
