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

package iotdb_go

import (
	"errors"

	"github.com/apache/iotdb-client-go/v2/client"
	"github.com/apache/iotdb-client-go/v2/database/column"
)

type rows struct {
	set     *client.SessionDataSet
	columns []column.Interface
	// release returns the underlying session to the pool. It must run exactly
	// once, when the result set is closed — not when query() returns — because
	// set keeps using the session's RPC client/session id for FetchResultsV2
	// and CloseOperation. Returning it early would let another goroutine borrow
	// the same session and use the transport concurrently while rows are live.
	release func()
}

func (r *rows) Next() (bool, error) {
	if r.set == nil {
		return false, errors.New("rows is nil")
	}

	return r.set.Next()
}

func (r *rows) Close() error {
	if r.set != nil {
		r.set.Close()
	}
	if r.release != nil {
		r.release()
		r.release = nil
	}
	return nil
}
