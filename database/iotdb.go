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
	"context"
	"errors"
)

// nativeTransport represents an implementation (TCP or HTTP) that can be pooled by the main iotdb struct.
// Implementations are not expected to be thread safe, which is why we provide acquire/release functions.
type nativeTransport interface {
	query(ctx context.Context, release nativeTransportRelease, query string, args ...any) (*rows, error)
}

// nativeTransport represents an implementation (TCP or HTTP) that can be pooled by the main iotdb struct.
// Implementations are not expected to be thread safe, which is why we provide acquire/release functions.
type nativeTransportAcquire func(context.Context) (nativeTransport, error)
type nativeTransportRelease func(nativeTransport, error)

var (
	ErrBatchInvalid              = errors.New("iotdb: batch is invalid. check appended data is correct")
	ErrBatchAlreadySent          = errors.New("iotdb: batch has already been sent")
	ErrBatchNotSent              = errors.New("iotdb: invalid retry, batch not sent yet")
	ErrAcquireConnTimeout        = errors.New("iotdb: acquire conn timeout. you can increase the number of max open conn or the dial timeout")
	ErrUnsupportedServerRevision = errors.New("iotdb: unsupported server revision")
	ErrBindMixedParamsFormats    = errors.New("iotdb [bind]: mixed named, numeric or positional parameters")
	ErrAcquireConnNoAddress      = errors.New("iotdb: no valid address supplied")
	ErrServerUnexpectedData      = errors.New("code: 101, message: Unexpected packet Data received from client")
	ErrConnectionClosed          = errors.New("iotdb: connection is closed")
	ErrTransactionsUnsupported   = errors.New("iotdb: transactions are not supported")
)
