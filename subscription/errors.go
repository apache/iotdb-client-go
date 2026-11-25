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

package subscription

import (
	"errors"
	"fmt"
)

// ErrorKind identifies the Java subscription exception category represented by
// an operation error. Transport errors remain compatible with errors.Is and
// errors.As through SubscriptionError.Unwrap.
type ErrorKind uint8

const (
	ErrorKindUnknown ErrorKind = iota
	ErrorKindConnection
	ErrorKindPipeTimeout
	ErrorKindRuntimeNonCritical
	ErrorKindRuntimeCritical
	ErrorKindParameterNotValid
	ErrorKindPayloadExceed
	ErrorKindOwnerFenced
	ErrorKindConsumerFenced
)

// SubscriptionError preserves a server status and its Java-compatible error
// category. Code is zero for transport or locally classified errors.
type SubscriptionError struct {
	Kind     ErrorKind
	Code     int32
	Message  string
	Critical bool
	Cause    error
}

func (e *SubscriptionError) Error() string {
	if e.Code != 0 {
		return fmt.Sprintf("subscription: server status %d: %s", e.Code, e.Message)
	}
	if e.Message != "" {
		return "subscription: " + e.Message
	}
	return "subscription: operation failed"
}

func (e *SubscriptionError) Unwrap() error {
	return e.Cause
}

// Sentinel errors can be matched through wrapped operation errors with errors.Is.
var (
	// ErrUnsupportedTsFile is raised only when decoding a TsFile payload. It
	// must not cause automatic connection retries or acknowledgment of that
	// payload. Management and Subscribe do not preemptively return this error.
	ErrUnsupportedTsFile = errors.New("subscription: TsFile payloads are not supported")
	// ErrClosed indicates that an operation cannot proceed after the client's
	// final close. A successfully closed consumer cannot reclaim its identity
	// by reopening the same instance.
	ErrClosed = errors.New("subscription: client is closed")
	// ErrNotOpen indicates that an operation requiring an open session or
	// consumer was called before its connection was established.
	ErrNotOpen = errors.New("subscription: client is not open")
	// ErrDataReleased indicates access to record buffers after message Release.
	// Releasing user data does not invalidate the message's commit identity.
	ErrDataReleased = errors.New("subscription: message data has been released")
	// ErrIncompatibleMessage indicates an accessor used with the wrong payload
	// kind, such as requesting records from a watermark message.
	ErrIncompatibleMessage = errors.New("subscription: incompatible message kind")
	// ErrConsumerFenced indicates that another connection has taken over the
	// same consumer identity. The fenced instance must stop issuing requests
	// and must not automatically reclaim ownership.
	ErrConsumerFenced = errors.New("subscription: consumer is fenced")
	// ErrOwnerFenced indicates that the server rejected topic ownership, for
	// example because the owner, epoch, or lease is no longer valid. It is
	// distinct from replacement of a consumer instance.
	ErrOwnerFenced = errors.New("subscription: owner is fenced")
)
