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

import "time"

// Dialect selects one data model for an instance. A shared subscription interface
// does not imply that tree and table connections or record decoding are identical.
type Dialect string

const (
	// TreeDialect selects tree-model connections and record interpretation.
	TreeDialect Dialect = "tree"
	// TableDialect selects table-model connections and record interpretation.
	TableDialect Dialect = "table"
)

// ConnectionConfig describes the RPC connections owned by a subscription client.
// Password and EncryptedPassword are mutually exclusive. NodeURLs contains
// host:port addresses and supports a single node as well as a cluster seed list.
type ConnectionConfig struct {
	// NodeURLs lists host:port endpoints to use when establishing connections.
	// A single entry selects a single seed; multiple entries provide alternatives.
	// SubscriptionSession and consumer config normalization use localhost:6667
	// when the list is empty.
	NodeURLs []string
	// Username is the IoTDB user used for session and subscription authentication.
	Username string
	// Password supplies the plaintext password and must not be combined with
	// EncryptedPassword.
	Password string
	// EncryptedPassword supplies an already encrypted password and must not be
	// combined with Password.
	EncryptedPassword string
	// ConnectTimeout limits connection establishment and authentication for each
	// seed endpoint. SubscriptionSession and consumer config normalization use
	// ten seconds for zero and reject negative values. Java consumers also
	// default to ten seconds.
	ConnectTimeout time.Duration
	// MaxFrameSize is the maximum accepted Thrift frame size in bytes.
	// SubscriptionSession and consumer config normalization use 64 MiB for zero
	// and reject negative values.
	MaxFrameSize int32
}

