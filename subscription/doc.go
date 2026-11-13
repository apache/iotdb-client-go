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

// Package subscription provides shared connection settings, model selection,
// and sentinel errors for the IoTDB subscription client module.
//
// Management sessions live in the session subpackage. Pull and push consumers
// live in consumer, message and progress contracts in payload, and optional
// processing stages in processor. Business types and constructors are defined
// in those packages; this package does not forward their APIs.
//
// Topic management and subscription accept TsFile topics. Decoding a file
// payload returns ErrUnsupportedTsFile before delivery or acknowledgment.
// The module exposes no TsFile readers or TLS configuration and adds no native
// library dependency. It is versioned separately from the Session client.
package subscription
