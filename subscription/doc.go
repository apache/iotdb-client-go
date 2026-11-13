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

// Package subscription defines the public contracts for IoTDB tree-model and
// table-model subscriptions. It is a separate Go module from the Session client.
//
// SubscriptionSession provides topic and subscription management through
// NewSubscriptionSession. Consumer configuration defaults and internal control
// protocol/provider support are available. Public consumer implementations,
// record decoding, and built-in processors remain subsequent implementation steps.
//
// Record messages are the supported payload format. Topic management and
// subscription do not reject TsFile topics; decoding a TsFile payload must return
// ErrUnsupportedTsFile before delivering or acknowledging it. This API exposes
// no TsFile handlers or TLS configuration and adds no native library dependency.
//
// Contexts passed to Open limit the opening operation. Background work belongs
// to the consumer and stops on Close, rather than when that context is canceled.
// Other operation contexts bound the corresponding operation only.
package subscription
