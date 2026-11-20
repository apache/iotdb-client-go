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

// Package consumer provides explicit Pull consumption and background Push
// delivery for tree-model and table-model record subscriptions.
//
// Open contexts limit opening only. Consumer background work belongs to the
// instance and stops on Close. A successfully closed consumer cannot reopen.
// Push Close cancels delivery without waiting for user Handler code to return;
// its borrowed message remains valid until the callback returns.
//
// File payloads fail with subscription.ErrUnsupportedTsFile. That failed poll
// round delivers no records and does not enter processing or acknowledgment.
package consumer
