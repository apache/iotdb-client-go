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
	"bytes"

	"github.com/apache/iotdb-client-go/v2/common"
)

type BatchError struct {
	statuses []*common.TSStatus
}

func (e *BatchError) Error() string {
	buff := bytes.Buffer{}
	for _, status := range e.statuses {
		buff.WriteString(*status.Message + ";")
	}
	return buff.String()
}

func (e *BatchError) GetStatuses() []*common.TSStatus {
	return e.statuses
}

func NewBatchError(statuses []*common.TSStatus) *BatchError {
	return &BatchError{
		statuses: statuses,
	}
}
