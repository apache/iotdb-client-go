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

package column

import (
	"time"

	"github.com/apache/iotdb-client-go/v2/client"
)

type Timestamp struct {
	name string
}

func (t *Timestamp) Name() string {
	return t.name
}
func (t *Timestamp) Type() Type {
	return "TIMESTAMP"
}

func (t *Timestamp) Row(stat *client.SessionDataSet, ptr bool) any {
	if stat == nil {
		if ptr {
			return nil
		}
		return time.Time{}
	}
	value, err := stat.GetTimestamp(t.name)
	if err != nil {
		if ptr {
			return nil
		}
		return time.Time{}
	}
	if ptr {
		return &value
	}
	return value
}
