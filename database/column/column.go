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
	"github.com/apache/iotdb-client-go/v2/client"
)

type Type string

type Interface interface {
	Name() string
	Type() Type
	Row(stat *client.SessionDataSet, ptr bool) any
}

func GenColumn(dataType string, name string) Interface {
	switch dataType {
	case "BOOLEAN":
		return &Bool{
			name: name,
		}
	case "INT32":
		return &Int32{
			name: name,
		}
	case "INT64":
		{
			return &Int64{
				name: name,
			}
		}
	case "FLOAT":
		return &Float{
			name: name,
		}
	case "DOUBLE":
		return &Double{
			name: name,
		}
	case "TEXT":
		{
			return &String{
				name: name,
			}
		}
	case "TIMESTAMP":
		return &Timestamp{
			name: name,
		}
	case "DATE":
		return &Date{
			name: name,
		}
	case "BLOB":
		return &Blob{
			name: name,
		}
	case "STRING":
		return &String{
			name: name,
		}
	}
	return nil
}
