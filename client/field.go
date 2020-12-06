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

type Field struct {
	dataType int32
	name     string
	value    interface{}
}

func (f *Field) IsNull() bool {
	return f.value == nil
}

func (f *Field) GetDataType() int32 {
	return f.dataType
}

func (f *Field) GetValue() interface{} {
	return f.value
}

func (f *Field) GetInt32() int32 {
	return f.value.(int32)
}

func (f *Field) GetInt64() int64 {
	return f.value.(int64)
}

func (f *Field) GetFloat32() float32 {
	return f.value.(float32)
}

func (f *Field) GetFloat64() float64 {
	return f.value.(float64)
}

func (f *Field) GetText() string {
	return f.value.(string)
}

func (f *Field) GetName() string {
	return f.name
}
