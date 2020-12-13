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

type TSDataType int16

type TSEncoding int16

type TSCompressionType int16

const (
	UNKNOW  TSDataType = -1
	BOOLEAN TSDataType = 0
	INT32   TSDataType = 1
	INT64   TSDataType = 2
	FLOAT   TSDataType = 3
	DOUBLE  TSDataType = 4
	TEXT    TSDataType = 5
)

const (
	PLAIN            TSEncoding = 0
	PLAIN_DICTIONARY TSEncoding = 1
	RLE              TSEncoding = 2
	DIFF             TSEncoding = 3
	TS_2DIFF         TSEncoding = 4
	BITMAP           TSEncoding = 5
	GORILLA_V1       TSEncoding = 6
	REGULAR          TSEncoding = 7
	GORILLA          TSEncoding = 8
)

const (
	UNCOMPRESSED TSCompressionType = 0
	SNAPPY       TSCompressionType = 1
	GZIP         TSCompressionType = 2
	LZO          TSCompressionType = 3
	SDT          TSCompressionType = 4
	PAA          TSCompressionType = 5
	PLA          TSCompressionType = 6
	LZ4          TSCompressionType = 7
)
