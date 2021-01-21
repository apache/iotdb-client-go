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

type TSDataType int8

type TSEncoding uint8

type TSCompressionType uint8

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

//TSStatusCode
const (
	SuccessStatus        int32 = 200
	StillExecutingStatus int32 = 201
	InvalidHandleStatus  int32 = 202
	IncompatibleVersion  int32 = 203

	NodeDeleteFailedError                  int32 = 298
	AliasAlreadyExistError                 int32 = 299
	PathAlreadyExistError                  int32 = 300
	PathNotExistError                      int32 = 301
	UnsupportedFetchMetadataOperationError int32 = 302
	MetadataError                          int32 = 303
	TimeseriesNotExist                     int32 = 304
	OutOfTTLError                          int32 = 305
	ConfigAdjuster                         int32 = 306
	MergeError                             int32 = 307
	SystemCheckError                       int32 = 308
	SyncDeviceOwnerConflictError           int32 = 309
	SyncConnectionException                int32 = 310
	StorageGroupProcessorError             int32 = 311
	StorageGroupError                      int32 = 312
	StorageEngineError                     int32 = 313
	TsfileProcessorError                   int32 = 314
	PathIllegal                            int32 = 315
	LoadFileError                          int32 = 316
	StorageGroupNotReady                   int32 = 317

	ExecuteStatementError    int32 = 400
	SQLParseError            int32 = 401
	GenerateTimeZoneError    int32 = 402
	SetTimeZoneError         int32 = 403
	NotStorageGroupError     int32 = 404
	QueryNotAllowed          int32 = 405
	AstFormatError           int32 = 406
	LogicalOperatorError     int32 = 407
	LogicalOptimizeError     int32 = 408
	UnsupportedFillTypeError int32 = 409
	PathErroRint32           int32 = 410
	QueryProcessError        int32 = 411
	WriteProcessError        int32 = 412
	WriteProcessReject       int32 = 413

	UnsupportedIndexFuncError int32 = 421
	UnsupportedIndexTypeError int32 = 422

	InternalServerError        int32 = 500
	CloseOperationError        int32 = 501
	ReadOnlySystemError        int32 = 502
	DiskSpaceInsufficientError int32 = 503
	StartUpError               int32 = 504
	ShutDownError              int32 = 505
	MultipleError              int32 = 506

	WrongLoginPasswordError int32 = 600
	NotLoginError           int32 = 601
	NoPermissionError       int32 = 602
	UninitializedAuthError  int32 = 603

	PartitionNotReady    int32 = 700
	TimeOut              int32 = 701
	NoLeader             int32 = 702
	UnsupportedOperation int32 = 703
	NodeReadOnly         int32 = 704
	ConsistencyFailure   int32 = 705
	NoConnection         int32 = 706
	NeedRedirection      int32 = 707
)
