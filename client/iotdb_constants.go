/**
 * Licensed to the Apache Software Foundation  =ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0  =the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package client

const (
	BOOLEAN int32 = 0
	INT32   int32 = 1
	INT64   int32 = 2
	FLOAT   int32 = 3
	DOUBLE  int32 = 4
	TEXT    int32 = 5
	INT     int32 = 6
)

const  (
	PLAIN            int32 = 0
	PLAIN_DICTIONARY int32 = 1
	RLE              int32 = 2
	DIFF             int32 = 3
	TS_2DIFF         int32 = 4
	BITMAP           int32 = 5
	GORILLA_V1       int32 = 6
	REGULAR          int32 = 7
	GORILLA          int32 = 8
)

const  (
	UNCOMPRESSED int32 = 0
	SNAPPY       int32 = 1
	GZIP         int32 = 2
	LZO          int32 = 3
	SDT          int32 = 4
	PAA          int32 = 5
	PLA          int32 = 6
	LZ4          int32 = 7
)

const (
	SUCCESS_STATUS = 200
	STILL_EXECUTING_STATUS = 201
	INVALID_HANDLE_STATUS =202
	INCOMPATIBLE_VERSION =203
	NODE_DELETE_FAILED_ERROR =298
	ALIAS_ALREADY_EXIST_ERROR =299
	PATH_ALREADY_EXIST_ERROR =300
	PATH_NOT_EXIST_ERROR =301
	UNSUPPORTED_FETCH_METADATA_OPERATION_ERROR =302
	METADATA_ERROR =303
	TIMESERIES_NOT_EXIST =304
	OUT_OF_TTL_ERROR =305
	CONFIG_ADJUSTER =306
	MERGE_ERROR =307
	SYSTEM_CHECK_ERROR =308
	SYNC_DEVICE_OWNER_CONFLICT_ERROR =309
	SYNC_CONNECTION_EXCEPTION =310
	STORAGE_GROUP_PROCESSOR_ERROR =311
	STORAGE_GROUP_ERROR =312
	STORAGE_ENGINE_ERROR =313
	TSFILE_PROCESSOR_ERROR =314
	PATH_ILLEGAL =315
	LOAD_FILE_ERROR =316
	STORAGE_GROUP_NOT_READY =317

	EXECUTE_STATEMENT_ERROR =400
	SQL_PARSE_ERROR =401
	GENERATE_TIME_ZONE_ERROR =402
	SET_TIME_ZONE_ERROR =403
	NOT_STORAGE_GROUP_ERROR =404
	QUERY_NOT_ALLOWED =405
	AST_FORMAT_ERROR =406
	LOGICAL_OPERATOR_ERROR =407
	LOGICAL_OPTIMIZE_ERROR =408
	UNSUPPORTED_FILL_TYPE_ERROR =409
	PATH_ERROR =410
	QUERY_PROCESS_ERROR =411
	WRITE_PROCESS_ERROR =412

	UNSUPPORTED_INDEX_FUNC_ERROR =421
	UNSUPPORTED_INDEX_TYPE_ERROR =422


	INTERNAL_SERVER_ERROR =500
	CLOSE_OPERATION_ERROR =501
	READ_ONLY_SYSTEM_ERROR =502
	DISK_SPACE_INSUFFICIENT_ERROR =503
	START_UP_ERROR =504
	SHUT_DOWN_ERROR =505
	MULTIPLE_ERROR =506

	WRONG_LOGIN_PASSWORD_ERROR =600
	NOT_LOGIN_ERROR =601
	NO_PERMISSION_ERROR =602
	UNINITIALIZED_AUTH_ERROR =603

	// TODO-Cluster: update docs when ready to merge
	PARTITION_NOT_READY =700
	TIME_OUT =701
	NO_LEADER =702
	UNSUPPORTED_OPERATION =703
	NODE_READ_ONLY =704
	CONSISTENCY_FAILURE =705
)
