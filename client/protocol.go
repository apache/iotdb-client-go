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

import "fmt"

type TSDataType int8

type TSEncoding uint8

type TSCompressionType uint8

const (
	UNKNOWN   TSDataType = -1
	BOOLEAN   TSDataType = 0
	INT32     TSDataType = 1
	INT64     TSDataType = 2
	FLOAT     TSDataType = 3
	DOUBLE    TSDataType = 4
	TEXT      TSDataType = 5
	TIMESTAMP TSDataType = 8
	DATE      TSDataType = 9
	BLOB      TSDataType = 10
	STRING    TSDataType = 11
	OBJECT    TSDataType = 12
)

var tsTypeMap = map[string]TSDataType{
	"BOOLEAN":   BOOLEAN,
	"INT32":     INT32,
	"INT64":     INT64,
	"FLOAT":     FLOAT,
	"DOUBLE":    DOUBLE,
	"TEXT":      TEXT,
	"TIMESTAMP": TIMESTAMP,
	"DATE":      DATE,
	"BLOB":      BLOB,
	"STRING":    STRING,
	"OBJECT":    OBJECT,
}

var byteToTsDataType = map[byte]TSDataType{
	0:  BOOLEAN,
	1:  INT32,
	2:  INT64,
	3:  FLOAT,
	4:  DOUBLE,
	5:  TEXT,
	8:  TIMESTAMP,
	9:  DATE,
	10: BLOB,
	11: STRING,
	12: OBJECT,
}

func GetDataTypeByStr(name string) (TSDataType, error) {
	dataType, exists := tsTypeMap[name]
	if !exists {
		return UNKNOWN, fmt.Errorf("invalid input: %v", name)
	}
	return dataType, nil
}

func getDataTypeByByte(b byte) (TSDataType, error) {
	dataType, exists := byteToTsDataType[b]
	if !exists {
		return UNKNOWN, fmt.Errorf("invalid input: %v", b)
	}
	return dataType, nil
}

const (
	PLAIN      TSEncoding = 0
	DICTIONARY TSEncoding = 1
	RLE        TSEncoding = 2
	DIFF       TSEncoding = 3
	TS_2DIFF   TSEncoding = 4
	BITMAP     TSEncoding = 5
	GORILLA_V1 TSEncoding = 6
	REGULAR    TSEncoding = 7
	GORILLA    TSEncoding = 8
	ZIGZAG     TSEncoding = 9
	FREQ       TSEncoding = 10
	CHIMP      TSEncoding = 11
	SPRINTZ    TSEncoding = 12
	RLBE       TSEncoding = 13
)

const (
	UNCOMPRESSED TSCompressionType = 0
	SNAPPY       TSCompressionType = 1
	GZIP         TSCompressionType = 2
	LZ4          TSCompressionType = 7
	ZSTD         TSCompressionType = 8
	LZMA2        TSCompressionType = 9
)

// TSStatusCode
const (
	SuccessStatus int32 = 200

	// System level
	IncompatibleVersion int32 = 201
	ConfigurationError  int32 = 202
	StartUpError        int32 = 203
	ShutDownError       int32 = 204

	UnsupportedSqlDialect int32 = 205

	PortOccupied int32 = 206

	// General Error
	UnsupportedOperation     int32 = 300
	ExecuteStatementError    int32 = 301
	MultipleError            int32 = 302
	IllegalParameter         int32 = 303
	OverlapWithExistingTask  int32 = 304
	InternalServerError      int32 = 305
	DispatchError            int32 = 306
	LicenseError             int32 = 307
	ThriftFrameOversize      int32 = 308
	RepeatedWithExistingTask int32 = 309

	// Client
	RedirectionRecommend int32 = 400

	UseOrDropDb int32 = 401

	// Schema Engine
	DatabaseNotExist                    int32 = 500
	DatabaseAlreadyExists               int32 = 501
	SeriesOverflow                      int32 = 502
	TimeseriesAlreadyExist              int32 = 503
	TimeseriesInBlackList               int32 = 504
	AliasAlreadyExist                   int32 = 505
	PathAlreadyExist                    int32 = 506
	MetadataError                       int32 = 507
	PathNotExist                        int32 = 508
	IllegalPath                         int32 = 509
	CreateTemplateError                 int32 = 510
	DuplicatedTemplate                  int32 = 511
	UndefinedTemplate                   int32 = 512
	TemplateNotSet                      int32 = 513
	DifferentTemplate                   int32 = 514
	TemplateIsInUse                     int32 = 515
	TemplateIncompatible                int32 = 516
	SegmentNotFound                     int32 = 517
	PageOutOfSpace                      int32 = 518
	RecordDuplicated                    int32 = 519
	SegmentOutOfSpace                   int32 = 520
	SchemaFileNotExists                 int32 = 521
	OversizeRecord                      int32 = 522
	SchemaFileRedoLogBroken             int32 = 523
	TemplateNotActivated                int32 = 524
	DatabaseConfigError                 int32 = 525
	SchemaQuotaExceeded                 int32 = 526
	MeasurementAlreadyExistsInTemplate  int32 = 527
	TypeNotFound                        int32 = 528
	DatabaseConflict                    int32 = 529
	DatabaseModel                       int32 = 530
	TemplateTimeseriesCannotCreateAlias int32 = 531
	ViewTimeseriesCannotCreateAlias     int32 = 532
	InvalidTimeseriesCannotCreateAlias  int32 = 533
	TimeseriesAlreadyRenaming           int32 = 534
	MetadataLeaseFenced                 int32 = 535
	MetadataLeaseFencedRetryRequired    int32 = 536

	TableNotExists      int32 = 550
	TableAlreadyExists  int32 = 551
	ColumnAlreadyExists int32 = 552
	TableIsLost         int32 = 553
	TableIncompatible   int32 = 554
	TableInPreDelete    int32 = 555
	ColumnInPreDelete   int32 = 556
	ColumnInPreAlter    int32 = 557
	OnlyLogicalView     int32 = 560

	// Storage Engine
	SystemReadOnly          int32 = 600
	StorageEngineError      int32 = 601
	StorageEngineNotReady   int32 = 602
	DataregionProcessError  int32 = 603
	TsfileProcessorError    int32 = 604
	WriteProcessError       int32 = 605
	WriteProcessReject      int32 = 606
	OutOfTtl                int32 = 607
	CompactionError         int32 = 608
	AlignedTimeseriesError  int32 = 609
	WalError                int32 = 610
	DiskSpaceInsufficient   int32 = 611
	OversizeTtl             int32 = 612
	TtlConfigError          int32 = 613
	DataTypeMismatch        int32 = 614
	ColumnCategoryMismatch  int32 = 615
	ColumnNotExists         int32 = 616
	MeasurementNameConflict int32 = 617

	WalEntryTooLarge int32 = 620

	// Query Engine
	SqlParseError                 int32 = 700
	SemanticError                 int32 = 701
	GenerateTimeZoneError         int32 = 702
	SetTimeZoneError              int32 = 703
	QueryNotAllowed               int32 = 704
	LogicalOperatorError          int32 = 705
	LogicalOptimizeError          int32 = 706
	UnsupportedFillType           int32 = 707
	QueryProcessError             int32 = 708
	MppMemoryNotEnough            int32 = 709
	CloseOperationError           int32 = 710
	TsblockSerializeError         int32 = 711
	InternalRequestTimeOut        int32 = 712
	InternalRequestRetryError     int32 = 713
	NoSuchQuery                   int32 = 714
	QueryWasKilled                int32 = 715
	ExplainAnalyzeFetchError      int32 = 716
	TooManyConcurrentQueriesError int32 = 717

	OperatorNotFound int32 = 718

	QueryExecutionMemoryNotEnough int32 = 719
	QueryTimeout                  int32 = 720
	PlanFailedNetworkPartition    int32 = 721
	CannotFetchFiState            int32 = 722
	RepeatedRpcCall               int32 = 723
	CannotReadTsfile              int32 = 724
	CopyToWriteError              int32 = 725
	DeviceEntrySpillNotFound      int32 = 726

	// OBJECT
	ObjectNotExists   int32 = 740
	ObjectInsertError int32 = 741
	ObjectReadError   int32 = 742

	// Arithmetic
	NumericValueOutOfRange int32 = 750
	DivisionByZero         int32 = 751
	DateOutOfRange         int32 = 752

	// Authentication
	InitAuthError              int32 = 800
	WrongLoginPassword         int32 = 801
	NotLogin                   int32 = 802
	NoPermission               int32 = 803
	UserNotExist               int32 = 804
	UserAlreadyExist           int32 = 805
	UserAlreadyHasRole         int32 = 806
	UserNotHasRole             int32 = 807
	RoleNotExist               int32 = 808
	RoleAlreadyExist           int32 = 809
	NotHasPrivilege            int32 = 811
	ClearPermissionCacheError  int32 = 812
	UnknownAuthPrivilege       int32 = 813
	UnsupportedAuthOperation   int32 = 814
	AuthIoException            int32 = 815
	IllegalPrivilege           int32 = 816
	NotHasPrivilegeGrantopt    int32 = 817
	AuthOperateException       int32 = 818
	OptimizerTimeout           int32 = 819
	IllegalPassword            int32 = 820
	SessionNumsExceeded        int32 = 821
	UserLoginLocked            int32 = 822
	TotpCodeRequired           int32 = 823
	TotpCodeInvalid            int32 = 824
	TotpAuthServiceUnavailable int32 = 825

	// Partition Error
	MigrateRegionError        int32 = 900
	CreateRegionError         int32 = 901
	DeleteRegionError         int32 = 902
	PartitionCacheUpdateError int32 = 903
	ConsensusNotInitialized   int32 = 904
	RegionLeaderChangeError   int32 = 905
	NoAvailableRegionGroup    int32 = 906
	LackPartitionAllocation   int32 = 907
	ReconstructRegionError    int32 = 908
	ExtendRegionError         int32 = 909
	RemoveRegionPeerError     int32 = 910
	RegionAlreadyExists       int32 = 911
	RegionNotExist            int32 = 912

	// Cluster Manager
	AddConfignodeError        int32 = 1000
	RemoveConfignodeError     int32 = 1001
	RejectNodeStart           int32 = 1002
	NoEnoughDatanode          int32 = 1003
	DatanodeNotExist          int32 = 1004
	DatanodeStopError         int32 = 1005
	RemoveDatanodeError       int32 = 1006
	CanNotConnectDatanode     int32 = 1007
	TransferLeaderError       int32 = 1008
	GetClusterIdError         int32 = 1009
	CanNotConnectConfignode   int32 = 1010
	CanNotConnectAinode       int32 = 1011
	NoAvailableReplica        int32 = 1012
	NoAvailableAinode         int32 = 1013
	ConfigNodeLeaderWarmingUp int32 = 1014

	// Sync, Load TsFile
	LoadFileError                     int32 = 1100
	LoadPieceOfTsfileError            int32 = 1101
	DeserializePieceOfTsfileError     int32 = 1102
	SyncConnectionError               int32 = 1103
	SyncFileRedirectionError          int32 = 1104
	SyncFileError                     int32 = 1105
	CreatePipeSinkError               int32 = 1106
	PipeError                         int32 = 1107
	PipeserverError                   int32 = 1108
	VerifyMetadataError               int32 = 1109
	LoadTemporaryUnavailableException int32 = 1110
	LoadIdempotentConflictException   int32 = 1111
	LoadUserConflictException         int32 = 1112

	// UDF
	UdfLoadClassError        int32 = 1200
	UdfDownloadError         int32 = 1201
	CreateUdfOnDatanodeError int32 = 1202
	DropUdfOnDatanodeError   int32 = 1203
	CreateUdfError           int32 = 1204
	DropUdfError             int32 = 1205
	ExecuteUdfError          int32 = 1206
	UdfAlreadyExists         int32 = 1207

	// Trigger
	CreateTriggerError         int32 = 1300
	DropTriggerError           int32 = 1301
	TriggerFireError           int32 = 1302
	TriggerLoadClassError      int32 = 1303
	TriggerDownloadError       int32 = 1304
	CreateTriggerInstanceError int32 = 1305
	ActiveTriggerInstanceError int32 = 1306
	DropTriggerInstanceError   int32 = 1307
	UpdateTriggerLocationError int32 = 1308

	// Continuous Query
	NoSuchCq                  int32 = 1400
	CqAlreadyActive           int32 = 1401
	CqAlreadyExist            int32 = 1402
	CqUpdateLastExecTimeError int32 = 1403
	NoPermissionError         int32 = 1404

	// AI
	NoRegisteredAiNodeError  int32 = 1500
	RegisterAiNodeError      int32 = 1501
	RemoveAiNodeError        int32 = 1502
	ModelExistedError        int32 = 1503
	ModelNotExistError       int32 = 1504
	CreateModelError         int32 = 1505
	DropBuiltinModelError    int32 = 1506
	DropModelError           int32 = 1507
	UnavailableAiDeviceError int32 = 1508

	AinodeInternalError int32 = 1599 // In case somebody too lazy to add a new error code

	// Pipe Plugin
	CreatePipePluginError           int32 = 1600
	DropPipePluginError             int32 = 1601
	PipePluginLoadClassError        int32 = 1602
	PipePluginDownloadError         int32 = 1603
	CreatePipePluginOnDatanodeError int32 = 1604
	DropPipePluginOnDatanodeError   int32 = 1605

	// Quota
	SpaceQuotaExceeded          int32 = 1700
	NumRequestsExceeded         int32 = 1701
	RequestSizeExceeded         int32 = 1702
	NumReadRequestsExceeded     int32 = 1703
	NumWriteRequestsExceeded    int32 = 1704
	WriteSizeExceeded           int32 = 1705
	ReadSizeExceeded            int32 = 1706
	QuotaMemQueryNotEnough      int32 = 1707
	QueryCpuQueryNotEnough      int32 = 1708
	QuotaTempDiskQueryNotEnough int32 = 1709

	// Pipe
	PipeVersionError                            int32 = 1800
	PipeTypeError                               int32 = 1801
	PipeHandshakeError                          int32 = 1802
	PipeTransferFileOffsetReset                 int32 = 1803
	PipeTransferFileError                       int32 = 1804
	PipeTransferExecuteStatementError           int32 = 1805
	PipeNotExistError                           int32 = 1806
	PipePushMetaError                           int32 = 1807
	PipeReceiverTemporaryUnavailableException   int32 = 1808
	PipeReceiverIdempotentConflictException     int32 = 1809
	PipeReceiverUserConflictException           int32 = 1810
	PipeConfigReceiverHandshakeNeeded           int32 = 1811
	PipeTransferSliceOutOfOrder                 int32 = 1812
	PipePushMetaTimeout                         int32 = 1813
	PipePushMetaNotEnoughMemory                 int32 = 1814
	PipeReceiverParallelOrUserConflictException int32 = 1815

	// Subscription
	SubscriptionVersionError       int32 = 1900
	SubscriptionTypeError          int32 = 1901
	SubscriptionHandshakeError     int32 = 1902
	SubscriptionHeartbeatError     int32 = 1903
	SubscriptionPollError          int32 = 1904
	SubscriptionCommitError        int32 = 1905
	SubscriptionCloseError         int32 = 1906
	SubscriptionSubscribeError     int32 = 1907
	SubscriptionUnsubscribeError   int32 = 1908
	SubscriptionMissingConsumer    int32 = 1909
	ShowSubscriptionError          int32 = 1910
	SubscriptionPipeTimeoutError   int32 = 1911
	SubscriptionNotEnabledError    int32 = 1912
	SubscriptionSeekError          int32 = 1913
	SubscriptionOwnerFenced        int32 = 1914
	SubscriptionOwnerRequired      int32 = 1915
	SubscriptionOwnerEpochRequired int32 = 1916
	SubscriptionOwnerLeaseExpired  int32 = 1917
	SubscriptionOwnerEpochConflict int32 = 1918
	SubscriptionConsumerFenced     int32 = 1919

	// Topic
	CreateTopicError   int32 = 2000
	DropTopicError     int32 = 2001
	AlterTopicError    int32 = 2002
	ShowTopicError     int32 = 2003
	TopicPushMetaError int32 = 2004
	TopicNotExistError int32 = 2005

	// Consumer
	CreateConsumerError   int32 = 2100
	DropConsumerError     int32 = 2101
	AlterConsumerError    int32 = 2102
	ConsumerPushMetaError int32 = 2103

	// Consensus Exception
	IotConsensusV2ConnectorRestartError   int32 = 2200
	IotConsensusV2VersionError            int32 = 2201
	IotConsensusV2DeprecatedRequest       int32 = 2202
	IotConsensusV2TransferFileOffsetReset int32 = 2203
	IotConsensusV2TransferFileError       int32 = 2204
	IotConsensusV2TypeError               int32 = 2205
	ConsensusGroupNotExist                int32 = 2206
	RatisReadUnavailable                  int32 = 2207
	IotConsensusV2CloseError              int32 = 2208
	IotConsensusV2WaitOrderTimeout        int32 = 2209

	// ExternalService
	NoSuchExternalService              int32 = 2300
	ExternalServiceAlreadyExist        int32 = 2301
	GetBuiltinExternalServiceError     int32 = 2302
	ExternalServiceInstanceCreateError int32 = 2303
	CannotDropBuiltinExternalService   int32 = 2304
	CannotDropRunningExternalService   int32 = 2305

	// LBAC
	LbacAccessDenied       int32 = 2400
	ComponentNotExists     int32 = 2401
	ComponentAlreadyExists int32 = 2402
	PolicyNotExists        int32 = 2403
	PolicyAlreadyExists    int32 = 2404
	LabelNotExists         int32 = 2405
	LabelAlreadyExists     int32 = 2406
	ElementNotExists       int32 = 2407
	ElementAlreadyExists   int32 = 2408
	InvalidComponentType   int32 = 2409
	InvalidElementClause   int32 = 2410
	InvalidCommentTarget   int32 = 2411
	ComponentIsReferenced  int32 = 2412
	PolicyIsReferenced     int32 = 2413
	LabelIsReferenced      int32 = 2414
	PolicyNotSet           int32 = 2415
	PolicyAlreadySet       int32 = 2416
	PolicyConflict         int32 = 2417
	LabelNotSet            int32 = 2418
	InvalidLabelValue      int32 = 2419

	// STREAM
	NoRegisteredStreamNodeError int32 = 9400
	RegisterStreamNodeError     int32 = 9401
	RemoveStreamNodeError       int32 = 9402
	StreamNodeNotExist          int32 = 9403
	StreamNodeStopError         int32 = 9404
	StreamTaskExistOnStreamnode int32 = 9405

	// Stream
	StreamAlreadyExists int32 = -3000
	StreamNotExist      int32 = -3001
	StreamStale         int32 = -3002
	StreamStartError    int32 = -3003
)

const (
	TimestampColumnName = "Time"
)
