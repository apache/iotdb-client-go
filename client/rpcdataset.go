package client

import (
	"context"
	"fmt"
	"github.com/apache/iotdb-client-go/common"
	"github.com/apache/iotdb-client-go/rpc"
	"strconv"
	"time"
)

const startIndex = int32(2)

type IoTDBRpcDataSet struct {
	sql                        string
	isClosed                   bool
	client                     *rpc.IClientRPCServiceClient
	columnNameList             []string
	columnTypeList             []string
	columnOrdinalMap           map[string]int32
	columnTypeDeduplicatedList []TSDataType
	fetchSize                  int32
	timeout                    *int64
	hasCachedRecord            bool
	lastReadWasNull            bool

	columnSize int32

	sessionId       int64
	queryId         int64
	statementId     int64
	time            int64
	ignoreTimestamp bool
	// indicates that there is still more data in server side and we can call fetchResult to get more
	moreData bool

	queryResult      [][]byte
	curTsBlock       *TsBlock
	queryResultSize  int32 // the length of queryResult
	queryResultIndex int32 // the index of bytebuffer in queryResult
	tsBlockSize      int32 // the size of current tsBlock
	tsBlockIndex     int32 // the row index in current tsBlock
}

func NewIoTDBRpcDataSet(sql string, columnNameList []string, columnTypeList []string, columnNameIndex map[string]int32, ignoreTimestamp bool, moreData bool, queryId int64, statementId int64, client *rpc.IClientRPCServiceClient, sessionId int64, queryResult [][]byte, fetchSize int32, timeout *int64) (rpcDataSet *IoTDBRpcDataSet, err error) {
	ds := &IoTDBRpcDataSet{
		sessionId:        sessionId,
		statementId:      statementId,
		ignoreTimestamp:  ignoreTimestamp,
		sql:              sql,
		queryId:          queryId,
		client:           client,
		fetchSize:        fetchSize,
		timeout:          timeout,
		moreData:         moreData,
		columnSize:       int32(len(columnNameList)),
		columnNameList:   make([]string, 0, len(columnNameList)+1),
		columnTypeList:   make([]string, 0, len(columnTypeList)+1),
		columnOrdinalMap: make(map[string]int32),
	}
	if !ignoreTimestamp {
		ds.columnNameList = append(ds.columnNameList, TimestampColumnName)
		ds.columnTypeList = append(ds.columnTypeList, "INT64")
		ds.columnOrdinalMap[TimestampColumnName] = 1
	}
	ds.columnNameList = append(ds.columnNameList, columnNameList...)
	ds.columnTypeList = append(ds.columnTypeList, columnTypeList...)

	if columnNameIndex != nil {
		deduplicatedColumnSize := getDeduplicatedColumnSize(columnNameIndex)
		ds.columnTypeDeduplicatedList = make([]TSDataType, deduplicatedColumnSize)
		for i, name := range columnNameList {
			if _, exists := ds.columnOrdinalMap[name]; exists {
				continue
			}

			index := columnNameIndex[name]
			targetIndex := index + startIndex

			valueExists := false
			for _, v := range ds.columnOrdinalMap {
				if v == targetIndex {
					valueExists = true
					break
				}
			}

			if !valueExists {
				if int(index) < len(ds.columnTypeDeduplicatedList) {
					if ds.columnTypeDeduplicatedList[index], err = GetDataTypeByStr(columnTypeList[i]); err != nil {
						return nil, err
					}
				}
			}
			ds.columnOrdinalMap[name] = targetIndex
		}
	} else {
		ds.columnTypeDeduplicatedList = make([]TSDataType, 0)
		index := startIndex
		for i := 0; i < len(columnNameList); i++ {
			name := columnNameList[i]
			if _, exists := ds.columnOrdinalMap[name]; !exists {
				dataType, err := GetDataTypeByStr(columnTypeList[i])
				if err != nil {
					return nil, err
				}
				ds.columnTypeDeduplicatedList = append(ds.columnTypeDeduplicatedList, dataType)
				ds.columnOrdinalMap[name] = int32(index)
				index++
			}
		}
	}
	ds.queryResult = queryResult
	if queryResult != nil {
		ds.queryResultSize = int32(len(queryResult))
	} else {
		ds.queryResultSize = 0
	}
	ds.queryResultIndex = 0
	ds.tsBlockSize = 0
	ds.tsBlockIndex = -1
	return ds, nil
}

func getDeduplicatedColumnSize(columnNameList map[string]int32) int {
	uniqueIndexes := make(map[int32]struct{})
	for _, idx := range columnNameList {
		uniqueIndexes[idx] = struct{}{}
	}
	return len(uniqueIndexes)
}

func (s *IoTDBRpcDataSet) Close() (err error) {
	if s.isClosed {
		return nil
	}
	closeRequest := &rpc.TSCloseOperationReq{
		SessionId:   s.sessionId,
		StatementId: &s.statementId,
		QueryId:     &s.queryId,
	}

	var status *common.TSStatus
	status, err = s.client.CloseOperation(context.Background(), closeRequest)
	if err == nil {
		err = VerifySuccess(status)
	}
	s.client = nil
	s.isClosed = true
	return err
}

func (s *IoTDBRpcDataSet) Next() (result bool, err error) {
	if s.hasCachedBlock() {
		s.lastReadWasNull = false
		err = s.constructOneRow()
		return true, err
	}
	if s.hasCachedByteBuffer() {
		if err = s.constructOneTsBlock(); err != nil {
			return false, err
		}
		err = s.constructOneRow()
		return true, err
	}

	if s.moreData {
		hasResultSet, err := s.fetchResults()
		if err != nil {
			return false, err
		}
		if hasResultSet && s.hasCachedByteBuffer() {
			if err = s.constructOneTsBlock(); err != nil {
				return false, err
			}
			err = s.constructOneRow()
			return true, err
		}
	}
	err = s.Close()
	if err != nil {
		return false, err
	}
	return false, nil
}

func (s *IoTDBRpcDataSet) fetchResults() (bool, error) {
	if s.isClosed {
		return false, fmt.Errorf("this data set is already closed")
	}
	req := rpc.TSFetchResultsReq{
		SessionId: s.sessionId,
		Statement: s.sql,
		FetchSize: s.fetchSize,
		QueryId:   s.queryId,
		IsAlign:   true,
	}
	req.Timeout = s.timeout

	resp, err := s.client.FetchResultsV2(context.Background(), &req)

	if err != nil {
		return false, err
	}

	if err = VerifySuccess(resp.Status); err != nil {
		return false, err
	}

	if !resp.HasResultSet {
		err = s.Close()
	} else {
		s.queryResult = resp.GetQueryResult_()
		s.queryResultIndex = 0
		if s.queryResult != nil {
			s.queryResultSize = int32(len(s.queryResult))
		} else {
			s.queryResultSize = 0
		}
		s.tsBlockSize = 0
		s.tsBlockIndex = -1
	}
	return resp.HasResultSet, err
}

func (s *IoTDBRpcDataSet) hasCachedBlock() bool {
	return s.curTsBlock != nil && s.tsBlockIndex < s.tsBlockSize-1
}

func (s *IoTDBRpcDataSet) hasCachedByteBuffer() bool {
	return s.queryResult != nil && s.queryResultIndex < s.queryResultSize
}

func (s *IoTDBRpcDataSet) constructOneRow() (err error) {
	s.tsBlockIndex++
	s.hasCachedRecord = true
	s.time, err = s.curTsBlock.GetTimeColumn().GetLong(s.tsBlockIndex)
	return err
}

func (s *IoTDBRpcDataSet) constructOneTsBlock() (err error) {
	s.lastReadWasNull = false
	curTsBlockBytes := s.queryResult[s.queryResultIndex]
	s.queryResultIndex = s.queryResultIndex + 1
	s.curTsBlock, err = DeserializeTsBlock(curTsBlockBytes)
	if err != nil {
		return err
	}
	s.tsBlockIndex = -1
	s.tsBlockSize = s.curTsBlock.GetPositionCount()
	return nil
}

func (s *IoTDBRpcDataSet) isNullByIndex(columnIndex int32) (bool, error) {
	columnName, err := s.findColumnNameByIndex(columnIndex)
	if err != nil {
		return false, err
	}
	index := s.columnOrdinalMap[columnName] - startIndex
	// time column will never be null
	if index < 0 {
		return false, nil
	}
	return s.isNull(index, s.tsBlockIndex), nil
}

func (s *IoTDBRpcDataSet) isNullByColumnName(columnName string) bool {
	index := s.columnOrdinalMap[columnName] - startIndex
	// time column will never be null
	if index < 0 {
		return false
	}
	return s.isNull(index, s.tsBlockIndex)
}

func (s *IoTDBRpcDataSet) isNull(index int32, rowNum int32) bool {
	return s.curTsBlock.GetColumn(index).IsNull(rowNum)
}

func (s *IoTDBRpcDataSet) getBooleanByIndex(columnIndex int32) (bool, error) {
	columnName, err := s.findColumnNameByIndex(columnIndex)
	if err != nil {
		return false, err
	}
	return s.getBoolean(columnName)
}

func (s *IoTDBRpcDataSet) getBoolean(columnName string) (bool, error) {
	err := s.checkRecord()
	if err != nil {
		return false, err
	}
	index := s.columnOrdinalMap[columnName] - startIndex
	if !s.isNull(index, s.tsBlockIndex) {
		s.lastReadWasNull = false
		return s.curTsBlock.GetColumn(index).GetBoolean(s.tsBlockIndex)
	} else {
		s.lastReadWasNull = true
		return false, nil
	}
}

func (s *IoTDBRpcDataSet) getDoubleByIndex(columnIndex int32) (float64, error) {
	columnName, err := s.findColumnNameByIndex(columnIndex)
	if err != nil {
		return 0, err
	}
	return s.getDouble(columnName)
}

func (s *IoTDBRpcDataSet) getDouble(columnName string) (float64, error) {
	err := s.checkRecord()
	if err != nil {
		return 0, err
	}
	index := s.columnOrdinalMap[columnName] - startIndex
	if !s.isNull(index, s.tsBlockIndex) {
		s.lastReadWasNull = false
		return s.curTsBlock.GetColumn(index).GetDouble(s.tsBlockIndex)
	} else {
		s.lastReadWasNull = true
		return 0, nil
	}
}

func (s *IoTDBRpcDataSet) getFloatByIndex(columnIndex int32) (float32, error) {
	columnName, err := s.findColumnNameByIndex(columnIndex)
	if err != nil {
		return 0, err
	}
	return s.getFloat(columnName)
}

func (s *IoTDBRpcDataSet) getFloat(columnName string) (float32, error) {
	err := s.checkRecord()
	if err != nil {
		return 0, err
	}
	index := s.columnOrdinalMap[columnName] - startIndex
	if !s.isNull(index, s.tsBlockIndex) {
		s.lastReadWasNull = false
		return s.curTsBlock.GetColumn(index).GetFloat(s.tsBlockIndex)
	} else {
		s.lastReadWasNull = true
		return 0, nil
	}
}

func (s *IoTDBRpcDataSet) getIntByIndex(columnIndex int32) (int32, error) {
	columnName, err := s.findColumnNameByIndex(columnIndex)
	if err != nil {
		return 0, err
	}
	return s.getInt(columnName)
}

func (s *IoTDBRpcDataSet) getInt(columnName string) (int32, error) {
	err := s.checkRecord()
	if err != nil {
		return 0, err
	}
	index := s.columnOrdinalMap[columnName] - startIndex
	if !s.isNull(index, s.tsBlockIndex) {
		s.lastReadWasNull = false
		dataType := s.curTsBlock.GetColumn(index).GetDataType()
		if dataType == INT64 {
			if v, err := s.curTsBlock.GetColumn(index).GetLong(s.tsBlockIndex); err != nil {
				return 0, err
			} else {
				return int32(v), nil
			}
		}
		return s.curTsBlock.GetColumn(index).GetInt(s.tsBlockIndex)
	} else {
		s.lastReadWasNull = true
		return 0, nil
	}
}

func (s *IoTDBRpcDataSet) getLongByIndex(columnIndex int32) (int64, error) {
	columnName, err := s.findColumnNameByIndex(columnIndex)
	if err != nil {
		return 0, err
	}
	return s.getLong(columnName)
}

func (s *IoTDBRpcDataSet) getLong(columnName string) (int64, error) {
	err := s.checkRecord()
	if err != nil {
		return 0, err
	}
	if columnName == TimestampColumnName {
		return s.curTsBlock.GetTimeByIndex(s.tsBlockIndex)
	}
	index := s.columnOrdinalMap[columnName] - startIndex
	if !s.isNull(index, s.tsBlockIndex) {
		s.lastReadWasNull = false
		return s.curTsBlock.GetColumn(index).GetLong(s.tsBlockIndex)
	} else {
		s.lastReadWasNull = true
		return 0, nil
	}
}

func (s *IoTDBRpcDataSet) getBinaryByIndex(columnIndex int32) (*Binary, error) {
	columnName, err := s.findColumnNameByIndex(columnIndex)
	if err != nil {
		return nil, err
	}
	return s.getBinary(columnName)
}

func (s *IoTDBRpcDataSet) getBinary(columnName string) (*Binary, error) {
	err := s.checkRecord()
	if err != nil {
		return nil, err
	}
	index := s.columnOrdinalMap[columnName] - startIndex
	if !s.isNull(index, s.tsBlockIndex) {
		s.lastReadWasNull = false
		return s.curTsBlock.GetColumn(index).GetBinary(s.tsBlockIndex)
	} else {
		s.lastReadWasNull = true
		return nil, nil
	}
}

func (s *IoTDBRpcDataSet) getStringByIndex(columnIndex int32) (string, error) {
	columnName, err := s.findColumnNameByIndex(columnIndex)
	if err != nil {
		return "", err
	}
	return s.getValueByName(columnName)
}

func (s *IoTDBRpcDataSet) GetTimestampByIndex(columnIndex int32) (time.Time, error) {
	columnName, err := s.findColumnNameByIndex(columnIndex)
	if err != nil {
		return time.Time{}, err
	}
	return s.GetTimestamp(columnName)
}

func (s *IoTDBRpcDataSet) GetTimestamp(columnName string) (time.Time, error) {
	err := s.checkRecord()
	if err != nil {
		return time.Time{}, err
	}
	index := s.columnOrdinalMap[columnName] - startIndex
	if !s.isNull(index, s.tsBlockIndex) {
		s.lastReadWasNull = false
		if longValue, err := s.curTsBlock.GetColumn(index).GetLong(s.tsBlockIndex); err != nil {
			return time.Time{}, err
		} else {
			return time.UnixMilli(longValue), nil
		}
	} else {
		s.lastReadWasNull = true
		return time.Time{}, nil
	}
}

func (s *IoTDBRpcDataSet) GetDateByIndex(columnIndex int32) (time.Time, error) {
	columnName, err := s.findColumnNameByIndex(columnIndex)
	if err != nil {
		return time.Time{}, err
	}
	return s.GetDate(columnName)
}

func (s *IoTDBRpcDataSet) GetDate(columnName string) (time.Time, error) {
	err := s.checkRecord()
	if err != nil {
		return time.Time{}, err
	}
	index := s.columnOrdinalMap[columnName] - startIndex
	if !s.isNull(index, s.tsBlockIndex) {
		s.lastReadWasNull = false
		if value, err := s.curTsBlock.GetColumn(index).GetInt(s.tsBlockIndex); err != nil {
			return time.Time{}, err
		} else {
			return Int32ToDate(value)
		}
	} else {
		s.lastReadWasNull = true
		return time.Time{}, nil
	}
}

func (s *IoTDBRpcDataSet) findColumn(columnName string) int32 {
	return s.columnOrdinalMap[columnName]
}

func (s *IoTDBRpcDataSet) getValueByName(columnName string) (string, error) {
	err := s.checkRecord()
	if err != nil {
		return "", err
	}
	if columnName == TimestampColumnName {
		if t, err := s.curTsBlock.GetTimeByIndex(s.tsBlockIndex); err != nil {
			return "", err
		} else {
			return int64ToString(t), nil
		}
	}
	index := s.columnOrdinalMap[columnName] - startIndex
	if index < 0 || index >= int32(len(s.columnTypeDeduplicatedList)) || s.isNull(index, s.tsBlockIndex) {
		s.lastReadWasNull = true
		return "", err
	}
	s.lastReadWasNull = false
	return s.getString(index, s.columnTypeDeduplicatedList[index])
}

func (s *IoTDBRpcDataSet) getString(index int32, tsDataType TSDataType) (string, error) {
	switch tsDataType {
	case BOOLEAN:
		if v, err := s.curTsBlock.GetColumn(index).GetBoolean(s.tsBlockIndex); err != nil {
			return "", nil
		} else {
			return strconv.FormatBool(v), nil
		}
	case INT32:
		if v, err := s.curTsBlock.GetColumn(index).GetInt(s.tsBlockIndex); err != nil {
			return "", err
		} else {
			return int32ToString(v), nil
		}
	case INT64, TIMESTAMP:
		if v, err := s.curTsBlock.GetColumn(index).GetLong(s.tsBlockIndex); err != nil {
			return "", err
		} else {
			return int64ToString(v), nil
		}
	case FLOAT:
		if v, err := s.curTsBlock.GetColumn(index).GetFloat(s.tsBlockIndex); err != nil {
			return "", err
		} else {
			return float32ToString(v), nil
		}
	case DOUBLE:
		if v, err := s.curTsBlock.GetColumn(index).GetDouble(s.tsBlockIndex); err != nil {
			return "", err
		} else {
			return float64ToString(v), nil
		}
	case TEXT, STRING:
		if v, err := s.curTsBlock.GetColumn(index).GetBinary(s.tsBlockIndex); err != nil {
			return "", err
		} else {
			return v.GetStringValue(), nil
		}
	case BLOB:
		if v, err := s.curTsBlock.GetColumn(index).GetBinary(s.tsBlockIndex); err != nil {
			return "", err
		} else {
			return bytesToHexString(v.values), nil
		}
	case DATE:
		v, err := s.curTsBlock.GetColumn(index).GetInt(s.tsBlockIndex)
		if err != nil {
			return "", err
		}
		t, err := Int32ToDate(v)
		if err != nil {
			return "", err
		}
		return t.Format("2006-01-02"), nil
	}
	return "", nil
}

func (s *IoTDBRpcDataSet) findColumnNameByIndex(columnIndex int32) (string, error) {
	if columnIndex <= 0 {
		return "", fmt.Errorf("column index should start from 1")
	}
	if columnIndex > int32(len(s.columnNameList)) {
		return "", fmt.Errorf("column index %d out of range %d", columnIndex, len(s.columnNameList))
	}
	return s.columnNameList[columnIndex-1], nil
}

func (s *IoTDBRpcDataSet) checkRecord() (err error) {
	if s.queryResultIndex > s.queryResultSize || s.tsBlockIndex >= s.tsBlockSize || s.queryResult == nil || s.curTsBlock == nil {
		err = fmt.Errorf("no record remains")
	}
	return err
}
