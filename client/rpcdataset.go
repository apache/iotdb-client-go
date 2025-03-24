package client

import (
	"context"
	"fmt"
	"github.com/apache/iotdb-client-go/common"
	"github.com/apache/iotdb-client-go/rpc"
	"strconv"
	"time"
)

const startIndex = 2

type IoTDBRpcDataSet struct {
	Sql                        string
	IsClosed                   bool
	Client                     *rpc.IClientRPCServiceClient
	ColumnNameList             []string
	ColumnTypeList             []string
	ColumnOrdinalMap           map[string]int32
	ColumnTypeDeduplicatedList []TSDataType
	FetchSize                  int32
	Timeout                    int64
	HasCachedRecord            bool
	LastReadWasNull            bool

	ColumnSize int32

	SessionId       int64
	QueryId         int64
	StatementId     int64
	Time            int64
	IgnoreTimestamp bool
	// indicates that there is still more data in server side and we can call fetchResult to get more
	MoreData bool

	QueryResult      [][]byte
	CurTsBlock       *TsBlock
	QueryResultSize  int32 // the length of queryResult
	QueryResultIndex int32 // the index of bytebuffer in queryResult
	TsBlockSize      int32 // the size of current tsBlock
	TsBlockIndex     int32 // the row index in current tsBlock
}

func NewIoTDBRpcDataSet(sql string, columnNameList []string, columnTypeList []string, columnNameIndex map[string]int32, ignoreTimestamp bool, moreData bool, queryId int64, statementId int64, client *rpc.IClientRPCServiceClient, sessionId int64, queryResult [][]byte, fetchSize int32, timeout int64) (rpcDataSet *IoTDBRpcDataSet, err error) {
	ds := &IoTDBRpcDataSet{
		SessionId:        sessionId,
		StatementId:      statementId,
		IgnoreTimestamp:  ignoreTimestamp,
		Sql:              sql,
		QueryId:          queryId,
		Client:           client,
		FetchSize:        fetchSize,
		Timeout:          timeout,
		MoreData:         moreData,
		ColumnSize:       int32(len(columnNameList)),
		ColumnNameList:   columnNameList,
		ColumnTypeList:   columnTypeList,
		ColumnOrdinalMap: make(map[string]int32),
	}
	if !ignoreTimestamp {
		ds.ColumnNameList = append(ds.ColumnNameList, TimestampColumnName)
		ds.ColumnTypeList = append(ds.ColumnTypeList, "INT64")
		ds.ColumnOrdinalMap[TimestampColumnName] = 1
	}
	ds.ColumnNameList = append(ds.ColumnNameList, columnNameList...)
	ds.ColumnTypeList = append(ds.ColumnTypeList, columnTypeList...)

	if columnNameIndex != nil {
		deduplicatedColumnSize := getDeduplicatedColumnSize(columnNameIndex)
		ds.ColumnTypeDeduplicatedList = make([]TSDataType, deduplicatedColumnSize)
		for i, name := range columnNameList {
			if _, exists := ds.ColumnOrdinalMap[name]; exists {
				continue
			}

			index := columnNameIndex[name]
			targetIndex := index + startIndex

			valueExists := false
			for _, v := range ds.ColumnOrdinalMap {
				if v == targetIndex {
					valueExists = true
					break
				}
			}

			if !valueExists {
				if int(index) < len(ds.ColumnTypeDeduplicatedList) {
					if ds.ColumnTypeDeduplicatedList[index], err = getDataTypeByStr(columnTypeList[i]); err != nil {
						return nil, err
					}
				}
			}
			ds.ColumnOrdinalMap[name] = targetIndex
		}
	} else {
		ds.ColumnTypeDeduplicatedList = make([]TSDataType, 0)
		index := startIndex
		for i := 0; i < len(columnNameList); i++ {
			name := columnNameList[i]
			if _, exists := ds.ColumnOrdinalMap[name]; !exists {
				dataType, err := getDataTypeByStr(columnTypeList[i])
				if err != nil {
					return nil, err
				}
				ds.ColumnTypeDeduplicatedList = append(ds.ColumnTypeDeduplicatedList, dataType)
				ds.ColumnOrdinalMap[name] = int32(index)
				index++
			}
		}
	}
	ds.QueryResult = queryResult
	if queryResult != nil {
		ds.QueryResultSize = int32(len(queryResult))
	} else {
		ds.QueryResultSize = 0
	}
	ds.QueryResultIndex = 0
	ds.TsBlockSize = 0
	ds.TsBlockIndex = -1
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
	if s.IsClosed {
		return nil
	}
	closeRequest := &rpc.TSCloseOperationReq{
		SessionId:   s.SessionId,
		StatementId: &s.StatementId,
		QueryId:     &s.QueryId,
	}

	var status *common.TSStatus
	status, err = s.Client.CloseOperation(context.Background(), closeRequest)
	if err == nil {
		err = VerifySuccess(status)
	}
	s.Client = nil
	s.IsClosed = true
	return err
}

func (s *IoTDBRpcDataSet) Next() (result bool, err error) {
	if s.hasCachedBlock() {
		s.LastReadWasNull = false
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

	if s.MoreData {
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
	if s.IsClosed {
		return false, fmt.Errorf("this data set is already closed")
	}
	req := rpc.TSFetchResultsReq{
		SessionId: s.SessionId,
		Statement: s.Sql,
		FetchSize: s.FetchSize,
		QueryId:   s.QueryId,
		IsAlign:   true,
	}
	req.Timeout = &s.Timeout

	resp, err := s.Client.FetchResults(context.Background(), &req)

	if err != nil {
		return false, err
	}

	if err = VerifySuccess(resp.Status); err != nil {
		return false, err
	}

	if !resp.HasResultSet {
		err = s.Close()
	} else {
		s.QueryResult = resp.GetQueryResult_()
		s.QueryResultIndex = 0
		if s.QueryResult != nil {
			s.QueryResultSize = int32(len(s.QueryResult))
		} else {
			s.QueryResultSize = 0
		}
		s.TsBlockSize = 0
		s.TsBlockIndex = -1
	}
	return resp.HasResultSet, err
}

func (s *IoTDBRpcDataSet) hasCachedBlock() bool {
	return s.CurTsBlock != nil && s.TsBlockIndex < s.TsBlockSize-1
}

func (s *IoTDBRpcDataSet) hasCachedByteBuffer() bool {
	return s.QueryResult != nil && s.QueryResultIndex < s.QueryResultSize
}

func (s *IoTDBRpcDataSet) constructOneRow() (err error) {
	s.TsBlockIndex++
	s.HasCachedRecord = true
	s.Time, err = s.CurTsBlock.GetTimeColumn().GetLong(s.TsBlockIndex)
	return err
}

func (s *IoTDBRpcDataSet) constructOneTsBlock() (err error) {
	s.LastReadWasNull = false
	curTsBlockBytes := s.QueryResult[s.QueryResultIndex]
	s.QueryResultIndex = s.QueryResultIndex + 1
	s.CurTsBlock, err = DeserializeTsBlock(curTsBlockBytes)
	s.TsBlockIndex = -1
	s.TsBlockSize = s.CurTsBlock.GetPositionCount()
	return err
}

func (s *IoTDBRpcDataSet) isNullByIndex(columnIndex int32) (bool, error) {
	columnName, err := s.findColumnNameByIndex(columnIndex)
	if err != nil {
		return false, err
	}
	index := s.ColumnOrdinalMap[columnName] - startIndex
	// time column will never be null
	if index < 0 {
		return true, nil
	}
	return s.isNull(index, s.TsBlockIndex), nil
}

func (s *IoTDBRpcDataSet) isNullByColumnName(columnName string) bool {
	index := s.ColumnOrdinalMap[columnName] - startIndex
	// time column will never be null
	if index < 0 {
		return true
	}
	return s.isNull(index, s.TsBlockIndex)
}

func (s *IoTDBRpcDataSet) isNull(index int32, rowNum int32) bool {
	return s.CurTsBlock.GetColumn(index).IsNull(rowNum)
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
	index := s.ColumnOrdinalMap[columnName] - startIndex
	if !s.isNull(index, s.TsBlockIndex) {
		s.LastReadWasNull = false
		return s.CurTsBlock.GetColumn(index).GetBoolean(s.TsBlockIndex)
	} else {
		s.LastReadWasNull = true
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
	index := s.ColumnOrdinalMap[columnName] - startIndex
	if !s.isNull(index, s.TsBlockIndex) {
		s.LastReadWasNull = false
		return s.CurTsBlock.GetColumn(index).GetDouble(s.TsBlockIndex)
	} else {
		s.LastReadWasNull = true
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
	index := s.ColumnOrdinalMap[columnName] - startIndex
	if !s.isNull(index, s.TsBlockIndex) {
		s.LastReadWasNull = false
		return s.CurTsBlock.GetColumn(index).GetFloat(s.TsBlockIndex)
	} else {
		s.LastReadWasNull = true
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
	index := s.ColumnOrdinalMap[columnName] - startIndex
	if !s.isNull(index, s.TsBlockIndex) {
		s.LastReadWasNull = false
		return s.CurTsBlock.GetColumn(index).GetInt(s.TsBlockIndex)
	} else {
		s.LastReadWasNull = true
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
		return s.CurTsBlock.GetTimeByIndex(s.TsBlockIndex)
	}
	index := s.ColumnOrdinalMap[columnName] - startIndex
	if !s.isNull(index, s.TsBlockIndex) {
		s.LastReadWasNull = false
		return s.CurTsBlock.GetColumn(index).GetLong(s.TsBlockIndex)
	} else {
		s.LastReadWasNull = true
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
	index := s.ColumnOrdinalMap[columnName] - startIndex
	if !s.isNull(index, s.TsBlockIndex) {
		s.LastReadWasNull = false
		return s.CurTsBlock.GetColumn(index).GetBinary(s.TsBlockIndex)
	} else {
		s.LastReadWasNull = true
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
	longValue, err := s.getLongByIndex(columnIndex)
	if err != nil {
		return time.Time{}, err
	}
	return time.UnixMilli(longValue), nil
}

func (s *IoTDBRpcDataSet) GetTimestamp(columnName string) (time.Time, error) {
	return s.GetTimestampByIndex(s.findColumn(columnName))
}

func (s *IoTDBRpcDataSet) findColumn(columnName string) int32 {
	return s.ColumnOrdinalMap[columnName]
}

func (s *IoTDBRpcDataSet) getValueByName(columnName string) (string, error) {
	err := s.checkRecord()
	if err != nil {
		return "", err
	}
	if columnName == TimestampColumnName {
		if t, err := s.CurTsBlock.GetTimeByIndex(s.TsBlockIndex); err != nil {
			return "", err
		} else {
			return int64ToString(t), nil
		}
	}
	index := s.ColumnOrdinalMap[columnName] - startIndex
	if index < 0 || index >= int32(len(s.ColumnTypeDeduplicatedList)) || s.isNull(index, s.TsBlockIndex) {
		s.LastReadWasNull = true
		return "", err
	}
	s.LastReadWasNull = false
	return s.getString(index, s.ColumnTypeDeduplicatedList[index])
}

func (s *IoTDBRpcDataSet) getString(index int32, tsDataType TSDataType) (string, error) {
	switch tsDataType {
	case BOOLEAN:
		if v, err := s.CurTsBlock.GetColumn(index).GetBoolean(s.TsBlockIndex); err != nil {
			return "", nil
		} else {
			return strconv.FormatBool(v), nil
		}
	case INT32:
		if v, err := s.CurTsBlock.GetColumn(index).GetInt(s.TsBlockIndex); err != nil {
			return "", err
		} else {
			return int32ToString(v), nil
		}
	case INT64:
	case TIMESTAMP:
		if v, err := s.CurTsBlock.GetColumn(index).GetLong(s.TsBlockIndex); err != nil {
			return "", err
		} else {
			return int64ToString(v), nil
		}
	case FLOAT:
		if v, err := s.CurTsBlock.GetColumn(index).GetFloat(s.TsBlockIndex); err != nil {
			return "", err
		} else {
			return float32ToString(v), nil
		}
	case DOUBLE:
		if v, err := s.CurTsBlock.GetColumn(index).GetDouble(s.TsBlockIndex); err != nil {
			return "", err
		} else {
			return float64ToString(v), nil
		}
	case TEXT:
	case STRING:
		if v, err := s.CurTsBlock.GetColumn(index).GetBinary(s.TsBlockIndex); err != nil {
			return "", err
		} else {
			return v.GetStringValue(), nil
		}
	case BLOB:
		if v, err := s.CurTsBlock.GetColumn(index).GetBinary(s.TsBlockIndex); err != nil {
			return "", err
		} else {
			return bytesToHexString(v.values), nil
		}
	case DATE:
		v, err := s.CurTsBlock.GetColumn(index).GetInt(s.TsBlockIndex)
		if err != nil {
			return "", err
		}
		t, err := int32ToDate(v)
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
	if columnIndex > int32(len(s.ColumnNameList)) {
		return "", fmt.Errorf("column index %d out of range %d", columnIndex, len(s.ColumnNameList))
	}
	return s.ColumnNameList[columnIndex-1], nil
}

func (s *IoTDBRpcDataSet) checkRecord() (err error) {
	if s.QueryResultIndex > s.QueryResultSize || s.TsBlockIndex >= s.TsBlockSize || s.QueryResult == nil || s.CurTsBlock == nil {
		err = fmt.Errorf("no record remains")
	}
	return err
}
