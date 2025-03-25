package client

import (
	"github.com/apache/iotdb-client-go/rpc"
	"time"
)

type SessionDataSet struct {
	ioTDBRpcDataSet *IoTDBRpcDataSet
}

func NewSessionDataSet(sql string, columnNameList []string, columnTypeList []string, columnNameIndex map[string]int32, queryId int64, statementId int64, client *rpc.IClientRPCServiceClient, sessionId int64, queryResult [][]byte, ignoreTimestamp bool, timeout int64, moreData bool, fetchSize int32) (*SessionDataSet, error) {
	rpcDataSet, err := NewIoTDBRpcDataSet(sql, columnNameList, columnTypeList, columnNameIndex, ignoreTimestamp, moreData, queryId, statementId, client, sessionId, queryResult, fetchSize, timeout)
	if err != nil {
		return nil, err
	}
	return &SessionDataSet{ioTDBRpcDataSet: rpcDataSet}, nil
}

func (s *SessionDataSet) Next() (bool, error) {
	return s.ioTDBRpcDataSet.Next()
}

func (s *SessionDataSet) Close() error {
	return s.ioTDBRpcDataSet.Close()
}

func (s *SessionDataSet) IsNull(columnName string) (bool, error) {
	return s.ioTDBRpcDataSet.isNullByColumnName(columnName), nil
}

func (s *SessionDataSet) IsNullByIndex(columnIndex int32) (bool, error) {
	return s.ioTDBRpcDataSet.isNullByIndex(columnIndex)
}

func (s *SessionDataSet) GetBooleanByIndex(columnIndex int32) (bool, error) {
	return s.ioTDBRpcDataSet.getBooleanByIndex(columnIndex)
}

func (s *SessionDataSet) GetBoolean(columnName string) (bool, error) {
	return s.ioTDBRpcDataSet.getBoolean(columnName)
}

func (s *SessionDataSet) GetDoubleByIndex(columnIndex int32) (float64, error) {
	return s.ioTDBRpcDataSet.getDoubleByIndex(columnIndex)
}

func (s *SessionDataSet) GetDouble(columnName string) (float64, error) {
	return s.ioTDBRpcDataSet.getDouble(columnName)
}

func (s *SessionDataSet) GetFloatByIndex(columnIndex int32) (float32, error) {
	return s.ioTDBRpcDataSet.getFloatByIndex(columnIndex)
}

func (s *SessionDataSet) GetFloat(columnName string) (float32, error) {
	return s.ioTDBRpcDataSet.getFloat(columnName)
}

func (s *SessionDataSet) GetIntByIndex(columnIndex int32) (int32, error) {
	return s.ioTDBRpcDataSet.getIntByIndex(columnIndex)
}

func (s *SessionDataSet) GetInt(columnName string) (int32, error) {
	return s.ioTDBRpcDataSet.getInt(columnName)
}

func (s *SessionDataSet) GetLongByIndex(columnIndex int32) (int64, error) {
	return s.ioTDBRpcDataSet.getLongByIndex(columnIndex)
}

func (s *SessionDataSet) GetLong(columnName string) (int64, error) {
	return s.ioTDBRpcDataSet.getLong(columnName)
}

func (s *SessionDataSet) GetStringByIndex(columnIndex int32) (string, error) {
	return s.ioTDBRpcDataSet.getStringByIndex(columnIndex)
}

func (s *SessionDataSet) GetString(columnName string) (string, error) {
	return s.ioTDBRpcDataSet.getValueByName(columnName)
}

func (s *SessionDataSet) GetTimestampByIndex(columnIndex int32) (time.Time, error) {
	return s.ioTDBRpcDataSet.GetTimestampByIndex(columnIndex)
}

func (s *SessionDataSet) GetTimestamp(columnName string) (time.Time, error) {
	return s.ioTDBRpcDataSet.GetTimestamp(columnName)
}

func (s *SessionDataSet) FindColumn(columnName string) int32 {
	return s.ioTDBRpcDataSet.findColumn(columnName)
}

func (s *SessionDataSet) GetColumnNames() []string {
	return s.ioTDBRpcDataSet.columnNameList
}

func (s *SessionDataSet) GetColumnTypes() []string {
	return s.ioTDBRpcDataSet.columnTypeList
}
