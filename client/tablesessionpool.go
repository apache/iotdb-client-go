package client

import (
	"github.com/apache/iotdb-client-go/common"
	"log"
	"sync/atomic"
)

type TableSessionPool struct {
	sessionPool SessionPool
}

func NewTableSessionPool(conf *PoolConfig, maxSize, connectionTimeoutInMs, waitToGetSessionTimeoutInMs int,
	enableCompression bool) TableSessionPool {
	return TableSessionPool{sessionPool: newSessionPoolWithSqlDialect(conf, maxSize, connectionTimeoutInMs, waitToGetSessionTimeoutInMs, enableCompression, TableSqlDialect)}
}

func (spool *TableSessionPool) GetSession() (ITableSession, error) {
	return spool.sessionPool.GetTableSession()
}

func (spool *TableSessionPool) Close() {
	spool.sessionPool.Close()
}

type PooledTableSession struct {
	session     Session
	sessionPool *SessionPool
	closed      atomic.Bool
}

func (s *PooledTableSession) Insert(tablet *Tablet) (r *common.TSStatus, err error) {
	r, err = s.session.InsertRelationalTablet(tablet)
	if err == nil {
		return
	}
	s.sessionPool.dropSession(s.session)
	s.closed.Store(true)
	s.session = Session{}
	return
}

func (s *PooledTableSession) ExecuteNonQueryStatement(sql string) (r *common.TSStatus, err error) {
	r, err = s.session.ExecuteNonQueryStatement(sql)
	if err == nil {
		return
	}
	s.sessionPool.dropSession(s.session)
	s.closed.Store(true)
	s.session = Session{}
	return
}

func (s *PooledTableSession) ExecuteQueryStatement(sql string, timeoutInMs *int64) (*SessionDataSet, error) {
	sessionDataSet, err := s.session.ExecuteQueryStatement(sql, timeoutInMs)
	if err == nil {
		return sessionDataSet, nil
	}
	s.sessionPool.dropSession(s.session)
	s.closed.Store(true)
	s.session = Session{}
	return nil, err
}

func (s *PooledTableSession) Close() error {
	if s.closed.CompareAndSwap(false, true) {
		if s.session.config.Database != s.sessionPool.config.Database && s.sessionPool.config.Database != "" {
			r, err := s.session.ExecuteNonQueryStatement("use " + s.sessionPool.config.Database)
			if r.Code == ExecuteStatementError || err != nil {
				log.Println("Failed to change back database by executing: use ", s.sessionPool.config.Database)
				s.session.Close()
				return nil
			}
		}
	}
	s.sessionPool.PutBack(s.session)
	s.session = Session{}
	return nil
}