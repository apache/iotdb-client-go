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
	return spool.sessionPool.getTableSession()
}

func (spool *TableSessionPool) Close() {
	spool.sessionPool.Close()
}

type PooledTableSession struct {
	session     Session
	sessionPool *SessionPool
	closed      int32
}

func (s *PooledTableSession) Insert(tablet *Tablet) (r *common.TSStatus, err error) {
	r, err = s.session.insertRelationalTablet(tablet)
	if err == nil {
		return
	}
	s.sessionPool.dropSession(s.session)
	atomic.StoreInt32(&s.closed, 1)
	s.session = Session{}
	return
}

func (s *PooledTableSession) ExecuteNonQueryStatement(sql string) (r *common.TSStatus, err error) {
	r, err = s.session.ExecuteNonQueryStatement(sql)
	if err == nil {
		return
	}
	s.sessionPool.dropSession(s.session)
	atomic.StoreInt32(&s.closed, 1)
	s.session = Session{}
	return
}

func (s *PooledTableSession) ExecuteQueryStatement(sql string, timeoutInMs *int64) (*SessionDataSet, error) {
	sessionDataSet, err := s.session.ExecuteQueryStatement(sql, timeoutInMs)
	if err == nil {
		return sessionDataSet, nil
	}
	s.sessionPool.dropSession(s.session)
	atomic.StoreInt32(&s.closed, 1)
	s.session = Session{}
	return nil, err
}

func (s *PooledTableSession) Close() error {
	if atomic.CompareAndSwapInt32(&s.closed, 0, 1) {
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
