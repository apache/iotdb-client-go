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
	"log"
	"sync/atomic"

	"github.com/apache/iotdb-client-go/v2/common"
)

// TableSessionPool manages a pool of ITableSession instances, enabling efficient
// reuse and management of resources. It provides methods to acquire a session
// from the pool and to close the pool, releasing all held resources.
//
// This implementation ensures proper lifecycle management of sessions,
// including efficient reuse and cleanup of resources.
type TableSessionPool struct {
	sessionPool SessionPool
}

// NewTableSessionPool creates a new TableSessionPool with the specified configuration.
//
// Parameters:
//   - conf: PoolConfig defining the configuration for the pool.
//   - maxSize: The maximum number of sessions the pool can hold.
//   - connectionTimeoutInMs: Timeout for establishing a connection in milliseconds.
//   - waitToGetSessionTimeoutInMs: Timeout for waiting to acquire a session in milliseconds.
//   - enableCompression: A boolean indicating whether to enable compression.
//
// Returns:
//   - A TableSessionPool instance.
func NewTableSessionPool(conf *PoolConfig, maxSize, connectionTimeoutInMs, waitToGetSessionTimeoutInMs int,
	enableCompression bool) TableSessionPool {
	return TableSessionPool{sessionPool: newSessionPoolWithSqlDialect(conf, maxSize, connectionTimeoutInMs, waitToGetSessionTimeoutInMs, enableCompression, TableSqlDialect)}
}

// GetSession acquires an ITableSession instance from the pool.
//
// Returns:
//   - A usable ITableSession instance for interacting with IoTDB.
//   - An error if a session cannot be acquired.
func (spool *TableSessionPool) GetSession() (ITableSession, error) {
	return spool.sessionPool.getTableSession()
}

// Close closes the TableSessionPool, releasing all held resources.
// Once closed, no further sessions can be acquired from the pool.
func (spool *TableSessionPool) Close() {
	spool.sessionPool.Close()
}

// PooledTableSession represents a session managed within a TableSessionPool.
// It ensures proper cleanup and reusability of the session.
type PooledTableSession struct {
	session     Session
	sessionPool *SessionPool
	closed      int32
}

// Insert inserts a Tablet into the database.
//
// Parameters:
//   - tablet: A pointer to a Tablet containing time-series data to be inserted.
//
// Returns:
//   - r: A pointer to TSStatus indicating the execution result.
//   - err: An error if an issue occurs during the operation.
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

// ExecuteNonQueryStatement executes a non-query SQL statement, such as a DDL or DML command.
//
// Parameters:
//   - sql: The SQL statement to execute.
//
// Returns:
//   - r: A pointer to TSStatus indicating the execution result.
//   - err: An error if an issue occurs during the operation.
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

// ExecuteQueryStatement executes a query SQL statement and returns the result set.
//
// Parameters:
//   - sql: The SQL query statement to execute.
//   - timeoutInMs: A pointer to the timeout duration in milliseconds for query execution.
//
// Returns:
//   - result: A pointer to SessionDataSet containing the query results.
//   - err: An error if an issue occurs during the operation.
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

// Close closes the PooledTableSession, releasing it back to the pool.
//
// Returns:
//   - err: An error if there is an issue with session closure or cleanup.
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
