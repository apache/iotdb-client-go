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

import "github.com/apache/iotdb-client-go/common"

type ITableSession interface {
	Insert(tablet *Tablet) (r *common.TSStatus, err error)
	ExecuteNonQueryStatement(sql string) (r *common.TSStatus, err error)
	ExecuteQueryStatement(sql string, timeoutInMs *int64) (*SessionDataSet, error)
	Close() (err error)
}

type TableSession struct {
	session Session
}

func NewTableSession(config *Config, enableRPCCompression bool, connectionTimeoutInMs int) (ITableSession, error) {
	session := newSessionWithSqlDialect(config, TableSqlDialect)

	if err := session.Open(enableRPCCompression, connectionTimeoutInMs); err != nil {
		return nil, err
	}
	return &TableSession{session: session}, nil
}

func (s *TableSession) Insert(tablet *Tablet) (r *common.TSStatus, err error) {
	return s.session.InsertRelationalTablet(tablet)
}

func (s *TableSession) ExecuteNonQueryStatement(sql string) (r *common.TSStatus, err error) {
	return s.session.ExecuteNonQueryStatement(sql)
}

func (s *TableSession) ExecuteQueryStatement(sql string, timeoutInMs *int64) (*SessionDataSet, error) {
	return s.session.ExecuteQueryStatement(sql, timeoutInMs)
}

func (s *TableSession) Close() error {
	return s.session.Close()
}
