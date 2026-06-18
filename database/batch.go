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

package iotdb_go

import (
	"context"
	"database/sql/driver"
)

// stdStmt is a prepared statement bound to a connection. It stores the query
// captured at prepare time and delegates execution to the connection's
// ExecContext/QueryContext, so db.Prepare(q).Exec/Query work as expected.
type stdStmt struct {
	std    *stdDriver
	query  string
	debugf func(format string, v ...any)
}

func (s *stdStmt) NumInput() int { return -1 }

func (s *stdStmt) Close() error { return nil }

func (s *stdStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return s.std.ExecContext(ctx, s.query, args)
}

func (s *stdStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.std.QueryContext(ctx, s.query, args)
}

func (s *stdStmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), valuesToNamedValues(args))
}

func (s *stdStmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), valuesToNamedValues(args))
}

var (
	_ driver.Stmt             = (*stdStmt)(nil)
	_ driver.StmtExecContext  = (*stdStmt)(nil)
	_ driver.StmtQueryContext = (*stdStmt)(nil)
)

// valuesToNamedValues adapts the legacy positional []driver.Value to
// []driver.NamedValue (1-based Ordinal), so the deprecated Exec/Query paths can
// reuse the context-aware implementations.
func valuesToNamedValues(args []driver.Value) []driver.NamedValue {
	named := make([]driver.NamedValue, len(args))
	for i, v := range args {
		named[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return named
}
