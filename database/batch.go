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

	"github.com/pkg/errors"
)

type stdBatch struct {
	debugf func(format string, v ...any)
}

func (s *stdBatch) NumInput() int { return -1 }

func (s *stdBatch) Exec(args []driver.Value) (driver.Result, error) {
	return nil, errors.New("not implemented")
}

func (s *stdBatch) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return nil, driver.ErrSkip
}

func (s *stdBatch) Query(args []driver.Value) (driver.Rows, error) {
	// Note: not implementing driver.StmtQueryContext accordingly
	return nil, errors.New("only Exec method supported in batch mode")
}

func (s *stdBatch) Close() error {
	return nil
}
