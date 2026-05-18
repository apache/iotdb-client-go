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
	"database/sql/driver"
	"io"

	"github.com/pkg/errors"
)

type stdRows struct {
	rows   *rows
	debugf func(format string, v ...any)
}

// Columns returns the names of the columns. The number of
// columns of the result is inferred from the length of the
// slice. If a particular column name isn't known, an empty
// string should be returned for that entry.
func (s *stdRows) Columns() []string {
	return s.rows.set.GetColumnNames()
}

// Close closes the rows iterator.
func (s *stdRows) Close() error {
	s.rows.set.Close()
	return nil
}

// Next is called to populate the next row of data into
// the provided slice. The provided slice will be the same
// size as the Columns() are wide.
//
// Next should return io.EOF when there are no more rows.
//
// The dest should not be written to outside of Next. Care
// should be taken when closing Rows not to modify
// a buffer held in dest.
func (s *stdRows) Next(dest []driver.Value) error {
	if len(s.rows.set.GetColumnNames()) != len(dest) {
		return errors.New("column count mismatch")
	}
	next, err := s.rows.Next()
	if err != nil {
		s.debugf("rows.Next() failed: %v", err)
		return err
	}
	if next {
		for i := range dest {
			if s.rows.columns[i] == nil {
				dest[i] = nil
				continue
			}
			switch value := s.rows.columns[i].Row(s.rows.set, false).(type) {
			case driver.Valuer:
				v, err := value.Value()
				if err != nil {
					s.debugf("Next row error: %v\n", err)
					return err
				}
				dest[i] = v
			default:
				dest[i] = value
				break
			}
		}
	} else {
		return io.EOF
	}
	return nil
}
