package iotdb_go

import (
	"database/sql/driver"
	"github.com/pkg/errors"
	"io"
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
	if !next {
		return io.EOF
	}
	if next {
		for i := range dest {

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
	}
	return nil
}
