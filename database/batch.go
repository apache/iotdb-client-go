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
