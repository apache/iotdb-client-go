package iotdb_go

import "context"

type stdConnect interface {
	isBad() bool
	close() error
	query(ctx context.Context, release nativeTransportRelease, query string, args ...any) (*rows, error)
	exec(ctx context.Context, query string, args ...any) error
	ping(ctx context.Context) (err error)
	commit() error
}
