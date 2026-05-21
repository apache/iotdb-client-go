package iotdb_go

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"log"
	"net"
	"os"
	"syscall"
)

var globalConnID int64

func init() {
	var debugf = func(format string, v ...any) {}
	sql.Register("iotdb", &stdDriver{debugf: debugf})
}

// isConnBrokenError returns true if the error class indicates that the
// db connection is no longer usable and should be marked bad
func isConnBrokenError(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) {
		return true
	}
	if _, ok := err.(*net.OpError); ok {
		return true
	}
	return false
}

type stdDriver struct {
	opt    *Options
	conn   stdConnect
	commit func() error
	debugf func(format string, v ...any)
}

var _ driver.Conn = (*stdDriver)(nil)
var _ driver.ConnBeginTx = (*stdDriver)(nil)
var _ driver.ExecerContext = (*stdDriver)(nil)
var _ driver.QueryerContext = (*stdDriver)(nil)
var _ driver.ConnPrepareContext = (*stdDriver)(nil)

func (std *stdDriver) Open(dsn string) (_ driver.Conn, err error) {
	var opt Options
	if err := opt.fromDSN(dsn); err != nil {
		std.debugf("Open dsn error: %v\n", err)
		return nil, err
	}
	var debugf = func(format string, v ...any) {}
	if opt.Debug {
		debugf = log.New(os.Stdout, "[iotdb-std][opener] ", 0).Printf
	}
	opt.ClientInfo.Comment = []string{"database/sql"}
	return (&stdConnOpener{opt: &opt, debugf: debugf}).Connect(context.Background())
}

var _ driver.Driver = (*stdDriver)(nil)

func (std *stdDriver) ResetSession(ctx context.Context) error {
	if std.conn.isBad() {
		std.debugf("Resetting session because connection is bad")
		return driver.ErrBadConn
	}
	return nil
}

var _ driver.SessionResetter = (*stdDriver)(nil)

func (std *stdDriver) Ping(ctx context.Context) error {
	if std.conn.isBad() {
		std.debugf("Ping: connection is bad")
		return driver.ErrBadConn
	}

	return std.conn.ping(ctx)
}

var _ driver.Pinger = (*stdDriver)(nil)

// Begin starts and returns a new transaction.
//
// Deprecated: Drivers should implement ConnBeginTx instead (or additionally).
func (std *stdDriver) Begin() (driver.Tx, error) {
	if std.conn.isBad() {
		std.debugf("Begin: connection is bad")
		return nil, driver.ErrBadConn
	}
	return std, nil
}

func (std *stdDriver) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if std.conn.isBad() {
		std.debugf("BeginTx: connection is bad")
		return nil, driver.ErrBadConn
	}

	return std, nil
}

func (std *stdDriver) Commit() error {
	if std.commit == nil {
		return nil
	}
	defer func() {
		std.commit = nil
	}()

	if err := std.commit(); err != nil {
		if isConnBrokenError(err) {
			std.debugf("Commit got EOF error: resetting connection")
			return driver.ErrBadConn
		}
		std.debugf("Commit error: %v\n", err)
		return err
	}
	return nil
}

func (std *stdDriver) Rollback() error {
	std.commit = nil
	std.conn.close()
	return nil
}

var _ driver.Tx = (*stdDriver)(nil)

func (std *stdDriver) CheckNamedValue(nv *driver.NamedValue) error { return nil }

var _ driver.NamedValueChecker = (*stdDriver)(nil)

// Prepare returns a prepared statement, bound to this connection.
func (std *stdDriver) Prepare(query string) (driver.Stmt, error) {
	return std.PrepareContext(context.Background(), query)
}

func (std *stdDriver) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if std.conn.isBad() {
		std.debugf("QueryContext: connection is bad")
		return nil, driver.ErrBadConn
	}

	r, err := std.conn.query(ctx, func(nativeTransport, error) {}, query, rebind(args)...)
	if isConnBrokenError(err) {
		std.debugf("QueryContext got a fatal error, resetting connection: %v\n", err)
		return nil, driver.ErrBadConn
	}
	if err != nil {
		std.debugf("QueryContext error: %v\n", err)
		return nil, err
	}
	return &stdRows{
		rows:   r,
		debugf: std.debugf,
	}, nil
}

func (std *stdDriver) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if std.conn.isBad() {
		std.debugf("PrepareContext: connection is bad")
		return nil, driver.ErrBadConn
	}

	std.commit = std.conn.commit
	return &stdBatch{
		debugf: std.debugf,
	}, nil
}

// Close invalidates and potentially stops any current
// prepared statements and transactions, marking this
// connection as no longer in use.
//
// Because the sql package maintains a free pool of
// connections and only calls Close when there's a surplus of
// idle connections, it shouldn't be necessary for drivers to
// do their own connection caching.
//
// Drivers must ensure all network calls made by Close
// do not block indefinitely (e.g. apply a timeout).
func (std *stdDriver) Close() error {
	err := std.conn.close()
	if err != nil {
		if isConnBrokenError(err) {
			std.debugf("Close got a fatal error, resetting connection: %v\n", err)
			return driver.ErrBadConn
		}
		std.debugf("Close error: %v\n", err)
	}
	return err
}

func (std *stdDriver) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if std.conn.isBad() {
		std.debugf("ExecContext: connection is bad")
		return nil, driver.ErrBadConn
	}
	err := std.conn.exec(ctx, query, rebind(args)...)
	if err != nil {
		std.debugf("ExecContext error: %v\n", err)
		return nil, err
	}
	return driver.RowsAffected(0), nil
}
