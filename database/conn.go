package iotdb_go

import (
	"context"
	"fmt"
	"github.com/apache/iotdb-client-go/v2/client"
	"github.com/apache/iotdb-client-go/v2/database/column"
	"github.com/pkg/errors"
	"log"
	"net"
	"os"
	"time"
)

func dial(ctx context.Context, addr string, num int, opt *Options) (*connect, error) {
	if addr == "" {
		return nil, errors.New("empty addr")
	}
	// 使用 net.SplitHostPort 分割地址和端口
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	var (
		conn   client.SessionPool
		debugf = func(format string, v ...any) {}
	)

	if opt.Debug {
		if opt.Debugf != nil {
			debugf = func(format string, v ...any) {
				opt.Debugf(
					"[iotdb][%s][id=%d] "+format,
					append([]interface{}{opt.Addr, num}, v...)...,
				)
			}
		} else {
			debugf = log.New(os.Stdout, fmt.Sprintf("[iotdb][%s][id=%d]", opt.Addr, num), 0).Printf
		}
	}

	config := &client.PoolConfig{
		Host:     host,
		Port:     port,
		UserName: opt.UserName,
		Password: opt.Password,
	}

	conn = client.NewSessionPool(config, 3, 60000, 60000, false)

	var (
		netConn = &connect{
			id:          num,
			opt:         opt,
			conn:        conn,
			debugfFunc:  debugf,
			connectedAt: time.Now(),
		}
	)

	return netConn, nil
}

type connect struct {
	id          int
	opt         *Options
	conn        client.SessionPool
	debugfFunc  func(format string, v ...any)
	connectedAt time.Time
	timeZone    *time.Location
}

func (c *connect) debugf(format string, v ...any) {
	c.debugfFunc(format, v...)
}

func (c *connect) isBad() bool {
	return false
}
func (c *connect) close() error {
	c.conn.Close()
	return nil
}

func (c *connect) ping(ctx context.Context) (err error) {
	session, err := c.conn.GetSession()
	defer c.conn.PutBack(session)
	if err != nil {
		return err
	}
	return session.Ping(ctx)
}

func (c *connect) query(ctx context.Context, release nativeTransportRelease, query string, args ...any) (*rows, error) {
	var (
		options   = queryOptions(ctx)
		body, err = bindQueryOrAppendParameters(&options, query, c.timeZone, args...)
	)
	session, err := c.conn.GetSession()
	defer c.conn.PutBack(session)
	if err != nil {
		release(c, err)
		return nil, err
	}
	var timeout int64 = int64(c.opt.DialTimeout.Seconds() * 1000)
	if timeout == 0 {
		timeout = 5000
	}
	statement, err := session.ExecuteQueryStatement(body, &timeout)
	if err != nil {
		release(c, err)
		return nil, err
	}

	// column list
	names := statement.GetColumnNames()
	columnsList := make([]column.Interface, len(names))
	for k, name := range names {
		dataType := statement.GetColumnTypes()[k]
		col := column.GenColumn(dataType, name)
		if col == nil {
			continue
		}
		columnsList[k] = col
	}
	return &rows{
		set:     statement,
		columns: columnsList,
	}, nil
}

func (c *connect) commit() error {
	return nil
}
