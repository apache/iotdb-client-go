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
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"github.com/apache/iotdb-client-go/v2/client"
	"github.com/apache/iotdb-client-go/v2/database/column"
	"github.com/pkg/errors"
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
	var (
		config = &client.PoolConfig{
			Host:     host,
			Port:     port,
			UserName: opt.UserName,
			Password: opt.Password,
		}
		poolMaxSize                     = 3
		poolWaitToGetSessionTimeoutInMs = 60000
		poolConnectionTimeoutInMs       = 60000
		poolEnableCompression           = false
	)
	if opt.PoolMaxSize != nil {
		poolMaxSize = *opt.PoolMaxSize
	}
	if opt.PoolWaitToGetSessionTimeoutInMs != nil {
		poolWaitToGetSessionTimeoutInMs = *opt.PoolWaitToGetSessionTimeoutInMs
	}
	if opt.PoolConnectionTimeoutInMs != nil {
		poolConnectionTimeoutInMs = *opt.PoolConnectionTimeoutInMs
	}
	if opt.PoolEnableCompression != nil {
		poolEnableCompression = *opt.PoolEnableCompression
	}
	conn = client.NewSessionPool(config, poolMaxSize, poolConnectionTimeoutInMs, poolWaitToGetSessionTimeoutInMs, poolEnableCompression)

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
	if err != nil {
		return err
	}
	defer c.conn.PutBack(session)
	return session.Ping(ctx)
}

func (c *connect) query(ctx context.Context, release nativeTransportRelease, query string, args ...any) (*rows, error) {
	options := queryOptions(ctx)
	body, err := bindQueryOrAppendParameters(&options, query, c.timeZone, args...)
	if err != nil {
		return nil, err
	}
	session, err := c.conn.GetSession()
	if err != nil {
		release(c, err)
		return nil, err
	}
	defer c.conn.PutBack(session)
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
