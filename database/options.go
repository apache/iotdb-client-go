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
	"crypto/tls"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/apache/iotdb-client-go/v2/client"
	"github.com/pkg/errors"
)

type ClientInfo struct {
	Products []struct {
		Name    string
		Version string
	}

	Comment []string
}

// Append returns a new copy of the combined ClientInfo structs
func (a ClientInfo) Append(b ClientInfo) ClientInfo {
	c := ClientInfo{
		Products: make([]struct {
			Name    string
			Version string
		}, 0, len(a.Products)+len(b.Products)),
		Comment: make([]string, 0, len(a.Comment)+len(b.Comment)),
	}

	for _, p := range a.Products {
		c.Products = append(c.Products, p)
	}
	for _, p := range b.Products {
		c.Products = append(c.Products, p)
	}

	for _, cm := range a.Comment {
		c.Comment = append(c.Comment, cm)
	}
	for _, cm := range b.Comment {
		c.Comment = append(c.Comment, cm)
	}

	return c
}

type ConnOpenStrategy uint8

const (
	ConnOpenInOrder ConnOpenStrategy = iota
	ConnOpenRoundRobin
	ConnOpenRandom
)

type Options struct {
	client.PoolConfig
	Debug                           bool
	ClientInfo                      ClientInfo
	Addr                            []string
	ConnOpenStrategy                ConnOpenStrategy
	DialContext                     func(ctx context.Context, addr string) (net.Conn, error)
	TLS                             *tls.Config
	DialTimeout                     time.Duration                 // default 30 second
	Debugf                          func(format string, v ...any) // only works when Debug is true
	PoolMaxSize                     *int
	PoolConnectionTimeoutInMs       *int
	PoolWaitToGetSessionTimeoutInMs *int
	PoolEnableCompression           *bool
}

func ParseDSN(dsn string) (*Options, error) {
	opt := &Options{}
	err := opt.fromDSN(dsn)
	if err != nil {
		return nil, err
	}
	return opt, nil
}

func (o *Options) fromDSN(in string) error {
	dsn, err := url.Parse(in)
	if err != nil {
		return err
	}
	if dsn.Host == "" {
		return errors.New("parse dsn address failed")
	}

	if dsn.User != nil {
		o.PoolConfig.UserName = dsn.User.Username()
		o.PoolConfig.Password, _ = dsn.User.Password()
	}

	//o.PoolConfig.Password = strings.TrimPrefix(dsn.Path, "/")
	o.Addr = append(o.Addr, strings.Split(dsn.Host, ",")...)

	var params = dsn.Query()

	for v := range params {
		switch v {
		case "fetch_size":
			{
				fetchSize, err := strconv.ParseInt(params.Get(v), 10, 32)
				if err != nil {
					return errors.Wrap(err, "fetch size invalid value")
				}
				o.PoolConfig.FetchSize = int32(fetchSize)
				break
			}
		case "time_zone":
			{
				o.PoolConfig.TimeZone = params.Get(v)
				break
			}
		case "connect_retry_max":
			{
				connectRetryMax, err := strconv.ParseInt(params.Get(v), 10, 32)
				if err != nil {
					return errors.Wrap(err, "connect retry max invalid value")
				}
				o.PoolConfig.ConnectRetryMax = int(connectRetryMax)
				break
			}
		case "username":
			{
				o.PoolConfig.UserName = params.Get(v)
				break
			}
		case "password":
			{
				o.PoolConfig.Password = params.Get(v)
				break
			}
		default:
			{
				break
			}
		}
	}
	return nil
}
