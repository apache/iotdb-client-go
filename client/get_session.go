/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package client

import (
	"github.com/apache/iotdb-client-go/rpc"
	"github.com/apache/thrift/lib/go/thrift"
)

const (
	DefaultUser            = "root"
	DefaultPasswd          = "root"
	DefaultZoneId          = "Asia/Shanghai"
	DefaultFetchSize int32 = 10000
)

/*
 *session config
 *Host:      server ip
 *Port:      server port
 *User       user name
 *Passwd     user passwd
 *FetchSize  int32
 *ZoneId     string
 */
type Config struct {
	Host      string
	Port      string
	User      string
	Passwd    string
	FetchSize int32
	ZoneId    string
}

type Session struct {
	config             *Config
	client             *rpc.TSIServiceClient
	sessionId          int64
	trans              thrift.TTransport
	requestStatementId int64
}

func NewConfig() *Config {
	return &Config{
		User:      DefaultUser,
		Passwd:    DefaultPasswd,
		FetchSize: DefaultFetchSize,
		ZoneId:    DefaultZoneId,
	}
}

func NewSession(config *Config) *Session {
	return &Session{config: config}
}
