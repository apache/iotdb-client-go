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

// Package transport owns the plain TCP/Thrift connection and cancellation
// boundary shared by subscription RPC callers. Authentication, session state,
// and server status handling remain with its callers.
package transport

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/apache/iotdb-client-go/v2/rpc"
)

// ErrClosed indicates that the underlying connection is no longer usable.
var ErrClosed = errors.New("subscription transport: connection is closed")

// Config specifies the connection timeout and Thrift frame/message size limit.
// Callers resolve defaults and validate these values before dialing.
type Config struct {
	ConnectTimeout time.Duration
	MaxFrameSize   int32
}

// Connection has one serialized RPC caller. Abort may run concurrently with
// that caller so cancellation and close can interrupt a blocked socket.
type Connection struct {
	network   net.Conn
	client    *rpc.IClientRPCServiceClient
	closed    atomic.Bool
	closeOnce sync.Once
	closeErr  error
}

// Dial establishes a plain TCP connection and constructs its framed binary
// Thrift client. It does not authenticate or make an RPC.
func Dial(ctx context.Context, endpoint string, config Config) (*Connection, error) {
	dialer := net.Dialer{Timeout: config.ConnectTimeout}
	network, err := dialer.DialContext(ctx, "tcp", endpoint)
	if err != nil {
		return nil, err
	}
	thriftConfig := &thrift.TConfiguration{
		ConnectTimeout: config.ConnectTimeout,
		MaxFrameSize:   config.MaxFrameSize,
		MaxMessageSize: config.MaxFrameSize,
	}
	socket := thrift.NewTSocketFromConnConf(network, thriftConfig)
	framed := thrift.NewTFramedTransportConf(socket, thriftConfig)
	protocol := thrift.NewTBinaryProtocolFactoryConf(thriftConfig)
	return &Connection{
		network: network,
		client: rpc.NewIClientRPCServiceClient(thrift.NewTStandardClient(
			protocol.GetProtocol(framed), protocol.GetProtocol(framed))),
	}, nil
}

// Client returns the client used by Call's invocation. RPC invocations must be
// serialized by the owner and passed through Call for cancellation handling.
func (c *Connection) Client() *rpc.IClientRPCServiceClient {
	return c.client
}

// Closed reports whether Abort has marked this connection unusable.
func (c *Connection) Closed() bool {
	return c.closed.Load()
}

// Abort marks the connection closed and closes its raw socket exactly once.
// All callers observe the same close result, including concurrent callers.
func (c *Connection) Abort() error {
	c.closed.Store(true)
	c.closeOnce.Do(func() {
		c.closeErr = c.network.Close()
	})
	return c.closeErr
}

// Call closes the raw connection on cancellation. A socket deadline alone is
// insufficient because TSocket sets its own deadline before every read/write.
// Waiting for a running cancellation callback prevents it from closing a later
// RPC's connection after this operation has returned.
func (c *Connection) Call(ctx context.Context, invoke func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.Closed() {
		return ErrClosed
	}
	canceled := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = c.Abort()
		close(canceled)
	})
	err := invoke()
	if !stop() {
		<-canceled
	}
	if contextErr := ctx.Err(); contextErr != nil {
		_ = c.Abort()
		return contextErr
	}
	if err != nil {
		// An older server can reject an optional RPC without corrupting the
		// stream. Keep that connection usable for the caller's fallback.
		var applicationError thrift.TApplicationException
		if !errors.As(err, &applicationError) || applicationError.TypeId() != thrift.UNKNOWN_METHOD {
			_ = c.Abort()
		}
	}
	return err
}
