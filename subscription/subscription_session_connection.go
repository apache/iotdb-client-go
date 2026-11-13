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

package subscription

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/apache/iotdb-client-go/v2/client"
	"github.com/apache/iotdb-client-go/v2/common"
	"github.com/apache/iotdb-client-go/v2/rpc"
)

// sessionConnection has one serialized RPC caller. abort may run concurrently
// with that caller so cancellation and Close can interrupt a blocked socket.
type sessionConnection struct {
	network net.Conn
	client *rpc.IClientRPCServiceClient
	sessionID int64
	statementID int64
	cleanupTimeout time.Duration
	closed atomic.Bool
	closeOnce sync.Once
	closeErr error
}

func openSubscriptionConnection(ctx context.Context, config SubscriptionSessionConfig) (*sessionConnection, error) {
	var failures []error
	for _, endpoint := range config.Connection.NodeURLs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		openingCtx, cancel := context.WithTimeout(ctx, config.Connection.ConnectTimeout)
		connection, err := openSubscriptionEndpoint(openingCtx, endpoint, config)
		cancel()
		if err == nil {
			return connection, nil
		}
		failures = append(failures, fmt.Errorf("subscription: open %s: %w", endpoint, err))
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, errors.Join(failures...)
}

func openSubscriptionEndpoint(ctx context.Context, endpoint string, config SubscriptionSessionConfig) (_ *sessionConnection, err error) {
	dialer := net.Dialer{Timeout: config.Connection.ConnectTimeout}
	network, err := dialer.DialContext(ctx, "tcp", endpoint)
	if err != nil {
		return nil, err
	}
	thriftConfig := &thrift.TConfiguration{
		ConnectTimeout: config.Connection.ConnectTimeout,
		MaxFrameSize: config.Connection.MaxFrameSize,
		MaxMessageSize: config.Connection.MaxFrameSize,
	}
	socket := thrift.NewTSocketFromConnConf(network, thriftConfig)
	transport := thrift.NewTFramedTransportConf(socket, thriftConfig)
	protocol := thrift.NewTBinaryProtocolFactoryConf(thriftConfig)
	connection := &sessionConnection{
		network: network,
		client: rpc.NewIClientRPCServiceClient(thrift.NewTStandardClient(
			protocol.GetProtocol(transport), protocol.GetProtocol(transport))),
		cleanupTimeout: config.Connection.ConnectTimeout,
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, connection.abort())
		}
	}()

	password := config.Connection.Password
	attributes := map[string]string{
		"version": string(client.DEFAULT_VERSION),
		"sql_dialect": string(config.Dialect),
	}
	if config.Connection.EncryptedPassword != "" {
		password = config.Connection.EncryptedPassword
		attributes["use_encrypted_password"] = "true"
	}
	request := &rpc.TSOpenSessionReq{
		ClientProtocol: rpc.TSProtocolVersion_IOTDB_SERVICE_PROTOCOL_V3,
		ZoneId: client.DefaultTimeZone,
		Username: config.Connection.Username,
		Password: &password,
		Configuration: attributes,
	}
	var response *rpc.TSOpenSessionResp
	if err = connection.call(ctx, func() error {
		var callErr error
		response, callErr = connection.client.OpenSession(ctx, request)
		return callErr
	}); err != nil {
		return nil, err
	}
	if response == nil {
		return nil, errors.New("subscription: open session returned no response")
	}
	if err = verifySubscriptionStatus(response.Status); err != nil {
		return nil, err
	}
	if response.SessionId == nil {
		return nil, errors.New("subscription: open session returned no session ID")
	}
	if response.ServerProtocolVersion == 0 {
		return nil, errors.New("subscription: server protocol version is not supported")
	}
	connection.sessionID = *response.SessionId
	if err = connection.call(ctx, func() error {
		var callErr error
		connection.statementID, callErr = connection.client.RequestStatementId(ctx, connection.sessionID)
		return callErr
	}); err != nil {
		return nil, err
	}
	return connection, nil
}

func (c *sessionConnection) isClosed() bool {
	return c.closed.Load()
}

func (c *sessionConnection) abort() error {
	c.closed.Store(true)
	c.closeOnce.Do(func() {
		c.closeErr = c.network.Close()
	})
	return c.closeErr
}

// call closes the raw connection on cancellation. A socket deadline alone is
// insufficient because TSocket sets its own deadline before every read/write.
// Waiting for a running cancellation callback prevents it from closing a later
// RPC's connection after this operation has returned.
func (c *sessionConnection) call(ctx context.Context, invoke func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.isClosed() {
		return ErrClosed
	}
	canceled := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = c.abort()
		close(canceled)
	})
	err := invoke()
	if !stop() {
		<-canceled
	}
	if contextErr := ctx.Err(); contextErr != nil {
		_ = c.abort()
		return contextErr
	}
	if err != nil {
		// An older server can reject an optional RPC without corrupting the
		// stream. Keep that connection usable for the caller's fallback.
		var applicationError thrift.TApplicationException
		if !errors.As(err, &applicationError) || applicationError.TypeId() != thrift.UNKNOWN_METHOD {
			_ = c.abort()
		}
	}
	return err
}

func (c *sessionConnection) close(ctx context.Context) (err error) {
	defer func() {
		err = errors.Join(err, c.abort())
	}()
	if c.isClosed() {
		return nil
	}
	request := &rpc.TSCloseSessionReq{SessionId: c.sessionID}
	var status *common.TSStatus
	if err = c.call(ctx, func() error {
		var callErr error
		status, callErr = c.client.CloseSession(ctx, request)
		return callErr
	}); err != nil {
		return err
	}
	return verifySubscriptionStatus(status)
}

func (c *sessionConnection) execute(ctx context.Context, sql string) error {
	request := &rpc.TSExecuteStatementReq{
		SessionId: c.sessionID,
		Statement: sql,
		StatementId: c.statementID,
	}
	var response *rpc.TSExecuteStatementResp
	if err := c.call(ctx, func() error {
		var callErr error
		response, callErr = c.client.ExecuteUpdateStatementV2(ctx, request)
		return callErr
	}); err != nil {
		return err
	}
	if response == nil {
		return errors.New("subscription: statement returned no response")
	}
	return verifySubscriptionStatus(response.Status)
}

func (c *sessionConnection) query(ctx context.Context, sql string, columnCount int, allowExtraColumns bool) (rows [][]string, err error) {
	fetchSize := int32(client.DefaultFetchSize)
	redirect := false
	request := &rpc.TSExecuteStatementReq{
		SessionId: c.sessionID,
		Statement: sql,
		StatementId: c.statementID,
		FetchSize: &fetchSize,
		Timeout: subscriptionQueryTimeout(ctx),
		EnableRedirectQuery: &redirect,
	}
	var response *rpc.TSExecuteStatementResp
	if err = c.call(ctx, func() error {
		var callErr error
		response, callErr = c.client.ExecuteQueryStatementV2(ctx, request)
		return callErr
	}); err != nil {
		return nil, err
	}
	if response == nil {
		return nil, errors.New("subscription: query returned no response")
	}
	if response.QueryId != nil {
		defer func() {
			if contextErr := ctx.Err(); contextErr != nil {
				rows = nil
				if !errors.Is(err, contextErr) {
					err = errors.Join(err, contextErr)
				}
				err = errors.Join(err, c.abort())
				return
			}
			if !c.isClosed() {
				err = errors.Join(err, c.closeQuery(ctx, *response.QueryId))
			}
		}()
	}
	if err = verifySubscriptionStatus(response.Status); err != nil {
		return nil, err
	}
	if response.QueryId == nil {
		return nil, errors.New("subscription: query returned no query ID")
	}
	if len(response.Columns) < columnCount || (!allowExtraColumns && len(response.Columns) != columnCount) {
		return nil, fmt.Errorf("subscription: query returned %d columns; expected %d", len(response.Columns), columnCount)
	}
	rows, err = decodeSubscriptionRows(response.QueryResult_, response.DataTypeList, response.ColumnIndex2TsBlockColumnIndexList, columnCount)
	if err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	moreData := response.GetMoreData()
	for moreData {
		fetch := &rpc.TSFetchResultsReq{
			SessionId: c.sessionID,
			Statement: sql,
			FetchSize: fetchSize,
			QueryId: *response.QueryId,
			IsAlign: true,
			Timeout: subscriptionQueryTimeout(ctx),
			StatementId: &c.statementID,
		}
		var page *rpc.TSFetchResultsResp
		if err = c.call(ctx, func() error {
			var callErr error
			page, callErr = c.client.FetchResultsV2(ctx, fetch)
			return callErr
		}); err != nil {
			return nil, err
		}
		if page == nil {
			return nil, errors.New("subscription: fetch results returned no response")
		}
		if err = verifySubscriptionStatus(page.Status); err != nil {
			return nil, err
		}
		if !page.HasResultSet {
			break
		}
		var next [][]string
		next, err = decodeSubscriptionRows(page.QueryResult_, response.DataTypeList, response.ColumnIndex2TsBlockColumnIndexList, columnCount)
		if err != nil {
			return nil, err
		}
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		rows = append(rows, next...)
		moreData = page.GetMoreData()
	}
	return rows, nil
}

func (c *sessionConnection) closeQuery(ctx context.Context, queryID int64) error {
	cleanupCtx, cancel := context.WithTimeout(ctx, c.cleanupTimeout)
	defer cancel()
	request := &rpc.TSCloseOperationReq{
		SessionId: c.sessionID,
		QueryId: &queryID,
		StatementId: &c.statementID,
	}
	var status *common.TSStatus
	if err := c.call(cleanupCtx, func() error {
		var callErr error
		status, callErr = c.client.CloseOperation(cleanupCtx, request)
		return callErr
	}); err != nil {
		return err
	}
	return verifySubscriptionStatus(status)
}

func verifySubscriptionStatus(status *common.TSStatus) error {
	if status == nil {
		return errors.New("subscription: server returned no status")
	}
	if status.Code == client.RedirectionRecommend {
		return &client.ExecutionError{Code: status.Code, Message: status.GetMessage()}
	}
	return client.VerifySuccess(status)
}

func subscriptionQueryTimeout(ctx context.Context) *int64 {
	deadline, ok := ctx.Deadline()
	if !ok {
		return nil
	}
	remaining := time.Until(deadline)
	milliseconds := remaining.Milliseconds()
	if remaining > 0 && remaining%time.Millisecond != 0 {
		milliseconds++
	}
	if milliseconds < 0 {
		milliseconds = 0
	}
	return &milliseconds
}
