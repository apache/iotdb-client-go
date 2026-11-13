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

// Package rpcsession owns authenticated subscription RPC sessions and SHOW
// result decoding, independently of management SQL and consumer coordination.
package rpcsession

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/apache/iotdb-client-go/subscription"
	"github.com/apache/iotdb-client-go/subscription/internal/transport"

	"github.com/apache/iotdb-client-go/v2/client"
	"github.com/apache/iotdb-client-go/v2/common"
	"github.com/apache/iotdb-client-go/v2/rpc"
)

// Connection owns authentication, statement state, and one serialized RPC caller.
// Abort may run concurrently with that caller to interrupt a blocked socket.
type Connection struct {
	transport *transport.Connection
	client *rpc.IClientRPCServiceClient
	sessionID int64
	statementID int64
	cleanupTimeout time.Duration
}

// Open tries the supplied endpoints and authenticates a session for dialect.
// Its caller supplies normalized connection settings.
func Open(ctx context.Context, config subscription.ConnectionConfig, dialect subscription.Dialect) (*Connection, error) {
	var failures []error
	for _, endpoint := range config.NodeURLs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		openingCtx, cancel := context.WithTimeout(ctx, config.ConnectTimeout)
		connection, err := openSubscriptionEndpoint(openingCtx, endpoint, config, dialect)
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

func openSubscriptionEndpoint(ctx context.Context, endpoint string, config subscription.ConnectionConfig, dialect subscription.Dialect) (_ *Connection, err error) {
	rpcConnection, err := transport.Dial(ctx, endpoint, transport.Config{
		ConnectTimeout: config.ConnectTimeout,
		MaxFrameSize: config.MaxFrameSize,
	})
	if err != nil {
		return nil, err
	}
	connection := &Connection{
		transport: rpcConnection,
		client: rpcConnection.Client(),
		cleanupTimeout: config.ConnectTimeout,
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, connection.Abort())
		}
	}()

	password := config.Password
	attributes := map[string]string{
		"version": string(client.DEFAULT_VERSION),
		"sql_dialect": string(dialect),
	}
	if config.EncryptedPassword != "" {
		password = config.EncryptedPassword
		attributes["use_encrypted_password"] = "true"
	}
	request := &rpc.TSOpenSessionReq{
		ClientProtocol: rpc.TSProtocolVersion_IOTDB_SERVICE_PROTOCOL_V3,
		ZoneId: client.DefaultTimeZone,
		Username: config.Username,
		Password: &password,
		Configuration: attributes,
	}
	var response *rpc.TSOpenSessionResp
	if err = connection.Call(ctx, func() error {
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
	if err = connection.Call(ctx, func() error {
		var callErr error
		connection.statementID, callErr = connection.client.RequestStatementId(ctx, connection.sessionID)
		return callErr
	}); err != nil {
		return nil, err
	}
	return connection, nil
}

// Client returns the RPC client associated with this session. Its invocations
// must be serialized by the owner and passed through Call.
func (c *Connection) Client() *rpc.IClientRPCServiceClient {
	return c.client
}

// Closed reports whether the connection has become unusable.
func (c *Connection) Closed() bool {
	return c.transport.Closed()
}

// Abort interrupts network I/O and closes the underlying connection once.
func (c *Connection) Abort() error {
	return c.transport.Abort()
}

// Call preserves the subscription error contract while the internal transport
// owns socket cancellation and the sticky closed state after RPC failures.
func (c *Connection) Call(ctx context.Context, invoke func() error) error {
	err := c.transport.Call(ctx, invoke)
	if err == transport.ErrClosed {
		return subscription.ErrClosed
	}
	return err
}

// Close requests session cleanup and always closes the underlying connection.
func (c *Connection) Close(ctx context.Context) (err error) {
	defer func() {
		err = errors.Join(err, c.Abort())
	}()
	if c.Closed() {
		return nil
	}
	request := &rpc.TSCloseSessionReq{SessionId: c.sessionID}
	var status *common.TSStatus
	if err = c.Call(ctx, func() error {
		var callErr error
		status, callErr = c.client.CloseSession(ctx, request)
		return callErr
	}); err != nil {
		return err
	}
	return verifySubscriptionStatus(status)
}

// Execute sends a non-query statement and validates its server status.
func (c *Connection) Execute(ctx context.Context, sql string) error {
	request := &rpc.TSExecuteStatementReq{
		SessionId: c.sessionID,
		Statement: sql,
		StatementId: c.statementID,
	}
	var response *rpc.TSExecuteStatementResp
	if err := c.Call(ctx, func() error {
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

// Query reads string fields from SHOW results, fetching all result pages and
// closing the query operation before returning.
func (c *Connection) Query(ctx context.Context, sql string, columnCount int, allowExtraColumns bool) (rows [][]string, err error) {
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
	if err = c.Call(ctx, func() error {
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
				err = errors.Join(err, c.Abort())
				return
			}
			if !c.Closed() {
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
		if err = c.Call(ctx, func() error {
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

func (c *Connection) closeQuery(ctx context.Context, queryID int64) error {
	cleanupCtx, cancel := context.WithTimeout(ctx, c.cleanupTimeout)
	defer cancel()
	request := &rpc.TSCloseOperationReq{
		SessionId: c.sessionID,
		QueryId: &queryID,
		StatementId: &c.statementID,
	}
	var status *common.TSStatus
	if err := c.Call(cleanupCtx, func() error {
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
