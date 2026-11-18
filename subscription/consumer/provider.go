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

package consumer

import (
	"github.com/apache/iotdb-client-go/subscription"
	"github.com/apache/iotdb-client-go/subscription/payload"
	"github.com/apache/iotdb-client-go/subscription/internal/protocol"
	"github.com/apache/iotdb-client-go/subscription/internal/rpcsession"
	"github.com/apache/iotdb-client-go/subscription/internal/wire"

	"context"
	"errors"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/apache/iotdb-client-go/v2/client"
	"github.com/apache/iotdb-client-go/v2/common"
	"github.com/apache/iotdb-client-go/v2/rpc"
)

const (
	subscriptionFrameReserve = 1024
)

var subscriptionSliceOrder atomic.Int32

// subscriptionProvider owns one handshaken connection. Its operation gate keeps
// normal RPCs and every slice of a logical request on the same serialized stream.
// Recovery creates a new provider; an old response cannot revive a failed one.
type subscriptionProvider struct {
	connection *rpcsession.Connection
	endpoint string
	dataNodeID int32
	consumerID string
	groupID string
	bodyLimit int
	operations chan struct{}
}

func openSubscriptionProvider(ctx context.Context, config SubscriptionConsumerConfig, endpoint, instanceID string) (_ *subscriptionProvider, err error) {
	if ctx == nil {
		return nil, errors.New("subscription: context must not be nil")
	}
	if instanceID == "" {
		return nil, errors.New("subscription: consumer instance ID must not be empty")
	}
	defer func() {
		if err == nil {
			return
		}
		err = classifyAndRedactConnectionError(err, config.Connection.Password, config.Connection.EncryptedPassword)
	}()
	openingCtx, cancel := context.WithTimeout(ctx, config.Connection.ConnectTimeout)
	defer cancel()
	connectionConfig := config.Connection
	connectionConfig.NodeURLs = []string{endpoint}
	connection, err := rpcsession.Open(openingCtx, connectionConfig, config.Dialect)
	if err != nil {
		return nil, err
	}
	provider := &subscriptionProvider{
		connection: connection,
		endpoint: endpoint,
		bodyLimit: int(config.Connection.MaxFrameSize) - subscriptionFrameReserve,
		operations: make(chan struct{}, 1),
	}
	defer func() {
		if err != nil {
			_ = connection.Abort()
		}
	}()
	if err = provider.negotiateFrameLimit(openingCtx); err != nil {
		return nil, err
	}
	attributes := map[string]string{
		"username": config.Connection.Username,
		"password": config.Connection.Password,
		"sql-dialect": string(config.Dialect),
		"consumer-instance-id": instanceID,
		"heartbeat-interval-ms": strconv.FormatInt(config.HeartbeatInterval.Milliseconds(), 10),
		"connection-timeout-ms": strconv.FormatInt(config.Connection.ConnectTimeout.Milliseconds(), 10),
	}
	if config.ConsumerID != "" {
		attributes["consumer-id"] = config.ConsumerID
	}
	if config.GroupID != "" {
		attributes["group-id"] = config.GroupID
	}
	if config.OwnerID != "" {
		attributes["owner-id"] = config.OwnerID
	}
	if config.OwnerEpoch != nil {
		attributes["owner-epoch"] = strconv.FormatInt(*config.OwnerEpoch, 10)
	}
	if config.Connection.EncryptedPassword != "" {
		attributes["encrypted-password"] = config.Connection.EncryptedPassword
	}
	request, err := protocol.EncodeHandshake(attributes)
	if err != nil {
		return nil, err
	}
	response, err := provider.sendLocked(openingCtx, request)
	if err != nil {
		return nil, err
	}
	identity, err := protocol.DecodeHandshake(response)
	if err != nil {
		return nil, err
	}
	if identity.ConsumerID == "" || identity.GroupID == "" {
		return nil, errors.New("subscription: handshake returned no consumer or group identity")
	}
	provider.dataNodeID = identity.DataNodeID
	provider.consumerID = identity.ConsumerID
	provider.groupID = identity.GroupID
	return provider, nil
}

func (p *subscriptionProvider) negotiateFrameLimit(ctx context.Context) error {
	var properties *rpc.ServerProperties
	err := p.connection.Call(ctx, func() error {
		var callErr error
		properties, callErr = p.connection.Client().GetProperties(ctx)
		return callErr
	})
	if err != nil {
		var applicationError thrift.TApplicationException
		if !errors.As(err, &applicationError) || applicationError.TypeId() != thrift.UNKNOWN_METHOD {
			return err
		}
	}
	if properties != nil && properties.IsSetThriftMaxFrameSize() {
		serverLimit := int(properties.GetThriftMaxFrameSize()) - subscriptionFrameReserve
		if properties.GetThriftMaxFrameSize() > 0 && serverLimit < p.bodyLimit {
			p.bodyLimit = serverLimit
		}
	}
	if p.bodyLimit <= 0 {
		return errors.New("subscription: negotiated frame size cannot hold a subscription request")
	}
	return nil
}

func (p *subscriptionProvider) acquire(ctx context.Context) error {
	if ctx == nil {
		return errors.New("subscription: context must not be nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case p.operations <- struct{}{}:
		if p.connection.Closed() {
			<-p.operations
			return subscription.ErrClosed
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *subscriptionProvider) call(ctx context.Context, request *rpc.TPipeSubscribeReq) (*rpc.TPipeSubscribeResp, error) {
	if err := p.acquire(ctx); err != nil {
		return nil, err
	}
	defer func() { <-p.operations }()
	return p.sendLocked(ctx, request)
}

func (p *subscriptionProvider) sendLocked(ctx context.Context, request *rpc.TPipeSubscribeReq) (*rpc.TPipeSubscribeResp, error) {
	if request == nil {
		return nil, errors.New("subscription: request must not be nil")
	}
	if len(request.Body) <= p.bodyLimit {
		return p.sendOne(ctx, request)
	}
	if request.Version != protocol.RequestVersion || request.Type <= protocol.RequestHandshake || request.Type >= protocol.RequestSlice || request.Type == protocol.RequestClose {
		return nil, errors.New("subscription: request exceeds the negotiated frame limit and cannot be sliced")
	}
	if len(request.Body) > int(^uint32(0)>>1) {
		return nil, errors.New("subscription: request body exceeds the protocol length limit")
	}
	count := (len(request.Body)-1)/p.bodyLimit + 1
	orderID := subscriptionSliceOrder.Add(1)
	var response *rpc.TPipeSubscribeResp
	for index := 0; index < count; index++ {
		start := index*p.bodyLimit
		end := start + min(p.bodyLimit, len(request.Body)-start)
		slice, err := protocol.EncodeSlice(request, orderID, int32(index), int32(count), request.Body[start:end])
		if err != nil {
			return nil, err
		}
		response, err = p.sendOne(ctx, slice)
		if err != nil {
			return nil, err
		}
	}
	return response, nil
}

func (p *subscriptionProvider) sendOne(ctx context.Context, request *rpc.TPipeSubscribeReq) (*rpc.TPipeSubscribeResp, error) {
	var response *rpc.TPipeSubscribeResp
	if err := p.connection.Call(ctx, func() error {
		var callErr error
		response, callErr = p.connection.Client().PipeSubscribe(ctx, request)
		return callErr
	}); err != nil {
		return nil, err
	}
	if response == nil {
		_ = p.connection.Abort()
		return nil, errors.New("subscription: server returned no subscription response")
	}
	if response.Status == nil {
		_ = p.connection.Abort()
		return nil, errors.New("subscription: server returned no subscription status")
	}
	if err := verifySubscriptionProviderStatus(response.Status); err != nil {
		var statusErr *subscriptionStatusError
		if errors.As(err, &statusErr) && statusErr.Code == client.SubscriptionConnectionReplaced {
			_ = p.connection.Abort()
		}
		return nil, err
	}
	if err := protocol.ValidateResponse(response); err != nil {
		_ = p.connection.Abort()
		return nil, err
	}
	return response, nil
}

func (p *subscriptionProvider) subscribe(ctx context.Context, topics []string, unsubscribe bool) (map[string]map[string]string, error) {
	request, err := protocol.EncodeSubscribe(topics, unsubscribe)
	if err != nil {
		return nil, err
	}
	response, err := p.call(ctx, request)
	if err != nil {
		return nil, err
	}
	return protocol.DecodeTopics(response)
}

func (p *subscriptionProvider) heartbeat(ctx context.Context, buffered []payload.CommitContext) (protocol.Heartbeat, error) {
	if err := p.acquire(ctx); err != nil {
		return protocol.Heartbeat{}, err
	}
	defer func() { <-p.operations }()
	batches, err := partitionSubscriptionContexts(buffered, p.bodyLimit, 4)
	if err != nil {
		return protocol.Heartbeat{}, err
	}
	var result protocol.Heartbeat
	var terminated []string
	seen := make(map[string]bool)
	for _, batch := range batches {
		request, err := protocol.EncodeHeartbeat(batch)
		if err != nil {
			return result, err
		}
		response, err := p.sendLocked(ctx, request)
		if err != nil {
			return result, err
		}
		result, err = protocol.DecodeHeartbeat(response)
		if err != nil {
			return result, err
		}
		for _, topic := range result.Unsubscribe {
			if !seen[topic] {
				seen[topic] = true
				terminated = append(terminated, topic)
			}
		}
	}
	result.Unsubscribe = terminated
	return result, nil
}

// commit returns every accepted result even when a later batch fails. The
// consumer must apply that progress before retrying or reporting the error.
func (p *subscriptionProvider) commit(ctx context.Context, contexts []payload.CommitContext, nack bool) (protocol.CommitResult, error) {
	result := protocol.CommitResult{Progress: make(map[string]payload.TopicProgress)}
	if err := p.acquire(ctx); err != nil {
		return result, err
	}
	defer func() { <-p.operations }()
	limit := p.bodyLimit/3
	if limit < 1 {
		limit = 1
	}
	batches, err := partitionSubscriptionContexts(contexts, limit, 5)
	if err != nil {
		return result, err
	}
	for _, batch := range batches {
		request, err := protocol.EncodeCommit(batch, nack)
		if err != nil {
			return result, err
		}
		response, err := p.sendLocked(ctx, request)
		if err != nil {
			return result, err
		}
		completed, err := protocol.DecodeCommit(response)
		if err != nil {
			return result, err
		}
		result.Accepted = append(result.Accepted, completed.Accepted...)
		for topic, progress := range completed.Progress {
			if result.Progress[topic] == nil {
				result.Progress[topic] = make(payload.TopicProgress)
			}
			for region, writers := range progress {
				result.Progress[topic][region] = writers
			}
		}
	}
	return result, nil
}

func (p *subscriptionProvider) seek(ctx context.Context, topic string, kind int16, progress payload.TopicProgress) error {
	request, err := protocol.EncodeSeek(topic, kind, 0, progress)
	if err != nil {
		return err
	}
	_, err = p.call(ctx, request)
	return err
}

func (p *subscriptionProvider) poll(ctx context.Context, topics []string, timeoutMs, maxBytes int64, progress map[string]payload.TopicProgress) ([]protocol.PollResponse, error) {
	request, err := protocol.EncodePoll(topics, timeoutMs, maxBytes, progress)
	if err != nil {
		return nil, err
	}
	return p.pollPayloads(ctx, request)
}

func (p *subscriptionProvider) pollTablets(ctx context.Context, commitContext payload.CommitContext, offset int32, timeoutMs, maxBytes int64) ([]protocol.PollResponse, error) {
	request, err := protocol.EncodePollTablets(commitContext, offset, timeoutMs, maxBytes)
	if err != nil {
		return nil, err
	}
	return p.pollPayloads(ctx, request)
}

func (p *subscriptionProvider) pollPayloads(ctx context.Context, request *rpc.TPipeSubscribeReq) ([]protocol.PollResponse, error) {
	response, err := p.call(ctx, request)
	if err != nil {
		var statusErr *subscriptionStatusError
		if errors.As(err, &statusErr) && statusErr.Code == client.SubscriptionPollError {
			// Broker loss is a server-reported connection failure. Rebuilding the
			// provider and handshaking again is required before another poll.
			group := "[" + p.groupID + "]"
			if strings.Contains(statusErr.Message, "no broker bound to consumer group " + group) ||
				strings.Contains(statusErr.Message, "broker bound to consumer group " + group + " does not exist") ||
				strings.Contains(statusErr.Message, "绑定到消费者组 " + group + " 的 Broker 不存在") {
				_ = p.connection.Abort()
			}
		}
		return nil, err
	}
	return protocol.DecodePollResponses(response.Body)
}

// close preserves subscriptions during fenced/recovery teardown. A regular close
// sends the subscription CLOSE before closing the authenticated session.
func (p *subscriptionProvider) close(ctx context.Context, preserveSubscriptions bool) error {
	if preserveSubscriptions {
		return p.connection.Abort()
	}
	if p.connection.Closed() {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return errors.Join(err, p.connection.Abort())
	}
	select {
	case p.operations <- struct{}{}:
		defer func() { <-p.operations }()
	case <-ctx.Done():
		return errors.Join(ctx.Err(), p.connection.Abort())
	}
	_, requestErr := p.sendLocked(ctx, protocol.EncodeClose())
	return errors.Join(requestErr, p.connection.Close(ctx))
}

func partitionSubscriptionContexts(contexts []payload.CommitContext, limit, fixed int) ([][]payload.CommitContext, error) {
	if len(contexts) == 0 {
		return [][]payload.CommitContext{nil}, nil
	}
	var batches [][]payload.CommitContext
	start, size := 0, fixed
	for index, commitContext := range contexts {
		writer := &wire.Writer{}
		protocol.WriteCommitContext(writer, commitContext)
		body, err := writer.Bytes()
		if err != nil {
			return nil, err
		}
		if index > start && size + len(body) > limit {
			batches = append(batches, contexts[start:index])
			start, size = index, fixed
		}
		size += len(body)
	}
	return append(batches, contexts[start:]), nil
}

type subscriptionStatusError = subscription.SubscriptionError

func verifySubscriptionProviderStatus(status *common.TSStatus) error {
	if status == nil {
		return errors.New("subscription: server returned no status")
	}
	if status.Code == client.SuccessStatus {
		return nil
	}
	failure := &subscription.SubscriptionError{Code: status.Code, Message: status.GetMessage()}
	switch status.Code {
	case client.SubscriptionHandshakeError,
		client.SubscriptionHeartbeatError,
		client.SubscriptionPollError,
		client.SubscriptionCommitError,
		client.SubscriptionCloseError,
		client.SubscriptionSubscribeError,
		client.SubscriptionUnsubscribeError,
		client.SubscriptionSeekError:
		failure.Kind = subscription.ErrorKindRuntimeNonCritical
	case client.SubscriptionPipeTimeoutError:
		failure.Kind = subscription.ErrorKindPipeTimeout
	case client.SubscriptionConnectionReplaced:
		failure.Kind = subscription.ErrorKindConnection
	case client.SubscriptionOwnerFenced,
		client.SubscriptionOwnerRequired,
		client.SubscriptionOwnerEpochRequired,
		client.SubscriptionOwnerLeaseExpired,
		client.SubscriptionOwnerEpochConflict:
		failure.Kind = subscription.ErrorKindOwnerFenced
		failure.Cause = subscription.ErrOwnerFenced
	case client.SubscriptionConsumerFenced:
		failure.Kind = subscription.ErrorKindConsumerFenced
		failure.Cause = subscription.ErrConsumerFenced
		failure.Critical = true
	default:
		failure.Kind = subscription.ErrorKindRuntimeCritical
		failure.Critical = true
	}
	return failure
}

func classifyAndRedactConnectionError(err error, secrets ...string) error {
	var status *subscription.SubscriptionError
	if errors.As(err, &status) {
		copy := *status
		copy.Message = redactSubscriptionSecrets(copy.Message, secrets)
		return &copy
	}
	return &subscription.SubscriptionError{
		Kind: subscription.ErrorKindConnection,
		Message: redactSubscriptionSecrets(err.Error(), secrets),
		Cause: err,
	}
}

func redactSubscriptionSecrets(message string, secrets []string) string {
	for _, secret := range secrets {
		if secret != "" {
			message = strings.ReplaceAll(message, secret, "[REDACTED]")
		}
	}
	return message
}
