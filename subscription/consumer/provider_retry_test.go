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
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/apache/iotdb-client-go/v2/client"
	"github.com/apache/iotdb-client-go/v2/common"
	"github.com/apache/iotdb-client-go/v2/rpc"
	"github.com/apache/iotdb-client-go/v2/subscription/internal/protocol"
	"github.com/apache/iotdb-client-go/v2/subscription/internal/wire"
	"github.com/apache/iotdb-client-go/v2/subscription/payload"
)

const fakeSubscriptionResponseVersion int8 = 1

type fakeSubscriptionBehavior struct {
	pollCalls      atomic.Int32
	failPolls      int32
	pollStatusCode int32
}

type subscriptionTestRPCService struct {
	*rpc.IClientRPCServiceClient
	behavior *fakeSubscriptionBehavior
}

var _ rpc.IClientRPCService = (*subscriptionTestRPCService)(nil)

func (f *subscriptionTestRPCService) OpenSession(context.Context, *rpc.TSOpenSessionReq) (*rpc.TSOpenSessionResp, error) {
	sessionID := int64(1)
	return &rpc.TSOpenSessionResp{
		Status:                successSubscriptionStatus(),
		ServerProtocolVersion: rpc.TSProtocolVersion_IOTDB_SERVICE_PROTOCOL_V3,
		SessionId:             &sessionID,
	}, nil
}

func (f *subscriptionTestRPCService) RequestStatementId(context.Context, int64) (int64, error) {
	return 1, nil
}

func (f *subscriptionTestRPCService) GetProperties(context.Context) (*rpc.ServerProperties, error) {
	return rpc.NewServerProperties(), nil
}

func (f *subscriptionTestRPCService) PipeSubscribe(_ context.Context, request *rpc.TPipeSubscribeReq) (*rpc.TPipeSubscribeResp, error) {
	switch request.Type {
	case protocol.RequestHandshake:
		var body wire.Writer
		body.WriteInt32(1)
		body.WriteString("fake-consumer")
		body.WriteString("fake-group")
		value, err := body.Bytes()
		if err != nil {
			return nil, err
		}
		return &rpc.TPipeSubscribeResp{
			Status:  successSubscriptionStatus(),
			Version: fakeSubscriptionResponseVersion,
			Body:    [][]byte{value},
		}, nil
	case protocol.RequestPoll:
		call := f.behavior.pollCalls.Add(1)
		if call <= f.behavior.failPolls {
			return subscriptionResponse(f.behavior.pollStatusCode), nil
		}
		return subscriptionResponse(client.SuccessStatus), nil
	case protocol.RequestClose:
		return subscriptionResponse(client.SuccessStatus), nil
	default:
		return subscriptionResponse(client.SuccessStatus), nil
	}
}

func successSubscriptionStatus() *common.TSStatus {
	return &common.TSStatus{Code: client.SuccessStatus}
}

func subscriptionResponse(code int32) *rpc.TPipeSubscribeResp {
	message := "fake subscription response"
	return &rpc.TPipeSubscribeResp{
		Status:  &common.TSStatus{Code: code, Message: &message},
		Version: fakeSubscriptionResponseVersion,
	}
}

type subscriptionTestServerTransport struct {
	listener net.Listener
	config   *thrift.TConfiguration
}

func (s *subscriptionTestServerTransport) Listen() error { return nil }
func (s *subscriptionTestServerTransport) Accept() (thrift.TTransport, error) {
	connection, err := s.listener.Accept()
	if err != nil {
		return nil, err
	}
	return thrift.NewTSocketFromConnConf(connection, s.config), nil
}
func (s *subscriptionTestServerTransport) Close() error     { return s.listener.Close() }
func (s *subscriptionTestServerTransport) Interrupt() error { return s.listener.Close() }

func startSubscriptionTestServer(t *testing.T, behavior *fakeSubscriptionBehavior) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	config := &thrift.TConfiguration{
		ConnectTimeout: time.Second,
		MaxFrameSize:   DefaultSubscriptionConsumerConfig().Connection.MaxFrameSize,
		MaxMessageSize: DefaultSubscriptionConsumerConfig().Connection.MaxFrameSize,
	}
	handler := &subscriptionTestRPCService{
		IClientRPCServiceClient: rpc.NewIClientRPCServiceClient(nil),
		behavior:                behavior,
	}
	server := thrift.NewTSimpleServer4(
		rpc.NewIClientRPCServiceProcessor(handler),
		&subscriptionTestServerTransport{listener: listener, config: config},
		thrift.NewTFramedTransportFactoryConf(thrift.NewTTransportFactory(), config),
		thrift.NewTBinaryProtocolFactoryConf(config),
	)
	go func() { _ = server.Serve() }()
	t.Cleanup(func() { _ = server.Stop() })
	return listener.Addr().String()
}

func openTestProvider(t *testing.T, endpoint string) *subscriptionProvider {
	t.Helper()
	config := DefaultSubscriptionConsumerConfig()
	config.Connection.NodeURLs = []string{endpoint}
	config.ConsumerID = "consumer"
	config.GroupID = "group"
	provider, err := openSubscriptionProvider(context.Background(), config, endpoint, "test-instance")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = provider.connection.Abort() })
	return provider
}

func testPullConsumer(ctx context.Context, provider *subscriptionProvider) *subscriptionPullConsumer {
	config := DefaultSubscriptionPullConsumerConfig()
	return &subscriptionPullConsumer{
		config:              config,
		flow:                make(chan struct{}, 1),
		progressOperations:  make(chan struct{}, 1),
		syncGate:            make(chan struct{}, 1),
		recovery:            make(chan struct{}, 1),
		providers:           map[int32]*subscriptionProvider{provider.dataNodeID: provider},
		endpoints:           map[int32]string{provider.dataNodeID: provider.endpoint},
		topics:              map[string]map[string]string{"topic": {}},
		heartbeatBusy:       make(map[*subscriptionProvider]bool),
		positions:           make(map[string]payload.TopicProgress),
		committed:           make(map[string]payload.TopicProgress),
		uncommitted:         make(map[subscriptionCommitKey]subscriptionPendingCommit),
		redirectAcks:        make(map[subscriptionCommitKey]payload.CommitContext),
		pendingTerminations: make(map[string]struct{}),
		pendingUnsubscribes: make(map[string]struct{}),
		lifeCtx:             ctx,
		opened:              true,
	}
}

func TestPollRetriesReplacedConnection(t *testing.T) {
	status := &fakeSubscriptionBehavior{failPolls: 1, pollStatusCode: client.SubscriptionConnectionReplaced}
	endpoint := startSubscriptionTestServer(t, status)
	provider := openTestProvider(t, endpoint)
	consumer := testPullConsumer(context.Background(), provider)

	messages, err := consumer.Poll(context.Background(), time.Second)
	if err != nil || len(messages) != 0 {
		t.Fatalf("Poll() = %d messages, %v; want no messages and no error", len(messages), err)
	}
	if !provider.connection.Closed() {
		t.Fatal("replaced connection remained open after a retryable poll failure")
	}
	select {
	case <-consumer.recovery:
	default:
		t.Fatal("replaced connection did not request provider recovery")
	}
}

func TestPushContinuesAfterCriticalPollError(t *testing.T) {
	status := &fakeSubscriptionBehavior{failPolls: 1, pollStatusCode: client.SubscriptionMissingConsumer}
	endpoint := startSubscriptionTestServer(t, status)
	provider := openTestProvider(t, endpoint)
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pull := testPullConsumer(runCtx, provider)
	push := &subscriptionPushConsumer{
		config: SubscriptionPushConsumerConfig{
			Consumer:     pull.config.Consumer,
			PollTimeout:  time.Second,
			PollInterval: 10 * time.Millisecond,
		},
		consumer:    pull,
		operations:  make(chan struct{}, 1),
		generations: make(map[string]uint64),
		errors:      make(chan error, 1),
	}
	done := make(chan struct{})
	go func() {
		push.runDelivery(runCtx)
		close(done)
	}()
	deadline := time.Now().Add(2 * time.Second)
	for status.pollCalls.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if calls := status.pollCalls.Load(); calls < 2 {
		cancel()
		<-done
		t.Fatalf("Push made %d poll calls, want at least 2", calls)
	}
	cancel()
	<-done
	select {
	case err, ok := <-push.errors:
		if ok || err != nil {
			t.Fatalf("Push reported a terminal critical error: %v", err)
		}
	default:
		t.Fatal("Push error channel was not closed")
	}
}
