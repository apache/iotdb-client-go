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
	"github.com/apache/iotdb-client-go/v2/client"
	"github.com/apache/iotdb-client-go/v2/subscription"
	"github.com/apache/iotdb-client-go/v2/subscription/internal/identifier"
	"github.com/apache/iotdb-client-go/v2/subscription/payload"
	messageprocessor "github.com/apache/iotdb-client-go/v2/subscription/processor"

	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	subscriptionTopicModeKey             = "mode"
	subscriptionTopicModeSnapshot        = "snapshot"
	subscriptionTopicRetainProgressKey   = "retain.progress.after.unsubscribe"
	subscriptionTopicRetainProgressValue = "true"
)

var subscriptionInstanceEpoch atomic.Int64

// NewSubscriptionPullConsumer copies and validates config without opening a
// connection. Start with DefaultSubscriptionPullConsumerConfig for root/root
// authentication defaults. Call Open before subscribing or polling; canceling
// Open's context after it returns does not stop the consumer's background work.
func NewSubscriptionPullConsumer(config SubscriptionPullConsumerConfig) (SubscriptionPullConsumer, error) {
	resolved, err := normalizeSubscriptionPullConsumerConfig(config)
	if err != nil {
		return nil, err
	}
	consumer, err := newSubscriptionPullConsumer(resolved)
	if err != nil {
		return nil, err
	}
	return consumer, nil
}

// newSubscriptionPullConsumer accepts normalized configuration so push consumers
// can reuse the connection and progress engine without parsing identities twice.
func newSubscriptionPullConsumer(resolved SubscriptionPullConsumerConfig) (*subscriptionPullConsumer, error) {
	var suffix [8]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return nil, fmt.Errorf("subscription: generate consumer instance identity: %w", err)
	}
	var epoch int64
	for {
		previous := subscriptionInstanceEpoch.Load()
		epoch = max(time.Now().UnixMilli(), previous+1)
		if subscriptionInstanceEpoch.CompareAndSwap(previous, epoch) {
			break
		}
	}
	consumer := &subscriptionPullConsumer{
		config:                       resolved,
		instanceID:                   fmt.Sprintf("%016x-%s", epoch, hex.EncodeToString(suffix[:])),
		flow:                         make(chan struct{}, 1),
		progressOperations:           make(chan struct{}, 1),
		syncGate:                     make(chan struct{}, 1),
		recovery:                     make(chan struct{}, 1),
		providers:                    make(map[int32]*subscriptionProvider),
		endpoints:                    make(map[int32]string),
		topics:                       make(map[string]map[string]string),
		heartbeatBusy:                make(map[*subscriptionProvider]bool),
		positions:                    make(map[string]payload.TopicProgress),
		committed:                    make(map[string]payload.TopicProgress),
		uncommitted:                  make(map[subscriptionCommitKey]subscriptionPendingCommit),
		uncommittedDeadlines:         make(subscriptionCommitDeadlineHeap, 0),
		redirectAcks:                 make(map[subscriptionCommitKey]payload.CommitContext),
		pendingTerminations:          make(map[string]struct{}),
		pendingUnsubscribes:          make(map[string]struct{}),
		terminatedDataNodeIDsByTopic: make(map[string]map[int32]struct{}),
		identity: SubscriptionConsumerInfo{
			ConsumerID: resolved.Consumer.ConsumerID,
			GroupID:    resolved.Consumer.GroupID,
			OwnerID:    resolved.Consumer.OwnerID,
			OwnerEpoch: resolved.Consumer.OwnerEpoch,
		},
	}
	return consumer, nil
}

type subscriptionPullConsumer struct {
	config                       SubscriptionPullConsumerConfig
	instanceID                   string
	mu                           sync.RWMutex
	flow                         chan struct{}
	progressOperations           chan struct{}
	syncGate                     chan struct{}
	recovery                     chan struct{}
	workers                      sync.WaitGroup
	providers                    map[int32]*subscriptionProvider
	endpoints                    map[int32]string
	topics                       map[string]map[string]string
	identity                     SubscriptionConsumerInfo
	opened                       bool
	released                     bool
	closing                      bool
	fenced                       error
	generation                   uint64
	topicRevision                uint64
	lifeCtx                      context.Context
	lifeCancel                   context.CancelFunc
	openingCancel                context.CancelFunc
	flowCancel                   context.CancelFunc
	heartbeatBusy                map[*subscriptionProvider]bool
	processors                   []messageprocessor.SubscriptionMessageProcessor
	drained                      []payload.SubscriptionMessage
	bufferedContexts             []payload.CommitContext
	positions                    map[string]payload.TopicProgress
	committed                    map[string]payload.TopicProgress
	uncommitted                  map[subscriptionCommitKey]subscriptionPendingCommit
	uncommittedDeadlines         subscriptionCommitDeadlineHeap
	redirectAcks                 map[subscriptionCommitKey]payload.CommitContext
	pendingTerminations          map[string]struct{}
	pendingUnsubscribes          map[string]struct{}
	terminatedDataNodeIDsByTopic map[string]map[int32]struct{}
	topologyGeneration           uint64
	lastAutoCommitError          error
	watermark                    int64
	hasWatermark                 bool
	pendingCritical              error
	pollCursor                   uint64
}

func (c *subscriptionPullConsumer) acquireFlow(ctx context.Context) error {
	if ctx == nil {
		return errors.New("subscription: context must not be nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case c.flow <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *subscriptionPullConsumer) releaseFlow() {
	<-c.flow
}

func (c *subscriptionPullConsumer) stateErrorLocked(allowClosing bool) error {
	if c.fenced != nil {
		return c.fenced
	}
	if c.released || (c.closing && !allowClosing) {
		return subscription.ErrClosed
	}
	if !c.opened {
		return subscription.ErrNotOpen
	}
	return nil
}

func (c *subscriptionPullConsumer) checkOpen() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.stateErrorLocked(false)
}

func (c *subscriptionPullConsumer) beginOperation(ctx context.Context) (context.Context, func(), error) {
	return c.beginQueuedOperation(ctx, ctx)
}

// beginQueuedOperation includes time spent waiting for the foreground gate in a
// poll's budget, while keeping the caller/lifetime context separately observable.
func (c *subscriptionPullConsumer) beginQueuedOperation(ctx, queueCtx context.Context) (context.Context, func(), error) {
	if ctx == nil {
		return nil, nil, errors.New("subscription: context must not be nil")
	}
	if err := c.acquireFlow(queueCtx); err != nil {
		return nil, nil, err
	}
	return c.beginAcquiredOperation(ctx)
}

func (c *subscriptionPullConsumer) beginNonblockingOperation(ctx context.Context) (context.Context, func(), bool, error) {
	if ctx == nil {
		return nil, nil, false, errors.New("subscription: context must not be nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, false, err
	}
	select {
	case c.flow <- struct{}{}:
		operationCtx, finish, err := c.beginAcquiredOperation(ctx)
		return operationCtx, finish, err == nil, err
	default:
		return nil, nil, false, c.checkOpen()
	}
}

func (c *subscriptionPullConsumer) beginAcquiredOperation(ctx context.Context) (context.Context, func(), error) {
	if err := ctx.Err(); err != nil {
		c.releaseFlow()
		return nil, nil, err
	}
	c.mu.Lock()
	if err := c.stateErrorLocked(false); err != nil {
		c.mu.Unlock()
		c.releaseFlow()
		return nil, nil, err
	}
	operationCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.lifeCtx, cancel)
	c.flowCancel = cancel
	c.mu.Unlock()
	return operationCtx, func() {
		stop()
		cancel()
		c.mu.Lock()
		c.flowCancel = nil
		c.mu.Unlock()
		c.releaseFlow()
	}, nil
}

// linkContext registers a concurrent operation before releasing the state lock,
// so Close cannot race a Wait with a late WaitGroup.Add.
func (c *subscriptionPullConsumer) linkContext(ctx context.Context) (context.Context, func(), error) {
	if ctx == nil {
		return nil, nil, errors.New("subscription: context must not be nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	c.mu.Lock()
	if err := c.stateErrorLocked(false); err != nil {
		c.mu.Unlock()
		return nil, nil, err
	}
	operationCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.lifeCtx, cancel)
	c.workers.Add(1)
	c.mu.Unlock()
	return operationCtx, func() {
		stop()
		cancel()
		c.workers.Done()
	}, nil
}

func (c *subscriptionPullConsumer) Open(ctx context.Context) error {
	if err := c.acquireFlow(ctx); err != nil {
		return err
	}
	defer c.releaseFlow()
	c.mu.Lock()
	if c.fenced != nil || c.released || c.closing {
		err := c.fenced
		if err == nil {
			err = subscription.ErrClosed
		}
		c.mu.Unlock()
		return err
	}
	if c.opened {
		c.mu.Unlock()
		return nil
	}
	openingCtx, cancel := context.WithCancel(ctx)
	c.openingCancel = cancel
	generation := c.generation
	c.mu.Unlock()
	defer cancel()
	err := c.recoverProviders(openingCtx)
	c.mu.Lock()
	c.openingCancel = nil
	if c.generation != generation || c.closing || c.released {
		err = subscription.ErrClosed
	}
	if err == nil {
		err = openingCtx.Err()
	}
	if err == nil && c.fenced != nil {
		err = c.fenced
	}
	if err != nil {
		providers := c.detachProvidersLocked()
		c.mu.Unlock()
		for _, provider := range providers {
			_ = provider.connection.Abort()
		}
		return err
	}
	c.lifeCtx, c.lifeCancel = context.WithCancel(context.Background())
	c.opened = true
	c.workers.Add(3)
	c.mu.Unlock()
	go c.runHeartbeatScheduler()
	go c.runEndpointSync()
	go c.runAutoCommit()
	return nil
}

func (c *subscriptionPullConsumer) Close() error {
	c.mu.Lock()
	if c.released || c.closing {
		c.mu.Unlock()
		return nil
	}
	c.closing = true
	c.generation++
	if c.openingCancel != nil {
		c.openingCancel()
	}
	if c.flowCancel != nil {
		c.flowCancel()
	}
	c.mu.Unlock()
	// The canceled foreground operation will release its gate after its RPC
	// unwinds. Processor callbacks are serialized through the same gate.
	c.flow <- struct{}{}
	defer c.releaseFlow()
	c.mu.RLock()
	opened, fenced := c.opened, c.fenced
	c.mu.RUnlock()
	if opened && fenced == nil {
		drained, err := c.drainForClose()
		if err != nil {
			c.mu.Lock()
			c.closing = false
			c.mu.Unlock()
			return err
		}
		if !*c.config.AutoCommit && (len(drained) != 0 || c.processorBufferedCount() != 0) {
			c.drained = append(c.drained, drained...)
			c.refreshBufferedContexts()
			c.mu.Lock()
			c.closing = false
			c.mu.Unlock()
			return errors.New("subscription: drain buffered messages, process and commit them, then retry Close")
		}
		if *c.config.AutoCommit {
			c.trackDelivered(drained)
		}
	}
	c.mu.Lock()
	if c.lifeCancel != nil {
		c.lifeCancel()
	}
	c.mu.Unlock()
	c.workers.Wait()
	ctx, cancel := context.WithTimeout(context.Background(), min(c.config.Consumer.Connection.ConnectTimeout, 5*time.Second))
	defer cancel()
	var failures []error
	if opened && fenced == nil && *c.config.AutoCommit {
		if err := c.commitUniqueContexts(ctx, c.pendingCommitContexts(true, time.Now()), false); err != nil {
			failures = append(failures, err)
		}
	}
	c.mu.Lock()
	fenced = c.fenced
	providers := c.detachProvidersLocked()
	c.opened = false
	c.released = true
	c.closing = false
	c.mu.Unlock()
	results := make(chan error, len(providers))
	for _, provider := range providers {
		go func(provider *subscriptionProvider) {
			results <- provider.close(ctx, fenced != nil)
		}(provider)
	}
	for range providers {
		if err := <-results; err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func (c *subscriptionPullConsumer) Info() SubscriptionConsumerInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	info := c.identity
	if info.OwnerEpoch != nil {
		epoch := *info.OwnerEpoch
		info.OwnerEpoch = &epoch
	}
	return info
}

func (c *subscriptionPullConsumer) LatestWatermark() (int64, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.watermark, c.hasWatermark
}

func (c *subscriptionPullConsumer) AllConsumed() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.topics) == 0
}

func (c *subscriptionPullConsumer) LastAutoCommitError() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastAutoCommitError
}

type subscriptionTermination struct {
	topic      string
	dataNodeID int32
}

func (c *subscriptionPullConsumer) stagePendingTerminations(
	terminations []subscriptionTermination,
) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stagePendingTerminationsLocked(terminations)
}

func (c *subscriptionPullConsumer) stagePendingTerminationsLocked(
	terminations []subscriptionTermination,
) {
	for _, termination := range terminations {
		topic := termination.topic
		if topic == "" {
			continue
		}
		if _, subscribed := c.topics[topic]; !subscribed {
			delete(c.terminatedDataNodeIDsByTopic, topic)
			continue
		}
		if c.isRetainedSnapshotTopicLocked(topic) {
			if termination.dataNodeID >= 0 {
				terminated := c.terminatedDataNodeIDsByTopic[topic]
				if terminated == nil {
					terminated = make(map[int32]struct{})
					c.terminatedDataNodeIDsByTopic[topic] = terminated
				}
				terminated[termination.dataNodeID] = struct{}{}
			}
			if !c.hasAllExpectedDataNodeIDsLocked(topic) {
				continue
			}
		}
		c.pendingTerminations[topic] = struct{}{}
		c.pendingUnsubscribes[topic] = struct{}{}
	}
}

func (c *subscriptionPullConsumer) isRetainedSnapshotTopicLocked(topic string) bool {
	attributes, exists := c.topics[topic]
	if !exists || attributes == nil {
		return false
	}
	return strings.EqualFold(attributes[subscriptionTopicModeKey], subscriptionTopicModeSnapshot) &&
		strings.EqualFold(
			attributes[subscriptionTopicRetainProgressKey],
			subscriptionTopicRetainProgressValue,
		)
}

func (c *subscriptionPullConsumer) expectedDataNodeIDsLocked() []int32 {
	expected := make(map[int32]struct{}, len(c.endpoints)+len(c.providers))
	for dataNodeID := range c.endpoints {
		expected[dataNodeID] = struct{}{}
	}
	for dataNodeID := range c.providers {
		expected[dataNodeID] = struct{}{}
	}
	ids := make([]int32, 0, len(expected))
	for dataNodeID := range expected {
		ids = append(ids, dataNodeID)
	}
	sort.Slice(ids, func(left, right int) bool { return ids[left] < ids[right] })
	return ids
}

func (c *subscriptionPullConsumer) hasAllExpectedDataNodeIDsLocked(topic string) bool {
	terminated := c.terminatedDataNodeIDsByTopic[topic]
	if len(terminated) == 0 {
		return false
	}
	expected := c.expectedDataNodeIDsLocked()
	if len(expected) == 0 {
		return false
	}
	for _, dataNodeID := range expected {
		if _, exists := terminated[dataNodeID]; !exists {
			return false
		}
	}
	return true
}

// invalidateProviderTopicCompletionsLocked revokes completion observations from
// one DataNode for every topic that its latest heartbeat no longer reports as
// complete. A completion notice is current state, not a permanent property of
// the DataNode.
func (c *subscriptionPullConsumer) invalidateProviderTopicCompletionsLocked(
	dataNodeID int32,
	completedTopics []string,
) {
	completed := make(map[string]struct{}, len(completedTopics))
	for _, topic := range completedTopics {
		completed[topic] = struct{}{}
	}
	for topic, terminated := range c.terminatedDataNodeIDsByTopic {
		if _, subscribed := c.topics[topic]; !subscribed {
			delete(c.terminatedDataNodeIDsByTopic, topic)
			delete(c.pendingTerminations, topic)
			delete(c.pendingUnsubscribes, topic)
			continue
		}
		if _, stillCompleted := completed[topic]; stillCompleted {
			continue
		}
		delete(terminated, dataNodeID)
		if len(terminated) == 0 {
			delete(c.terminatedDataNodeIDsByTopic, topic)
		}
		if !c.hasAllExpectedDataNodeIDsLocked(topic) {
			delete(c.pendingTerminations, topic)
			delete(c.pendingUnsubscribes, topic)
		}
	}
}

func (c *subscriptionPullConsumer) clearTerminatedProviderStatesLocked() {
	clear(c.terminatedDataNodeIDsByTopic)
	for topic := range c.pendingTerminations {
		if c.isRetainedSnapshotTopicLocked(topic) {
			delete(c.pendingTerminations, topic)
			delete(c.pendingUnsubscribes, topic)
		}
	}
}

func (c *subscriptionPullConsumer) observeEndpointLocked(dataNodeID int32, endpoint string) bool {
	if previous, exists := c.endpoints[dataNodeID]; exists && previous == endpoint {
		return false
	}
	if c.endpoints == nil {
		c.endpoints = make(map[int32]string)
	}
	c.endpoints[dataNodeID] = endpoint
	c.topologyGeneration++
	c.clearTerminatedProviderStatesLocked()
	return true
}

func (c *subscriptionPullConsumer) replaceEndpointsLocked(endpoints map[int32]string) bool {
	if sameSubscriptionEndpoints(c.endpoints, endpoints) {
		return false
	}
	c.endpoints = make(map[int32]string, len(endpoints))
	for dataNodeID, endpoint := range endpoints {
		c.endpoints[dataNodeID] = endpoint
	}
	c.topologyGeneration++
	c.clearTerminatedProviderStatesLocked()
	return true
}

func sameSubscriptionEndpoints(left, right map[int32]string) bool {
	if len(left) != len(right) {
		return false
	}
	for dataNodeID, endpoint := range left {
		if rightEndpoint, exists := right[dataNodeID]; !exists || rightEndpoint != endpoint {
			return false
		}
	}
	return true
}

func (c *subscriptionPullConsumer) pendingTerminationTopics() []string {
	c.mu.RLock()
	topics := make([]string, 0, len(c.pendingTerminations))
	for topic := range c.pendingTerminations {
		topics = append(topics, topic)
	}
	c.mu.RUnlock()
	sort.Strings(topics)
	return topics
}

func (c *subscriptionPullConsumer) reconcileTopicStateLocked() {
	for topic := range c.pendingTerminations {
		if _, exists := c.topics[topic]; !exists {
			delete(c.pendingTerminations, topic)
		}
	}
	for topic := range c.pendingUnsubscribes {
		if _, exists := c.topics[topic]; !exists {
			delete(c.pendingUnsubscribes, topic)
		}
	}
	for topic := range c.terminatedDataNodeIDsByTopic {
		if _, exists := c.topics[topic]; !exists {
			delete(c.terminatedDataNodeIDsByTopic, topic)
		}
	}
}

func (c *subscriptionPullConsumer) flushPendingTerminations(ctx context.Context) error {
	topics := c.pendingTerminationTopics()
	if len(topics) == 0 {
		return nil
	}
	return c.changeSubscriptions(ctx, topics, true)
}

func (c *subscriptionPullConsumer) Subscribe(ctx context.Context, topics ...string) error {
	return c.updateSubscriptions(ctx, topics, false)
}

func (c *subscriptionPullConsumer) Unsubscribe(ctx context.Context, topics ...string) error {
	return c.updateSubscriptions(ctx, topics, true)
}

func (c *subscriptionPullConsumer) updateSubscriptions(ctx context.Context, topics []string, unsubscribe bool) error {
	operationCtx, finish, err := c.beginOperation(ctx)
	if err != nil {
		return err
	}
	defer finish()
	parsed := make([]string, 0, len(topics))
	for _, topic := range topics {
		name, err := identifier.Parse(topic)
		if err != nil {
			return err
		}
		parsed = append(parsed, name)
	}
	return c.changeSubscriptions(operationCtx, parsed, unsubscribe)
}

// changeSubscriptions assumes the flow gate is held and names are already in
// protocol form. Heartbeat and termination names must not be parsed a second time.
func (c *subscriptionPullConsumer) changeSubscriptions(ctx context.Context, topics []string, unsubscribe bool) error {
	if err := c.checkOpen(); err != nil {
		return err
	}
	if len(topics) == 0 {
		return nil
	}
	c.mu.Lock()
	c.topicRevision++
	if unsubscribe {
		for _, topic := range topics {
			c.pendingUnsubscribes[topic] = struct{}{}
		}
	}
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.topicRevision++
		c.mu.Unlock()
	}()
	providers := c.providerSnapshot()
	if !hasAvailableSubscriptionProvider(providers) {
		if err := c.recoverProviders(ctx); err != nil {
			return err
		}
		providers = c.providerSnapshot()
	}
	var failures []error
	clearRedirectAcks := func() {
		if !unsubscribe {
			return
		}
		c.mu.Lock()
		for key, commitContext := range c.redirectAcks {
			for _, topic := range topics {
				if commitContext.Topic == topic {
					delete(c.redirectAcks, key)
					break
				}
			}
		}
		c.mu.Unlock()
	}
	for _, provider := range providers {
		if provider.connection.Closed() {
			continue
		}
		current, err := provider.subscribe(ctx, topics, unsubscribe)
		if err == nil {
			c.mu.Lock()
			c.topics = current
			for _, topic := range topics {
				delete(c.pendingTerminations, topic)
				delete(c.pendingUnsubscribes, topic)
				delete(c.terminatedDataNodeIDsByTopic, topic)
			}
			c.reconcileTopicStateLocked()
			c.mu.Unlock()
			clearRedirectAcks()
			return nil
		}
		err = c.handleProviderError(provider, err)
		if errors.Is(err, subscription.ErrConsumerFenced) || errors.Is(err, subscription.ErrOwnerFenced) || ctx.Err() != nil {
			return err
		}
		var status *subscriptionStatusError
		if errors.As(err, &status) && status.Code == client.SubscriptionPipeTimeoutError {
			// As in Java, a pipe procedure can finish after its timeout. A later
			// heartbeat reconciles the server's subscription configuration.
			clearRedirectAcks()
			return nil
		}
		failures = append(failures, err)
	}
	if len(failures) == 0 {
		return errors.New("subscription: no available subscription providers")
	}
	return errors.Join(failures...)
}

type subscriptionProviderSet struct {
	single   *subscriptionProvider
	multiple []*subscriptionProvider
}

func (p subscriptionProviderSet) Len() int {
	if p.single != nil {
		return 1
	}
	return len(p.multiple)
}

func (p subscriptionProviderSet) Provider(index int) *subscriptionProvider {
	if p.single != nil {
		if index == 0 {
			return p.single
		}
		return nil
	}
	if index < 0 || index >= len(p.multiple) {
		return nil
	}
	return p.multiple[index]
}

func (c *subscriptionPullConsumer) pollProviderSet() subscriptionProviderSet {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var single *subscriptionProvider
	count := 0
	for _, provider := range c.providers {
		if provider.connection.Closed() {
			continue
		}
		single = provider
		count++
	}
	if count == 1 {
		return subscriptionProviderSet{single: single}
	}
	if count == 0 {
		return subscriptionProviderSet{}
	}
	providers := make([]*subscriptionProvider, 0, count)
	for _, provider := range c.providers {
		if !provider.connection.Closed() {
			providers = append(providers, provider)
		}
	}
	sort.Slice(providers, func(i, j int) bool { return providers[i].dataNodeID < providers[j].dataNodeID })
	return subscriptionProviderSet{multiple: providers}
}

func (c *subscriptionPullConsumer) providerSnapshot() []*subscriptionProvider {
	c.mu.RLock()
	defer c.mu.RUnlock()
	providers := make([]*subscriptionProvider, 0, len(c.providers))
	for _, provider := range c.providers {
		providers = append(providers, provider)
	}
	if len(providers) > 1 {
		sort.Slice(providers, func(i, j int) bool { return providers[i].dataNodeID < providers[j].dataNodeID })
	}
	return providers
}

func (c *subscriptionPullConsumer) providerForNode(id int32) *subscriptionProvider {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.providers[id]
}

func (c *subscriptionPullConsumer) detachProvidersLocked() []*subscriptionProvider {
	providers := make([]*subscriptionProvider, 0, len(c.providers))
	for _, provider := range c.providers {
		providers = append(providers, provider)
	}
	c.providers = make(map[int32]*subscriptionProvider)
	return providers
}

func hasAvailableSubscriptionProvider(providers []*subscriptionProvider) bool {
	for _, provider := range providers {
		if !provider.connection.Closed() {
			return true
		}
	}
	return false
}
