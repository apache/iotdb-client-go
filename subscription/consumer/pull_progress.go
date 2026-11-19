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
	"github.com/apache/iotdb-client-go/v2/subscription"
	"github.com/apache/iotdb-client-go/v2/subscription/payload"
	"github.com/apache/iotdb-client-go/v2/subscription/internal/protocol"
	"github.com/apache/iotdb-client-go/v2/subscription/internal/record"
	"github.com/apache/iotdb-client-go/v2/subscription/internal/identifier"
	"github.com/apache/iotdb-client-go/v2/client"

	"container/heap"
	"context"
	"errors"
	"fmt"
	"sort"
	"time"
)

// subscriptionCommitKey includes every identity field without borrowing a
// mutable Writer pointer, so accepted contexts remove exactly their delivery.
type subscriptionCommitKey struct {
	DataNodeID int32
	RebootTimes int32
	Topic string
	GroupID string
	CommitID int64
	SeekGeneration int64
	RegionID string
	PhysicalTime int64
	HasWriter bool
	Writer payload.WriterPosition
}

type subscriptionPendingCommit struct {
	Context payload.CommitContext
	ExpiresAt time.Time
}

type subscriptionCommitDeadline struct {
	key subscriptionCommitKey
	ExpiresAt time.Time
}

type subscriptionCommitDeadlineHeap []subscriptionCommitDeadline

const subscriptionDeadlineCompactionSlack = 64

func (h subscriptionCommitDeadlineHeap) Len() int { return len(h) }
func (h subscriptionCommitDeadlineHeap) Less(i, j int) bool {
	if h[i].ExpiresAt.Equal(h[j].ExpiresAt) {
		return h[i].key.CommitID < h[j].key.CommitID
	}
	return h[i].ExpiresAt.Before(h[j].ExpiresAt)
}
func (h subscriptionCommitDeadlineHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *subscriptionCommitDeadlineHeap) Push(value any) {
	*h = append(*h, value.(subscriptionCommitDeadline))
}
func (h *subscriptionCommitDeadlineHeap) Pop() any {
	old := *h
	last := len(old) - 1
	value := old[last]
	old[last] = subscriptionCommitDeadline{}
	*h = old[:last]
	return value
}

func subscriptionContextKey(value payload.CommitContext) subscriptionCommitKey {
	key := subscriptionCommitKey{
		DataNodeID: value.DataNodeID,
		RebootTimes: value.RebootTimes,
		Topic: value.Topic,
		GroupID: value.GroupID,
		CommitID: value.CommitID,
		SeekGeneration: value.SeekGeneration,
		RegionID: value.RegionID,
		PhysicalTime: value.PhysicalTime,
	}
	if value.Writer != nil {
		key.HasWriter = true
		key.Writer = *value.Writer
	}
	return key
}

// compactUncommittedDeadlinesLocked bounds stale heap entries left by early
// acknowledgments and seeks. It runs only at safe commit/seek boundaries, so
// deadlines already popped for an in-progress auto-commit are not duplicated.
func (c *subscriptionPullConsumer) compactUncommittedDeadlinesLocked() {
	if len(c.uncommittedDeadlines) <= 2*len(c.uncommitted)+subscriptionDeadlineCompactionSlack {
		return
	}
	deadlines := make(subscriptionCommitDeadlineHeap, 0, len(c.uncommitted))
	for key, pending := range c.uncommitted {
		deadlines = append(deadlines, subscriptionCommitDeadline{key: key, ExpiresAt: pending.ExpiresAt})
	}
	heap.Init(&deadlines)
	c.uncommittedDeadlines = deadlines
}

func cloneTopicProgress(progress payload.TopicProgress) payload.TopicProgress {
	result := make(payload.TopicProgress, len(progress))
	for region, writers := range progress {
		positions := make(payload.RegionProgress, len(writers))
		for writer, position := range writers {
			positions[writer] = position
		}
		result[region] = positions
	}
	return result
}

func (c *subscriptionPullConsumer) Commit(ctx context.Context, messages ...payload.SubscriptionMessage) error {
	operationCtx, finish, err := c.linkContext(ctx)
	if err != nil {
		return err
	}
	defer finish()
	contexts, err := c.messageCommitContexts(messages)
	if err != nil {
		return err
	}
	return c.commitUniqueContexts(operationCtx, contexts, false)
}

func (c *subscriptionPullConsumer) CommitAsync(ctx context.Context, messages ...payload.SubscriptionMessage) <-chan error {
	result := make(chan error, 1)
	operationCtx, finish, err := c.linkContext(ctx)
	if err != nil {
		result <- err
		close(result)
		return result
	}
	contexts, err := c.messageCommitContexts(messages)
	if err != nil {
		finish()
		result <- err
		close(result)
		return result
	}
	go func() {
		defer finish()
		defer close(result)
		result <- c.commitUniqueContexts(operationCtx, contexts, false)
	}()
	return result
}

func (c *subscriptionPullConsumer) messageCommitContexts(messages []payload.SubscriptionMessage) ([]payload.CommitContext, error) {
	c.mu.RLock()
	groupID := c.identity.GroupID
	c.mu.RUnlock()
	var seen map[subscriptionCommitKey]struct{}
	if len(messages) > 1 {
		seen = make(map[subscriptionCommitKey]struct{}, len(messages))
	}
	contexts := make([]payload.CommitContext, 0, len(messages))
	for _, message := range messages {
		if record.IsNilMessage(message) {
			return nil, errors.New("subscription: cannot commit a nil message")
		}
		if message.Kind() != payload.RecordMessage {
			return nil, subscription.ErrIncompatibleMessage
		}
		commitContext := record.ReadCommitContext(message)
		if commitContext.Topic == "" || commitContext.GroupID != groupID {
			return nil, errors.New("subscription: message does not belong to this consumer group")
		}
		if !record.Committable(commitContext) {
			return nil, errors.New("subscription: message has no committable context")
		}
		if seen == nil {
			contexts = append(contexts, commitContext)
			continue
		}
		key := subscriptionContextKey(commitContext)
		if _, exists := seen[key]; !exists {
			seen[key] = struct{}{}
			contexts = append(contexts, commitContext)
		}
	}
	return contexts, nil
}

func (c *subscriptionPullConsumer) acquireProgress(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case c.progressOperations <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// commitContexts serializes acknowledgment responses with seek state changes,
// but does not occupy the poll flow gate. A retry never repeats accepted entries.
func (c *subscriptionPullConsumer) commitContexts(ctx context.Context, contexts []payload.CommitContext, nack bool) error {
	if err := c.acquireProgress(ctx); err != nil {
		return err
	}
	defer func() { <-c.progressOperations }()
	err := c.commitUniqueContextsLocked(ctx, normalizeCommitContexts(contexts), nack)
	c.mu.Lock()
	c.compactUncommittedDeadlinesLocked()
	c.mu.Unlock()
	return err
}

func (c *subscriptionPullConsumer) commitUniqueContexts(ctx context.Context, contexts []payload.CommitContext, nack bool) error {
	if err := c.acquireProgress(ctx); err != nil {
		return err
	}
	defer func() { <-c.progressOperations }()
	err := c.commitUniqueContextsLocked(ctx, contexts, nack)
	c.mu.Lock()
	c.compactUncommittedDeadlinesLocked()
	c.mu.Unlock()
	return err
}

func normalizeCommitContexts(contexts []payload.CommitContext) []payload.CommitContext {
	if len(contexts) == 0 {
		return nil
	}
	if len(contexts) == 1 {
		if !record.Committable(contexts[0]) {
			return nil
		}
		return []payload.CommitContext{record.CloneCommitContext(contexts[0])}
	}
	unique := make([]payload.CommitContext, 0, len(contexts))
	seen := make(map[subscriptionCommitKey]struct{}, len(contexts))
	for _, commitContext := range contexts {
		if !record.Committable(commitContext) {
			continue
		}
		key := subscriptionContextKey(commitContext)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		unique = append(unique, record.CloneCommitContext(commitContext))
	}
	return unique
}

func (c *subscriptionPullConsumer) commitUniqueContextsLocked(ctx context.Context, contexts []payload.CommitContext, nack bool) error {
	groups := make(map[int32][]payload.CommitContext)
	for _, commitContext := range contexts {
		groups[commitContext.DataNodeID] = append(groups[commitContext.DataNodeID], commitContext)
	}
	var nodes []int
	if len(groups) > 1 {
		nodes = make([]int, 0, len(groups))
		for node := range groups {
			nodes = append(nodes, int(node))
		}
		sort.Ints(nodes)
	} else {
		for node := range groups {
			nodes = []int{int(node)}
		}
	}
	var failures []error
	for _, node := range nodes {
		if err := c.commitNode(ctx, int32(node), groups[int32(node)], nack); err != nil {
			failures = append(failures, err)
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
				errors.Is(err, subscription.ErrConsumerFenced) || errors.Is(err, subscription.ErrOwnerFenced) || subscriptionCriticalCommitError(err) {
				break
			}
		}
	}
	return errors.Join(failures...)
}

func (c *subscriptionPullConsumer) commitNode(ctx context.Context, node int32, contexts []payload.CommitContext, nack bool) error {
	remaining := contexts
	var lastFailure error
	for attempt := 0; len(remaining) != 0; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		c.mu.RLock()
		stateErr := c.stateErrorLocked(true)
		closing := c.closing || c.released
		c.mu.RUnlock()
		if stateErr != nil {
			return stateErr
		}
		provider := c.providerForNode(node)
		var completed protocol.CommitResult
		var failure error
		if provider == nil || provider.connection.Closed() {
			failure = fmt.Errorf("subscription: no available provider for commit node %d", node)
			if !nack {
				c.stageRedirectAcks(remaining)
			}
		} else {
			completed, failure = provider.commit(ctx, remaining, nack)
			if failure != nil {
				failure = c.handleProviderError(provider, failure)
			}
		}
		remaining = c.applyCommitResult(remaining, completed, nack)
		if !nack && (provider == nil || provider.connection.Closed()) {
			c.stageRedirectAcks(remaining)
		}
		if failure != nil {
			lastFailure = failure
			if errors.Is(failure, subscription.ErrOwnerFenced) || errors.Is(failure, subscription.ErrConsumerFenced) ||
				subscriptionCriticalCommitError(failure) || ctx.Err() != nil {
				return failure
			}
			var status *subscriptionStatusError
			if errors.As(failure, &status) && status.Code != client.SubscriptionConnectionReplaced {
				return failure
			}
		}
		if len(remaining) == 0 {
			return nil
		}
		c.mu.RLock()
		closing = c.closing || c.released
		c.mu.RUnlock()
		if !closing && (provider == nil || provider.connection.Closed()) {
			c.requestRecovery()
		}
		if nack || closing || attempt >= 3 {
			break
		}
		if provider == nil || provider.connection.Closed() {
			if err := c.recoverProviders(ctx); err != nil {
				lastFailure = errors.Join(lastFailure, err)
				if errors.Is(err, subscription.ErrConsumerFenced) || errors.Is(err, subscription.ErrOwnerFenced) || ctx.Err() != nil {
					return lastFailure
				}
			}
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}
	return errors.Join(lastFailure, fmt.Errorf("subscription: commit accepted %d of %d contexts", len(contexts)-len(remaining), len(contexts)))
}

func subscriptionCriticalCommitError(err error) bool {
	var status *subscriptionStatusError
	if errors.As(err, &status) {
		return status.Critical
	}
	var payload *protocol.PollError
	return errors.As(err, &payload) && payload.Critical
}

func (c *subscriptionPullConsumer) applyCommitResult(requested []payload.CommitContext, completed protocol.CommitResult, nack bool) []payload.CommitContext {
	accepted := make(map[subscriptionCommitKey]struct{}, len(completed.Accepted))
	for _, commitContext := range completed.Accepted {
		accepted[subscriptionContextKey(commitContext)] = struct{}{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !nack {
		for topic, progress := range completed.Progress {
			mergeSubscriptionProgress(c.committed, topic, progress)
		}
	}
	remaining := make([]payload.CommitContext, 0, len(requested))
	for _, commitContext := range requested {
		key := subscriptionContextKey(commitContext)
		if _, ok := accepted[key]; !ok {
			remaining = append(remaining, commitContext)
			continue
		}
		if !nack {
			delete(c.uncommitted, key)
			delete(c.redirectAcks, key)
		}
	}
	return remaining
}

func (c *subscriptionPullConsumer) stageRedirectAcks(contexts []payload.CommitContext) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, commitContext := range contexts {
		if commitContext.Writer != nil && commitContext.RegionID != "" {
			c.redirectAcks[subscriptionContextKey(commitContext)] = record.CloneCommitContext(commitContext)
		}
	}
}

func (c *subscriptionPullConsumer) flushRedirectAcks(ctx context.Context, messages []payload.SubscriptionMessage) {
	if err := c.acquireProgress(ctx); err != nil {
		return
	}
	defer func() { <-c.progressOperations }()
	if err := c.checkOpen(); err != nil {
		return
	}
	type topicRegion struct { topic, region string }
	targets := make(map[topicRegion]int32)
	for _, message := range messages {
		commitContext := record.ReadCommitContext(message)
		if commitContext.Writer != nil && commitContext.RegionID != "" {
			targets[topicRegion{commitContext.Topic, commitContext.RegionID}] = commitContext.DataNodeID
		}
	}
	for target, node := range targets {
		if ctx.Err() != nil || c.checkOpen() != nil {
			return
		}
		provider := c.providerForNode(node)
		if provider == nil || provider.connection.Closed() {
			continue
		}
		c.mu.RLock()
		var contexts []payload.CommitContext
		for _, pending := range c.redirectAcks {
			if pending.Topic == target.topic && pending.RegionID == target.region {
				contexts = append(contexts, record.CloneCommitContext(pending))
			}
		}
		c.mu.RUnlock()
		if len(contexts) == 0 {
			continue
		}
		completed, err := provider.commit(ctx, contexts, false)
		c.applyCommitResult(contexts, completed, false)
		c.mu.Lock()
		c.compactUncommittedDeadlinesLocked()
		c.mu.Unlock()
		if err != nil {
			c.handleProviderError(provider, err)
		}
	}
}

func (c *subscriptionPullConsumer) trackDelivered(messages []payload.SubscriptionMessage) {
	if c.config.AutoCommit == nil || !*c.config.AutoCommit {
		return
	}
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, message := range messages {
		if message.Kind() != payload.RecordMessage {
			continue
		}
		commitContext := record.ReadCommitContext(message)
		if !record.Committable(commitContext) {
			continue
		}
		key := subscriptionContextKey(commitContext)
		if _, exists := c.uncommitted[key]; !exists {
			expiresAt := now.Add(c.config.AutoCommitInterval)
			c.uncommitted[key] = subscriptionPendingCommit{Context: commitContext, ExpiresAt: expiresAt}
			heap.Push(&c.uncommittedDeadlines, subscriptionCommitDeadline{key: key, ExpiresAt: expiresAt})
		}
	}
}

func (c *subscriptionPullConsumer) pendingCommitContexts(all bool, now time.Time) []payload.CommitContext {
	c.mu.Lock()
	defer c.mu.Unlock()
	if all {
		contexts := make([]payload.CommitContext, 0, len(c.uncommitted))
		for _, pending := range c.uncommitted {
			contexts = append(contexts, pending.Context)
		}
		return contexts
	}
	var contexts []payload.CommitContext
	for c.uncommittedDeadlines.Len() != 0 {
		deadline := c.uncommittedDeadlines[0]
		if now.Before(deadline.ExpiresAt) {
			break
		}
		heap.Pop(&c.uncommittedDeadlines)
		pending, exists := c.uncommitted[deadline.key]
		if !exists || !pending.ExpiresAt.Equal(deadline.ExpiresAt) {
			continue
		}
		contexts = append(contexts, pending.Context)
	}
	return contexts
}

func (c *subscriptionPullConsumer) requeuePendingCommitContexts(contexts []payload.CommitContext, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, commitContext := range contexts {
		key := subscriptionContextKey(commitContext)
		pending, exists := c.uncommitted[key]
		if !exists || pending.ExpiresAt.After(now) {
			continue
		}
		heap.Push(&c.uncommittedDeadlines, subscriptionCommitDeadline{key: key, ExpiresAt: pending.ExpiresAt})
	}
}

func (c *subscriptionPullConsumer) autoCommit(ctx context.Context, now time.Time) {
	if c.config.AutoCommit == nil || !*c.config.AutoCommit {
		return
	}
	if err := c.acquireProgress(ctx); err != nil {
		return
	}
	defer func() { <-c.progressOperations }()
	contexts := c.pendingCommitContexts(false, now)
	if len(contexts) != 0 {
		err := c.commitUniqueContextsLocked(ctx, contexts, false)
		c.mu.Lock()
		c.lastAutoCommitError = err
		c.mu.Unlock()
		c.requeuePendingCommitContexts(contexts, now)
		c.mu.Lock()
		c.compactUncommittedDeadlinesLocked()
		c.mu.Unlock()
	}
}

func (c *subscriptionPullConsumer) advancePositions(messages []payload.SubscriptionMessage) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, message := range messages {
		commitContext := record.ReadCommitContext(message)
		writer, progress, ok := subscriptionContextProgress(commitContext)
		if ok {
			mergeSubscriptionProgress(c.positions, commitContext.Topic, payload.TopicProgress{writer.RegionID: payload.RegionProgress{writer: progress}})
		}
	}
}

func subscriptionContextProgress(commitContext payload.CommitContext) (payload.WriterID, payload.WriterProgress, bool) {
	if commitContext.Writer != nil {
		return commitContext.Writer.ID, commitContext.Writer.Progress, true
	}
	if commitContext.RegionID == "" || commitContext.CommitID < 0 {
		return payload.WriterID{}, payload.WriterProgress{}, false
	}
	return payload.WriterID{RegionID: commitContext.RegionID, NodeID: commitContext.DataNodeID},
		payload.WriterProgress{PhysicalTime: commitContext.PhysicalTime, LocalSeq: commitContext.CommitID}, true
}

func mergeSubscriptionProgress(target map[string]payload.TopicProgress, topic string, progress payload.TopicProgress) {
	if topic == "" {
		return
	}
	for _, writers := range progress {
		for writer, next := range writers {
			if target[topic] == nil {
				target[topic] = make(payload.TopicProgress)
			}
			if target[topic][writer.RegionID] == nil {
				target[topic][writer.RegionID] = make(payload.RegionProgress)
			}
			previous, exists := target[topic][writer.RegionID][writer]
			if !exists || next.PhysicalTime > previous.PhysicalTime ||
				(next.PhysicalTime == previous.PhysicalTime && next.LocalSeq > previous.LocalSeq) {
				target[topic][writer.RegionID][writer] = next
			}
		}
	}
}

func (c *subscriptionPullConsumer) Positions(topic string) (payload.TopicProgress, error) {
	name, err := identifier.Parse(topic)
	if err != nil {
		return nil, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if err := c.stateErrorLocked(false); err != nil {
		return nil, err
	}
	return cloneTopicProgress(c.positions[name]), nil
}

func (c *subscriptionPullConsumer) CommittedPositions(topic string) (payload.TopicProgress, error) {
	name, err := identifier.Parse(topic)
	if err != nil {
		return nil, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if err := c.stateErrorLocked(false); err != nil {
		return nil, err
	}
	return cloneTopicProgress(c.committed[name]), nil
}

func (c *subscriptionPullConsumer) SeekToBeginning(ctx context.Context, topic string) error {
	return c.seekProgress(ctx, topic, protocol.SeekBeginning, nil)
}

func (c *subscriptionPullConsumer) SeekToEnd(ctx context.Context, topic string) error {
	return c.seekProgress(ctx, topic, protocol.SeekEnd, nil)
}

func (c *subscriptionPullConsumer) Seek(ctx context.Context, topic string, progress payload.TopicProgress) error {
	return c.seekProgress(ctx, topic, protocol.SeekProgress, progress)
}

func (c *subscriptionPullConsumer) SeekAfter(ctx context.Context, topic string, progress payload.TopicProgress) error {
	return c.seekProgress(ctx, topic, protocol.SeekAfterProgress, progress)
}

func (c *subscriptionPullConsumer) seekProgress(ctx context.Context, topic string, kind int16, progress payload.TopicProgress) error {
	operationCtx, finish, err := c.beginOperation(ctx)
	if err != nil {
		return err
	}
	defer finish()
	name, err := identifier.Parse(topic)
	if err != nil {
		return err
	}
	if err := c.validateProcessorReset(name); err != nil {
		return err
	}
	if err := c.acquireProgress(operationCtx); err != nil {
		return err
	}
	defer func() { <-c.progressOperations }()
	progress = cloneTopicProgress(progress)
	providers := c.providerSnapshot()
	if !hasAvailableSubscriptionProvider(providers) {
		if err := c.recoverProviders(operationCtx); err != nil {
			return err
		}
		providers = c.providerSnapshot()
	}
	available := 0
	type seekFailure struct {
		provider *subscriptionProvider
		err error
	}
	var providerFailures []seekFailure
	for _, provider := range providers {
		if provider.connection.Closed() {
			continue
		}
		available++
		if err := provider.seek(operationCtx, name, kind, progress); err != nil {
			if operationCtx.Err() != nil {
				return operationCtx.Err()
			}
			providerFailures = append(providerFailures, seekFailure{provider: provider, err: err})
		}
	}
	if available == 0 {
		return errors.New("subscription: no available provider for seek")
	}
	if err := operationCtx.Err(); err != nil {
		return err
	}
	var failures []error
	for _, failure := range providerFailures {
		err := c.handleProviderError(failure.provider, failure.err)
		failures = append(failures, fmt.Errorf("subscription: seek provider %s: %w", failure.provider.endpoint, err))
	}
	if len(failures) != 0 {
		return errors.Join(failures...)
	}
	c.mu.Lock()
	if err := c.stateErrorLocked(false); err != nil {
		c.mu.Unlock()
		return err
	}
	if kind == protocol.SeekBeginning || kind == protocol.SeekEnd {
		delete(c.positions, name)
		delete(c.committed, name)
	} else {
		overlaySubscriptionSeekProgress(c.positions, name, progress)
		overlaySubscriptionSeekProgress(c.committed, name, progress)
	}
	for key := range c.uncommitted {
		if key.Topic == name {
			delete(c.uncommitted, key)
		}
	}
	c.compactUncommittedDeadlinesLocked()
	for key := range c.redirectAcks {
		if key.Topic == name {
			delete(c.redirectAcks, key)
		}
	}
	delete(c.pendingTerminations, name)
	delete(c.pendingUnsubscribes, name)
	c.mu.Unlock()
	c.resetProcessors(name)
	return nil
}

func overlaySubscriptionSeekProgress(target map[string]payload.TopicProgress, topic string, progress payload.TopicProgress) {
	for region, writers := range progress {
		if region == "" || len(writers) == 0 {
			continue
		}
		if target[topic] == nil {
			target[topic] = make(payload.TopicProgress)
		}
		copy := make(payload.RegionProgress, len(writers))
		for writer, position := range writers {
			copy[writer] = position
		}
		target[topic][region] = copy
	}
}
