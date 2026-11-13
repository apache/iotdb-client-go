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
	"github.com/apache/iotdb-client-go/subscription/internal/record"

	"context"
	"errors"
	"fmt"
	"sort"
	"time"
)

func (c *subscriptionPullConsumer) requestRecovery() {
	select {
	case c.recovery <- struct{}{}:
	default:
	}
}

func (c *subscriptionPullConsumer) handleProviderError(provider *subscriptionProvider, failure error) error {
	if failure == nil || errors.Is(failure, subscription.ErrUnsupportedTsFile) {
		return failure
	}
	if errors.Is(failure, subscription.ErrConsumerFenced) {
		c.mu.Lock()
		current := c.providers[provider.dataNodeID] == provider
		if current && c.fenced == nil {
			c.fenced = failure
			if c.lifeCancel != nil {
				c.lifeCancel()
			}
			if c.flowCancel != nil {
				c.flowCancel()
			}
		}
		fenced := c.fenced
		c.mu.Unlock()
		if !current && fenced == nil {
			_ = provider.connection.Abort()
			c.requestRecovery()
			return fmt.Errorf("subscription: obsolete provider %s lost its connection", provider.endpoint)
		}
		return failure
	}
	if provider.connection.Closed() {
		c.requestRecovery()
	}
	return failure
}

func (c *subscriptionPullConsumer) providerConfig() SubscriptionConsumerConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()
	config := c.config.Consumer
	config.ConsumerID = c.identity.ConsumerID
	config.GroupID = c.identity.GroupID
	return config
}

func (c *subscriptionPullConsumer) publishProvider(provider *subscriptionProvider, generation uint64) error {
	c.mu.Lock()
	if c.generation != generation || c.released || c.closing || c.fenced != nil {
		failure := c.fenced
		if failure == nil {
			failure = subscription.ErrClosed
		}
		c.mu.Unlock()
		_ = provider.connection.Abort()
		return failure
	}
	if (c.identity.ConsumerID != "" && c.identity.ConsumerID != provider.consumerID) ||
		(c.identity.GroupID != "" && c.identity.GroupID != provider.groupID) {
		c.mu.Unlock()
		_ = provider.connection.Abort()
		return errors.New("subscription: providers returned inconsistent consumer identities")
	}
	c.identity.ConsumerID = provider.consumerID
	c.identity.GroupID = provider.groupID
	previous := c.providers[provider.dataNodeID]
	c.providers[provider.dataNodeID] = provider
	c.endpoints[provider.dataNodeID] = provider.endpoint
	c.mu.Unlock()
	if previous != nil && previous != provider {
		// Replacement must preserve the server-side consumer and subscriptions.
		_ = previous.connection.Abort()
	}
	return nil
}

// recoverProviders serializes discovery and replacement without taking the flow
// gate. Foreground subscription/seek operations can request recovery while they
// hold that gate; heartbeat callbacks never need it to publish endpoint changes.
func (c *subscriptionPullConsumer) recoverProviders(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case c.syncGate <- struct{}{}:
		defer func() { <-c.syncGate }()
	case <-ctx.Done():
		return ctx.Err()
	}
	c.mu.RLock()
	if c.released || c.closing || c.fenced != nil {
		failure := c.fenced
		if failure == nil {
			failure = subscription.ErrClosed
		}
		c.mu.RUnlock()
		return failure
	}
	generation := c.generation
	c.mu.RUnlock()
	var failures []error
	var discovered map[int32]string
	for _, provider := range c.providerSnapshot() {
		if provider.connection.Closed() {
			continue
		}
		response, _, err := c.providerHeartbeat(ctx, provider)
		if err == nil {
			discovered = response.Endpoints
			break
		}
		err = c.handleProviderError(provider, err)
		if errors.Is(err, subscription.ErrConsumerFenced) || ctx.Err() != nil {
			return err
		}
		failures = append(failures, err)
	}
	if !hasAvailableSubscriptionProvider(c.providerSnapshot()) {
		addresses := append([]string(nil), c.config.Consumer.Connection.NodeURLs...)
		c.mu.RLock()
		for _, address := range c.endpoints {
			addresses = append(addresses, address)
		}
		c.mu.RUnlock()
		seen := make(map[string]bool)
		for _, address := range addresses {
			if seen[address] {
				continue
			}
			seen[address] = true
			provider, err := openSubscriptionProvider(ctx, c.providerConfig(), address, c.instanceID)
			if err != nil {
				if errors.Is(err, subscription.ErrConsumerFenced) {
					c.mu.Lock()
					if c.fenced == nil && c.generation == generation {
						c.fenced = err
					}
					c.mu.Unlock()
					return err
				}
				failures = append(failures, fmt.Errorf("subscription: connect seed %s: %w", address, err))
				if ctx.Err() != nil {
					return ctx.Err()
				}
				continue
			}
			if err := c.publishProvider(provider, generation); err != nil {
				return err
			}
			response, _, err := c.providerHeartbeat(ctx, provider)
			if err == nil {
				discovered = response.Endpoints
				break
			}
			err = c.handleProviderError(provider, err)
			if errors.Is(err, subscription.ErrConsumerFenced) || ctx.Err() != nil {
				return err
			}
			failures = append(failures, err)
			if !provider.connection.Closed() {
				break
			}
		}
	}
	c.mu.RLock()
	known := make(map[int32]string, len(c.endpoints))
	for id, endpoint := range c.endpoints {
		known[id] = endpoint
	}
	c.mu.RUnlock()
	if len(discovered) != 0 {
		known = discovered
	}
	ids := make([]int32, 0, len(known))
	for id := range known {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return err
		}
		previous := c.providerForNode(id)
		if previous != nil && !previous.connection.Closed() && previous.endpoint == known[id] {
			continue
		}
		provider, err := openSubscriptionProvider(ctx, c.providerConfig(), known[id], c.instanceID)
		if err != nil {
			if errors.Is(err, subscription.ErrConsumerFenced) {
				c.mu.Lock()
				if c.fenced == nil && c.generation == generation {
					c.fenced = err
					if c.lifeCancel != nil {
						c.lifeCancel()
					}
				}
				c.mu.Unlock()
				return err
			}
			failures = append(failures, err)
			continue
		}
		if provider.dataNodeID != id {
			_ = provider.connection.Abort()
			failures = append(failures, fmt.Errorf("subscription: endpoint %s returned DataNode %d instead of %d", known[id], provider.dataNodeID, id))
			continue
		}
		if err := c.publishProvider(provider, generation); err != nil {
			return err
		}
	}
	if len(discovered) != 0 {
		var stale []*subscriptionProvider
		c.mu.Lock()
		if c.generation == generation && !c.closing && !c.released {
			c.endpoints = known
			for id, provider := range c.providers {
				if _, exists := known[id]; !exists {
					delete(c.providers, id)
					stale = append(stale, provider)
				}
			}
		}
		c.mu.Unlock()
		for _, provider := range stale {
			_ = provider.connection.Abort()
		}
	}
	if !hasAvailableSubscriptionProvider(c.providerSnapshot()) {
		if len(failures) == 0 {
			return errors.New("subscription: no available subscription providers")
		}
		return errors.Join(failures...)
	}
	return nil
}

func (c *subscriptionPullConsumer) providerHeartbeat(ctx context.Context, provider *subscriptionProvider) (protocol.Heartbeat, uint64, error) {
	c.mu.RLock()
	revision := c.topicRevision
	contexts := make([]payload.CommitContext, 0)
	for _, commitContext := range c.bufferedContexts {
		if commitContext.DataNodeID == provider.dataNodeID {
			contexts = append(contexts, record.CloneCommitContext(commitContext))
		}
	}
	c.mu.RUnlock()
	heartbeatCtx, cancel := context.WithTimeout(ctx, c.config.Consumer.Connection.ConnectTimeout)
	defer cancel()
	response, err := provider.heartbeat(heartbeatCtx, contexts)
	if err != nil {
		// An expired wait for a busy provider's gate must not kill the healthy
		// RPC that currently owns it. Actual RPC cancellation already aborts it.
		if heartbeatCtx.Err() == nil {
			_ = provider.connection.Abort()
		}
		return response, revision, err
	}
	c.mu.Lock()
	if c.providers[provider.dataNodeID] == provider && !provider.connection.Closed() && !c.released && !c.closing {
		if c.topicRevision == revision {
			c.topics = response.Topics
		} else {
			response.Unsubscribe = nil
		}
		for id, endpoint := range response.Endpoints {
			c.endpoints[id] = endpoint
		}
	} else {
		response.Unsubscribe = nil
	}
	c.mu.Unlock()
	return response, revision, nil
}

func (c *subscriptionPullConsumer) runHeartbeatScheduler() {
	defer c.workers.Done()
	ticker := time.NewTicker(c.config.Consumer.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.lifeCtx.Done():
			return
		case <-ticker.C:
			for _, provider := range c.providerSnapshot() {
				c.mu.Lock()
				if c.closing || c.released || c.fenced != nil || c.heartbeatBusy[provider] || provider.connection.Closed() {
					c.mu.Unlock()
					continue
				}
				c.heartbeatBusy[provider] = true
				c.workers.Add(1)
				c.mu.Unlock()
				go c.runProviderHeartbeat(provider)
			}
		}
	}
}

func (c *subscriptionPullConsumer) runProviderHeartbeat(provider *subscriptionProvider) {
	defer c.workers.Done()
	defer func() {
		c.mu.Lock()
		delete(c.heartbeatBusy, provider)
		c.mu.Unlock()
	}()
	response, revision, err := c.providerHeartbeat(c.lifeCtx, provider)
	if err != nil {
		c.handleProviderError(provider, err)
		return
	}
	if len(response.Unsubscribe) != 0 {
		ctx, cancel := context.WithTimeout(c.lifeCtx, c.config.Consumer.Connection.ConnectTimeout)
		defer cancel()
		operationCtx, finish, err := c.beginOperation(ctx)
		if err != nil {
			return
		}
		defer finish()
		c.mu.RLock()
		current := c.providers[provider.dataNodeID] == provider && c.topicRevision == revision
		c.mu.RUnlock()
		if current {
			_ = c.changeSubscriptions(operationCtx, response.Unsubscribe, true)
		}
	}
}

func (c *subscriptionPullConsumer) runEndpointSync() {
	defer c.workers.Done()
	ticker := time.NewTicker(c.config.Consumer.EndpointsSyncInterval)
	defer ticker.Stop()
	var lastRecovery time.Time
	for {
		select {
		case <-c.lifeCtx.Done():
			return
		case <-ticker.C:
		case <-c.recovery:
		}
		if c.lifeCtx.Err() != nil {
			return
		}
		if delay := time.Until(lastRecovery.Add(5 * time.Second)); delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-c.lifeCtx.Done():
				timer.Stop()
				return
			}
		}
		lastRecovery = time.Now()
		_ = c.recoverProviders(c.lifeCtx)
	}
}

func (c *subscriptionPullConsumer) runAutoCommit() {
	defer c.workers.Done()
	if !*c.config.AutoCommit {
		return
	}
	ticker := time.NewTicker(c.config.AutoCommitInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.lifeCtx.Done():
			return
		case now := <-ticker.C:
			ctx, cancel := context.WithTimeout(c.lifeCtx, c.config.Consumer.Connection.ConnectTimeout)
			c.autoCommit(ctx, now)
			cancel()
		}
	}
}
