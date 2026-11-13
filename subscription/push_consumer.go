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
	"sync"
	"time"
)

// NewSubscriptionPushConsumer copies and validates config without connecting.
// Handler is required. Open starts sequential background delivery with the
// configured acknowledgment strategy; its context limits opening only.
// Close cancels delivery and closes connections without waiting for an active
// Handler. Its message remains valid until that Handler returns.
func NewSubscriptionPushConsumer(config SubscriptionPushConsumerConfig) (SubscriptionPushConsumer, error) {
	resolved, err := normalizeSubscriptionPushConsumerConfig(config)
	if err != nil {
		return nil, err
	}
	autoCommit := false
	consumer, err := newSubscriptionPullConsumer(SubscriptionPullConsumerConfig{
		Consumer: resolved.Consumer,
		AutoCommit: &autoCommit,
		AutoCommitInterval: DefaultSubscriptionPullConsumerConfig().AutoCommitInterval,
	})
	if err != nil {
		return nil, err
	}
	return &subscriptionPushConsumer{
		config: resolved,
		consumer: consumer,
		operations: make(chan struct{}, 1),
		generations: make(map[string]uint64),
		errors: make(chan error, 1),
	}, nil
}

type subscriptionPushConsumer struct {
	config SubscriptionPushConsumerConfig
	consumer *subscriptionPullConsumer
	operations chan struct{}
	mu sync.Mutex
	generations map[string]uint64
	started bool
	closed bool
	terminal error
	deliveryCancel context.CancelFunc
	callbackTopic string
	callbackCancel context.CancelFunc
	errors chan error
	errorsClosed sync.Once
}

type subscriptionPushDelivery struct {
	message SubscriptionMessage
	generation uint64
}

func (c *subscriptionPushConsumer) Open(ctx context.Context) error {
	if ctx == nil {
		return errors.New("subscription: context must not be nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	err := c.deliveryStateLocked()
	c.mu.Unlock()
	if err != nil {
		return err
	}
	if err := c.consumer.Open(ctx); err != nil {
		return err
	}
	c.mu.Lock()
	if err := c.deliveryStateLocked(); err != nil {
		c.mu.Unlock()
		return err
	}
	if c.started {
		c.mu.Unlock()
		return nil
	}
	c.consumer.mu.RLock()
	lifeCtx := c.consumer.lifeCtx
	c.consumer.mu.RUnlock()
	deliveryCtx, cancel := context.WithCancel(lifeCtx)
	c.deliveryCancel = cancel
	c.started = true
	c.mu.Unlock()
	go c.runDelivery(deliveryCtx)
	return nil
}

func (c *subscriptionPushConsumer) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	if c.deliveryCancel != nil {
		c.deliveryCancel()
	}
	started := c.started
	c.mu.Unlock()
	// The loop is not a core worker: a Handler may close its own consumer.
	// It retains ownership of its messages until the Handler has returned.
	err := c.consumer.Close()
	if !started {
		c.closeErrors()
	}
	return err
}

func (c *subscriptionPushConsumer) Errors() <-chan error {
	return c.errors
}

func (c *subscriptionPushConsumer) Info() SubscriptionConsumerInfo {
	return c.consumer.Info()
}

func (c *subscriptionPushConsumer) LatestWatermark() (int64, bool) {
	return c.consumer.LatestWatermark()
}

func (c *subscriptionPushConsumer) AllConsumed() bool {
	return c.consumer.AllConsumed()
}

func (c *subscriptionPushConsumer) Subscribe(ctx context.Context, topics ...string) error {
	return c.consumer.Subscribe(ctx, topics...)
}

func (c *subscriptionPushConsumer) Unsubscribe(ctx context.Context, topics ...string) error {
	return c.consumer.Unsubscribe(ctx, topics...)
}

func (c *subscriptionPushConsumer) Positions(topic string) (TopicProgress, error) {
	return c.consumer.Positions(topic)
}

func (c *subscriptionPushConsumer) CommittedPositions(topic string) (TopicProgress, error) {
	return c.consumer.CommittedPositions(topic)
}

func (c *subscriptionPushConsumer) SeekToBeginning(ctx context.Context, topic string) error {
	return c.seek(ctx, topic, subscriptionSeekBeginning, nil)
}

func (c *subscriptionPushConsumer) SeekToEnd(ctx context.Context, topic string) error {
	return c.seek(ctx, topic, subscriptionSeekEnd, nil)
}

func (c *subscriptionPushConsumer) Seek(ctx context.Context, topic string, progress TopicProgress) error {
	return c.seek(ctx, topic, subscriptionSeekProgress, progress)
}

func (c *subscriptionPushConsumer) SeekAfter(ctx context.Context, topic string, progress TopicProgress) error {
	return c.seek(ctx, topic, subscriptionSeekAfterProgress, progress)
}

func (c *subscriptionPushConsumer) seek(ctx context.Context, topic string, kind int16, progress TopicProgress) error {
	name, err := parseSubscriptionIdentifier(topic)
	if err != nil {
		return err
	}
	if err := c.acquireOperations(ctx); err != nil {
		return err
	}
	defer c.releaseOperations()
	if err := c.consumer.checkOpen(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrClosed
	}
	// Even a failed multi-provider seek can change server state. Never confirm
	// or start another callback for an older delivery of the affected topic.
	c.generations[name]++
	var cancelCallback context.CancelFunc
	if c.callbackTopic == name {
		cancelCallback = c.callbackCancel
	}
	c.mu.Unlock()
	// Keep a callback's context usable for a seek invoked from that callback.
	// Cancel the obsolete delivery after the request completes instead.
	if cancelCallback != nil {
		defer cancelCallback()
	}
	return c.consumer.seekProgress(ctx, topic, kind, progress)
}

func (c *subscriptionPushConsumer) acquireOperations(ctx context.Context) error {
	if ctx == nil {
		return errors.New("subscription: context must not be nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case c.operations <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *subscriptionPushConsumer) releaseOperations() {
	<-c.operations
}

func (c *subscriptionPushConsumer) deliveryStateLocked() error {
	if c.closed {
		return ErrClosed
	}
	return c.terminal
}

func (c *subscriptionPushConsumer) pollDeliveries(ctx context.Context) ([]subscriptionPushDelivery, error) {
	if err := c.acquireOperations(ctx); err != nil {
		return nil, err
	}
	defer c.releaseOperations()
	c.mu.Lock()
	err := c.deliveryStateLocked()
	c.mu.Unlock()
	if err != nil {
		return nil, err
	}
	messages, err := c.consumer.Poll(ctx, c.config.PollTimeout)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.deliveryStateLocked(); err != nil {
		for _, message := range messages {
			message.Release()
		}
		return nil, err
	}
	deliveries := make([]subscriptionPushDelivery, 0, len(messages))
	for _, message := range messages {
		deliveries = append(deliveries, subscriptionPushDelivery{
			message: message,
			generation: c.generations[message.CommitContext().Topic],
		})
	}
	return deliveries, nil
}

func (c *subscriptionPushConsumer) runDelivery(ctx context.Context) {
	var terminal error
	defer func() { c.finishDelivery(terminal) }()
	for {
		if err := ctx.Err(); err != nil {
			terminal = c.terminalDeliveryError(err)
			return
		}
		deliveries, err := c.pollDeliveries(ctx)
		if err == nil {
			err = c.deliver(ctx, deliveries)
		}
		if err != nil {
			terminal = c.terminalDeliveryError(err)
			if terminal != nil || ctx.Err() != nil {
				return
			}
		}
		// Wait after the entire round, including callbacks and acknowledgment.
		// A slow round therefore never overlaps the next round.
		timer := time.NewTimer(c.config.PollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			terminal = c.terminalDeliveryError(ctx.Err())
			return
		case <-timer.C:
		}
	}
}

func (c *subscriptionPushConsumer) deliver(ctx context.Context, deliveries []subscriptionPushDelivery) error {
	defer func() {
		for _, delivery := range deliveries {
			delivery.message.Release()
		}
	}()
	if c.config.AckStrategy == AckBeforeConsume {
		if err := c.commitDeliveries(ctx, deliveries, false); err != nil {
			return err
		}
	}
	var succeeded, failed []subscriptionPushDelivery
	for _, delivery := range deliveries {
		if err := ctx.Err(); err != nil {
			return err
		}
		callbackCtx, finish, current := c.beginCallback(ctx, delivery)
		if !current {
			continue
		}
		err := c.invokeHandler(callbackCtx, delivery.message)
		finish()
		delivery.message.Release()
		if err == nil {
			succeeded = append(succeeded, delivery)
		} else {
			failed = append(failed, delivery)
		}
	}
	if c.config.AckStrategy == AckBeforeConsume {
		return nil
	}
	// Java confirms successful callbacks before nacking failures. An ACK error
	// leaves the failed group unconfirmed for server-side recovery.
	if err := c.commitDeliveries(ctx, succeeded, false); err != nil {
		return err
	}
	return c.commitDeliveries(ctx, failed, true)
}

func (c *subscriptionPushConsumer) beginCallback(ctx context.Context, delivery subscriptionPushDelivery) (context.Context, func(), bool) {
	topic := delivery.message.CommitContext().Topic
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.deliveryStateLocked() != nil || ctx.Err() != nil || c.generations[topic] != delivery.generation {
		return nil, nil, false
	}
	callbackCtx, cancel := context.WithCancel(ctx)
	c.callbackTopic = topic
	c.callbackCancel = cancel
	return callbackCtx, func() {
		cancel()
		c.mu.Lock()
		c.callbackTopic = ""
		c.callbackCancel = nil
		c.mu.Unlock()
	}, true
}

func (c *subscriptionPushConsumer) invokeHandler(ctx context.Context, message SubscriptionMessage) (err error) {
	defer func() {
		if failure := recover(); failure != nil {
			err = fmt.Errorf("subscription: push handler panicked: %v", failure)
		}
	}()
	return c.config.Handler(ctx, message)
}

func (c *subscriptionPushConsumer) commitDeliveries(ctx context.Context, deliveries []subscriptionPushDelivery, nack bool) error {
	if len(deliveries) == 0 {
		return nil
	}
	if err := c.acquireOperations(ctx); err != nil {
		return err
	}
	defer c.releaseOperations()
	c.mu.Lock()
	if err := c.deliveryStateLocked(); err != nil {
		c.mu.Unlock()
		return err
	}
	messages := make([]SubscriptionMessage, 0, len(deliveries))
	for _, delivery := range deliveries {
		if c.generations[delivery.message.CommitContext().Topic] == delivery.generation {
			messages = append(messages, delivery.message)
		}
	}
	c.mu.Unlock()
	if len(messages) == 0 {
		return nil
	}
	commitCtx, cancel := context.WithTimeout(ctx, c.config.Consumer.Connection.ConnectTimeout)
	defer cancel()
	operationCtx, finish, err := c.consumer.linkContext(commitCtx)
	if err != nil {
		return err
	}
	defer finish()
	contexts, err := c.consumer.messageCommitContexts(messages)
	if err != nil {
		return err
	}
	return c.consumer.commitContexts(operationCtx, contexts, nack)
}

func (c *subscriptionPushConsumer) terminalDeliveryError(failure error) error {
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return nil
	}
	if err := c.consumer.checkOpen(); err != nil {
		return err
	}
	if flowFatalPollError(failure) || flowCriticalPollError(failure) {
		return failure
	}
	return nil
}

func (c *subscriptionPushConsumer) finishDelivery(failure error) {
	c.mu.Lock()
	notify := failure != nil && !c.closed
	if notify {
		c.terminal = failure
	}
	if c.deliveryCancel != nil {
		c.deliveryCancel()
	}
	c.mu.Unlock()
	if notify {
		c.errors <- failure
	}
	c.closeErrors()
}

func (c *subscriptionPushConsumer) closeErrors() {
	c.errorsClosed.Do(func() { close(c.errors) })
}
