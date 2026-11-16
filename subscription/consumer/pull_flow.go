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
	messageprocessor "github.com/apache/iotdb-client-go/subscription/processor"
	"github.com/apache/iotdb-client-go/subscription/internal/protocol"
	"github.com/apache/iotdb-client-go/subscription/internal/record"
	"github.com/apache/iotdb-client-go/subscription/internal/identifier"

	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"
	"time"
)

type flowPollResult struct {
	messages []payload.SubscriptionMessage
	emptyContexts []payload.CommitContext
	nackContexts []payload.CommitContext
	rollbackContexts []payload.CommitContext
	terminated []string
	err error
}

func (c *subscriptionPullConsumer) Poll(ctx context.Context, timeout time.Duration, topics ...string) ([]payload.SubscriptionMessage, error) {
	operationCtx, pollCtx, finish, acquired, err := c.beginFlowPoll(ctx, timeout)
	if err != nil || !acquired {
		return nil, err
	}
	defer finish()
	return c.pollLocked(operationCtx, pollCtx, timeout == 0, topics)
}

func (c *subscriptionPullConsumer) PollWithInfo(ctx context.Context, timeout time.Duration, topics ...string) (PollResult, error) {
	operationCtx, pollCtx, finish, acquired, err := c.beginFlowPoll(ctx, timeout)
	if err != nil || !acquired {
		return PollResult{}, err
	}
	defer finish()
	messages, err := c.pollLocked(operationCtx, pollCtx, timeout == 0, topics)
	if err != nil {
		return PollResult{}, err
	}
	result := PollResult{Messages: messages, BufferedCount: c.processorBufferedCount()}
	for _, processor := range c.processors {
		if reporter, ok := processor.(messageprocessor.WatermarkReporter); ok {
			result.Watermark, result.HasWatermark = reporter.Watermark()
		}
	}
	return result, nil
}

// beginFlowPoll starts the network budget before waiting for the flow gate.
// The operation context stays tied to the caller and consumer lifetime, so a
// budget expiration remains distinguishable from cancellation of consumption.
func (c *subscriptionPullConsumer) beginFlowPoll(ctx context.Context, timeout time.Duration) (operationCtx, pollCtx context.Context, finish func(), acquired bool, err error) {
	if ctx == nil {
		return nil, nil, nil, false, errors.New("subscription: context must not be nil")
	}
	if timeout < 0 {
		return nil, nil, nil, false, errors.New("subscription: poll timeout must not be negative")
	}
	if timeout == 0 {
		operationCtx, finish, acquired, err = c.beginNonblockingOperation(ctx)
		return operationCtx, operationCtx, finish, acquired, err
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, nil, false, err
	}
	c.mu.RLock()
	lifeCtx := c.lifeCtx
	stateErr := c.stateErrorLocked(false)
	c.mu.RUnlock()
	if stateErr != nil {
		return nil, nil, nil, false, stateErr
	}
	operationCtx, cancelOperation := context.WithCancel(ctx)
	stop := context.AfterFunc(lifeCtx, cancelOperation)
	pollCtx, cancelPoll := context.WithTimeout(operationCtx, timeout)
	if err := c.acquireFlow(pollCtx); err != nil {
		stop()
		cancelPoll()
		cancelOperation()
		if stateErr := c.checkOpen(); stateErr != nil {
			return nil, nil, nil, false, stateErr
		}
		if callerErr := ctx.Err(); callerErr != nil {
			return nil, nil, nil, false, callerErr
		}
		if operationCtx.Err() != nil {
			return nil, nil, nil, false, operationCtx.Err()
		}
		return nil, nil, nil, false, nil
	}
	c.mu.Lock()
	if stateErr := c.stateErrorLocked(false); stateErr != nil {
		c.mu.Unlock()
		c.releaseFlow()
		stop()
		cancelPoll()
		cancelOperation()
		return nil, nil, nil, false, stateErr
	}
	c.flowCancel = cancelOperation
	c.mu.Unlock()
	return operationCtx, pollCtx, func() {
		stop()
		cancelPoll()
		cancelOperation()
		c.mu.Lock()
		c.flowCancel = nil
		c.mu.Unlock()
		c.releaseFlow()
	}, true, nil
}

func (c *subscriptionPullConsumer) pollLocked(operationCtx, pollCtx context.Context, immediate bool, topics []string) ([]payload.SubscriptionMessage, error) {
	selected, err := c.flowTopics(topics)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	pending := c.pendingCritical
	c.pendingCritical = nil
	c.mu.Unlock()
	if pending != nil {
		return nil, pending
	}
	if len(selected) == 0 {
		return nil, nil
	}
	providers := c.pollProviderSet()
	networkReady := providers.Len() != 0
	if !networkReady {
		if immediate {
			// A zero-wait poll can drive existing processor state, but must
			// not turn into a synchronous connection-recovery attempt.
			c.requestRecovery()
		} else if err := c.recoverProviders(pollCtx); err != nil {
			if stateErr := c.checkOpen(); stateErr != nil {
				return nil, stateErr
			}
			if operationCtx.Err() == nil && errors.Is(err, context.DeadlineExceeded) && pollCtx.Err() != nil {
				// Still let processors advance or flush their retained records on
				// an empty input when only the network budget was exhausted.
				networkReady = false
			} else {
				return nil, err
			}
		} else {
			providers = c.pollProviderSet()
			networkReady = providers.Len() != 0
		}
	}
	result := flowPollResult{}
	if networkReady {
		cursor := c.pollCursor
		c.pollCursor++
		result = c.pollFlowPartition(pollCtx, selected, cursor, immediate, providers)
	}
	allContexts := make([]payload.CommitContext, 0, len(result.rollbackContexts)+len(result.nackContexts))
	allContexts = append(allContexts, result.rollbackContexts...)
	allContexts = append(allContexts, result.nackContexts...)
	failedContexts := make([]payload.CommitContext, len(result.nackContexts))
	copy(failedContexts, result.nackContexts)
	var fatal, critical, ordinary error
	if result.err != nil {
		failedContexts = append(failedContexts, result.rollbackContexts...)
		if flowFatalPollError(result.err) {
			fatal = result.err
		} else if flowCriticalPollError(result.err) {
			critical = result.err
		} else if !((errors.Is(result.err, context.DeadlineExceeded) || errors.Is(result.err, context.Canceled)) && pollCtx.Err() != nil) {
			ordinary = result.err
		}
	}
	messages := result.messages
	emptyContexts := result.emptyContexts
	terminations := result.terminated
	if err := c.checkOpen(); err != nil {
		c.rollbackFlow(pollCtx, allContexts)
		return nil, err
	}
	if err := operationCtx.Err(); err != nil {
		c.rollbackFlow(pollCtx, allContexts)
		return nil, err
	}
	if fatal != nil {
		c.rollbackFlow(pollCtx, allContexts)
		return nil, fatal
	}
	if len(messages) == 0 && critical != nil {
		c.rollbackFlow(pollCtx, allContexts)
		return nil, critical
	}
	if len(messages) == 0 && len(emptyContexts) == 0 && len(terminations) == 0 && ordinary != nil {
		c.rollbackFlow(pollCtx, allContexts)
		return nil, ordinary
	}
	if critical != nil {
		c.mu.Lock()
		if c.pendingCritical == nil {
			c.pendingCritical = critical
		}
		c.mu.Unlock()
	}
	// The caller/lifetime context aborts the entire round. Exhausting only the
	// polling budget leaves undelivered events unconfirmed; no cleanup RPC is
	// started after that budget has expired.
	c.rollbackFlow(pollCtx, failedContexts)
	if pollCtx.Err() == nil && len(emptyContexts) != 0 {
		if err := c.commitContexts(pollCtx, emptyContexts, false); err != nil {
			if !(operationCtx.Err() == nil && pollCtx.Err() != nil && errors.Is(err, context.DeadlineExceeded)) {
				c.rollbackFlow(pollCtx, allContexts)
				return nil, err
			}
		}
	}
	if pollCtx.Err() == nil && len(terminations) != 0 {
		names := uniqueFlowTopics(terminations)
		if err := c.changeSubscriptions(pollCtx, names, true); err != nil {
			if !(operationCtx.Err() == nil && pollCtx.Err() != nil && errors.Is(err, context.DeadlineExceeded)) {
				c.rollbackFlow(pollCtx, allContexts)
				return nil, err
			}
		}
	}
	processed := messages
	for _, processor := range c.processors {
		if err := operationCtx.Err(); err != nil {
			c.rollbackFlow(pollCtx, allContexts)
			return nil, err
		}
		if err := c.checkOpen(); err != nil {
			c.rollbackFlow(pollCtx, allContexts)
			return nil, err
		}
		processed, err = processor.Process(processed)
		if err != nil {
			c.refreshBufferedContexts()
			c.rollbackFlow(pollCtx, allContexts)
			return nil, err
		}
	}
	if len(c.processors) != 0 || len(c.drained) != 0 {
		c.refreshBufferedContexts()
	}
	if err := operationCtx.Err(); err != nil {
		c.rollbackFlow(pollCtx, allContexts)
		return nil, err
	}
	if err := c.checkOpen(); err != nil {
		c.rollbackFlow(pollCtx, allContexts)
		return nil, err
	}
	c.flushRedirectAcks(pollCtx, messages)
	if err := c.checkOpen(); err != nil {
		c.rollbackFlow(pollCtx, allContexts)
		return nil, err
	}
	if err := operationCtx.Err(); err != nil {
		c.rollbackFlow(pollCtx, allContexts)
		return nil, err
	}
	// Fetch progress can include records retained by a successful processor, but
	// automatic commit only tracks the records actually handed to the caller.
	c.advancePositions(messages)
	// Observe raw watermark signals even if a processor consumed them. Without
	// processors, one pass over the same batch is sufficient.
	if len(c.processors) != 0 {
		_ = c.visibleFlowMessages(messages)
	}
	processed = c.visibleFlowMessages(processed)
	c.trackDelivered(processed)
	return processed, nil
}

func (c *subscriptionPullConsumer) flowTopics(requested []string) ([]string, error) {
	names := make([]string, 0, len(requested))
	for _, topic := range requested {
		name, err := identifier.Parse(topic)
		if err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(requested) == 0 {
		for name := range c.topics {
			names = append(names, name)
		}
	} else {
		selected := names[:0]
		for _, name := range names {
			if _, subscribed := c.topics[name]; subscribed {
				selected = append(selected, name)
			}
		}
		names = selected
	}
	return uniqueFlowTopics(names), nil
}

func uniqueFlowTopics(topics []string) []string {
	seen := make(map[string]struct{}, len(topics))
	for _, topic := range topics {
		seen[topic] = struct{}{}
	}
	names := make([]string, 0, len(seen))
	for topic := range seen {
		names = append(names, topic)
	}
	sort.Strings(names)
	return names
}

func (c *subscriptionPullConsumer) pollFlowPartition(ctx context.Context, topics []string, cursor uint64, immediate bool, providers subscriptionProviderSet) (result flowPollResult) {
	var lastFailure error
	refreshProviders := false
	for {
		if err := ctx.Err(); err != nil {
			result.err = err
			return result
		}
		if err := c.checkOpen(); err != nil {
			result.err = err
			return result
		}
		if refreshProviders {
			providers = c.pollProviderSet()
			refreshProviders = false
		}
		if providers.Len() == 0 {
			c.requestRecovery()
			if lastFailure == nil {
				lastFailure = errors.New("subscription: no available provider for poll")
			}
			result.err = lastFailure
			return result
		}
		var progress map[string]payload.TopicProgress
		c.mu.RLock()
		for _, topic := range topics {
			if position := c.positions[topic]; len(position) != 0 {
				if progress == nil {
					progress = make(map[string]payload.TopicProgress, len(topics))
				}
				progress[topic] = cloneTopicProgress(position)
			}
		}
		c.mu.RUnlock()
		healthy := false
		attempted := 0
		for attempt := 0; attempt < providers.Len(); attempt++ {
			provider := providers.Provider((int(cursor%uint64(providers.Len())) + attempt) % providers.Len())
			if provider == nil || provider.connection.Closed() {
				refreshProviders = true
				continue
			}
			attempted++
			responses, err := provider.poll(ctx, topics, flowRemainingMilliseconds(ctx, providers.Len()-attempt, immediate), int64(provider.bodyLimit), progress)
			if err != nil {
				err = c.handleProviderError(provider, err)
				if flowFatalPollError(err) || flowCriticalPollError(err) || ctx.Err() != nil {
					result.err = err
					return result
				}
				lastFailure = err
				refreshProviders = true
				continue
			}
			healthy = true
			for _, response := range responses {
				switch response.Type {
				case protocol.ResponseTablets:
					rollbackIndex := len(result.rollbackContexts)
					if record.Committable(response.CommitContext) {
						result.rollbackContexts = append(result.rollbackContexts, response.CommitContext)
					}
					message, stale, err := c.completeFlowRecord(ctx, provider, response)
					if stale {
						result.rollbackContexts = result.rollbackContexts[:rollbackIndex]
						continue
					}
					if err != nil {
						err = c.handleProviderError(provider, err)
						result.err = err
						return result
					}
					if message == nil {
						if record.Committable(response.CommitContext) {
							result.emptyContexts = append(result.emptyContexts, response.CommitContext)
						}
					} else {
						result.messages = append(result.messages, message)
					}
				case protocol.ResponseWatermark:
					message, err := response.ToMessage(c.config.Consumer.Dialect)
					if err != nil {
						result.err = err
						return result
					}
					result.messages = append(result.messages, message)
				case protocol.ResponseTermination:
					result.terminated = append(result.terminated, response.CommitContext.Topic)
				case protocol.ResponseError:
					if isOutdatedFlowResponse(response) {
						continue
					}
					failure := newSubscriptionPollError(response.ErrorMessage, response.Critical)
					if response.Critical {
						result.err = failure
						return result
					}
					if record.Committable(response.CommitContext) {
						result.nackContexts = append(result.nackContexts, response.CommitContext)
					}
					lastFailure = failure
				default:
					result.err = fmt.Errorf("subscription: unsupported poll response type %d", response.Type)
					return result
				}
			}
			if len(result.messages) != 0 || len(result.emptyContexts) != 0 || len(result.terminated) != 0 {
				return result
			}
		}
		if attempted == 0 {
			c.requestRecovery()
			if lastFailure == nil {
				lastFailure = errors.New("subscription: no available provider for poll")
			}
			result.err = lastFailure
			return result
		}
		if !healthy && lastFailure != nil {
			result.err = lastFailure
			return result
		}
		if immediate {
			return result
		}
		// Back off only after visiting all available providers. A slow or idle
		// provider must not insert a pause before a ready sibling is checked.
		timer := time.NewTimer(time.Duration(50+rand.IntN(100))*time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			result.err = ctx.Err()
			return result
		case <-timer.C:
		}
		cursor++
	}
}

func (c *subscriptionPullConsumer) AddProcessor(processor messageprocessor.SubscriptionMessageProcessor) error {
	if processor == nil {
		return errors.New("subscription: processor must not be nil")
	}
	if err := c.acquireFlow(context.Background()); err != nil {
		return err
	}
	defer c.releaseFlow()
	c.mu.RLock()
	stateErr := c.fenced
	if stateErr == nil && (c.released || c.closing) {
		stateErr = subscription.ErrClosed
	}
	c.mu.RUnlock()
	if stateErr != nil {
		return stateErr
	}
	c.processors = append(c.processors, processor)
	c.refreshBufferedContexts()
	return nil
}

func (c *subscriptionPullConsumer) DrainBufferedMessages(ctx context.Context) ([]payload.SubscriptionMessage, error) {
	operationCtx, finish, err := c.beginOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer finish()
	defer c.refreshBufferedContexts()
	var messages []payload.SubscriptionMessage
	if len(c.drained) != 0 {
		messages, c.drained = c.drained, nil
	} else {
		messages, err = c.flushProcessors()
		if err != nil {
			c.drained = append(c.drained, messages...)
			return nil, err
		}
	}
	if err := operationCtx.Err(); err != nil {
		c.drained = append(c.drained, messages...)
		return nil, err
	}
	if err := c.checkOpen(); err != nil {
		c.drained = append(c.drained, messages...)
		return nil, err
	}
	messages = c.visibleFlowMessages(messages)
	c.trackDelivered(messages)
	return messages, nil
}

// flushProcessors feeds upstream output through each later processing stage
// before appending that stage's own flushed output. The caller owns the flow gate.
func (c *subscriptionPullConsumer) flushProcessors() (messages []payload.SubscriptionMessage, err error) {
	defer c.refreshBufferedContexts()
	for _, processor := range c.processors {
		if len(messages) != 0 {
			processed, processErr := processor.Process(messages)
			if processErr != nil {
				return messages, processErr
			}
			messages = processed
		}
		flushed, flushErr := processor.Flush()
		messages = append(messages, flushed...)
		if flushErr != nil {
			return messages, flushErr
		}
	}
	return c.visibleFlowMessages(messages), nil
}

func (c *subscriptionPullConsumer) drainForClose() ([]payload.SubscriptionMessage, error) {
	defer c.refreshBufferedContexts()
	messages := c.drained
	c.drained = nil
	flushed, err := c.flushProcessors()
	messages = append(messages, flushed...)
	if err != nil {
		c.drained = append(c.drained, messages...)
		return nil, err
	}
	return c.visibleFlowMessages(messages), nil
}

func (c *subscriptionPullConsumer) refreshBufferedContexts() {
	if len(c.drained) == 0 && len(c.processors) == 0 {
		c.mu.Lock()
		c.bufferedContexts = nil
		c.mu.Unlock()
		return
	}
	var contexts []payload.CommitContext
	for _, message := range c.drained {
		if message != nil {
			commitContext := record.ReadCommitContext(message)
			if record.Committable(commitContext) {
				contexts = append(contexts, record.CloneCommitContext(commitContext))
			}
		}
	}
	for _, processor := range c.processors {
		if buffered, ok := processor.(messageprocessor.BufferingProcessor); ok {
			for _, commitContext := range buffered.Buffered().Contexts {
				if record.Committable(commitContext) {
					contexts = append(contexts, record.CloneCommitContext(commitContext))
				}
			}
		}
	}
	c.mu.Lock()
	c.bufferedContexts = contexts
	c.mu.Unlock()
}

func (c *subscriptionPullConsumer) processorBufferedCount() int {
	count := 0
	for _, processor := range c.processors {
		if buffered, ok := processor.(messageprocessor.BufferingProcessor); ok {
			count += buffered.Buffered().Count
		}
	}
	return count
}

func (c *subscriptionPullConsumer) validateProcessorReset(topic string) error {
	c.mu.RLock()
	multiple := len(c.topics) > 1
	c.mu.RUnlock()
	if !multiple {
		return nil
	}
	for _, processor := range c.processors {
		if _, ok := processor.(messageprocessor.TopicResetter); !ok {
			return fmt.Errorf("subscription: processor %T cannot reset topic %q while multiple topics are subscribed", processor, topic)
		}
	}
	return nil
}

func (c *subscriptionPullConsumer) resetProcessors(topic string) {
	for _, processor := range c.processors {
		if resetter, ok := processor.(messageprocessor.TopicResetter); ok {
			resetter.ResetTopic(topic)
		} else {
			processor.Reset()
		}
	}
	retained := c.drained[:0]
	for _, message := range c.drained {
		if message != nil && record.ReadCommitContext(message).Topic == topic {
			message.Release()
			continue
		}
		retained = append(retained, message)
	}
	clear(c.drained[len(retained):])
	c.drained = retained
	c.refreshBufferedContexts()
}

// visibleFlowMessages also observes watermarks consumed by a processor. Callers
// pass the original successful poll messages separately when a stage removes them.
func (c *subscriptionPullConsumer) visibleFlowMessages(messages []payload.SubscriptionMessage) []payload.SubscriptionMessage {
	filter := false
	for _, message := range messages {
		if message == nil {
			filter = true
			continue
		}
		if timestamp, ok := message.Watermark(); ok {
			c.mu.Lock()
			if !c.hasWatermark || timestamp > c.watermark {
				c.watermark, c.hasWatermark = timestamp, true
			}
			c.mu.Unlock()
			filter = true
			continue
		}
		if message.Kind() != payload.RecordMessage {
			filter = true
		}
	}
	if !filter {
		return messages
	}
	visible := make([]payload.SubscriptionMessage, 0, len(messages))
	for _, message := range messages {
		if message != nil && message.Kind() == payload.RecordMessage {
			visible = append(visible, message)
		}
	}
	return visible
}

func isOutdatedFlowResponse(response protocol.PollResponse) bool {
	return response.Type == protocol.ResponseError && !response.Critical &&
		(response.ErrorMessage == "outdated subscription event" || response.ErrorMessage == "过期的订阅事件")
}

func flowCriticalPollError(err error) bool {
	var status *subscription.SubscriptionError
	return errors.As(err, &status) && status.Critical
}

func newSubscriptionPollError(message string, critical bool) *subscription.SubscriptionError {
	kind := subscription.ErrorKindRuntimeNonCritical
	if critical {
		kind = subscription.ErrorKindRuntimeCritical
	}
	if strings.Contains(message, "SUBSCRIPTION_OVERSIZED_ENTRY") {
		kind = subscription.ErrorKindPayloadExceed
	}
	return &subscription.SubscriptionError{Kind: kind, Message: message, Critical: critical}
}

func flowFatalPollError(err error) bool {
	return errors.Is(err, subscription.ErrUnsupportedTsFile) || errors.Is(err, subscription.ErrConsumerFenced) || errors.Is(err, subscription.ErrOwnerFenced)
}

func (c *subscriptionPullConsumer) rollbackFlow(ctx context.Context, contexts []payload.CommitContext) {
	if len(contexts) == 0 || ctx.Err() != nil || c.checkOpen() != nil {
		return
	}
	// A failed cleanup never replaces the original poll error. In particular,
	// NACK is not replayed when a transport failure schedules provider recovery.
	_ = c.commitContexts(ctx, contexts, true)
}

func (c *subscriptionPullConsumer) completeFlowRecord(ctx context.Context, provider *subscriptionProvider, response protocol.PollResponse) (payload.SubscriptionMessage, bool, error) {
	for response.NextOffset > 0 {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		if err := c.checkOpen(); err != nil {
			return nil, false, err
		}
		pages, err := provider.pollTablets(ctx, response.CommitContext, response.NextOffset, flowRemainingMilliseconds(ctx, 1, false), int64(provider.bodyLimit))
		if err != nil {
			return nil, false, err
		}
		if len(pages) == 0 {
			return nil, true, nil
		}
		if len(pages) != 1 {
			return nil, false, fmt.Errorf("subscription: tablet continuation returned %d responses", len(pages))
		}
		next := pages[0]
		if isOutdatedFlowResponse(next) {
			return nil, true, nil
		}
		if next.Type == protocol.ResponseError {
			return nil, false, newSubscriptionPollError(next.ErrorMessage, next.Critical)
		}
		if next.NextOffset > 0 && next.NextOffset <= response.NextOffset {
			return nil, false, errors.New("subscription: tablet continuation offset did not advance")
		}
		if err := response.AppendTablets(next); err != nil {
			return nil, false, err
		}
	}
	message, err := response.ToMessage(c.config.Consumer.Dialect)
	return message, false, err
}

func flowRemainingMilliseconds(ctx context.Context, providers int, immediate bool) int64 {
	if immediate {
		return 0
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return 0
	}
	remaining := time.Until(deadline)
	if providers > 1 {
		remaining /= time.Duration(providers)
	}
	if remaining <= 0 {
		return 0
	}
	milliseconds := remaining.Milliseconds()
	if remaining%time.Millisecond != 0 {
		milliseconds++
	}
	return milliseconds
}
