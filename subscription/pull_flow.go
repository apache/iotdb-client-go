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
	"math/rand/v2"
	"sort"
	"time"
)

type flowPollResult struct {
	messages []SubscriptionMessage
	emptyContexts []CommitContext
	nackContexts []CommitContext
	rollbackContexts []CommitContext
	terminated []string
	err error
}

func (c *subscriptionPullConsumer) Poll(ctx context.Context, timeout time.Duration, topics ...string) ([]SubscriptionMessage, error) {
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
		if reporter, ok := processor.(WatermarkReporter); ok {
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
	queueCtx, cancelQueue := context.WithTimeout(ctx, timeout)
	operationCtx, release, err := c.beginQueuedOperation(ctx, queueCtx)
	if err != nil {
		cancelQueue()
		if stateErr := c.checkOpen(); stateErr != nil {
			return nil, nil, nil, false, stateErr
		}
		if callerErr := ctx.Err(); callerErr != nil {
			return nil, nil, nil, false, callerErr
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, nil, nil, false, nil
		}
		return nil, nil, nil, false, err
	}
	if queueCtx.Err() != nil {
		release()
		cancelQueue()
		if stateErr := c.checkOpen(); stateErr != nil {
			return nil, nil, nil, false, stateErr
		}
		if callerErr := ctx.Err(); callerErr != nil {
			return nil, nil, nil, false, callerErr
		}
		return nil, nil, nil, false, nil
	}
	deadline, _ := queueCtx.Deadline()
	pollCtx, cancelPoll := context.WithDeadline(operationCtx, deadline)
	return operationCtx, pollCtx, func() {
		cancelPoll()
		cancelQueue()
		release()
	}, true, nil
}

func (c *subscriptionPullConsumer) pollLocked(operationCtx, pollCtx context.Context, immediate bool, topics []string) ([]SubscriptionMessage, error) {
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
	networkReady := true
	if !hasAvailableSubscriptionProvider(c.providerSnapshot()) {
		if immediate {
			// A zero-wait poll can drive existing processor state, but must
			// not turn into a synchronous connection-recovery attempt.
			c.requestRecovery()
			networkReady = false
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
		}
	}
	var results []flowPollResult
	if networkReady {
		results = c.pollFlowPartitions(pollCtx, selected, immediate)
	}
	var messages []SubscriptionMessage
	var emptyContexts, failedContexts, allContexts []CommitContext
	var terminations []string
	var fatal, critical, ordinary error
	for _, result := range results {
		allContexts = append(allContexts, result.rollbackContexts...)
		allContexts = append(allContexts, result.nackContexts...)
		failedContexts = append(failedContexts, result.nackContexts...)
		if result.err != nil {
			failedContexts = append(failedContexts, result.rollbackContexts...)
			if flowFatalPollError(result.err) {
				if fatal == nil {
					fatal = result.err
				}
			} else if flowCriticalPollError(result.err) {
				critical = result.err
			} else if !((errors.Is(result.err, context.DeadlineExceeded) || errors.Is(result.err, context.Canceled)) && pollCtx.Err() != nil) {
				ordinary = result.err
			}
			continue
		}
		messages = append(messages, result.messages...)
		emptyContexts = append(emptyContexts, result.emptyContexts...)
		terminations = append(terminations, result.terminated...)
	}
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
	// polling budget instead preserves already complete healthy partitions, as
	// Java does for completed sibling futures. Do not start cleanup RPCs after
	// that budget has expired; undelivered events remain unconfirmed.
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
	c.refreshBufferedContexts()
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
	// Observe raw watermark signals even if a processor consumed them.
	_ = c.visibleFlowMessages(messages)
	processed = c.visibleFlowMessages(processed)
	c.trackDelivered(processed)
	return processed, nil
}

func (c *subscriptionPullConsumer) flowTopics(requested []string) ([]string, error) {
	names := make([]string, 0, len(requested))
	for _, topic := range requested {
		name, err := parseSubscriptionIdentifier(topic)
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

func (c *subscriptionPullConsumer) pollFlowPartitions(ctx context.Context, topics []string, immediate bool) []flowPollResult {
	parallelism := min(c.config.Consumer.MaxPollParallelism, len(topics))
	if parallelism < 1 {
		parallelism = 1
	}
	partitions := make([][]string, parallelism)
	for index, topic := range topics {
		partitions[index%parallelism] = append(partitions[index%parallelism], topic)
	}
	cursor := c.pollCursor
	c.pollCursor++
	partitionCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	type partitionResult struct {
		index int
		result flowPollResult
	}
	completed := make(chan partitionResult, parallelism)
	for index, partition := range partitions {
		go func(index int, topics []string) {
			completed <- partitionResult{index: index, result: c.pollFlowPartition(partitionCtx, topics, cursor+uint64(index), immediate)}
		}(index, partition)
	}
	results := make([]flowPollResult, parallelism)
	for range partitions {
		result := <-completed
		results[result.index] = result.result
		if flowFatalPollError(result.result.err) {
			cancel()
		}
	}
	return results
}

func (c *subscriptionPullConsumer) pollFlowPartition(ctx context.Context, topics []string, cursor uint64, immediate bool) (result flowPollResult) {
	var lastFailure error
	for {
		if err := ctx.Err(); err != nil {
			result.err = err
			return result
		}
		if err := c.checkOpen(); err != nil {
			result.err = err
			return result
		}
		providers := c.providerSnapshot()
		available := providers[:0]
		for _, provider := range providers {
			if !provider.connection.isClosed() {
				available = append(available, provider)
			}
		}
		if len(available) == 0 {
			c.requestRecovery()
			if lastFailure == nil {
				lastFailure = errors.New("subscription: no available provider for poll")
			}
			result.err = lastFailure
			return result
		}
		progress := make(map[string]TopicProgress, len(topics))
		c.mu.RLock()
		for _, topic := range topics {
			if position := c.positions[topic]; len(position) != 0 {
				progress[topic] = cloneTopicProgress(position)
			}
		}
		c.mu.RUnlock()
		healthy := false
		for attempt := range available {
			provider := available[(int(cursor%uint64(len(available)))+attempt)%len(available)]
			responses, err := provider.poll(ctx, topics, flowRemainingMilliseconds(ctx, len(available)-attempt, immediate), int64(provider.bodyLimit), progress)
			if err != nil {
				err = c.handleProviderError(provider, err)
				if flowFatalPollError(err) || flowCriticalPollError(err) || ctx.Err() != nil {
					result.err = err
					return result
				}
				lastFailure = err
				continue
			}
			healthy = true
			for _, response := range responses {
				switch response.Type {
				case subscriptionResponseTablets:
					rollbackIndex := len(result.rollbackContexts)
					if flowCommitContextCommittable(response.CommitContext) {
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
						if flowCommitContextCommittable(response.CommitContext) {
							result.emptyContexts = append(result.emptyContexts, response.CommitContext)
						}
					} else {
						result.messages = append(result.messages, message)
					}
				case subscriptionResponseWatermark:
					message, err := response.message(c.config.Consumer.Dialect)
					if err != nil {
						result.err = err
						return result
					}
					result.messages = append(result.messages, message)
				case subscriptionResponseTermination:
					result.terminated = append(result.terminated, response.CommitContext.Topic)
				case subscriptionResponseError:
					if isOutdatedFlowResponse(response) {
						continue
					}
					failure := &subscriptionPollError{Message: response.ErrorMessage, Critical: response.Critical}
					if response.Critical {
						result.err = failure
						return result
					}
					if flowCommitContextCommittable(response.CommitContext) {
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

func (c *subscriptionPullConsumer) AddProcessor(processor SubscriptionMessageProcessor) error {
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
		stateErr = ErrClosed
	}
	c.mu.RUnlock()
	if stateErr != nil {
		return stateErr
	}
	c.processors = append(c.processors, processor)
	c.refreshBufferedContexts()
	return nil
}

func (c *subscriptionPullConsumer) DrainBufferedMessages(ctx context.Context) ([]SubscriptionMessage, error) {
	operationCtx, finish, err := c.beginOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer finish()
	defer c.refreshBufferedContexts()
	var messages []SubscriptionMessage
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
func (c *subscriptionPullConsumer) flushProcessors() (messages []SubscriptionMessage, err error) {
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

func (c *subscriptionPullConsumer) drainForClose() ([]SubscriptionMessage, error) {
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
	var contexts []CommitContext
	for _, message := range c.drained {
		if message != nil {
			commitContext := message.CommitContext()
			if flowCommitContextCommittable(commitContext) {
				contexts = append(contexts, cloneMessageCommitContext(commitContext))
			}
		}
	}
	for _, processor := range c.processors {
		if buffered, ok := processor.(BufferingProcessor); ok {
			for _, commitContext := range buffered.Buffered().Contexts {
				if flowCommitContextCommittable(commitContext) {
					contexts = append(contexts, cloneMessageCommitContext(commitContext))
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
		if buffered, ok := processor.(BufferingProcessor); ok {
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
		if _, ok := processor.(TopicResetter); !ok {
			return fmt.Errorf("subscription: processor %T cannot reset topic %q while multiple topics are subscribed", processor, topic)
		}
	}
	return nil
}

func (c *subscriptionPullConsumer) resetProcessors(topic string) {
	for _, processor := range c.processors {
		if resetter, ok := processor.(TopicResetter); ok {
			resetter.ResetTopic(topic)
		} else {
			processor.Reset()
		}
	}
	retained := c.drained[:0]
	for _, message := range c.drained {
		if message != nil && message.CommitContext().Topic == topic {
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
func (c *subscriptionPullConsumer) visibleFlowMessages(messages []SubscriptionMessage) []SubscriptionMessage {
	visible := make([]SubscriptionMessage, 0, len(messages))
	for _, message := range messages {
		if message == nil {
			continue
		}
		if timestamp, ok := message.Watermark(); ok {
			c.mu.Lock()
			if !c.hasWatermark || timestamp > c.watermark {
				c.watermark, c.hasWatermark = timestamp, true
			}
			c.mu.Unlock()
			continue
		}
		if message.Kind() == RecordMessage {
			visible = append(visible, message)
		}
	}
	return visible
}

func flowCommitContextCommittable(commitContext CommitContext) bool {
	return commitContext.Writer != nil || commitContext.CommitID != InvalidCommitID
}

func isOutdatedFlowResponse(response subscriptionPollResponse) bool {
	return response.Type == subscriptionResponseError && !response.Critical &&
		(response.ErrorMessage == "outdated subscription event" || response.ErrorMessage == "过期的订阅事件")
}

func flowCriticalPollError(err error) bool {
	var status *subscriptionStatusError
	if errors.As(err, &status) {
		return status.Critical
	}
	var payload *subscriptionPollError
	return errors.As(err, &payload) && payload.Critical
}

func flowFatalPollError(err error) bool {
	return errors.Is(err, ErrUnsupportedTsFile) || errors.Is(err, ErrConsumerFenced) || errors.Is(err, ErrOwnerFenced)
}

func (c *subscriptionPullConsumer) rollbackFlow(ctx context.Context, contexts []CommitContext) {
	if len(contexts) == 0 || ctx.Err() != nil || c.checkOpen() != nil {
		return
	}
	// A failed cleanup never replaces the original poll error. In particular,
	// NACK is not replayed when a transport failure schedules provider recovery.
	_ = c.commitContexts(ctx, contexts, true)
}

func (c *subscriptionPullConsumer) completeFlowRecord(ctx context.Context, provider *subscriptionProvider, response subscriptionPollResponse) (SubscriptionMessage, bool, error) {
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
		if next.Type == subscriptionResponseError {
			return nil, false, &subscriptionPollError{Message: next.ErrorMessage, Critical: next.Critical}
		}
		if next.NextOffset > 0 && next.NextOffset <= response.NextOffset {
			return nil, false, errors.New("subscription: tablet continuation offset did not advance")
		}
		if err := response.appendTablets(next); err != nil {
			return nil, false, err
		}
	}
	message, err := response.message(c.config.Consumer.Dialect)
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
