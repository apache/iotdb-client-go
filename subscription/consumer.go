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
	"time"
)

// SubscriptionConsumer is the shared lifecycle, subscription, and positioning contract.
// Repeated Open while open and repeated Close are harmless. A successfully
// closed consumer cannot be reopened. A failed Close that requires buffered
// messages to be drained leaves that recovery path available.
type SubscriptionConsumer interface {
	Positioner

	// Open establishes the consumer's connections and starts its background
	// work. It returns an error if opening fails. The context limits this
	// operation only; canceling it after Open returns does not stop consumption.
	Open(ctx context.Context) error
	// Close stops background work and releases connections. In manual-commit
	// mode, buffered processor output must first be drained, processed, and
	// committed before retrying a close that reports undelivered messages.
	// Closing does not guarantee that every outstanding commit succeeded.
	Close() error
	// Subscribe establishes subscriptions to one or more named topics. It
	// returns an error if the subscriptions cannot be established. TsFile topic
	// configuration is accepted; unsupported payloads fail during decoding.
	Subscribe(ctx context.Context, topics ...string) error
	// Unsubscribe removes subscriptions to one or more named topics and returns
	// an error if removal fails. It does not mean "all topics" when none are
	// supplied; the all-topics shorthand belongs to polling only.
	Unsubscribe(ctx context.Context, topics ...string) error
	// Info returns an independent identity and ownership snapshot. Consumer and
	// group IDs omitted from configuration are assigned when Open succeeds.
	Info() SubscriptionConsumerInfo
	// LatestWatermark reports the latest received server watermark. The boolean
	// is false until a watermark is observed. This is not a processor watermark.
	LatestWatermark() (timestamp int64, ok bool)
	// AllConsumed reports whether all subscribed topics have completed and been
	// automatically unsubscribed. An active unbounded topic prevents completion;
	// no subscriptions means true. This does not assert that delivered messages
	// were committed or that client-side processor buffers have been drained.
	AllConsumed() bool
}

// SubscriptionConsumerInfo contains identity and ownership, without exposing credentials.
type SubscriptionConsumerInfo struct {
	// ConsumerID identifies this consumer and is assigned on Open if omitted.
	ConsumerID string
	// GroupID identifies the consumer group and is assigned on Open if omitted.
	GroupID string
	// OwnerID is the configured topic owner identity, if any.
	OwnerID string
	// OwnerEpoch is nil when no epoch was supplied; a pointer to zero is an
	// explicitly supplied epoch. The pointer is part of the returned snapshot.
	OwnerEpoch *int64
}

// SubscriptionPullConsumer fetches record messages explicitly. Poll and PollWithInfo use
// server timestamp units for message data and time.Duration for timeouts. The
// shorter of the timeout and the context deadline bounds a polling operation.
// No topics means all subscribed topics.
//
// A TsFile decoding error returns no messages for that poll round, including
// records from parallel requests. That round is not acknowledged, added to the
// processor or auto-commit buffers, or used to advance positions. Previously
// delivered records and confirmed progress are unaffected. The caller can
// unsubscribe the offending topic and poll again; no automatic retry is made.
type SubscriptionPullConsumer interface {
	SubscriptionConsumer

	// Poll retrieves messages from the named topics, waiting up to timeout.
	// Omitting topics selects all current subscriptions. An empty result with a
	// nil error is normal when no records arrive within the polling period.
	// It returns an error when polling, context cancellation, or decoding fails.
	Poll(ctx context.Context, timeout time.Duration, topics ...string) ([]SubscriptionMessage, error)
	// PollWithInfo polls with the same topic, timeout, and error semantics as
	// Poll and also reports processor buffering and ordering-watermark state.
	PollWithInfo(ctx context.Context, timeout time.Duration, topics ...string) (PollResult, error)
	// DrainBufferedMessages retrieves user-visible records held by client-side
	// processors without starting a new server poll. The result can be empty.
	// Manual-commit callers must process and explicitly commit these records
	// before closing when processors still have buffered output.
	DrainBufferedMessages(ctx context.Context) ([]SubscriptionMessage, error)

	// Commit synchronously confirms that one or more previously delivered
	// messages were successfully processed. A returned error does not imply
	// that every message failed; accepted progress is not rolled back.
	Commit(ctx context.Context, messages ...SubscriptionMessage) error
	// CommitAsync confirms the same processing outcome as Commit without
	// waiting for completion. Its result channel has capacity one, receives
	// exactly one error or nil on success, and is then closed. An unread result
	// never blocks cleanup. Context cancellation does not undo accepted commits.
	CommitAsync(ctx context.Context, messages ...SubscriptionMessage) <-chan error

	// AddProcessor appends a processor to the pipeline. Each poll passes messages
	// through processors in their addition order before making records visible.
	AddProcessor(processor SubscriptionMessageProcessor) error
}

// ConsumeFunc handles a delivered record message. A nil error means success;
// a non-nil error means failure. Acknowledgment and redelivery depend on the
// configured AckStrategy rather than on the callback alone.
// The callback context is canceled when the consumer stops delivery. The message
// remains valid for the callback; the callback must not release it or retain
// borrowed record data after returning without copying that data.
type ConsumeFunc func(ctx context.Context, message SubscriptionMessage) error

// SubscriptionPushConsumer polls in the background and delivers records to SubscriptionPushConsumerConfig.Handler.
// A TsFile decoding error stops automatic polling before it is reported; no
// record callback or acknowledgment is performed for that failed poll round.
type SubscriptionPushConsumer interface {
	SubscriptionConsumer

	// Errors reports the first terminal background error, then delivery stops.
	// The channel has capacity at least one; the error is sent once even if the
	// caller is not receiving. The lifecycle owner closes it after all senders
	// have stopped. An ordinary callback failure is not a terminal error.
	Errors() <-chan error
}

// PollResult adds processing metadata to a successful poll. Watermark belongs to
// the last WatermarkReporter in the pipeline, not SubscriptionConsumer.LatestWatermark.
type PollResult struct {
	// Messages contains the user-visible records emitted by the processing chain.
	Messages []SubscriptionMessage
	// BufferedCount is the sum of messages retained by all processors.
	BufferedCount int
	// Watermark is the last reporter's ordering watermark, valid only when
	// HasWatermark is true. It is not the latest received server watermark.
	Watermark int64
	// HasWatermark distinguishes an available watermark from an uninitialized
	// value; zero can itself be a valid watermark.
	HasWatermark bool
}
