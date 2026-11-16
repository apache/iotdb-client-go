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
	"time"

	"github.com/apache/iotdb-client-go/subscription"
)

// SubscriptionConsumerConfig holds settings shared by pull and push consumers. Empty consumer
// and group IDs request server-assigned identities. A nil OwnerEpoch means that
// no owner epoch was supplied; a pointer to zero is an explicit epoch.
type SubscriptionConsumerConfig struct {
	// Connection supplies the seed endpoints, authentication, and transport settings.
	Connection subscription.ConnectionConfig
	// Dialect selects the model used by the consumer's connections and records.
	Dialect subscription.Dialect
	// ConsumerID identifies this consumer. Empty requests an assigned identity.
	ConsumerID string
	// GroupID identifies the consumer group. Empty requests an assigned group ID.
	GroupID string
	// OwnerID supplies the identity used when a topic requires ownership fencing.
	OwnerID string
	// OwnerEpoch supplies the owner's epoch. Nil omits it; a pointer to zero
	// explicitly supplies epoch zero.
	OwnerEpoch *int64
	// HeartbeatInterval controls provider heartbeat frequency. Java consumers
	// default to 30 seconds and enforce a minimum of one second.
	HeartbeatInterval time.Duration
	// EndpointsSyncInterval controls how often the consumer refreshes cluster
	// endpoints. Java consumers default to two minutes and enforce a five-second
	// minimum.
	EndpointsSyncInterval time.Duration
	// MaxPollParallelism is retained for Java configuration compatibility. The
	// current Java client executes poll work serially even when this is greater
	// than one, so Go also polls serially.
	MaxPollParallelism int
}

// SubscriptionPullConsumerConfig adds automatic commit settings. A nil AutoCommit selects the Java
// default, true; an explicit false selects manual commit. Use
// DefaultSubscriptionPullConsumerConfig to obtain explicit timing defaults.
type SubscriptionPullConsumerConfig struct {
	// Consumer contains the shared connection, identity, and polling settings.
	Consumer SubscriptionConsumerConfig
	// AutoCommit selects periodic acknowledgment of delivered messages. Nil
	// selects true; an explicit false leaves acknowledgment to the caller.
	AutoCommit *bool
	// AutoCommitInterval controls periodic acknowledgment when AutoCommit is
	// enabled. The Java default is five seconds, with a 500-millisecond minimum.
	AutoCommitInterval time.Duration
}

// AckStrategy controls when push messages are acknowledged.
type AckStrategy uint8

const (
	// AckAfterConsume runs callbacks sequentially, acknowledges the successful
	// group, then nacks the failed group. An ACK error skips the subsequent NACK.
	AckAfterConsume AckStrategy = iota
	// AckBeforeConsume acknowledges the entire batch before invoking callbacks.
	// A partial or failed ACK prevents callbacks for that batch; callback failure
	// does not cause a NACK or redelivery under this strategy.
	AckBeforeConsume
)

// SubscriptionPushConsumerConfig configures background polling and delivery. Handler is required.
// The zero AckStrategy is AckAfterConsume.
type SubscriptionPushConsumerConfig struct {
	// Consumer contains the shared connection, identity, and polling settings.
	Consumer SubscriptionConsumerConfig
	// AckStrategy selects acknowledgment before delivery or after successful
	// processing. Its zero value is AckAfterConsume.
	AckStrategy AckStrategy
	// Handler receives record messages sequentially. It is required; nil returned
	// by a call means success, while an error or panic means consumption failed.
	// It must not release the borrowed message and should return promptly when
	// its context is canceled. Close does not wait for Handler to return.
	Handler ConsumeFunc
	// PollInterval is the fixed delay after one polling and delivery round
	// finishes before another starts. Java defaults to 100 milliseconds and
	// requires an interval greater than zero.
	PollInterval time.Duration
	// PollTimeout limits one automatic poll. Java defaults to ten seconds and
	// enforces a one-second minimum. Message timestamps retain server precision.
	PollTimeout time.Duration
}
