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
	"github.com/apache/iotdb-client-go/v2/subscription/internal/configutil"
	"github.com/apache/iotdb-client-go/v2/subscription/internal/identifier"

	"fmt"
	"time"
)

// DefaultSubscriptionConsumerConfig returns tree-model settings with the same
// connection defaults as the management session, a 30-second heartbeat,
// two-minute endpoint synchronization, and polling parallelism of one. Empty
// consumer and group IDs request assigned identities. Each call owns its endpoint
// slice and does not establish a connection.
func DefaultSubscriptionConsumerConfig() SubscriptionConsumerConfig {
	return SubscriptionConsumerConfig{
		Connection:            configutil.DefaultConnection(),
		Dialect:               subscription.TreeDialect,
		HeartbeatInterval:     30 * time.Second,
		EndpointsSyncInterval: 2 * time.Minute,
		MaxPollParallelism:    1,
	}
}

// DefaultSubscriptionPullConsumerConfig enables automatic commit with a
// five-second interval and uses DefaultSubscriptionConsumerConfig for shared
// settings. Each call returns its own AutoCommit pointer and endpoint slice.
func DefaultSubscriptionPullConsumerConfig() SubscriptionPullConsumerConfig {
	autoCommit := true
	return SubscriptionPullConsumerConfig{
		Consumer:           DefaultSubscriptionConsumerConfig(),
		AutoCommit:         &autoCommit,
		AutoCommitInterval: 5 * time.Second,
	}
}

// DefaultSubscriptionPushConsumerConfig selects acknowledgment after successful
// consumption, a 100-millisecond polling interval, and a ten-second poll timeout.
// Shared settings come from DefaultSubscriptionConsumerConfig. Handler remains
// nil: callers must supply their own handler before the configuration is usable.
func DefaultSubscriptionPushConsumerConfig() SubscriptionPushConsumerConfig {
	return SubscriptionPushConsumerConfig{
		Consumer:     DefaultSubscriptionConsumerConfig(),
		AckStrategy:  AckAfterConsume,
		PollInterval: 100 * time.Millisecond,
		PollTimeout:  10 * time.Second,
	}
}

// normalizeSubscriptionConsumerConfig copies caller-owned connection and epoch
// data, resolves defaults, and parses consumer identities for protocol use.
// Unlike management SQL identifiers, protocol identities have their surrounding
// backticks removed and doubled backticks unescaped.
func normalizeSubscriptionConsumerConfig(config SubscriptionConsumerConfig) (SubscriptionConsumerConfig, error) {
	dialect, err := configutil.NormalizeDialect(config.Dialect)
	if err != nil {
		return SubscriptionConsumerConfig{}, err
	}
	connection, err := configutil.NormalizeConnection(config.Connection)
	if err != nil {
		return SubscriptionConsumerConfig{}, err
	}
	config.Connection = connection
	config.Dialect = dialect
	if config.ConsumerID != "" {
		config.ConsumerID, err = identifier.Parse(config.ConsumerID)
		if err != nil {
			return SubscriptionConsumerConfig{}, fmt.Errorf("subscription: ConsumerID: %w", err)
		}
	}
	if config.GroupID != "" {
		config.GroupID, err = identifier.Parse(config.GroupID)
		if err != nil {
			return SubscriptionConsumerConfig{}, fmt.Errorf("subscription: GroupID: %w", err)
		}
	}
	if config.OwnerEpoch != nil {
		epoch := *config.OwnerEpoch
		config.OwnerEpoch = &epoch
	}
	defaults := DefaultSubscriptionConsumerConfig()
	config.HeartbeatInterval, err = normalizeConsumerInterval("HeartbeatInterval", config.HeartbeatInterval, defaults.HeartbeatInterval, time.Second)
	if err != nil {
		return SubscriptionConsumerConfig{}, err
	}
	config.EndpointsSyncInterval, err = normalizeConsumerInterval("EndpointsSyncInterval", config.EndpointsSyncInterval, defaults.EndpointsSyncInterval, 5*time.Second)
	if err != nil {
		return SubscriptionConsumerConfig{}, err
	}
	if config.MaxPollParallelism < 0 {
		return SubscriptionConsumerConfig{}, fmt.Errorf("subscription: MaxPollParallelism must not be negative")
	}
	if config.MaxPollParallelism == 0 {
		config.MaxPollParallelism = defaults.MaxPollParallelism
	}
	return config, nil
}

// normalizeSubscriptionPullConsumerConfig preserves an explicit manual-commit
// choice, copies AutoCommit, and clamps a positive interval below Java's minimum.
func normalizeSubscriptionPullConsumerConfig(config SubscriptionPullConsumerConfig) (SubscriptionPullConsumerConfig, error) {
	consumer, err := normalizeSubscriptionConsumerConfig(config.Consumer)
	if err != nil {
		return SubscriptionPullConsumerConfig{}, err
	}
	config.Consumer = consumer
	autoCommit := true
	if config.AutoCommit != nil {
		autoCommit = *config.AutoCommit
	}
	config.AutoCommit = &autoCommit
	defaults := DefaultSubscriptionPullConsumerConfig()
	config.AutoCommitInterval, err = normalizeConsumerInterval("AutoCommitInterval", config.AutoCommitInterval, defaults.AutoCommitInterval, 500*time.Millisecond)
	if err != nil {
		return SubscriptionPullConsumerConfig{}, err
	}
	return config, nil
}

// normalizeSubscriptionPushConsumerConfig requires a real handler and a known
// acknowledgment strategy. Zero intervals select defaults; positive intervals
// below the Java minimum are clamped and negative intervals are rejected.
func normalizeSubscriptionPushConsumerConfig(config SubscriptionPushConsumerConfig) (SubscriptionPushConsumerConfig, error) {
	consumer, err := normalizeSubscriptionConsumerConfig(config.Consumer)
	if err != nil {
		return SubscriptionPushConsumerConfig{}, err
	}
	config.Consumer = consumer
	if config.AckStrategy != AckAfterConsume && config.AckStrategy != AckBeforeConsume {
		return SubscriptionPushConsumerConfig{}, fmt.Errorf("subscription: unsupported acknowledgment strategy %d", config.AckStrategy)
	}
	if config.Handler == nil {
		return SubscriptionPushConsumerConfig{}, fmt.Errorf("subscription: push consumer Handler must not be nil")
	}
	defaults := DefaultSubscriptionPushConsumerConfig()
	config.PollInterval, err = normalizeConsumerInterval("PollInterval", config.PollInterval, defaults.PollInterval, time.Millisecond)
	if err != nil {
		return SubscriptionPushConsumerConfig{}, err
	}
	config.PollTimeout, err = normalizeConsumerInterval("PollTimeout", config.PollTimeout, defaults.PollTimeout, time.Second)
	if err != nil {
		return SubscriptionPushConsumerConfig{}, err
	}
	return config, nil
}

func normalizeConsumerInterval(name string, value, fallback, minimum time.Duration) (time.Duration, error) {
	if value < 0 {
		return 0, fmt.Errorf("subscription: %s must not be negative", name)
	}
	if value == 0 {
		return fallback, nil
	}
	if value < minimum {
		return minimum, nil
	}
	return value, nil
}
