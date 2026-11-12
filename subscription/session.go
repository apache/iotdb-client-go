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

// SubscriptionSession manages topics and subscriptions for one configured
// dialect. Tree and table sessions share this method contract, while their
// connection and SQL dialect remain model-specific. TsFile topic attributes pass
// through to the server; support is checked only when a consumer decodes a
// payload. Management operations require an open session and report connection,
// statement-execution, or context errors. Returned collections are caller-owned
// snapshots.
type SubscriptionSession interface {
	// Open establishes the connection to IoTDB using the configured dialect and
	// credentials. The context limits this opening operation; it does not own
	// the lifetime of a successfully opened session.
	Open(ctx context.Context) error
	// Close closes the session and releases its connection resources, returning
	// any error encountered while closing them.
	Close() error

	// CreateTopic creates name with the supplied attributes and existence
	// condition. A name containing single quotes must be enclosed in backticks.
	// Attribute keys and values are quoted and escaped for SQL automatically;
	// a nil or empty Attributes map creates the topic without a WITH clause.
	// Connection failures, invalid identifiers, and rejected statements are
	// returned as errors.
	CreateTopic(ctx context.Context, name string, options CreateTopicOptions) error
	// AlterTopic changes the supplied properties of name. Attributes must not
	// be empty; ownership properties must be changed with AlterTopicOwner.
	// A name containing single quotes must be enclosed in backticks. Invalid
	// input, connection failures, and rejected statements are returned as errors.
	AlterTopic(ctx context.Context, name string, attributes map[string]string) error
	// AlterTopicOwner transfers name to the supplied owner ID and monotonically
	// increasing epoch, optionally setting its lease duration. The owner ID must
	// not be empty. A name containing single quotes must be enclosed in backticks.
	// Ownership validation and statement failures are returned as errors.
	AlterTopicOwner(ctx context.Context, name string, owner TopicOwner) error
	// DropTopic removes name, using options.IfExists to allow a missing topic.
	// A name containing single quotes must be enclosed in backticks. Connection
	// failures and rejected statements are returned as errors.
	DropTopic(ctx context.Context, name string, options DropOptions) error
	// Topics retrieves all existing topics. A failed query or connection is
	// reported as an error rather than an empty successful result.
	Topics(ctx context.Context) ([]Topic, error)
	// Topic retrieves name and returns found=false when it does not exist.
	// A name containing single quotes must be enclosed in backticks. Query and
	// connection failures are reported through err.
	Topic(ctx context.Context, name string) (topic Topic, found bool, err error)

	// Subscriptions retrieves subscriptions belonging to topic, or every
	// subscription when topic is empty. A nonempty topic containing single
	// quotes must be enclosed in backticks. Query and connection failures are
	// returned as errors.
	Subscriptions(ctx context.Context, topic string) ([]Subscription, error)
	// DropSubscription removes the subscription identified by id. When
	// options.IfExists is true, a missing subscription is not an error. Invalid
	// identifiers, connection failures, and rejected statements return errors.
	DropSubscription(ctx context.Context, id string, options DropOptions) error
}

// CreateTopicOptions combines topic attributes and conditional creation.
type CreateTopicOptions struct {
	// Attributes contains the topic properties to include in the CREATE TOPIC
	// statement. Nil or empty uses the server's topic defaults.
	Attributes map[string]string
	// IfNotExists adds IF NOT EXISTS, allowing an existing topic without error.
	IfNotExists bool
}

// DropOptions controls conditional deletion of a topic or subscription.
type DropOptions struct {
	// IfExists adds IF EXISTS, allowing the named object to be absent.
	IfExists bool
}

// TopicOwner sets the owner and its epoch. A nil LeaseDuration preserves the
// server's default lease behavior; a non-nil duration is an explicit setting.
type TopicOwner struct {
	// ID is the nonempty owner identity to assign to the topic.
	ID string
	// Epoch is the new monotonically increasing owner epoch.
	Epoch int64
	// LeaseDuration optionally sets the owner's lease. Unlike the Java API's
	// millisecond argument, this field is expressed as a time.Duration.
	LeaseDuration *time.Duration
}

// Topic contains the name and the server's textual topic attributes. The raw
// representation is preserved rather than assuming it is a client-side map.
type Topic struct {
	// Name is the topic name returned by the server.
	Name string
	// Attributes is the topic's property description as returned by the server.
	Attributes string
}

// Subscription is one server-side subscription description. ConsumerIDs retains
// the server's textual representation, which is not assumed to be a CSV list.
type Subscription struct {
	// ID uniquely identifies the server-side subscription.
	ID string
	// Topic is the topic to which this subscription belongs.
	Topic string
	// GroupID identifies the consumer group sharing the subscription.
	GroupID string
	// ConsumerIDs is the server's description of the group's consumer IDs.
	ConsumerIDs string
}
