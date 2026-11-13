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

package session

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/apache/iotdb-client-go/subscription"
	"github.com/apache/iotdb-client-go/subscription/internal/identifier"
	"github.com/apache/iotdb-client-go/subscription/internal/rpcsession"
)

// NewSubscriptionSession validates and copies config without opening a network
// connection. Start with DefaultSubscriptionSessionConfig for root/root defaults.
// An explicitly empty Password is preserved, including when config is otherwise
// zero-valued. Use Open to establish the session and Close to release it.
func NewSubscriptionSession(config SubscriptionSessionConfig) (SubscriptionSession, error) {
	resolved, err := normalizeSubscriptionSessionConfig(config)
	if err != nil {
		return nil, err
	}
	return &subscriptionSession{
		config: resolved,
		operations: make(chan struct{}, 1),
	}, nil
}

type subscriptionSession struct {
	config SubscriptionSessionConfig
	operations chan struct{}
	stateMu sync.Mutex
	connection *rpcsession.Connection
	openingCancel context.CancelFunc
	generation uint64
	closed bool
}

func (s *subscriptionSession) acquire(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("subscription: context must not be nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case s.operations <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *subscriptionSession) connect(ctx context.Context, generation uint64) (*rpcsession.Connection, error) {
	openingCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	s.stateMu.Lock()
	if s.generation != generation {
		s.stateMu.Unlock()
		return nil, subscription.ErrClosed
	}
	s.openingCancel = cancel
	s.stateMu.Unlock()
	connection, err := rpcsession.Open(openingCtx, s.config.Connection, s.config.Dialect)
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	s.openingCancel = nil
	if s.generation != generation {
		if connection != nil {
			connection.Abort()
		}
		return nil, subscription.ErrClosed
	}
	if err != nil {
		return nil, err
	}
	if err := openingCtx.Err(); err != nil {
		connection.Abort()
		return nil, err
	}
	s.connection = connection
	s.closed = false
	return connection, nil
}

func (s *subscriptionSession) Open(ctx context.Context) error {
	if err := s.acquire(ctx); err != nil {
		return err
	}
	defer func() { <-s.operations }()
	s.stateMu.Lock()
	connection, generation := s.connection, s.generation
	s.stateMu.Unlock()
	if connection != nil && !connection.Closed() {
		return nil
	}
	_, err := s.connect(ctx, generation)
	return err
}

func (s *subscriptionSession) Close() error {
	s.stateMu.Lock()
	s.generation++
	if s.openingCancel != nil {
		s.openingCancel()
		s.openingCancel = nil
	}
	connection := s.connection
	s.connection = nil
	s.closed = true
	s.stateMu.Unlock()
	if connection == nil {
		return nil
	}
	select {
	case s.operations <- struct{}{}:
		defer func() { <-s.operations }()
		ctx, cancel := context.WithTimeout(context.Background(), s.config.Connection.ConnectTimeout)
		defer cancel()
		return connection.Close(ctx)
	default:
		// Interrupt an in-flight RPC instead of waiting for its operation lock.
		return connection.Abort()
	}
}

func (s *subscriptionSession) withConnection(ctx context.Context, operation func(*rpcsession.Connection) error) error {
	if err := s.acquire(ctx); err != nil {
		return err
	}
	defer func() { <-s.operations }()
	s.stateMu.Lock()
	connection, generation, closed := s.connection, s.generation, s.closed
	s.stateMu.Unlock()
	if connection == nil {
		if closed {
			return subscription.ErrClosed
		}
		return subscription.ErrNotOpen
	}
	if connection.Closed() {
		var err error
		connection, err = s.connect(ctx, generation)
		if err != nil {
			return err
		}
	}
	return operation(connection)
}

func (s *subscriptionSession) execute(ctx context.Context, statement string) error {
	return s.withConnection(ctx, func(connection *rpcsession.Connection) error {
		return connection.Execute(ctx, statement)
	})
}

func (s *subscriptionSession) query(ctx context.Context, statement string, columns int, allowExtra bool) ([][]string, error) {
	var rows [][]string
	err := s.withConnection(ctx, func(connection *rpcsession.Connection) error {
		var err error
		rows, err = connection.Query(ctx, statement, columns, allowExtra)
		return err
	})
	return rows, err
}

func (s *subscriptionSession) CreateTopic(ctx context.Context, name string, options CreateTopicOptions) error {
	sqlIdentifier, err := identifier.CheckSQL(name)
	if err != nil {
		return err
	}
	statement := "CREATE TOPIC "
	if options.IfNotExists {
		statement += "IF NOT EXISTS "
	}
	statement += sqlIdentifier
	if len(options.Attributes) != 0 {
		statement += " WITH " + buildTopicAttributesClause(options.Attributes)
	}
	return s.execute(ctx, statement)
}

func (s *subscriptionSession) AlterTopic(ctx context.Context, name string, attributes map[string]string) error {
	return s.alterTopic(ctx, name, attributes, false)
}

func (s *subscriptionSession) alterTopic(ctx context.Context, name string, attributes map[string]string, allowOwner bool) error {
	sqlIdentifier, err := identifier.CheckSQL(name)
	if err != nil {
		return err
	}
	if err := validateAlterTopicAttributes(attributes, allowOwner); err != nil {
		return err
	}
	return s.execute(ctx, "ALTER TOPIC " + sqlIdentifier + " WITH " + buildTopicAttributesClause(attributes))
}

func (s *subscriptionSession) AlterTopicOwner(ctx context.Context, name string, owner TopicOwner) error {
	if owner.ID == "" {
		return fmt.Errorf("subscription: topic owner ID must not be empty")
	}
	attributes := map[string]string{
		"owner-id": owner.ID,
		"owner-epoch": strconv.FormatInt(owner.Epoch, 10),
	}
	if owner.LeaseDuration != nil {
		if *owner.LeaseDuration % time.Millisecond != 0 {
			return fmt.Errorf("subscription: owner lease duration must be a whole number of milliseconds")
		}
		attributes["owner-lease-duration-ms"] = strconv.FormatInt(owner.LeaseDuration.Milliseconds(), 10)
	}
	return s.alterTopic(ctx, name, attributes, true)
}

func (s *subscriptionSession) DropTopic(ctx context.Context, name string, options DropOptions) error {
	sqlIdentifier, err := identifier.CheckSQL(name)
	if err != nil {
		return err
	}
	statement := "DROP TOPIC "
	if options.IfExists {
		statement += "IF EXISTS "
	}
	return s.execute(ctx, statement + sqlIdentifier)
}

func (s *subscriptionSession) Topics(ctx context.Context) ([]Topic, error) {
	rows, err := s.query(ctx, "SHOW TOPICS", 2, true)
	if err != nil {
		return nil, err
	}
	topics := make([]Topic, 0, len(rows))
	for _, row := range rows {
		topics = append(topics, Topic{Name: row[0], Attributes: row[1]})
	}
	return topics, nil
}

func (s *subscriptionSession) Topic(ctx context.Context, name string) (Topic, bool, error) {
	sqlIdentifier, err := identifier.CheckSQL(name)
	if err != nil {
		return Topic{}, false, err
	}
	rows, err := s.query(ctx, "SHOW TOPIC " + sqlIdentifier, 2, true)
	if err != nil {
		return Topic{}, false, err
	}
	if len(rows) == 0 {
		return Topic{}, false, nil
	}
	return Topic{Name: rows[0][0], Attributes: rows[0][1]}, true, nil
}

func (s *subscriptionSession) Subscriptions(ctx context.Context, topic string) ([]Subscription, error) {
	statement := "SHOW SUBSCRIPTIONS"
	if topic != "" {
		sqlIdentifier, err := identifier.CheckSQL(topic)
		if err != nil {
			return nil, err
		}
		statement += " ON " + sqlIdentifier
	}
	rows, err := s.query(ctx, statement, 4, false)
	if err != nil {
		return nil, err
	}
	subscriptions := make([]Subscription, 0, len(rows))
	for _, row := range rows {
		subscriptions = append(subscriptions, Subscription{
			ID: row[0], Topic: row[1], GroupID: row[2], ConsumerIDs: row[3],
		})
	}
	return subscriptions, nil
}

func (s *subscriptionSession) DropSubscription(ctx context.Context, id string, options DropOptions) error {
	sqlIdentifier, err := identifier.CheckSQL(id)
	if err != nil {
		return err
	}
	statement := "DROP SUBSCRIPTION "
	if options.IfExists {
		statement += "IF EXISTS "
	}
	return s.execute(ctx, statement + sqlIdentifier)
}
