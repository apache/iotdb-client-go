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

package e2e

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/apache/iotdb-client-go/subscription"
	subconsumer "github.com/apache/iotdb-client-go/subscription/consumer"
	subpayload "github.com/apache/iotdb-client-go/subscription/payload"
)

func subscriptionTreePullWithIdentity(
	t *testing.T,
	ctx context.Context,
	connection subscription.ConnectionConfig,
	consumerID, groupID string,
	autoCommit bool,
	topics ...string,
) subconsumer.SubscriptionPullConsumer {
	t.Helper()
	config := subconsumer.DefaultSubscriptionPullConsumerConfig()
	config.Consumer.Connection = connection
	config.Consumer.Dialect = subscription.TreeDialect
	config.Consumer.ConsumerID = consumerID
	config.Consumer.GroupID = groupID
	config.Consumer.HeartbeatInterval = subscriptionE2EHeartbeatInterval
	config.Consumer.EndpointsSyncInterval = subscriptionE2EEndpointsSyncInterval
	config.AutoCommit, config.AutoCommitInterval = &autoCommit, subscriptionE2EAutoCommitInterval
	consumer, err := subconsumer.NewSubscriptionPullConsumer(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := consumer.Close(); err != nil {
			t.Errorf("close pull consumer: %v", err)
		}
	})
	if err := consumer.Open(ctx); err != nil {
		t.Fatal(err)
	}
	if err := consumer.Subscribe(ctx, topics...); err != nil {
		t.Fatal(err)
	}
	return consumer
}

// Java reference: IoTDBConsensusSubscriptionConsumerGroupIT.testDifferentConsumerGroupsReceiveIndependentFullData.
func TestSubscriptionTreeConsumerGroupsReceiveIndependentData(t *testing.T) {
	connection := subscriptionE2EConnection(t)
	ctx := subscriptionE2EContext(t)
	writer, database := subscriptionTreeWriter(t, connection)
	want := []subscriptionTreeRow{
		subscriptionTreeInsert(t, writer, database, "d1", "s1", 1, 101),
		subscriptionTreeInsert(t, writer, database, "d1", "s1", 2, 102),
		subscriptionTreeInsert(t, writer, database, "d1", "s1", 3, 103),
	}
	subscriptionTreeSQL(t, writer, "FLUSH")

	manager := subscriptionE2EManager(t, connection, subscription.TreeDialect)
	topic := subscriptionE2EName("tree_groups")
	subscriptionE2ETopic(t, ctx, manager, topic, map[string]string{
		"format": "SubscriptionRecordHandler", "mode": "snapshot", "path": database + ".d1.s1",
	})

	first := subscriptionTreePullWithIdentity(t, ctx, connection, subscriptionE2EName("tree_group_consumer"), subscriptionE2EName("tree_group_a"), true, topic)
	second := subscriptionTreePullWithIdentity(t, ctx, connection, subscriptionE2EName("tree_group_consumer"), subscriptionE2EName("tree_group_b"), true, topic)
	subscriptionTreeRequireRows(t, subscriptionTreeCollect(t, ctx, first, len(want), false, true), want...)
	subscriptionTreeRequireRows(t, subscriptionTreeCollect(t, ctx, second, len(want), false, true), want...)
}

func TestSubscriptionTreeConsumerReadsMultipleTopics(t *testing.T) {
	connection := subscriptionE2EConnection(t)
	ctx := subscriptionE2EContext(t)
	writer, database := subscriptionTreeWriter(t, connection)
	firstRow := subscriptionTreeInsert(t, writer, database, "d1", "s1", 10, 110)
	secondRow := subscriptionTreeInsert(t, writer, database, "d2", "s1", 20, 220)
	subscriptionTreeSQL(t, writer, "FLUSH")

	manager := subscriptionE2EManager(t, connection, subscription.TreeDialect)
	firstTopic := subscriptionE2EName("tree_multi_topic")
	secondTopic := subscriptionE2EName("tree_multi_topic")
	subscriptionE2ETopic(t, ctx, manager, firstTopic, map[string]string{
		"format": "SubscriptionRecordHandler", "mode": "snapshot", "path": database + ".d1.s1",
	})
	subscriptionE2ETopic(t, ctx, manager, secondTopic, map[string]string{
		"format": "SubscriptionRecordHandler", "mode": "snapshot", "path": database + ".d2.s1",
	})
	consumer := subscriptionTreePullWithIdentity(
		t, ctx, connection, subscriptionE2EName("tree_multi_topic_consumer"), subscriptionE2EName("tree_multi_topic_group"), true,
		firstTopic, secondTopic,
	)
	seen := map[string]map[subscriptionTreeRow]int{firstTopic: {}, secondTopic: {}}
	for round := 0; round < subscriptionE2EMaxPollRounds; round++ {
		messages, err := consumer.Poll(ctx, subscriptionE2EPollTimeout)
		if err != nil {
			t.Fatal(err)
		}
		for _, message := range messages {
			topic := message.CommitContext().Topic
			rows, err := subscriptionTreeRows(ctx, message)
			message.Release()
			if err != nil {
				t.Fatal(err)
			}
			if _, expected := seen[topic]; !expected {
				t.Fatalf("unexpected topic in multi-topic consumer: %s", topic)
			}
			for _, row := range rows {
				seen[topic][row]++
			}
		}
		if consumer.AllConsumed() {
			subscriptionTreeRequireRows(t, seen[firstTopic], firstRow)
			subscriptionTreeRequireRows(t, seen[secondTopic], secondRow)
			return
		}
	}
	t.Fatalf("multi-topic snapshot did not complete: seen=%v", seen)
}

// Java reference: IoTDBConsensusSubscriptionConsumerGroupIT.testTwoConsumersInSameGroupDoNotDuplicateRows.
func TestSubscriptionTreeConsumerGroupDoesNotDuplicateData(t *testing.T) {
	connection := subscriptionE2EConnection(t)
	ctx := subscriptionE2EContext(t)
	writer, database := subscriptionTreeWriter(t, connection)
	subscriptionTreeInsert(t, writer, database, "d1", "s1", 0, -1)
	subscriptionTreeSQL(t, writer, "FLUSH")

	manager := subscriptionE2EManager(t, connection, subscription.TreeDialect)
	topic := subscriptionE2EName("tree_same_group")
	subscriptionE2ETopic(t, ctx, manager, topic, map[string]string{
		"format": "SubscriptionRecordHandler", "mode": "incremental", "order-mode": "per-writer", "path": database + ".d1.s1",
	})
	group := subscriptionE2EName("tree_same_group_id")
	first := subscriptionTreePullWithIdentity(t, ctx, connection, subscriptionE2EName("tree_same_group_consumer"), group, false, topic)
	second := subscriptionTreePullWithIdentity(t, ctx, connection, subscriptionE2EName("tree_same_group_consumer"), group, false, topic)
	// Give both members time to observe the other through heartbeat-driven group
	// state before writes begin. The assertion covers the complete, duplicate-free
	// union rather than which member receives a partition.
	time.Sleep(2 * subscriptionE2EHeartbeatInterval)

	var want []subscriptionTreeRow
	for timestamp := int64(1); timestamp <= 8; timestamp++ {
		want = append(want, subscriptionTreeInsert(t, writer, database, "d1", "s1", timestamp, timestamp*10))
	}
	subscriptionTreeSQL(t, writer, "FLUSH")
	subscriptionTreeRequireGroupRows(t, ctx, []subconsumer.SubscriptionPullConsumer{first, second}, want)
}

func subscriptionTreeRequireGroupRows(t *testing.T, ctx context.Context, consumers []subconsumer.SubscriptionPullConsumer, want []subscriptionTreeRow) {
	t.Helper()
	expected := make(map[subscriptionTreeRow]struct{}, len(want))
	for _, row := range want {
		expected[row] = struct{}{}
	}
	seen := make(map[subscriptionTreeRow]int, len(want))
	quiet := 0
	for round := 0; round < subscriptionE2EMaxPollRounds; round++ {
		progressed := false
		for _, consumer := range consumers {
			messages, err := consumer.Poll(ctx, subscriptionE2EPollTimeout)
			if err != nil {
				t.Fatal(err)
			}
			rows, err := subscriptionTreeReadAndCommit(ctx, consumer, messages, true)
			if err != nil {
				t.Fatal(err)
			}
			if len(messages) != 0 {
				progressed = true
			}
			for _, row := range rows {
				seen[row]++
				if seen[row] > 1 {
					t.Fatalf("same-group consumers received duplicate row %+v", row)
				}
				if _, exists := expected[row]; !exists {
					t.Fatalf("same-group consumers received unexpected row %+v", row)
				}
			}
		}
		if len(seen) == len(expected) && !progressed {
			quiet++
			if quiet == subscriptionE2EQuietPollRounds {
				return
			}
		} else {
			quiet = 0
		}
	}
	t.Fatalf("same-group consumers did not drain rows: seen=%v want=%v", seen, expected)
}

// Java reference: IoTDBConsensusSubscriptionSeekIT.testSeekToBeginningReplaysRows.
func TestSubscriptionTreeSeekToBeginningReplaysRows(t *testing.T) {
	connection := subscriptionE2EConnection(t)
	ctx := subscriptionE2EContext(t)
	writer, database := subscriptionTreeWriter(t, connection)
	bootstrap := subscriptionTreeInsert(t, writer, database, "d1", "s1", 0, -1)
	subscriptionTreeSQL(t, writer, "FLUSH")

	manager := subscriptionE2EManager(t, connection, subscription.TreeDialect)
	topic := subscriptionE2EName("tree_seek_beginning")
	subscriptionE2ETopic(t, ctx, manager, topic, map[string]string{
		"format": "SubscriptionRecordHandler", "mode": "incremental", "order-mode": "per-writer", "path": database + ".d1.s1",
	})
	consumer := subscriptionE2EPull(t, ctx, connection, subscription.TreeDialect, topic, false)
	want := []subscriptionTreeRow{
		subscriptionTreeInsert(t, writer, database, "d1", "s1", 1, 101),
		subscriptionTreeInsert(t, writer, database, "d1", "s1", 2, 102),
	}
	subscriptionTreeSQL(t, writer, "FLUSH")
	subscriptionTreeRequireRows(t, subscriptionTreeCollect(t, ctx, consumer, len(want), true, false), want...)
	if err := consumer.SeekToBeginning(ctx, topic); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	replayed := subscriptionTreeCollect(t, ctx, consumer, len(want), true, false)
	for _, row := range want {
		if replayed[row] != 1 {
			t.Fatalf("row was not replayed exactly once: row=%+v replay=%v", row, replayed)
		}
	}
	for row := range replayed {
		if !reflect.DeepEqual(row, bootstrap) && !containsSubscriptionTreeRow(want, row) {
			t.Fatalf("unexpected row after seek-to-beginning: %+v", row)
		}
	}
}

func containsSubscriptionTreeRow(rows []subscriptionTreeRow, candidate subscriptionTreeRow) bool {
	for _, row := range rows {
		if reflect.DeepEqual(row, candidate) {
			return true
		}
	}
	return false
}

// Java reference: IoTDBConsensusSubscriptionSeekIT.testSeekAfterCheckpointReplaysExactTail,
// extended across closing and recreating the client with the same group.
func TestSubscriptionTreeCheckpointSurvivesConsumerRecreation(t *testing.T) {
	connection := subscriptionE2EConnection(t)
	ctx := subscriptionE2EContext(t)
	writer, database := subscriptionTreeWriter(t, connection)
	subscriptionTreeInsert(t, writer, database, "d1", "s1", 0, -1)
	subscriptionTreeSQL(t, writer, "FLUSH")

	manager := subscriptionE2EManager(t, connection, subscription.TreeDialect)
	topic := subscriptionE2EName("tree_checkpoint_reopen")
	subscriptionE2ETopic(t, ctx, manager, topic, map[string]string{
		"format": "SubscriptionRecordHandler", "mode": "incremental", "order-mode": "per-writer", "path": database + ".d1.s1",
	})
	group := subscriptionE2EName("tree_checkpoint_group")
	first := subscriptionTreePullWithIdentity(t, ctx, connection, subscriptionE2EName("tree_checkpoint_consumer"), group, false, topic)
	prefix := []subscriptionTreeRow{
		subscriptionTreeInsert(t, writer, database, "d1", "s1", 1, 101),
		subscriptionTreeInsert(t, writer, database, "d1", "s1", 2, 102),
	}
	subscriptionTreeSQL(t, writer, "FLUSH")
	subscriptionTreeRequireRows(t, subscriptionTreeCollect(t, ctx, first, len(prefix), true, false), prefix...)
	checkpoint, err := first.CommittedPositions(topic)
	if err != nil || len(checkpoint) == 0 {
		t.Fatalf("no checkpoint after first consumer: %+v error=%v", checkpoint, err)
	}
	tail := []subscriptionTreeRow{
		subscriptionTreeInsert(t, writer, database, "d1", "s1", 3, 103),
		subscriptionTreeInsert(t, writer, database, "d1", "s1", 4, 104),
	}
	subscriptionTreeSQL(t, writer, "FLUSH")
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second := subscriptionTreePullWithIdentity(t, ctx, connection, subscriptionE2EName("tree_checkpoint_consumer"), group, false, topic)
	if err := second.SeekAfter(ctx, topic, checkpoint); err != nil {
		t.Fatal(err)
	}
	subscriptionTreeRequireRows(t, subscriptionTreeCollect(t, ctx, second, len(tail), true, false), tail...)
}

// Java reference: IoTDBConsensusSubscriptionSeekIT.testSeekAfterFencesStaleCommitContexts.
func TestSubscriptionTreeSeekAfterIgnoresStaleCommitContext(t *testing.T) {
	connection := subscriptionE2EConnection(t)
	ctx := subscriptionE2EContext(t)
	writer, database := subscriptionTreeWriter(t, connection)
	subscriptionTreeInsert(t, writer, database, "d1", "s1", 0, -1)
	subscriptionTreeSQL(t, writer, "FLUSH")

	manager := subscriptionE2EManager(t, connection, subscription.TreeDialect)
	topic := subscriptionE2EName("tree_seek_stale_context")
	subscriptionE2ETopic(t, ctx, manager, topic, map[string]string{
		"format": "SubscriptionRecordHandler", "mode": "incremental", "order-mode": "per-writer", "path": database + ".d1.s1",
	})
	consumer := subscriptionE2EPull(t, ctx, connection, subscription.TreeDialect, topic, false)
	prefix := []subscriptionTreeRow{
		subscriptionTreeInsert(t, writer, database, "d1", "s1", 1, 101),
		subscriptionTreeInsert(t, writer, database, "d1", "s1", 2, 102),
	}
	subscriptionTreeSQL(t, writer, "FLUSH")
	subscriptionTreeRequireRows(t, subscriptionTreeCollect(t, ctx, consumer, len(prefix), true, false), prefix...)
	checkpoint, err := consumer.CommittedPositions(topic)
	if err != nil || len(checkpoint) == 0 {
		t.Fatalf("no checkpoint before stale batch: %+v error=%v", checkpoint, err)
	}
	tail := subscriptionTreeInsert(t, writer, database, "d1", "s1", 3, 103)
	subscriptionTreeSQL(t, writer, "FLUSH")
	stale := subscriptionTreePollFirstNonEmpty(t, ctx, consumer)
	if err := consumer.SeekAfter(ctx, topic, checkpoint); err != nil {
		for _, message := range stale {
			message.Release()
		}
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	for _, message := range stale {
		_ = consumer.Commit(ctx, message)
		message.Release()
	}
	subscriptionTreeRequireRows(t, subscriptionTreeCollect(t, ctx, consumer, 1, true, false), tail)
}

func subscriptionTreePollFirstNonEmpty(t *testing.T, ctx context.Context, consumer subconsumer.SubscriptionPullConsumer) []subpayload.SubscriptionMessage {
	t.Helper()
	for round := 0; round < subscriptionE2EMaxPollRounds; round++ {
		messages, err := consumer.Poll(ctx, subscriptionE2EPollTimeout)
		if err != nil {
			t.Fatal(err)
		}
		if len(messages) != 0 {
			return messages
		}
	}
	t.Fatal("expected a non-empty poll batch")
	return nil
}

// Java Push commit-before-consume behavior: callback failures after ACK do not
// NACK or redeliver the message.
func TestSubscriptionTreeAckBeforeConsumeCallbackFailureIsNotRedelivered(t *testing.T) {
	connection := subscriptionE2EConnection(t)
	ctx := subscriptionE2EContext(t)
	writer, database := subscriptionTreeWriter(t, connection)
	subscriptionTreeInsert(t, writer, database, "d1", "s1", 0, -1)
	subscriptionTreeSQL(t, writer, "FLUSH")

	manager := subscriptionE2EManager(t, connection, subscription.TreeDialect)
	topic := subscriptionE2EName("tree_ack_before")
	subscriptionE2ETopic(t, ctx, manager, topic, map[string]string{
		"format": "SubscriptionRecordHandler", "mode": "incremental", "order-mode": "per-writer",
		"path": database + ".d1.s1", "recycle-uncommitted-event-interval-ms": "1000",
	})
	type callbackResult struct {
		rows    []subscriptionTreeRow
		context subpayload.CommitContext
		err     error
	}
	results := make(chan callbackResult, 4)
	config := subconsumer.DefaultSubscriptionPushConsumerConfig()
	config.Consumer.Connection = connection
	config.Consumer.Dialect = subscription.TreeDialect
	config.Consumer.HeartbeatInterval = subscriptionE2EHeartbeatInterval
	config.PollTimeout, config.PollInterval = subscriptionE2EPollTimeout, subscriptionE2EPushPollInterval
	config.AckStrategy = subconsumer.AckBeforeConsume
	config.Handler = func(callbackCtx context.Context, message subpayload.SubscriptionMessage) error {
		rows, err := subscriptionTreeRows(callbackCtx, message)
		select {
		case results <- callbackResult{rows: rows, context: message.CommitContext(), err: err}:
		case <-callbackCtx.Done():
			return callbackCtx.Err()
		}
		if err != nil {
			return err
		}
		return errors.New("application rejected an already acknowledged message")
	}
	consumer, err := subconsumer.NewSubscriptionPushConsumer(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := consumer.Close(); err != nil {
			t.Errorf("close AckBeforeConsume consumer: %v", err)
		}
	})
	if err := consumer.Open(ctx); err != nil {
		t.Fatal(err)
	}
	if err := consumer.Subscribe(ctx, topic); err != nil {
		t.Fatal(err)
	}

	want := subscriptionTreeInsert(t, writer, database, "d1", "s1", 1, 101)
	subscriptionTreeSQL(t, writer, "FLUSH")
	var result callbackResult
	select {
	case result = <-results:
		if result.err != nil || !reflect.DeepEqual(result.rows, []subscriptionTreeRow{want}) {
			t.Fatalf("AckBeforeConsume callback got rows=%v error=%v", result.rows, result.err)
		}
	case err := <-consumer.Errors():
		t.Fatalf("AckBeforeConsume push terminated: %v", err)
	case <-ctx.Done():
		t.Fatalf("AckBeforeConsume callback not invoked: %v", ctx.Err())
	}
	if result.context.Writer == nil {
		t.Fatal("AckBeforeConsume delivery omitted writer progress")
	}
	subscriptionTreeRequireWriterProgress(t, ctx, consumer, topic, *result.context.Writer)
	select {
	case duplicate := <-results:
		t.Fatalf("AckBeforeConsume redelivered rejected message: %+v", duplicate)
	case err := <-consumer.Errors():
		t.Fatalf("ordinary callback failure became terminal: %v", err)
	case <-time.After(2 * time.Second):
	}
}

func subscriptionTreeRequireWriterProgress(t *testing.T, ctx context.Context, consumer subconsumer.SubscriptionPullConsumer, topic string, writer subpayload.WriterPosition) {
	t.Helper()
	ticker := time.NewTicker(subscriptionTreeE2EProgressPollInterval)
	defer ticker.Stop()
	for {
		progress, err := consumer.CommittedPositions(topic)
		if err != nil {
			t.Fatal(err)
		}
		position, found := progress[writer.ID.RegionID][writer.ID]
		if found && (position.PhysicalTime > writer.Progress.PhysicalTime ||
			(position.PhysicalTime == writer.Progress.PhysicalTime && position.LocalSeq >= writer.Progress.LocalSeq)) {
			return
		}
		select {
		case err := <-consumer.Errors():
			t.Fatalf("push terminated before ACK became visible: %v", err)
		case <-ctx.Done():
			t.Fatalf("ACK progress did not become visible: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}
