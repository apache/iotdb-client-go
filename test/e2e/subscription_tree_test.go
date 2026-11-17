/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/iotdb-client-go/subscription"
	subconsumer "github.com/apache/iotdb-client-go/subscription/consumer"
	subpayload "github.com/apache/iotdb-client-go/subscription/payload"
	subsession "github.com/apache/iotdb-client-go/subscription/session"
	"github.com/apache/iotdb-client-go/v2/client"
)

func subscriptionE2EContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), subscriptionE2ETestTimeout)
	t.Cleanup(cancel)
	return ctx
}

var subscriptionE2ESequence atomic.Uint64

func subscriptionE2EName(prefix string) string {
	return fmt.Sprintf("%s_%d_%d", prefix, time.Now().UnixNano(), subscriptionE2ESequence.Add(1))
}

func subscriptionE2EManager(t *testing.T, connection subscription.ConnectionConfig, dialect subscription.Dialect) subsession.SubscriptionSession {
	t.Helper()
	config := subsession.DefaultSubscriptionSessionConfig()
	config.Connection, config.Dialect = connection, dialect
	manager, err := subsession.NewSubscriptionSession(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Errorf("close subscription manager: %v", err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), subscriptionE2EOpenTimeout)
	defer cancel()
	if err := manager.Open(ctx); err != nil {
		t.Fatal(err)
	}
	return manager
}

func subscriptionE2ETopic(t *testing.T, ctx context.Context, manager subsession.SubscriptionSession, name string, attributes map[string]string) {
	t.Helper()
	if err := manager.CreateTopic(ctx, name, subsession.CreateTopicOptions{Attributes: attributes}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), subscriptionE2ECleanupTimeout)
		defer cancel()
		for {
			err := manager.DropTopic(cleanupCtx, name, subsession.DropOptions{IfExists: true})
			if err == nil {
				return
			}
			select {
			case <-cleanupCtx.Done():
				t.Errorf("drop topic %s: %v", name, err)
				return
			case <-time.After(100 * time.Millisecond):
			}
		}
	})
}

func subscriptionE2EPull(t *testing.T, ctx context.Context, connection subscription.ConnectionConfig, dialect subscription.Dialect, topic string, autoCommit bool) subconsumer.SubscriptionPullConsumer {
	t.Helper()
	config := subconsumer.DefaultSubscriptionPullConsumerConfig()
	config.Consumer.Connection, config.Consumer.Dialect = connection, dialect
	config.Consumer.HeartbeatInterval = subscriptionE2EHeartbeatInterval
	config.Consumer.EndpointsSyncInterval = subscriptionE2EEndpointsSyncInterval
	config.AutoCommit, config.AutoCommitInterval = &autoCommit, subscriptionE2EAutoCommitInterval
	consumer, err := subconsumer.NewSubscriptionPullConsumer(config)
	if err != nil {
		t.Fatal(err)
	}
	// Keep subscriptions until Close can finish any pending automatic ACKs.
	t.Cleanup(func() {
		if err := consumer.Close(); err != nil {
			t.Errorf("close pull consumer: %v", err)
		}
	})
	if err := consumer.Open(ctx); err != nil {
		t.Fatal(err)
	}
	if err := consumer.Subscribe(ctx, topic); err != nil {
		t.Fatal(err)
	}
	return consumer
}

func subscriptionTreeWriter(t *testing.T, connection subscription.ConnectionConfig) (*client.Session, string) {
	t.Helper()
	session, err := client.NewClusterSession(&client.ClusterConfig{
		NodeUrls: connection.NodeURLs,
		UserName: connection.Username,
		Password: connection.Password,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Open(false, subscriptionE2EWriterTimeoutMs); err != nil {
		_ = session.Close()
		t.Fatal(err)
	}
	database := "root." + subscriptionE2EName("sub_tree")
	t.Cleanup(func() {
		// The existing Session API has no per-statement context argument.
		if err := session.ExecuteNonQueryStatement("DELETE DATABASE " + database); err != nil {
			t.Errorf("delete tree database: %v", err)
		}
		if err := session.Close(); err != nil {
			t.Errorf("close tree writer: %v", err)
		}
	})
	subscriptionTreeSQL(t, &session, "CREATE DATABASE "+database)
	for _, series := range []string{".d1.s1", ".d1.s2", ".d2.s1"} {
		subscriptionTreeSQL(t, &session, "CREATE TIMESERIES "+database+series+" WITH DATATYPE=INT64, ENCODING=RLE")
	}
	return &session, database
}

func subscriptionTreeSQL(t *testing.T, writer *client.Session, sql string) {
	t.Helper()
	if err := writer.ExecuteNonQueryStatement(sql); err != nil {
		t.Fatalf("tree statement failed: %v", err)
	}
}

type subscriptionTreeRow struct {
	target    string
	column    string
	timestamp int64
	value     int64
}

func subscriptionTreeInsert(t *testing.T, writer *client.Session, database, device, column string, timestamp, value int64) subscriptionTreeRow {
	t.Helper()
	target := database + "." + device
	subscriptionTreeSQL(t, writer, fmt.Sprintf("INSERT INTO %s(time,%s) VALUES (%d,%d)", target, column, timestamp, value))
	return subscriptionTreeRow{target: target, column: column, timestamp: timestamp, value: value}
}

func subscriptionTreeRows(ctx context.Context, message subpayload.SubscriptionMessage) ([]subscriptionTreeRow, error) {
	batches, err := message.Records()
	if err != nil {
		return nil, err
	}
	var records []subscriptionTreeRow
	for _, batch := range batches {
		if batch.Dialect() != subscription.TreeDialect {
			return nil, fmt.Errorf("unexpected batch dialect %s", batch.Dialect())
		}
		columns := batch.Columns()
		for row, err := range batch.Rows() {
			if err != nil {
				return nil, err
			}
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if row.Timestamp == nil || len(row.Values) != len(columns) {
				return nil, errors.New("tree row lost timestamp or column mapping")
			}
			for index, value := range row.Values {
				if value == nil {
					continue
				}
				integer, ok := value.(int64)
				if !ok {
					return nil, fmt.Errorf("unexpected tree value %T", value)
				}
				records = append(records, subscriptionTreeRow{target: batch.Target(), column: columns[index].Name, timestamp: *row.Timestamp, value: integer})
			}
		}
	}
	return records, nil
}

func subscriptionTreeCollect(t *testing.T, ctx context.Context, consumer subconsumer.SubscriptionPullConsumer, expected int, manual, snapshot bool) map[subscriptionTreeRow]int {
	t.Helper()
	seen := make(map[subscriptionTreeRow]int)
	empty := 0
	for round := 0; round < subscriptionE2EMaxPollRounds; round++ {
		messages, err := consumer.Poll(ctx, subscriptionE2EPollTimeout)
		if err != nil {
			t.Fatal(err)
		}
		rows, err := subscriptionTreeReadAndCommit(ctx, consumer, messages, manual)
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range rows {
			seen[row]++
		}
		if len(messages) == 0 {
			empty++
		} else {
			empty = 0
		}
		if snapshot {
			if consumer.AllConsumed() {
				return seen
			}
		} else if len(seen) >= expected && empty >= subscriptionE2EQuietPollRounds {
			return seen
		}
	}
	t.Fatalf("did not reach completion/quiet tail; rows=%v snapshot=%t", seen, snapshot)
	return nil
}

func subscriptionTreeReadAndCommit(ctx context.Context, consumer subconsumer.SubscriptionPullConsumer, messages []subpayload.SubscriptionMessage, manual bool) ([]subscriptionTreeRow, error) {
	defer func() {
		for _, message := range messages {
			message.Release()
		}
	}()
	var records []subscriptionTreeRow
	for _, message := range messages {
		rows, err := subscriptionTreeRows(ctx, message)
		if err != nil {
			return nil, err
		}
		if manual {
			if err := consumer.Commit(ctx, message); err != nil {
				return nil, err
			}
		}
		records = append(records, rows...)
	}
	return records, nil
}

func subscriptionTreeRequireRows(t *testing.T, actual map[subscriptionTreeRow]int, rows ...subscriptionTreeRow) {
	t.Helper()
	expected := make(map[subscriptionTreeRow]int)
	for _, row := range rows {
		expected[row]++
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("tree rows differ: actual=%v expected=%v", actual, expected)
	}
}

// Reference: local/IoTDBSubscriptionTopicIT.testBasicCreateTopic* and
// testBasicDropTopic* under Java's subscription/it directory. SHOW assertions
// follow an actual subscription rather than inspecting consumer configuration.
func TestSubscriptionTreeTopicManagement(t *testing.T) {
	connection := subscriptionE2EConnection(t)
	ctx := subscriptionE2EContext(t)
	writer, database := subscriptionTreeWriter(t, connection)
	subscriptionTreeInsert(t, writer, database, "d1", "s1", 0, 0)
	subscriptionTreeSQL(t, writer, "FLUSH")
	manager := subscriptionE2EManager(t, connection, subscription.TreeDialect)
	topic := subscriptionE2EName("tree_management")
	path := database + ".d1.s1"
	subscriptionE2ETopic(t, ctx, manager, topic, map[string]string{"format": "SubscriptionRecordHandler", "path": path})
	if err := manager.CreateTopic(ctx, topic, subsession.CreateTopicOptions{IfNotExists: true, Attributes: map[string]string{"path": database + ".d2.**"}}); err != nil {
		t.Fatal(err)
	}
	metadata, found, err := manager.Topic(ctx, topic)
	if err != nil || !found || !strings.Contains(metadata.Attributes, "path="+path) {
		t.Fatalf("conditional creation changed existing topic: %+v found=%t error=%v", metadata, found, err)
	}
	topics, err := manager.Topics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	listed := false
	for _, record := range topics {
		if record.Name == topic {
			listed = true
		}
	}
	if !listed {
		t.Fatalf("created topic missing from SHOW TOPICS: %+v", topics)
	}
	if err := manager.AlterTopic(ctx, topic, map[string]string{"start-time": "10"}); err != nil {
		t.Fatal(err)
	}
	metadata, found, err = manager.Topic(ctx, topic)
	if err != nil || !found || !strings.Contains(metadata.Attributes, "start-time=10") {
		t.Fatalf("altered topic not visible: %+v found=%t error=%v", metadata, found, err)
	}
	consumer := subscriptionE2EPull(t, ctx, connection, subscription.TreeDialect, topic, false)
	subscriptions, err := manager.Subscriptions(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	visible := false
	for _, record := range subscriptions {
		if record.Topic == topic && record.GroupID == consumer.Info().GroupID {
			visible = true
		}
	}
	if !visible {
		t.Fatalf("subscribed group missing from SHOW SUBSCRIPTIONS: %+v", subscriptions)
	}
	if err := consumer.Unsubscribe(ctx, topic); err != nil {
		t.Fatal(err)
	}
	if err := consumer.Close(); err != nil {
		t.Fatal(err)
	}
	for repeat := 0; repeat < 2; repeat++ {
		if err := manager.DropTopic(ctx, topic, subsession.DropOptions{IfExists: true}); err != nil {
			t.Fatal(err)
		}
	}
	if _, found, err := manager.Topic(ctx, topic); err != nil || found {
		t.Fatalf("dropped topic still visible: found=%t error=%v", found, err)
	}
}

// Reference: dual/treemodel/IoTDBSubscriptionTopicIT.testTabletTopicWithPath
// and testTabletTopicWithTime, combined with explicit excluded-data assertions.
func TestSubscriptionTreePullFiltersPathAndTime(t *testing.T) {
	connection := subscriptionE2EConnection(t)
	ctx := subscriptionE2EContext(t)
	writer, database := subscriptionTreeWriter(t, connection)
	want := []subscriptionTreeRow{
		subscriptionTreeInsert(t, writer, database, "d1", "s1", 25, 125),
		subscriptionTreeInsert(t, writer, database, "d1", "s1", 50, 150),
		subscriptionTreeInsert(t, writer, database, "d1", "s1", 75, 175),
	}
	for _, timestamp := range []int64{0, 24, 76, 100} {
		subscriptionTreeInsert(t, writer, database, "d1", "s1", timestamp, -timestamp-1)
	}
	subscriptionTreeInsert(t, writer, database, "d1", "s2", 50, -2)
	subscriptionTreeInsert(t, writer, database, "d2", "s1", 50, -3)
	subscriptionTreeSQL(t, writer, "FLUSH")
	manager := subscriptionE2EManager(t, connection, subscription.TreeDialect)
	topic := subscriptionE2EName("tree_filter")
	subscriptionE2ETopic(t, ctx, manager, topic, map[string]string{
		"format": "SubscriptionRecordHandler", "mode": "initial", "path": database + ".d1.s1", "start-time": "25", "end-time": "75", "strict": "true",
	})
	consumer := subscriptionE2EPull(t, ctx, connection, subscription.TreeDialect, topic, false)
	subscriptionTreeRequireRows(t, subscriptionTreeCollect(t, ctx, consumer, len(want), true, false), want...)
}

// Reference: dual/treemodel/IoTDBSubscriptionTopicIT.testTabletTopicWithSnapshotMode.
// The automatic-commit variant also covers released record buffers while ACKs
// and termination are still pending.
func TestSubscriptionTreeSnapshotCompletesWithAutomaticCommit(t *testing.T) {
	connection := subscriptionE2EConnection(t)
	ctx := subscriptionE2EContext(t)
	writer, database := subscriptionTreeWriter(t, connection)
	want := []subscriptionTreeRow{
		subscriptionTreeInsert(t, writer, database, "d1", "s1", 10, 110),
		subscriptionTreeInsert(t, writer, database, "d1", "s1", 20, 120),
	}
	subscriptionTreeSQL(t, writer, "FLUSH")
	manager := subscriptionE2EManager(t, connection, subscription.TreeDialect)
	topic := subscriptionE2EName("tree_snapshot")
	subscriptionE2ETopic(t, ctx, manager, topic, map[string]string{"format": "SubscriptionRecordHandler", "mode": "snapshot", "path": database + ".d1.s1"})
	consumer := subscriptionE2EPull(t, ctx, connection, subscription.TreeDialect, topic, true)
	// Post-subscribe rows must not extend the historical snapshot.
	subscriptionTreeInsert(t, writer, database, "d1", "s1", 30, -30)
	subscriptionTreeSQL(t, writer, "FLUSH")
	subscriptionTreeRequireRows(t, subscriptionTreeCollect(t, ctx, consumer, len(want), false, true), want...)
	if !consumer.AllConsumed() {
		t.Fatal("snapshot did not terminate")
	}
}

// Reference: consensus/local/IoTDBConsensusSubscriptionBasicIT.testRealtimeOnlyAfterSubscribe
// and IoTDBConsensusSubscriptionSeekIT.testSeekAfterCheckpointReplaysExactTail.
func TestSubscriptionTreeIncrementalSeekAfterReplaysExactTail(t *testing.T) {
	connection := subscriptionE2EConnection(t)
	ctx := subscriptionE2EContext(t)
	writer, database := subscriptionTreeWriter(t, connection)
	subscriptionTreeInsert(t, writer, database, "d1", "s1", 0, -1)
	subscriptionTreeSQL(t, writer, "FLUSH")
	manager := subscriptionE2EManager(t, connection, subscription.TreeDialect)
	topic := subscriptionE2EName("tree_seek")
	subscriptionE2ETopic(t, ctx, manager, topic, map[string]string{"format": "SubscriptionRecordHandler", "mode": "incremental", "order-mode": "per-writer", "path": database + ".d1.s1"})
	consumer := subscriptionE2EPull(t, ctx, connection, subscription.TreeDialect, topic, false)
	var prefix []subscriptionTreeRow
	for timestamp := int64(1); timestamp <= 3; timestamp++ {
		prefix = append(prefix, subscriptionTreeInsert(t, writer, database, "d1", "s1", timestamp, timestamp*10))
	}
	subscriptionTreeSQL(t, writer, "FLUSH")
	subscriptionTreeRequireRows(t, subscriptionTreeCollect(t, ctx, consumer, len(prefix), true, false), prefix...)
	checkpoint, err := consumer.CommittedPositions(topic)
	if err != nil || len(checkpoint) == 0 {
		t.Fatalf("no confirmed writer checkpoint: %+v error=%v", checkpoint, err)
	}
	var tail []subscriptionTreeRow
	for timestamp := int64(4); timestamp <= 6; timestamp++ {
		tail = append(tail, subscriptionTreeInsert(t, writer, database, "d1", "s1", timestamp, timestamp*10))
	}
	subscriptionTreeSQL(t, writer, "FLUSH")
	subscriptionTreeRequireRows(t, subscriptionTreeCollect(t, ctx, consumer, len(tail), true, false), tail...)
	if err := consumer.SeekAfter(ctx, topic, checkpoint); err != nil {
		t.Fatal(err)
	}
	subscriptionTreeRequireRows(t, subscriptionTreeCollect(t, ctx, consumer, len(tail), true, false), tail...)
}

type subscriptionTreePushResult struct {
	rows    []subscriptionTreeRow
	context subpayload.CommitContext
	failed  bool
	err     error
}

// This supplements Java's local/IoTDBSubscriptionBasicIT record Push scenarios
// with a callback failure and actual confirmed-progress evidence after retry.
func TestSubscriptionTreePushAfterConsumeRedeliversOneFailure(t *testing.T) {
	connection := subscriptionE2EConnection(t)
	ctx := subscriptionE2EContext(t)
	writer, database := subscriptionTreeWriter(t, connection)
	subscriptionTreeInsert(t, writer, database, "d1", "s1", 0, -1)
	subscriptionTreeSQL(t, writer, "FLUSH")
	manager := subscriptionE2EManager(t, connection, subscription.TreeDialect)
	topic := subscriptionE2EName("tree_push")
	subscriptionE2ETopic(t, ctx, manager, topic, map[string]string{
		"format": "SubscriptionRecordHandler", "mode": "incremental", "order-mode": "per-writer", "path": database + ".d1.s1", "recycle-uncommitted-event-interval-ms": "1000",
	})
	var failedOnce atomic.Bool
	results := make(chan subscriptionTreePushResult, 8)
	config := subconsumer.DefaultSubscriptionPushConsumerConfig()
	config.Consumer.Connection = connection
	config.Consumer.HeartbeatInterval = subscriptionE2EHeartbeatInterval
	config.PollTimeout, config.PollInterval = subscriptionE2EPollTimeout, subscriptionE2EPushPollInterval
	config.AckStrategy = subconsumer.AckAfterConsume
	config.Handler = func(callbackCtx context.Context, message subpayload.SubscriptionMessage) error {
		rows, err := subscriptionTreeRows(callbackCtx, message)
		failed := err == nil && failedOnce.CompareAndSwap(false, true)
		result := subscriptionTreePushResult{rows: rows, context: message.CommitContext(), failed: failed, err: err}
		select {
		case results <- result:
		case <-callbackCtx.Done():
			return callbackCtx.Err()
		}
		if err != nil {
			return err
		}
		if failed {
			return errors.New("intentional one-time processing failure")
		}
		return nil
	}
	consumer, err := subconsumer.NewSubscriptionPushConsumer(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := consumer.Close(); err != nil {
			t.Errorf("close push consumer: %v", err)
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), subscriptionE2ECleanupTimeout)
		defer cancel()
		for {
			select {
			case err, open := <-consumer.Errors():
				if !open {
					return
				}
				t.Errorf("terminal push error: %v", err)
			case <-cleanupCtx.Done():
				t.Error("push callbacks did not stop")
				return
			}
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
	attempts := 0
	var confirmed subpayload.CommitContext
	for attempts < 2 {
		select {
		case result := <-results:
			if result.err != nil {
				t.Fatal(result.err)
			}
			if !reflect.DeepEqual(result.rows, []subscriptionTreeRow{want}) || result.failed != (attempts == 0) {
				t.Fatalf("unexpected callback attempt %d: %+v", attempts, result)
			}
			confirmed = result.context
			attempts++
		case err, open := <-consumer.Errors():
			t.Fatalf("push stopped after ordinary failure: open=%t error=%v", open, err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if confirmed.Writer == nil {
		t.Fatal("incremental delivery did not expose writer progress")
	}
	ticker := time.NewTicker(subscriptionTreeE2EProgressPollInterval)
	defer ticker.Stop()
	for {
		progress, err := consumer.CommittedPositions(topic)
		if err != nil {
			t.Fatal(err)
		}
		position, found := progress[confirmed.Writer.ID.RegionID][confirmed.Writer.ID]
		if found && (position.PhysicalTime > confirmed.Writer.Progress.PhysicalTime ||
			(position.PhysicalTime == confirmed.Writer.Progress.PhysicalTime && position.LocalSeq >= confirmed.Writer.Progress.LocalSeq)) {
			return
		}
		select {
		case err, open := <-consumer.Errors():
			t.Fatalf("push terminated before successful ACK: open=%t error=%v", open, err)
		case extra := <-results:
			t.Fatalf("message redelivered after successful processing: %+v", extra)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
		}
	}
}

// This Go-specific rejection contract replaces the Java TsFile read-success
// scenarios: topic management is supported, file decoding is not.
func TestSubscriptionTreeTsFilePollIsExplicitlyUnsupported(t *testing.T) {
	connection := subscriptionE2EConnection(t)
	ctx := subscriptionE2EContext(t)
	writer, database := subscriptionTreeWriter(t, connection)
	subscriptionTreeInsert(t, writer, database, "d1", "s1", 1, 101)
	subscriptionTreeSQL(t, writer, "FLUSH")
	manager := subscriptionE2EManager(t, connection, subscription.TreeDialect)
	topic := subscriptionE2EName("tree_tsfile")
	subscriptionE2ETopic(t, ctx, manager, topic, map[string]string{"format": "SubscriptionTsFileHandler", "mode": "initial", "path": database + ".d1.s1"})
	consumer := subscriptionE2EPull(t, ctx, connection, subscription.TreeDialect, topic, false)
	for round := 0; round < subscriptionE2EMaxPollRounds; round++ {
		messages, err := consumer.Poll(ctx, subscriptionE2EPollTimeout)
		if len(messages) != 0 {
			for _, message := range messages {
				message.Release()
			}
			t.Fatal("unsupported TsFile poll delivered records")
		}
		if errors.Is(err, subscription.ErrUnsupportedTsFile) {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), subscriptionE2ECleanupTimeout)
			defer cancel()
			if unsubscribeErr := consumer.Unsubscribe(cleanupCtx, topic); unsubscribeErr != nil {
				t.Fatalf("unsubscribe after unsupported TsFile: %v", unsubscribeErr)
			}
			return
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Fatal("no TsFile payload reached the decoding boundary")
}

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
		commitErr := consumer.Commit(ctx, message)
		message.Release()
		if commitErr != nil {
			t.Fatalf("commit stale message after seek: %v", commitErr)
		}
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

func subscriptionTreeRequireWriterProgress(t *testing.T, ctx context.Context, consumer subconsumer.SubscriptionPushConsumer, topic string, writer subpayload.WriterPosition) {
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
