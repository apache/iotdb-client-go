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
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/iotdb-client-go/v2/client"
	"github.com/apache/iotdb-client-go/v2/subscription"
	subconsumer "github.com/apache/iotdb-client-go/v2/subscription/consumer"
	subpayload "github.com/apache/iotdb-client-go/v2/subscription/payload"
	subsession "github.com/apache/iotdb-client-go/v2/subscription/session"
)

// These cases require subscription support and, for incremental topics, a
// consensus protocol that supports WAL subscriptions. Unsupported server
// configurations fail after the shared explicit E2E opt-in.
// Subscription operations honor the test context. The existing ITableSession
// write/DDL API has no context parameter, including for database cleanup.
func subscriptionTableWriter(t *testing.T, connection subscription.ConnectionConfig) client.ITableSession {
	t.Helper()
	writer, err := client.NewClusterTableSession(&client.ClusterConfig{
		NodeUrls: connection.NodeURLs,
		UserName: connection.Username,
		Password: connection.Password,
	}, false)
	if err != nil {
		t.Fatalf("open table writer: %v", err)
	}
	t.Cleanup(func() {
		if err := writer.Close(); err != nil {
			t.Errorf("close table writer: %v", err)
		}
	})
	return writer
}

func subscriptionTableSQL(t *testing.T, writer client.ITableSession, statements ...string) {
	t.Helper()
	for _, statement := range statements {
		if err := writer.ExecuteNonQueryStatement(statement); err != nil {
			t.Fatalf("table SQL %q: %v", statement, err)
		}
	}
}

func subscriptionTableDatabase(t *testing.T, connection subscription.ConnectionConfig, writer client.ITableSession) string {
	t.Helper()
	database := subscriptionE2EName("sub_table_db")
	subscriptionTableSQL(t, writer, "create database "+database)
	t.Cleanup(func() {
		// Cleanup uses a fresh connection, independent of the test's deadline.
		cleanup, err := client.NewClusterTableSession(&client.ClusterConfig{
			NodeUrls: connection.NodeURLs, UserName: connection.Username, Password: connection.Password,
		}, false)
		if err != nil {
			t.Errorf("open database cleanup connection: %v", err)
			return
		}
		defer func() {
			if err := cleanup.Close(); err != nil {
				t.Errorf("close database cleanup connection: %v", err)
			}
		}()
		if err := cleanup.ExecuteNonQueryStatement("drop database if exists "+database); err != nil {
			t.Errorf("drop table database %s: %v", database, err)
		}
	})
	return database
}

func subscriptionTableCreate(t *testing.T, writer client.ITableSession, database, table string) {
	t.Helper()
	subscriptionTableSQL(t, writer, "create table "+database+"."+table+
		" (tag1 STRING TAG, s1 INT64 FIELD, s2 DOUBLE FIELD, s3 BOOLEAN FIELD)")
}

func subscriptionTableInsert(t *testing.T, writer client.ITableSession, database, table string, timestamps ...int64) {
	t.Helper()
	for _, timestamp := range timestamps {
		subscriptionTableSQL(t, writer, fmt.Sprintf(
			"insert into %s.%s(time,tag1,s1,s2,s3) values (%d,'device_%d',%d,%d.5,true)",
			database, table, timestamp, timestamp, timestamp*10, timestamp))
	}
	subscriptionTableSQL(t, writer, "flush")
	if len(timestamps) != 0 {
		unique := make(map[int64]struct{}, len(timestamps))
		start, end := timestamps[0], timestamps[0]
		for _, timestamp := range timestamps {
			unique[timestamp] = struct{}{}
			start = min(start, timestamp)
			end = max(end, timestamp)
		}
		// Flush completion does not guarantee that a newly created snapshot queue can
		// immediately observe every write. Wait for the same rows through the read path.
		subscriptionTableWaitVisibleRows(t, writer, database, table, start, end, len(unique))
	}
}

func subscriptionTableInsertSameDevice(t *testing.T, writer client.ITableSession, database, table, tag string, timestamps ...int64) {
	t.Helper()
	for _, timestamp := range timestamps {
		subscriptionTableSQL(t, writer, fmt.Sprintf(
			"insert into %s.%s(time,tag1,s1,s2,s3) values (%d,'%s',%d,%d.5,true)",
			database, table, timestamp, tag, timestamp*10, timestamp))
	}
	subscriptionTableSQL(t, writer, "flush")
	if len(timestamps) != 0 {
		start, end := timestamps[0], timestamps[0]
		unique := make(map[int64]struct{}, len(timestamps))
		for _, timestamp := range timestamps {
			unique[timestamp] = struct{}{}
			start = min(start, timestamp)
			end = max(end, timestamp)
		}
		subscriptionTableWaitVisibleRows(t, writer, database, table, start, end, len(unique))
	}
}

func subscriptionTableExpectedSameDevice(database, table, tag string, timestamps ...int64) map[int64]subscriptionTableRecord {
	result := make(map[int64]subscriptionTableRecord, len(timestamps))
	for _, timestamp := range timestamps {
		result[timestamp] = subscriptionTableRecord{
			Database: database,
			Table: table,
			Timestamp: timestamp,
			Values: map[string]any{
				"tag1": tag,
				"s1": timestamp * 10,
				"s2": float64(timestamp) + 0.5,
				"s3": true,
			},
		}
	}
	return result
}

type subscriptionTableRecord struct {
	Database string
	Table string
	Timestamp int64
	Values map[string]any
}

// Validate visible time projection and copy public raw records, so push
// callbacks never retain borrowed data or require time in the visible schema.
func subscriptionTableRead(message subpayload.SubscriptionMessage, timeSelected bool) ([]subscriptionTableRecord, error) {
	batches, err := message.Records()
	if err != nil {
		return nil, err
	}
	var records []subscriptionTableRecord
	for _, batch := range batches {
		if batch.Dialect() != subscription.TableDialect {
			return nil, fmt.Errorf("table message decoded using %s", batch.Dialect())
		}
		if batch.TimeSelected() != timeSelected {
			return nil, fmt.Errorf("table time projection: got=%t, want=%t", batch.TimeSelected(), timeSelected)
		}
		columnCount := batch.ColumnCount()
		visibleRows := 0
		for row, err := range batch.Rows() {
			if err != nil {
				return nil, err
			}
			if (row.Timestamp != nil) != timeSelected || len(row.Values) != columnCount {
				return nil, fmt.Errorf("invalid table row: %+v", row)
			}
			visibleRows++
		}
		if visibleRows != batch.Len() {
			return nil, fmt.Errorf("visible table row count: got=%d, want=%d", visibleRows, batch.Len())
		}
		// Time can be excluded from Rows by column-filter. Public raw access
		// still exposes timestamps, keeping exact identity/value assertions
		// independent of the visible projection and of iterator ordering.
		for row := 0; row < batch.Len(); row++ {
			timestamp, err := batch.Timestamp(row)
			if err != nil {
				return nil, err
			}
			record := subscriptionTableRecord{
				Database: batch.Database(), Table: batch.Target(), Timestamp: timestamp,
				Values: make(map[string]any, columnCount),
			}
			for index := 0; index < columnCount; index++ {
				column, err := batch.ColumnName(index)
				if err != nil {
					return nil, err
				}
				if _, duplicate := record.Values[column]; duplicate {
					return nil, fmt.Errorf("duplicate column %s", column)
				}
				value, err := batch.Value(row, index)
				if err != nil {
					return nil, err
				}
				record.Values[column] = value
			}
			records = append(records, record)
		}
	}
	return records, nil
}

func subscriptionTableExpected(database, table string, projected bool, timestamps ...int64) map[int64]subscriptionTableRecord {
	result := make(map[int64]subscriptionTableRecord)
	for _, timestamp := range timestamps {
		values := map[string]any{"tag1": fmt.Sprintf("device_%d", timestamp), "s2": float64(timestamp)+0.5}
		if !projected {
			values["s1"], values["s3"] = timestamp*10, true
		}
		result[timestamp] = subscriptionTableRecord{database, table, timestamp, values}
	}
	return result
}

func subscriptionTablePollExact(t *testing.T, ctx context.Context, consumer subconsumer.SubscriptionPullConsumer,
	want map[int64]subscriptionTableRecord, manualCommit, snapshot, timeSelected bool, stage string) {
	t.Helper()
	seen := make(map[int64]subscriptionTableRecord)
	emptyAfterRows := 0
	startedAt := time.Now()
	lastProgressLog := startedAt
	polls := 0
	deadline, hasDeadline := ctx.Deadline()
	t.Logf("[%s] polling for table records: want=%+v deadline=%v", stage, want, deadline)
	for {
		messages, err := consumer.Poll(ctx, subscriptionE2EPollTimeout)
		polls++
		if err != nil {
			t.Fatalf("[%s] poll failed after %s (%d polls), seen=%+v want=%+v: %v",
				stage, time.Since(startedAt), polls, seen, want, err)
		}
		for _, message := range messages {
			records, readErr := subscriptionTableRead(message, timeSelected)
			if readErr != nil {
				message.Release()
				t.Fatal(readErr)
			}
			for _, record := range records {
				expected, exists := want[record.Timestamp]
				if !exists || record.Database != expected.Database || record.Table != expected.Table {
					message.Release()
					t.Fatalf("unexpected table record identity: got=%+v, want=%+v", record, expected)
				}
				combined, found := seen[record.Timestamp]
				if !found {
					combined = subscriptionTableRecord{
						Database:  record.Database,
						Table:     record.Table,
						Timestamp: record.Timestamp,
						Values:    make(map[string]any, len(record.Values)),
					}
				}
				for column, value := range record.Values {
					wantedValue, selected := expected.Values[column]
					if !selected || !reflect.DeepEqual(value, wantedValue) {
						message.Release()
						t.Fatalf(
							"unexpected table value at time %d column %s: got=%v, want=%v",
							record.Timestamp,
							column,
							value,
							wantedValue,
						)
					}
					if previous, duplicate := combined.Values[column]; duplicate && !reflect.DeepEqual(previous, value) {
						message.Release()
						t.Fatalf(
							"conflicting table value at time %d column %s: got=%v, previous=%v",
							record.Timestamp,
							column,
							value,
							previous,
						)
					}
					combined.Values[column] = value
				}
				seen[record.Timestamp] = combined
			}
			if manualCommit {
				if err := consumer.Commit(ctx, message); err != nil {
					message.Release()
					t.Fatalf("commit table records: %v", err)
				}
			}
			message.Release()
		}
		if snapshot && consumer.AllConsumed() {
			if !reflect.DeepEqual(seen, want) {
				t.Fatalf("snapshot finished with wrong records: got=%+v, want=%+v", seen, want)
			}
			return
		}
		if !snapshot && subscriptionTableRecordsComplete(seen, want) {
			if len(messages) == 0 {
				emptyAfterRows++
			} else {
				emptyAfterRows = 0
			}
			if emptyAfterRows == subscriptionE2EQuietPollRounds {
				t.Logf("[%s] table records complete after %s (%d polls): seen=%+v",
					stage, time.Since(startedAt), polls, seen)
				return
			}
		}
		if err := ctx.Err(); err != nil {
			t.Fatalf("[%s] table delivery incomplete after %s (%d polls): got=%+v, want=%+v: %v",
				stage, time.Since(startedAt), polls, seen, want, err)
		}
		if time.Since(lastProgressLog) >= 5*time.Second {
			remaining := time.Duration(0)
			if hasDeadline {
				remaining = time.Until(deadline)
			}
			t.Logf("[%s] still waiting after %s (%d polls, %s deadline remaining): seen=%+v want=%+v",
				stage, time.Since(startedAt), polls, remaining, seen, want)
			lastProgressLog = time.Now()
		}
	}
}

func subscriptionTableRecordsComplete(seen, want map[int64]subscriptionTableRecord) bool {
	if len(seen) != len(want) {
		return false
	}
	for timestamp, expected := range want {
		if !reflect.DeepEqual(seen[timestamp], expected) {
			return false
		}
	}
	return true
}

func subscriptionTableRecordMatchesFragment(got, want subscriptionTableRecord) bool {
	if got.Database != want.Database || got.Table != want.Table ||
		got.Timestamp != want.Timestamp || len(got.Values) == 0 {
		return false
	}
	for column, value := range got.Values {
		if expected, selected := want.Values[column]; !selected || !reflect.DeepEqual(value, expected) {
			return false
		}
	}
	return true
}

// Java reference: local/tablemodel/IoTDBSubscriptionIsolationIT.testTopicIsolation.
func TestSubscriptionTableTopicIsolation(t *testing.T) {
	connection := subscriptionE2EConnection(t)
	ctx := subscriptionE2EContext(t)
	tree := subscriptionE2EManager(t, connection, subscription.TreeDialect)
	table := subscriptionE2EManager(t, connection, subscription.TableDialect)
	name := subscriptionE2EName("sub_isolation")
	subscriptionE2ETopic(t, ctx, tree, name, map[string]string{"path": "root.subscription_tree.**"})
	subscriptionE2ETopic(t, ctx, table, name, map[string]string{"database": "subscription_table", "table": "events"})
	if err := table.AlterTopic(ctx, name, map[string]string{"database": "subscription_table_altered"}); err != nil {
		t.Fatal(err)
	}
	treeTopic, treeFound, treeErr := tree.Topic(ctx, name)
	tableTopic, tableFound, tableErr := table.Topic(ctx, name)
	if treeErr != nil || tableErr != nil || !treeFound || !tableFound ||
		!strings.Contains(treeTopic.Attributes, "root.subscription_tree.**") ||
		strings.Contains(treeTopic.Attributes, "subscription_table_altered") ||
		!strings.Contains(tableTopic.Attributes, "subscription_table_altered") {
		t.Fatalf("tree/table topic alteration leaked: tree=%+v/%v/%v, table=%+v/%v/%v",
			treeTopic, treeFound, treeErr, tableTopic, tableFound, tableErr)
	}
	if err := tree.DropTopic(ctx, name, subsession.DropOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, found, err := tree.Topic(ctx, name); err != nil || found {
		t.Fatalf("tree topic survived drop: found=%t, error=%v", found, err)
	}
	if _, found, err := table.Topic(ctx, name); err != nil || !found {
		t.Fatalf("dropping tree topic removed table topic: found=%t, error=%v", found, err)
	}
}

// Java references: dual/treemodel/IoTDBSubscriptionTopicIT.testTabletTopicWithTime
// and testTabletTopicWithSnapshotMode. Database/table interference is a Go
// table-model extension of those time-filter and snapshot-completion scenarios.
func TestSubscriptionTableSnapshotFiltersAndAutoCommitCompletion(t *testing.T) {
	connection := subscriptionE2EConnection(t)
	ctx := subscriptionE2EContext(t)
	writer := subscriptionTableWriter(t, connection)
	database := subscriptionTableDatabase(t, connection, writer)
	otherDatabase := subscriptionTableDatabase(t, connection, writer)
	for _, target := range [][2]string{{database, "events"}, {database, "other"}, {otherDatabase, "events"}} {
		subscriptionTableCreate(t, writer, target[0], target[1])
		subscriptionTableInsert(t, writer, target[0], target[1], 99, 100, 101, 102, 103)
	}
	manager := subscriptionE2EManager(t, connection, subscription.TableDialect)
	topic := subscriptionE2EName("sub_table_snapshot")
	subscriptionE2ETopic(t, ctx, manager, topic, map[string]string{
		"database": database, "table": "events", "mode": "snapshot",
		"format": "SubscriptionRecordHandler", "start-time": "100", "end-time": "102",
	})
	consumer := subscriptionE2EPull(t, ctx, connection, subscription.TableDialect, topic, true)
	subscriptionTablePollExact(t, ctx, consumer, subscriptionTableExpected(database, "events", false, 100, 101, 102), false, true, true, "snapshot")
}

// Java reference: consensus/local/tablemodel/IoTDBConsensusSubscriptionFilterTableIT.testColumnFiltering.
// The column filter excludes time as well as s1/s3; raw timestamps remain usable.
func TestSubscriptionTableIncrementalColumnProjection(t *testing.T) {
	connection := subscriptionE2EConnection(t)
	ctx := subscriptionE2EContext(t)
	writer := subscriptionTableWriter(t, connection)
	database := subscriptionTableDatabase(t, connection, writer)
	subscriptionTableCreate(t, writer, database, "events")
	subscriptionTableInsert(t, writer, database, "events", 0)
	manager := subscriptionE2EManager(t, connection, subscription.TableDialect)
	topic := subscriptionE2EName("sub_table_columns")
	subscriptionE2ETopic(t, ctx, manager, topic, map[string]string{
		"database": database, "table": "events", "mode": "incremental",
		"format": "SubscriptionRecordHandler", "column-filter": `column_name REGEXP "(tag1|s2)"`,
	})
	consumer := subscriptionE2EPull(t, ctx, connection, subscription.TableDialect, topic, false)
	subscriptionTableInsert(t, writer, database, "events", 101, 102, 103)
	subscriptionTablePollExact(t, ctx, consumer, subscriptionTableExpected(database, "events", true, 101, 102, 103), true, false, false, "incremental column projection")
}

// Java references: consensus/local/IoTDBConsensusSubscriptionSeekIT.testSeekAfterCheckpointReplaysExactTail
// and consensus/local/tablemodel/IoTDBConsensusSubscriptionBasicTableIT.testRealtimeOnlyAfterSubscribe.
// Exact per-writer checkpoint replay in the table model is a Go extension.
func TestSubscriptionTableSeekAfterReplaysOnlyTail(t *testing.T) {
	t.Log("seek-after: begin test")
	connection := subscriptionE2EConnection(t)
	ctx := subscriptionE2EContext(t)
	writer := subscriptionTableWriter(t, connection)
	database := subscriptionTableDatabase(t, connection, writer)
	subscriptionTableCreate(t, writer, database, "events")
	const seekDevice = "seek_device"
	t.Logf("seek-after: created table %s.events; inserting bootstrap row", database)
	subscriptionTableInsertSameDevice(t, writer, database, "events", seekDevice, 0)
	t.Log("seek-after: bootstrap insert and flush completed; opening subscription manager")
	manager := subscriptionE2EManager(t, connection, subscription.TableDialect)
	topic := subscriptionE2EName("sub_table_seek")
	subscriptionE2ETopic(t, ctx, manager, topic, map[string]string{
		"database": database, "table": "events", "mode": "incremental", "format": "SubscriptionRecordHandler",
		"order-mode": "per-writer",
	})
	t.Logf("seek-after: topic %s created; opening consumer", topic)
	consumer := subscriptionE2EPull(t, ctx, connection, subscription.TableDialect, topic, false)
	t.Log("seek-after: consumer subscribed; inserting checkpoint rows 101/102")
	subscriptionTableInsertSameDevice(t, writer, database, "events", seekDevice, 101, 102)
	t.Log("seek-after: checkpoint rows inserted and flushed; polling initial delivery")
	subscriptionTablePollExact(t, ctx, consumer, subscriptionTableExpectedSameDevice(database, "events", seekDevice, 101, 102), true, false, true, "seek initial checkpoint rows")
	t.Log("seek-after: initial rows committed; reading committed checkpoint")
	checkpoint, err := consumer.CommittedPositions(topic)
	if err != nil || len(checkpoint) == 0 {
		t.Fatalf("committed prefix has no resumable checkpoint: %v, %v", checkpoint, err)
	}
	t.Logf("seek-after: captured checkpoint=%+v", checkpoint)
	t.Log("seek-after: inserting tail rows 201/202")
	subscriptionTableInsertSameDevice(t, writer, database, "events", seekDevice, 201, 202)
	t.Log("seek-after: tail rows inserted and flushed")
	tail := subscriptionTableExpectedSameDevice(database, "events", seekDevice, 201, 202)
	t.Log("seek-after: polling and committing tail before seek")
	subscriptionTablePollExact(t, ctx, consumer, tail, true, false, true, "seek tail before seek")
	t.Logf("seek-after: checkpoint=%+v", checkpoint)
	seekStartedAt := time.Now()
	t.Log("seek-after: calling SeekAfter")
	if err := consumer.SeekAfter(ctx, topic, checkpoint); err != nil {
		t.Fatalf("seek after committed prefix after %s: %v", time.Since(seekStartedAt), err)
	}
	t.Logf("seek-after: SeekAfter returned after %s; polling replay tail", time.Since(seekStartedAt))
	subscriptionTablePollExact(t, ctx, consumer, tail, true, false, true, "seek replay tail")
}

func subscriptionTablePush(t *testing.T, ctx context.Context, connection subscription.ConnectionConfig,
	topic string, handler subconsumer.ConsumeFunc) subconsumer.SubscriptionPushConsumer {
	t.Helper()
	config := subconsumer.DefaultSubscriptionPushConsumerConfig()
	config.Consumer.Connection = connection
	config.Consumer.Dialect = subscription.TableDialect
	config.Consumer.ConsumerID = subscriptionE2EName("sub_table_consumer")
	config.Consumer.GroupID = subscriptionE2EName("sub_table_group")
	config.AckStrategy = subconsumer.AckAfterConsume
	config.PollTimeout = subscriptionE2EPollTimeout
	config.Handler = handler
	consumer, err := subconsumer.NewSubscriptionPushConsumer(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := consumer.Close(); err != nil {
			t.Errorf("close table push consumer: %v", err)
		}
	})
	if err := consumer.Open(ctx); err != nil {
		t.Fatal(err)
	}
	if err := consumer.Subscribe(ctx, topic); err != nil {
		t.Fatalf("subscribe table push topic: %v", err)
	}
	return consumer
}

// Go-specific coverage: an ordinary after-consumption callback error must cause
// redelivery, and the successful retry must produce observable writer ACK progress.
func TestSubscriptionTablePushRetriesFailedCallback(t *testing.T) {
	connection := subscriptionE2EConnection(t)
	ctx := subscriptionE2EContext(t)
	writer := subscriptionTableWriter(t, connection)
	database := subscriptionTableDatabase(t, connection, writer)
	subscriptionTableCreate(t, writer, database, "events")
	subscriptionTableInsert(t, writer, database, "events", 0)
	manager := subscriptionE2EManager(t, connection, subscription.TableDialect)
	topic := subscriptionE2EName("sub_table_push")
	subscriptionE2ETopic(t, ctx, manager, topic, map[string]string{
		"database": database, "table": "events", "mode": "incremental", "format": "SubscriptionRecordHandler",
		"order-mode": "per-writer",
	})
	type attempt struct {
		records []subscriptionTableRecord
		err error
		context subpayload.CommitContext
	}
	attempts := make(chan attempt, 8)
	var calls atomic.Int32
	consumer := subscriptionTablePush(t, ctx, connection, topic, func(callbackCtx context.Context, message subpayload.SubscriptionMessage) error {
		records, err := subscriptionTableRead(message, true)
		select {
		case attempts <- attempt{records, err, message.CommitContext()}:
		case <-callbackCtx.Done():
			return callbackCtx.Err()
		}
		if calls.Add(1) == 1 {
			return errors.New("retry this application failure")
		}
		return err
	})
	subscriptionTableInsert(t, writer, database, "events", 101)
	want := subscriptionTableExpected(database, "events", false, 101)[101]
	var first subpayload.CommitContext
	for index := 0; index < 2; index++ {
		select {
		case got := <-attempts:
			if got.err != nil || len(got.records) == 0 {
				t.Fatalf("callback received wrong table data: %+v, %v", got.records, got.err)
			}
			for _, record := range got.records {
				if !subscriptionTableRecordMatchesFragment(record, want) {
					t.Fatalf("callback received wrong table data: %+v, %v", got.records, got.err)
				}
			}
			if index == 0 {
				first = got.context
			} else if !reflect.DeepEqual(first, got.context) {
				t.Fatalf("retry changed delivery identity: first=%+v, retry=%+v", first, got.context)
			}
		case err := <-consumer.Errors():
			t.Fatalf("ordinary callback failure terminated push delivery: %v", err)
		case <-ctx.Done():
			t.Fatalf("callback was not redelivered: %v", ctx.Err())
		}
	}
	if first.Writer == nil {
		t.Fatal("per-writer delivery omitted the progress needed to verify acknowledgment")
	}
	// Observe confirmation after callback success, rather than treating the
	// callback itself or absence of a terminal error as proof of acknowledgment.
	ticker := time.NewTicker(subscriptionTableE2EProgressPollInterval)
	defer ticker.Stop()
	for {
		progress, err := consumer.CommittedPositions(topic)
		if err != nil {
			t.Fatal(err)
		}
		confirmed, found := progress[first.Writer.ID.RegionID][first.Writer.ID]
		wanted := first.Writer.Progress
		if found && (confirmed.PhysicalTime > wanted.PhysicalTime ||
			(confirmed.PhysicalTime == wanted.PhysicalTime && confirmed.LocalSeq >= wanted.LocalSeq)) {
			break
		}
		select {
		case err := <-consumer.Errors():
			t.Fatalf("push terminated before confirming retry: %v", err)
		case <-ctx.Done():
			t.Fatalf("successful retry was not confirmed: %v", ctx.Err())
		case <-ticker.C:
		}
	}
	if err := consumer.Unsubscribe(ctx, topic); err != nil {
		t.Fatalf("unsubscribe push retry topic: %v", err)
	}
	if err := consumer.Close(); err != nil {
		t.Fatal(err)
	}
	for {
		select {
		case err, ok := <-consumer.Errors():
			if !ok {
				return
			}
			if err != nil {
				t.Fatalf("ordinary callback failure became terminal: %v", err)
			}
		case <-ctx.Done():
			t.Fatalf("push loop did not exit after Close: %v", ctx.Err())
		}
	}
}

// Go-specific coverage: TsFile management/subscription is allowed, while actual
// payload decoding must stop push delivery before any record handler callback.
func TestSubscriptionTableTsFilePushFailsBeforeCallback(t *testing.T) {
	connection := subscriptionE2EConnection(t)
	ctx := subscriptionE2EContext(t)
	writer := subscriptionTableWriter(t, connection)
	database := subscriptionTableDatabase(t, connection, writer)
	subscriptionTableCreate(t, writer, database, "events")
	subscriptionTableInsert(t, writer, database, "events", 101)
	manager := subscriptionE2EManager(t, connection, subscription.TableDialect)
	topic := subscriptionE2EName("sub_table_tsfile")
	subscriptionE2ETopic(t, ctx, manager, topic, map[string]string{
		"database": database, "table": "events", "mode": "snapshot", "format": "SubscriptionTsFileHandler",
	})
	var calls atomic.Int32
	consumer := subscriptionTablePush(t, ctx, connection, topic, func(context.Context, subpayload.SubscriptionMessage) error {
		calls.Add(1)
		return nil
	})
	select {
	case err, ok := <-consumer.Errors():
		if !ok || !errors.Is(err, subscription.ErrUnsupportedTsFile) {
			t.Fatalf("TsFile did not report unsupported decoding: %v, channel open=%t", err, ok)
		}
	case <-ctx.Done():
		t.Fatalf("TsFile topic produced no decoding error: %v", ctx.Err())
	}
	if calls.Load() != 0 {
		t.Fatalf("TsFile payload reached record callback %d times", calls.Load())
	}
}

// These cases separate snapshot completion into observable stages. Existing
// permissive snapshot coverage remains unchanged; these strict cases allow
// repeated TAGs in disjoint field fragments, reject repeated fields or tag-only
// redeliveries, and verify writer progress before termination-driven completion.
func TestSubscriptionTableSnapshotStrictAutoCommitCompletion(t *testing.T) {
	runSubscriptionTableStrictSnapshot(t, true)
}

func TestSubscriptionTableSnapshotStrictManualCommitCompletion(t *testing.T) {
	runSubscriptionTableStrictSnapshot(t, false)
}

func runSubscriptionTableStrictSnapshot(t *testing.T, autoCommit bool) {
	t.Helper()
	connection := subscriptionE2EConnection(t)
	ctx := subscriptionE2EContext(t)
	writer := subscriptionTableWriter(t, connection)
	database := subscriptionTableDatabase(t, connection, writer)
	subscriptionTableCreate(t, writer, database, "events")
	subscriptionTableInsert(t, writer, database, "events", 99, 100, 101, 102, 103)

	manager := subscriptionE2EManager(t, connection, subscription.TableDialect)
	topic := subscriptionE2EName("sub_table_strict_snapshot")
	subscriptionE2ETopic(t, ctx, manager, topic, map[string]string{
		"database": database, "table": "events", "mode": "snapshot",
		"format": "SubscriptionRecordHandler", "start-time": "100", "end-time": "102",
	})
	consumer := subscriptionE2EPull(t, ctx, connection, subscription.TableDialect, topic, autoCommit)
	want := subscriptionTableExpected(database, "events", false, 100, 101, 102)
	tagColumns := map[string]struct{}{"tag1": {}}
	subscriptionTablePollStrictSnapshot(t, ctx, consumer, topic, want, tagColumns, !autoCommit)
}

func subscriptionTableWaitVisibleRows(t *testing.T, writer client.ITableSession, database, table string, start, end int64, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		timeout := int64(subscriptionE2EWriterTimeoutMs)
		dataSet, err := writer.ExecuteQueryStatement(fmt.Sprintf(
			"select time from %s.%s where time >= %d and time <= %d order by time",
			database, table, start, end), &timeout)
		if err != nil {
			t.Fatalf("query table rows before snapshot topic: %v", err)
		}
		count := 0
		for {
			hasNext, nextErr := dataSet.Next()
			if nextErr != nil {
				_ = dataSet.Close()
				t.Fatalf("read table rows before snapshot topic: %v", nextErr)
			}
			if !hasNext {
				break
			}
			count++
		}
		if closeErr := dataSet.Close(); closeErr != nil {
			t.Fatalf("close visibility query: %v", closeErr)
		}
		if count == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("snapshot source rows not visible before topic creation: got=%d want=%d", count, want)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func subscriptionTablePollStrictSnapshot(
	t *testing.T,
	ctx context.Context,
	consumer subconsumer.SubscriptionPullConsumer,
	topic string,
	want map[int64]subscriptionTableRecord,
	tagColumns map[string]struct{},
	manualCommit bool,
) {
	t.Helper()
	seen := make(map[int64]subscriptionTableRecord)
	startedAt := time.Now()
	polls := 0
	positionChecked := false
	requiredProgress := make(subpayload.TopicProgress)
	var manualCommitContexts []subpayload.CommitContext

	for {
		messages, err := consumer.Poll(ctx, subscriptionE2EPollTimeout)
		polls++
		if err != nil {
			t.Fatalf("strict snapshot poll %d failed after %s: %v", polls, time.Since(startedAt), err)
		}
		recordCount := 0
		contexts := make([]string, 0, len(messages))
		for messageIndex, message := range messages {
			context := message.CommitContext()
			contexts = append(contexts, fmt.Sprintf("topic=%s group=%s node=%d reboot=%d commit=%d seek=%d region=%s physical=%d writer=%v",
				context.Topic, context.GroupID, context.DataNodeID, context.RebootTimes, context.CommitID, context.SeekGeneration, context.RegionID, context.PhysicalTime, context.Writer))
			subscriptionTableTrackWriterProgress(requiredProgress, context)
			records, readErr := subscriptionTableRead(message, true)
			if readErr != nil {
				message.Release()
				t.Fatalf("strict snapshot read after %d polls: %v", polls, readErr)
			}
			recordCount += len(records)
			t.Logf("[strict snapshot] received poll=%d message=%d at=%s context=%+v records=%+v",
				polls, messageIndex, time.Since(startedAt), context, records)
			if mergeErr := subscriptionTableMergeStrict(seen, want, records, tagColumns); mergeErr != nil {
				message.Release()
				t.Fatalf("strict snapshot merge failure at poll=%d message=%d context=%+v records=%+v: %v",
					polls, messageIndex, context, records, mergeErr)
			}
			if manualCommit {
				if subscriptionTableContextSeen(manualCommitContexts, context) {
					t.Logf("[strict snapshot] skipped duplicate manual ACK at poll=%d context=%+v", polls, context)
				} else {
					if err := consumer.Commit(ctx, message); err != nil {
						message.Release()
						t.Fatalf("strict snapshot manual commit after %d polls: %v", polls, err)
					}
					manualCommitContexts = append(manualCommitContexts, context)
				}
			}
			message.Release()
		}

		positions, positionErr := consumer.CommittedPositions(topic)
		autoCommitErr := consumer.LastAutoCommitError()
		t.Logf("[strict snapshot] progress poll=%d at=%s positions=%+v positionError=%v autoCommitError=%v",
			polls, time.Since(startedAt), positions, positionErr, autoCommitErr)
		if !manualCommit && autoCommitErr != nil {
			t.Fatalf("strict snapshot auto-commit failed after %d polls: %v positions=%+v positionError=%v",
				polls, autoCommitErr, positions, positionErr)
		}
		complete := subscriptionTableRecordsComplete(seen, want)
		t.Logf("[strict snapshot] poll=%d messages=%d records=%d seen=%d/%d complete=%t consumed=%t manualCommit=%t positions=%+v contexts=%v",
			polls, len(messages), recordCount, len(seen), len(want), complete, consumer.AllConsumed(), manualCommit, positions, contexts)

		if complete && len(requiredProgress) == 0 && !positionChecked {
			t.Logf("[strict snapshot] delivered contexts use legacy commit metadata; writer-position assertion unavailable")
			positionChecked = true
		}
		if complete && len(requiredProgress) != 0 && !positionChecked {
			if manualCommit {
				subscriptionTableRequireCommittedPosition(t, consumer, topic, requiredProgress)
			} else {
				subscriptionTableWaitCommittedPosition(t, ctx, consumer, topic, requiredProgress)
			}
			positionChecked = true
		}
		if consumer.AllConsumed() && !complete {
			t.Fatalf("snapshot reported completion before all records arrived: seen=%+v want=%+v", seen, want)
		}
		if complete && consumer.AllConsumed() {
			if !reflect.DeepEqual(seen, want) {
				t.Fatalf("strict snapshot completed with records=%+v, want=%+v", seen, want)
			}
			return
		}
		if err := ctx.Err(); err != nil {
			positions, positionErr := consumer.CommittedPositions(topic)
			t.Fatalf("strict snapshot incomplete after %s (%d polls): complete=%t requiredProgress=%+v positionChecked=%t positions=%+v positionError=%v consumed=%t error=%v",
				time.Since(startedAt), polls, complete, requiredProgress, positionChecked, positions, positionErr, consumer.AllConsumed(), err)
		}
	}
}

func subscriptionTableMergeStrict(
	seen map[int64]subscriptionTableRecord,
	want map[int64]subscriptionTableRecord,
	records []subscriptionTableRecord,
	tagColumns map[string]struct{},
) error {
	for _, record := range records {
		expected, exists := want[record.Timestamp]
		if !exists || record.Database != expected.Database || record.Table != expected.Table {
			return fmt.Errorf("strict snapshot unexpected identity: got=%+v, want=%+v", record, expected)
		}
		combined, found := seen[record.Timestamp]
		if !found {
			combined = subscriptionTableRecord{
				Database:  record.Database,
				Table:     record.Table,
				Timestamp: record.Timestamp,
				Values:    make(map[string]any, len(record.Values)),
			}
		}
		newFields := 0
		for column, value := range record.Values {
			wantedValue, selected := expected.Values[column]
			if !selected || !reflect.DeepEqual(value, wantedValue) {
				return fmt.Errorf("strict snapshot unexpected value at time %d column %s: got=%v, want=%v", record.Timestamp, column, value, wantedValue)
			}
			previous, duplicate := combined.Values[column]
			if _, tag := tagColumns[column]; tag {
				// Table events can repeat the same TAG in separate disjoint field
				// fragments. The TAG must stay identical, but its repetition alone is
				// not evidence of redelivery.
				if duplicate && !reflect.DeepEqual(previous, value) {
					return fmt.Errorf("strict snapshot conflicting TAG at time %d column %s: got=%v, previous=%v", record.Timestamp, column, value, previous)
				}
			} else {
				if duplicate {
					return fmt.Errorf("strict snapshot duplicate field delivery at time %d column %s: got=%v, previous=%v", record.Timestamp, column, value, previous)
				}
				newFields++
			}
			combined.Values[column] = value
		}
		if newFields == 0 {
			return fmt.Errorf("strict snapshot redelivered time %d without a new field fragment", record.Timestamp)
		}
		seen[record.Timestamp] = combined
	}
	return nil
}

func subscriptionTableContextSeen(seen []subpayload.CommitContext, context subpayload.CommitContext) bool {
	for _, existing := range seen {
		if reflect.DeepEqual(existing, context) {
			return true
		}
	}
	return false
}

func subscriptionTableTrackWriterProgress(required subpayload.TopicProgress, context subpayload.CommitContext) {
	if context.Writer == nil {
		return
	}
	region := context.Writer.ID.RegionID
	if region == "" {
		return
	}
	regionProgress := required[region]
	if regionProgress == nil {
		regionProgress = make(subpayload.RegionProgress)
		required[region] = regionProgress
	}
	previous, exists := regionProgress[context.Writer.ID]
	if !exists || context.Writer.Progress.PhysicalTime > previous.PhysicalTime ||
		(context.Writer.Progress.PhysicalTime == previous.PhysicalTime && context.Writer.Progress.LocalSeq > previous.LocalSeq) {
		regionProgress[context.Writer.ID] = context.Writer.Progress
	}
}

func subscriptionTableProgressCovers(committed, required subpayload.TopicProgress) bool {
	for region, requiredWriters := range required {
		committedWriters := committed[region]
		for writer, requiredProgress := range requiredWriters {
			committedProgress, exists := committedWriters[writer]
			if !exists || committedProgress.PhysicalTime < requiredProgress.PhysicalTime ||
				(committedProgress.PhysicalTime == requiredProgress.PhysicalTime && committedProgress.LocalSeq < requiredProgress.LocalSeq) {
				return false
			}
		}
	}
	return true
}

func subscriptionTableRequireCommittedPosition(
	t *testing.T,
	consumer subconsumer.SubscriptionPullConsumer,
	topic string,
	required subpayload.TopicProgress,
) {
	t.Helper()
	positions, err := consumer.CommittedPositions(topic)
	if err != nil {
		t.Fatalf("read committed positions after manual commit: %v", err)
	}
	if !subscriptionTableProgressCovers(positions, required) {
		t.Fatalf("manual commit did not cover delivered writer progress: committed=%+v required=%+v", positions, required)
	}
}

func subscriptionTableWaitCommittedPosition(
	t *testing.T,
	ctx context.Context,
	consumer subconsumer.SubscriptionPullConsumer,
	topic string,
	required subpayload.TopicProgress,
) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	var lastPositions subpayload.TopicProgress
	var lastErr error
	for {
		if autoCommitErr := consumer.LastAutoCommitError(); autoCommitErr != nil {
			t.Fatalf("auto-commit failed before writer position advanced: %v", autoCommitErr)
		}
		positions, err := consumer.CommittedPositions(topic)
		if err != nil {
			lastErr = err
		} else {
			lastPositions = positions
			if subscriptionTableProgressCovers(positions, required) {
				t.Logf("[strict snapshot] auto-commit position covered delivered progress: committed=%+v required=%+v", positions, required)
				return
			}
		}
		select {
		case <-waitCtx.Done():
			t.Fatalf("auto-commit did not cover delivered writer progress for topic %s: committed=%+v required=%+v lastError=%v waitError=%v",
				topic, lastPositions, required, lastErr, waitCtx.Err())
		case <-ticker.C:
		}
	}
}

// Java reference: IoTDBConsensusSubscriptionFilterTableIT.testColumnFilteringAlterRebindsConsensusQueue.
func TestSubscriptionTableColumnFilterAlterRebinds(t *testing.T) {
	connection := subscriptionE2EConnection(t)
	ctx := subscriptionE2EContext(t)
	writer := subscriptionTableWriter(t, connection)
	database := subscriptionTableDatabase(t, connection, writer)
	subscriptionTableCreate(t, writer, database, "events")

	manager := subscriptionE2EManager(t, connection, subscription.TableDialect)
	topic := subscriptionE2EName("sub_table_filter_rebind")
	subscriptionE2ETopic(t, ctx, manager, topic, map[string]string{
		"database": database, "table": "events", "mode": "incremental",
		"format": "SubscriptionRecordHandler", "column-filter": `column_name = "s1"`,
	})
	consumer := subscriptionE2EPull(t, ctx, connection, subscription.TableDialect, topic, true)

	subscriptionTableInsert(t, writer, database, "events", 100)
	beforeAlter := map[int64]subscriptionTableRecord{
		100: {
			Database:  database,
			Table:     "events",
			Timestamp: 100,
			Values:    map[string]any{"tag1": "device_100", "s1": int64(1000)},
		},
	}
	subscriptionTablePollExact(t, ctx, consumer, beforeAlter, false, false, false, "column-filter before alter")

	if err := manager.AlterTopic(ctx, topic, map[string]string{"column-filter": `column_name = "s2"`}); err != nil {
		t.Fatalf("alter column filter: %v", err)
	}
	subscriptionTableInsert(t, writer, database, "events", 200)
	afterAlter := map[int64]subscriptionTableRecord{
		200: {
			Database:  database,
			Table:     "events",
			Timestamp: 200,
			Values:    map[string]any{"tag1": "device_200", "s2": 200.5},
		},
	}
	subscriptionTablePollExact(t, ctx, consumer, afterAlter, false, false, false, "column-filter after alter")
}

// Java reference: IoTDBConsensusSubscriptionFilterTableIT.testColumnFilteringWithNoMatchedColumnsReturnsNothing.
func TestSubscriptionTableColumnFilterNoMatchReturnsNothing(t *testing.T) {
	connection := subscriptionE2EConnection(t)
	ctx := subscriptionE2EContext(t)
	writer := subscriptionTableWriter(t, connection)
	database := subscriptionTableDatabase(t, connection, writer)
	subscriptionTableCreate(t, writer, database, "events")

	manager := subscriptionE2EManager(t, connection, subscription.TableDialect)
	topic := subscriptionE2EName("sub_table_filter_no_match")
	subscriptionE2ETopic(t, ctx, manager, topic, map[string]string{
		"database": database, "table": "events", "mode": "incremental",
		"format": "SubscriptionRecordHandler", "column-filter": `column_name = "missing"`,
	})
	consumer := subscriptionE2EPull(t, ctx, connection, subscription.TableDialect, topic, true)
	subscriptionTableInsert(t, writer, database, "events", 50)
	for round := 0; round < subscriptionE2EQuietPollRounds; round++ {
		messages, err := consumer.Poll(ctx, subscriptionE2EPollTimeout)
		if err != nil {
			t.Fatal(err)
		}
		if len(messages) != 0 {
			for _, message := range messages {
				message.Release()
			}
			t.Fatalf("zero-match column filter delivered %d messages", len(messages))
		}
	}
}

// Java reference: IoTDBConsensusSubscriptionFilterTableIT.testTagFilteringAndAlterInConsensusQueue.
func TestSubscriptionTableTagFilterSelectsMatchingDevices(t *testing.T) {
	connection := subscriptionE2EConnection(t)
	ctx := subscriptionE2EContext(t)
	writer := subscriptionTableWriter(t, connection)
	database := subscriptionTableDatabase(t, connection, writer)
	subscriptionTableCreate(t, writer, database, "events")

	manager := subscriptionE2EManager(t, connection, subscription.TableDialect)
	topic := subscriptionE2EName("sub_table_tag_filter")
	subscriptionE2ETopic(t, ctx, manager, topic, map[string]string{
		"database": database, "table": "events", "mode": "incremental",
		"format": "SubscriptionRecordHandler", "tag-filter": `tag1 = "keep"`,
	})
	consumer := subscriptionE2EPull(t, ctx, connection, subscription.TableDialect, topic, true)
	subscriptionTableInsertSameDevice(t, writer, database, "events", "keep", 200, 201)
	subscriptionTableInsertSameDevice(t, writer, database, "events", "drop", 202)
	want := subscriptionTableExpectedSameDevice(database, "events", "keep", 200, 201)
	subscriptionTablePollExact(t, ctx, consumer, want, false, false, true, "tag-filter")
	if err := manager.AlterTopic(ctx, topic, map[string]string{"tag-filter": `tag1 = "drop"`}); err != nil {
		t.Fatalf("alter tag filter: %v", err)
	}
	subscriptionTableInsertSameDevice(t, writer, database, "events", "keep", 203)
	subscriptionTableInsertSameDevice(t, writer, database, "events", "drop", 204)
	want = subscriptionTableExpectedSameDevice(database, "events", "drop", 204)
	subscriptionTablePollExact(t, ctx, consumer, want, false, false, true, "tag-filter alter")
}
