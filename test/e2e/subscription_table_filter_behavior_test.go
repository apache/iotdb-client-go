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
	"testing"

	"github.com/apache/iotdb-client-go/subscription"
)

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
