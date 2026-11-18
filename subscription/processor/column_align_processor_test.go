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

package processor_test

import (
	"github.com/apache/iotdb-client-go/subscription/payload"
	subprocessor "github.com/apache/iotdb-client-go/subscription/processor"

	"bytes"
	"errors"
	"testing"

	"github.com/apache/iotdb-client-go/subscription"
	"github.com/apache/iotdb-client-go/subscription/internal/record"
	"github.com/apache/iotdb-client-go/v2/client"
)

func newTestColumnAlignProcessor(t *testing.T, dialect subscription.Dialect) *subprocessor.ColumnAlignProcessor {
	t.Helper()
	processor, err := subprocessor.NewColumnAlignProcessor(dialect)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(processor.Reset)
	return processor
}

func processAlignment(t *testing.T, processor *subprocessor.ColumnAlignProcessor, input ...payload.SubscriptionMessage) []payload.SubscriptionMessage {
	t.Helper()
	messages, err := processor.Process(input)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { releaseProcessorMessages(messages) })
	return messages
}

func TestColumnAlignNeverFillsFromEqualOrFutureTimestamps(t *testing.T) {
	processor := newTestColumnAlignProcessor(t, subscription.TreeDialect)
	seed := processorRecord("topic", "region", 1, processorTreeBatch([]int64{10}, int64(100)))
	processAlignment(t, processor, seed)
	input := processorRecord("topic", "region", 2, processorTreeBatch(
		[]int64{10, 5, 6, 11, 12, 13}, nil, nil, int64(60), nil, int64(120), nil,
	))
	messages := processAlignment(t, processor, input)
	for row, expected := range []any{nil, nil, int64(60), int64(100), int64(120), int64(120)} {
		if value := processorOutputValue(t, messages, row, 0); value != expected {
			t.Fatalf("raw row %d value=%v, want %v", row, value, expected)
		}
	}
	if _, err := input.Records(); !errors.Is(err, subscription.ErrDataReleased) {
		t.Fatalf("consumed input was not released: %v", err)
	}
}

func TestColumnAlignMutatesReusedSessionTablet(t *testing.T) {
	processor := newTestColumnAlignProcessor(t, subscription.TreeDialect)
	context := payload.CommitContext{CommitID: 1, Topic: "topic", GroupID: "group"}
	seedTablet := client.NewDecodedTablet(
		"root.db.device",
		[]*client.MeasurementSchema{{Measurement: "value", DataType: client.INT64}},
		[]client.ColumnCategory{client.FIELD},
		1,
		[]int64{1},
		[]interface{}{[]int64{101}},
		nil,
	)
	seed := record.NewRecordMessage(context, []payload.RecordBatch{record.NewRecordBatch(
		subscription.TreeDialect,
		"",
		seedTablet,
		true,
	)})
	processAlignment(t, processor, seed)

	inputTablet := client.NewDecodedTablet(
		"root.db.device",
		[]*client.MeasurementSchema{{Measurement: "value", DataType: client.INT64}},
		[]client.ColumnCategory{client.FIELD},
		2,
		[]int64{2, 3},
		[]interface{}{[]int64{0, 0}},
		[][]byte{{1}},
	)
	input := record.NewRecordMessage(context, []payload.RecordBatch{record.NewRecordBatch(
		subscription.TreeDialect,
		"",
		inputTablet,
		true,
	)})
	messages := processAlignment(t, processor, input)
	if value := processorOutputValue(t, messages, 0, 0); value != int64(101) {
		t.Fatalf("aligned value = %v, want 101", value)
	}
	if _, err := input.Records(); !errors.Is(err, subscription.ErrDataReleased) {
		t.Fatalf("in-place input was not released: %v", err)
	}
}

func TestColumnAlignMutatesDecodedObject(t *testing.T) {
	processor := newTestColumnAlignProcessor(t, subscription.TreeDialect)
	context := payload.CommitContext{CommitID: 1, Topic: "topic", GroupID: "group"}
	categories := []client.ColumnCategory{client.FIELD}
	schemas := []*client.MeasurementSchema{{Measurement: "value", DataType: client.OBJECT}}

	seedData, err := client.NewRelationalTablet("root.db.device", schemas, categories, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := seedData.SetObjectBytesAt([]byte{1, 2}, 0, 0); err != nil {
		t.Fatal(err)
	}
	seedData.SetTimestampAt(1, 0)
	seedData.RowSize = 1
	seed := record.NewRecordMessage(context, []payload.RecordBatch{record.NewRecordBatch(
		subscription.TreeDialect,
		"",
		seedData,
		true,
	)})
	processAlignment(t, processor, seed)

	inputData, err := client.NewRelationalTablet("root.db.device", schemas, categories, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := inputData.SetValueAt(nil, 0, 0); err != nil {
		t.Fatal(err)
	}
	if err := inputData.SetValueAt(nil, 0, 1); err != nil {
		t.Fatal(err)
	}
	inputData.SetTimestampAt(2, 0)
	inputData.SetTimestampAt(3, 1)
	inputData.RowSize = 2
	input := record.NewRecordMessage(context, []payload.RecordBatch{record.NewRecordBatch(
		subscription.TreeDialect,
		"",
		inputData,
		true,
	)})
	messages := processAlignment(t, processor, input)
	value, ok := processorOutputValue(t, messages, 0, 0).([]byte)
	if !ok || !bytes.Equal(value, []byte{1, 2}) {
		t.Fatalf("aligned OBJECT value = %v", value)
	}
}

func TestColumnAlignMatchesColumnNamesAfterSchemaReordering(t *testing.T) {
	processor := newTestColumnAlignProcessor(t, subscription.TreeDialect)
	seed := processorTreeBatch([]int64{1})
	seed.columnNames = []string{"left", "right"}
	seed.columnTypes = []client.TSDataType{client.INT64, client.INT64}
	seed.columnCategories = []client.ColumnCategory{client.FIELD, client.FIELD}
	seed.values = [][]any{{int64(11), int64(22)}}
	processAlignment(t, processor, processorRecord("topic", "region", 1, seed))
	input := processorTreeBatch([]int64{2})
	input.columnNames = []string{"right", "left", "new"}
	input.columnTypes = []client.TSDataType{client.INT64, client.INT64, client.INT64}
	input.columnCategories = []client.ColumnCategory{client.FIELD, client.FIELD, client.FIELD}
	input.values = [][]any{{nil, nil, nil}}
	messages := processAlignment(t, processor, processorRecord("topic", "region", 2, input))
	for column, expected := range []any{int64(22), int64(11), nil} {
		if value := processorOutputValue(t, messages, 0, column); value != expected {
			t.Fatalf("column %d reused another column's value: got=%v want=%v", column, value, expected)
		}
	}
}

func TestColumnAlignTableIdentityCannotLeakValuesAcrossDevices(t *testing.T) {
	for _, test := range []struct {
		name string
		topic string
		database string
		table string
		seedTags []any
		otherTags []any
	}{
		{name: "topic", topic: "other", database: "db", table: "table", seedTags: []any{"a"}, otherTags: []any{"a"}},
		{name: "database", topic: "topic", database: "other", table: "table", seedTags: []any{"a"}, otherTags: []any{"a"}},
		{name: "table", topic: "topic", database: "db", table: "other", seedTags: []any{"a"}, otherTags: []any{"a"}},
		{name: "tag", topic: "topic", database: "db", table: "table", seedTags: []any{"a"}, otherTags: []any{"b"}},
		{name: "null_and_empty", topic: "topic", database: "db", table: "table", seedTags: []any{nil, "x"}, otherTags: []any{"", "x"}},
		{name: "embedded_delimiters", topic: "topic", database: "db", table: "table", seedTags: []any{"a|b", "c"}, otherTags: []any{"a", "b|c"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			processor := newTestColumnAlignProcessor(t, subscription.TableDialect)
			processAlignment(t, processor, processorTableRecord("topic", "db", "table", test.seedTags, 1, int64(101)))
			other := processAlignment(t, processor, processorTableRecord(test.topic, test.database, test.table, test.otherTags, 2, nil))
			if value := processorOutputValue(t, other, 0, len(test.otherTags)); value != nil {
				t.Fatalf("another device inherited the seed value: %v", value)
			}
			same := processAlignment(t, processor, processorTableRecord("topic", "db", "table", test.seedTags, 3, nil))
			if value := processorOutputValue(t, same, 0, len(test.seedTags)); value != int64(101) {
				t.Fatalf("same device lost its value: %v", value)
			}
		})
	}
}

func TestColumnAlignTableIdentityIgnoresTrailingNullTags(t *testing.T) {
	for _, test := range []struct {
		name string
		seedTags []any
		nextTags []any
	}{
		{name: "trailing_null", seedTags: []any{"device"}, nextTags: []any{"device", nil}},
		{name: "no_tags_and_all_null", seedTags: nil, nextTags: []any{nil, nil}},
	} {
		t.Run(test.name, func(t *testing.T) {
			processor := newTestColumnAlignProcessor(t, subscription.TableDialect)
			processAlignment(t, processor, processorTableRecord("topic", "db", "table", test.seedTags, 1, int64(101)))
			messages := processAlignment(t, processor, processorTableRecord("topic", "db", "table", test.nextTags, 2, nil))
			if value := processorOutputValue(t, messages, 0, len(test.nextTags)); value != int64(101) {
				t.Fatalf("equivalent device identity did not fill its field: %v", value)
			}
		})
	}
}

func TestColumnAlignResetTopicDoesNotDiscardOtherTopicValues(t *testing.T) {
	processor := newTestColumnAlignProcessor(t, subscription.TreeDialect)
	processAlignment(t, processor,
		processorRecord("a", "region", 1, processorTreeBatch([]int64{10}, int64(11))),
		processorRecord("b", "region", 2, processorTreeBatch([]int64{10}, int64(22))),
	)
	processor.ResetTopic("a")
	a := processAlignment(t, processor, processorRecord("a", "region", 3, processorTreeBatch([]int64{20})))
	if value := processorOutputValue(t, a, 0, 0); value != nil {
		t.Fatalf("reset topic reused its obsolete cached value: %v", value)
	}
	b := processAlignment(t, processor, processorRecord("b", "region", 4, processorTreeBatch([]int64{20})))
	if value := processorOutputValue(t, b, 0, 0); value != int64(22) {
		t.Fatalf("reset discarded another topic's value: %v", value)
	}
}

func TestColumnAlignCopiedBytesSurviveInputAndOutputRelease(t *testing.T) {
	processor := newTestColumnAlignProcessor(t, subscription.TreeDialect)
	seedBatch := processorTreeBatch([]int64{1}, []byte{1, 2, 3})
	seedBatch.columnTypes[0] = client.BLOB
	seed := processorRecord("topic", "region", 1, seedBatch)
	messages := processAlignment(t, processor, seed)
	if _, err := seed.Records(); !errors.Is(err, subscription.ErrDataReleased) {
		t.Fatalf("input storage was not reclaimed: %v", err)
	}
	if value, ok := processorOutputValue(t, messages, 0, 0).([]byte); !ok || !bytes.Equal(value, []byte{1, 2, 3}) {
		t.Fatalf("output borrowed storage reclaimed by input Release: %v", value)
	}
	releaseProcessorMessages(messages)
	for timestamp := int64(2); timestamp <= 3; timestamp++ {
		batch := processorTreeBatch([]int64{timestamp})
		batch.columnTypes[0] = client.BLOB
		messages = processAlignment(t, processor, processorRecord("topic", "region", timestamp, batch))
		if value, ok := processorOutputValue(t, messages, 0, 0).([]byte); !ok || !bytes.Equal(value, []byte{1, 2, 3}) {
			t.Fatalf("cached bytes failed after earlier Release: %v", value)
		}
		releaseProcessorMessages(messages)
	}
}

func TestColumnAlignFailedInputCannotPolluteEarlierValues(t *testing.T) {
	for _, failure := range []string{"dialect", "read"} {
		t.Run(failure, func(t *testing.T) {
			processor := newTestColumnAlignProcessor(t, subscription.TreeDialect)
			processAlignment(t, processor, processorRecord("topic", "region", 1, processorTreeBatch([]int64{10}, int64(11))))
			good := processorRecord("topic", "region", 2, processorTreeBatch([]int64{20}, int64(99)))
			brokenBatch := processorTreeBatch([]int64{21}, int64(123))
			readFailure := errors.New("input value unavailable")
			if failure == "dialect" {
				brokenBatch.dialect = subscription.TableDialect
			} else {
				brokenBatch.valueErr = readFailure
			}
			broken := processorRecord("topic", "region", 3, brokenBatch)
			messages, err := processor.Process([]payload.SubscriptionMessage{good, broken})
			if err == nil || len(messages) != 0 || (failure == "read" && !errors.Is(err, readFailure)) {
				t.Fatalf("failed round delivered data or lost the read failure: messages=%v error=%v", messages, err)
			}
			for _, input := range []*processorInputMessage{good, broken} {
				if _, err := input.Records(); err != nil {
					t.Fatalf("failed round consumed caller-owned input: %v", err)
				}
				defer input.Release()
			}
			messages = processAlignment(t, processor, processorRecord("topic", "region", 4, processorTreeBatch([]int64{30})))
			if value := processorOutputValue(t, messages, 0, 0); value != int64(11) {
				t.Fatalf("failed round changed the cache: %v", value)
			}
		})
	}
}
