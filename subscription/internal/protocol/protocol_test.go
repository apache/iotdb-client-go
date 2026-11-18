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

package protocol

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/apache/iotdb-client-go/subscription"
	"github.com/apache/iotdb-client-go/subscription/payload"
	"github.com/apache/iotdb-client-go/v2/client"
)

// This fixture is transcribed independently from Java SubscriptionPollResponse,
// TabletsPayload and SubscriptionCommitContext.serialize, and from Tablet and
// MeasurementSchema.serializeTo in TsFile 2.5.1-261023-SNAPSHOT. It does not use
// the Go production encoders. The Tablet contains one INT64 FIELD, with raw
// timestamp/value pairs (20, 200), (10, 101), (10, 102).
func javaSubscriptionTabletFrame(t *testing.T) []byte {
	t.Helper()
	body, err := hex.DecodeString(
		"0001" + // TABLETS response.
		"00000001" + "000000026462" + "00000001" + // One database db, one Tablet.
		"000000066576656e7473" + "00000003" + // Target events, three rows.
		"01" + "00000001" + "01" + // Schemas present, one non-null schema.
		"000000027331" + "020000" + "00000000" + "01" + // s1, INT64/PLAIN/UNCOMPRESSED, no properties, FIELD.
		"01" + "0000000000000014" + "000000000000000a" + "000000000000000a" +
		"00" + "01" + "01" + // No bitmaps, values and column present.
		"00000000000000c8" + "0000000000000065" + "0000000000000066" +
		"ffffffff" + // Final offset: one Tablet, rather than three rows.
		"02" + "00000007" + "00000002" + // Context version, node, restart count.
		"00000005746f706963" + "0000000567726f7570" + // topic, group.
		"000000000000002a" + "0000000000000003" + // Commit ID 42, seek generation 3.
		"0000000172" + "0000000000000064" + "00" + // Region r, physical time 100, no writer.
		"01" + "00000000") // Time selected, no per-table overrides.
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func javaSubscriptionTabletFrameWithoutValues(t *testing.T) []byte {
	t.Helper()
	frame := javaSubscriptionTabletFrame(t)
	// Keep everything through the no-bitmap flag, mark the values section as
	// absent, then append the final offset and commit context unchanged.
	const valuesFlagOffset = 76
	const finalOffset = 102
	body := append([]byte{}, frame[:valuesFlagOffset]...)
	body = append(body, 0)
	body = append(body, frame[finalOffset:]...)
	return body
}

func decodedJavaSubscriptionMessage(t *testing.T, body []byte, dialect subscription.Dialect) payload.SubscriptionMessage {
	t.Helper()
	response, err := DecodePollResponse(body)
	if err != nil {
		t.Fatalf("decode Java-format frame: %v", err)
	}
	message, err := response.ToMessage(dialect)
	if err != nil || message == nil {
		t.Fatalf("construct record message: message=%v, error=%v", message, err)
	}
	return message
}

func TestSubscriptionPollRejectsFileBeforeSiblingDecode(t *testing.T) {
	for _, kind := range []byte{2, 3, 4} {
		for _, fileFirst := range []bool{false, true} {
			for _, malformedSibling := range []bool{false, true} {
				t.Run(fmt.Sprintf("kind=%d/first=%t/malformed=%t", kind, fileFirst, malformedSibling), func(t *testing.T) {
					sibling := javaSubscriptionTabletFrame(t)
					if malformedSibling {
						sibling = sibling[:1]
					}
					// A tag alone must reject files without attempting file I/O.
					file := []byte{0, kind}
					bodies := [][]byte{sibling, file}
					if fileFirst {
						bodies = [][]byte{file, sibling}
					}
					responses, err := DecodePollResponses(bodies)
					if !errors.Is(err, subscription.ErrUnsupportedTsFile) || responses != nil {
						t.Fatalf("file response returned partial data or wrong error: %v, %v", responses, err)
					}
				})
			}
		}
	}
}

func TestSubscriptionPollRejectsMalformedJavaFramesWithoutPartialResults(t *testing.T) {
	valid := javaSubscriptionTabletFrame(t)
	cases := map[string][]byte{}
	cases["trailing-byte"] = append(append([]byte(nil), valid...), 0)
	// Mandatory fields and the optional projection-map header must be complete.
	// Ending exactly after the context or the global flag is valid in older Java clients.
	for _, length := range []int{1, 5, 9, 29, 41, 60, 100, 105, 106, 120, 148, 162, 165} {
		cases[fmt.Sprintf("truncated-%d", length)] = valid[:length]
	}
	for _, field := range []struct {
		name string
		offset int
		value uint32
	}{
		{"negative-database-count", 2, 0xffffffff},
		{"invalid-database-name-length", 6, 0xfffffffe},
		{"negative-tablet-count", 12, 0xffffffff},
		{"negative-row-count", 26, 0xffffffff},
		{"oversized-column-count", 31, 0x7fffffff},
		{"invalid-column-name-length", 36, 0xfffffffe},
	} {
		body := append([]byte(nil), valid...)
		binary.BigEndian.PutUint32(body[field.offset:field.offset+4], field.value)
		cases[field.name] = body
	}
	for name, malformed := range cases {
		t.Run(name, func(t *testing.T) {
			responses, err := DecodePollResponses([][]byte{valid, malformed})
			if err == nil || responses != nil {
				t.Fatalf("malformed sibling returned partial results: %v, %v", responses, err)
			}
		})
	}
}

func TestSubscriptionPollAcceptsLegacyProjectionTails(t *testing.T) {
	for _, length := range []int{163, 164} { // Context only, or context and global flag.
		t.Run(fmt.Sprintf("frame-length-%d", length), func(t *testing.T) {
			message := decodedJavaSubscriptionMessage(t, javaSubscriptionTabletFrame(t)[:length], subscription.TableDialect)
			defer message.Release()
			batches, err := message.Records()
			if err != nil || len(batches) != 1 {
				t.Fatalf("legacy record batches: %v, %v", batches, err)
			}
			rows := 0
			for row, err := range batches[0].Rows() {
				if err != nil || row.Timestamp == nil {
					t.Fatalf("legacy response lost projected timestamps: %v, %v", row, err)
				}
				rows++
			}
			if rows != 3 {
				t.Fatalf("legacy response yielded %d rows, want 3", rows)
			}
		})
	}
}

func TestSubscriptionDecodedRowsKeepStableOrderAndReleaseBorrowedData(t *testing.T) {
	message := decodedJavaSubscriptionMessage(t, javaSubscriptionTabletFrame(t), subscription.TableDialect)
	batches, err := message.Records()
	if err != nil || len(batches) != 1 {
		t.Fatalf("record batches: %v, %v", batches, err)
	}
	batch := batches[0]
	if batch.ColumnCount() != 1 {
		t.Fatalf("column count = %d, want 1", batch.ColumnCount())
	}
	if name, err := batch.ColumnName(0); err != nil || name != "s1" {
		t.Fatalf("column name = %q, %v", name, err)
	}
	if dataType, err := batch.ColumnDataType(0); err != nil || dataType != client.INT64 {
		t.Fatalf("column data type = %v, %v", dataType, err)
	}
	if category, err := batch.ColumnCategory(0); err != nil || category != client.FIELD {
		t.Fatalf("column category = %v, %v", category, err)
	}
	var timestamps, values []int64
	for row, err := range batch.Rows() {
		if err != nil || row.Timestamp == nil || len(row.Values) != 1 {
			t.Fatalf("row: %v, %v", row, err)
		}
		timestamps = append(timestamps, *row.Timestamp)
		value, ok := row.Values[0].(int64)
		if !ok {
			t.Fatalf("INT64 value has type %T", row.Values[0])
		}
		values = append(values, value)
	}
	if !reflect.DeepEqual(timestamps, []int64{10, 10, 20}) || !reflect.DeepEqual(values, []int64{101, 102, 200}) {
		t.Fatalf("unstable user-visible row ordering: timestamps=%v, values=%v", timestamps, values)
	}
	for index, want := range []int64{20, 10, 10} {
		got, err := batch.Timestamp(index)
		if err != nil || got != want {
			t.Fatalf("raw timestamp %d was reordered: %d, %v", index, got, err)
		}
		value, err := batch.Value(index, 0)
		if err != nil || value != []int64{200, 101, 102}[index] {
			t.Fatalf("raw value %d was reordered: %v, %v", index, value, err)
		}
	}
	context := message.CommitContext()
	message.Release()
	message.Release()
	if _, err := message.Records(); !errors.Is(err, subscription.ErrDataReleased) {
		t.Fatalf("records after release: %v", err)
	}
	if _, err := batch.Timestamp(0); !errors.Is(err, subscription.ErrDataReleased) {
		t.Fatalf("borrowed timestamp after release: %v", err)
	}
	if _, err := batch.Value(0, 0); !errors.Is(err, subscription.ErrDataReleased) {
		t.Fatalf("borrowed value after release: %v", err)
	}
	if _, err := batch.ColumnName(0); !errors.Is(err, subscription.ErrDataReleased) {
		t.Fatalf("column metadata after release: %v", err)
	}
	yields := 0
	for _, err := range batch.Rows() {
		yields++
		if !errors.Is(err, subscription.ErrDataReleased) {
			t.Fatalf("iteration after release: %v", err)
		}
	}
	if yields != 1 {
		t.Fatalf("released iterator yielded %d times", yields)
	}
	if message.Kind() != payload.RecordMessage || message.CommitContext() != context || context.CommitID != 42 || context.SeekGeneration != 3 {
		t.Fatal("release discarded the delivery identity required for later acknowledgment")
	}
}

func TestSubscriptionMissingValuesColumnMaterializesAsNull(t *testing.T) {
	frame := javaSubscriptionTabletFrameWithoutValues(t)
	message := decodedJavaSubscriptionMessage(t, frame, subscription.TableDialect)
	defer message.Release()
	batches, err := message.Records()
	if err != nil || len(batches) != 1 {
		t.Fatalf("record batches: %v, %v", batches, err)
	}
	for row := 0; row < batches[0].Len(); row++ {
		value, err := batches[0].Value(row, 0)
		if err != nil || value != nil {
			t.Fatalf("row %d missing value = %v, %v", row, value, err)
		}
	}
}

func TestSubscriptionReleaseInsideRowCallbackStopsIteration(t *testing.T) {
	message := decodedJavaSubscriptionMessage(t, javaSubscriptionTabletFrame(t), subscription.TableDialect)
	batches, err := message.Records()
	if err != nil || len(batches) != 1 {
		t.Fatalf("record batches: %v, %v", batches, err)
	}
	finished := make(chan error, 1)
	go func() {
		rows, released := 0, 0
		for _, err := range batches[0].Rows() {
			if err != nil {
				if !errors.Is(err, subscription.ErrDataReleased) {
					finished <- err
					return
				}
				released++
				continue
			}
			rows++
			message.Release()
		}
		if rows != 1 || released != 1 {
			finished <- fmt.Errorf("iteration after callback release: rows=%d, errors=%d", rows, released)
			return
		}
		finished <- nil
	}()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Release deadlocked inside the row callback")
	}
}

func TestSubscriptionContinuationRejectsDifferentDeliveryWithoutLeakingRows(t *testing.T) {
	for _, changedOffset := range []int{133, 141} { // Commit ID and seek generation in the Java fixture.
		t.Run(fmt.Sprintf("context-offset-%d", changedOffset), func(t *testing.T) {
			firstBody := javaSubscriptionTabletFrame(t)
			binary.BigEndian.PutUint32(firstBody[102:106], 1) // Request continuation at offset 1.
			first, err := DecodePollResponse(firstBody)
			if err != nil {
				t.Fatal(err)
			}
			if message, err := first.ToMessage(subscription.TableDialect); err == nil || message != nil {
				t.Fatal("incomplete delivery exposed rows before continuation")
			}
			lastBody := javaSubscriptionTabletFrame(t)
			binary.BigEndian.PutUint32(lastBody[102:106], 0xfffffffe) // Two Tablets total.
			badBody := append([]byte(nil), lastBody...)
			binary.BigEndian.PutUint64(badBody[changedOffset:changedOffset+8], 999)
			bad, err := DecodePollResponse(badBody)
			if err != nil {
				t.Fatal(err)
			}
			if err := first.AppendTablets(bad); err == nil {
				t.Fatal("accepted a continuation from another delivery")
			}
			last, err := DecodePollResponse(lastBody)
			if err != nil {
				t.Fatal(err)
			}
			if err := first.AppendTablets(last); err != nil {
				t.Fatalf("rejected continuation corrupted the current delivery: %v", err)
			}
			message, err := first.ToMessage(subscription.TableDialect)
			if err != nil || message == nil {
				t.Fatalf("completed delivery: %v, %v", message, err)
			}
			defer message.Release()
			batches, err := message.Records()
			if err != nil || len(batches) != 2 {
				t.Fatalf("rejected continuation leaked records: %v, %v", batches, err)
			}
			rows := 0
			for _, batch := range batches {
				for _, err := range batch.Rows() {
					if err != nil {
						t.Fatal(err)
					}
					rows++
				}
			}
			if rows != 6 {
				t.Fatalf("completed delivery contains %d rows, want 6", rows)
			}
		})
	}
}

func TestSubscriptionProjectionChangesVisibleRowsByModel(t *testing.T) {
	for _, dialect := range []subscription.Dialect{subscription.TreeDialect, subscription.TableDialect} {
		for _, override := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/override=%t", dialect, override), func(t *testing.T) {
				body := javaSubscriptionTabletFrame(t)
				body[163] = 0 // Global time projection excludes timestamps.
				if override {
					// One database/table override: db.events selects time.
					tail, err := hex.DecodeString("0000000100000002646200000001000000066576656e747301")
					if err != nil {
						t.Fatal(err)
					}
					body = append(body[:164], tail...)
				}
				message := decodedJavaSubscriptionMessage(t, body, dialect)
				defer message.Release()
				batches, err := message.Records()
				if err != nil || len(batches) != 1 {
					t.Fatalf("record batches: %v, %v", batches, err)
				}
				rows := 0
				for row, err := range batches[0].Rows() {
					if err != nil {
						t.Fatal(err)
					}
					wantTime := dialect == subscription.TreeDialect || override
					if (row.Timestamp != nil) != wantTime {
						t.Fatalf("projection exposed wrong timestamp: %v", row.Timestamp)
					}
					rows++
				}
				if rows != 3 {
					t.Fatalf("projection changed row count: %d", rows)
				}
				if timestamp, err := batches[0].Timestamp(0); err != nil || timestamp != 20 {
					t.Fatalf("projection hid raw processing timestamp: %d, %v", timestamp, err)
				}
			})
		}
	}
}
