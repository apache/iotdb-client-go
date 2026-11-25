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
	"fmt"
	"strings"

	"github.com/apache/iotdb-client-go/v2/client"
	"github.com/apache/iotdb-client-go/v2/subscription"
	"github.com/apache/iotdb-client-go/v2/subscription/internal/record"
	internalwire "github.com/apache/iotdb-client-go/v2/subscription/internal/wire"
	"github.com/apache/iotdb-client-go/v2/subscription/payload"
)

// Poll response tags retain their Java protocol numbers.
const (
	ResponseError       int16 = 0
	ResponseTablets     int16 = 1
	ResponseFileInit    int16 = 2
	ResponseFilePiece   int16 = 3
	ResponseFileSeal    int16 = 4
	ResponseTermination int16 = 5
	ResponseWatermark   int16 = 7
)

// PollBatch associates one decoded Tablet with its source database.
type PollBatch struct {
	Database string
	Tablet   *client.Tablet
}

func decodeSubscriptionTablet(reader *subscriptionWireReader) (*client.Tablet, error) {
	tablet, bytesRead, err := client.DecodeTablet(reader.RemainingBytes())
	if err != nil {
		return nil, err
	}
	reader.Advance(bytesRead)
	if err := reader.Err(); err != nil {
		return nil, err
	}
	return tablet, nil
}

// PollResponse retains transport events separately from deliverable
// messages. In particular, termination and empty record deliveries still need
// consumer-level unsubscribe or confirmation; decoding does neither.
type PollResponse struct {
	Type                int16
	CommitContext       payload.CommitContext
	Records             []PollBatch
	NextOffset          int32
	TimeSelected        bool
	TimeSelectedByTable map[string]map[string]bool
	Watermark           int64
	DataNodeID          int32
	ErrorMessage        string
	Critical            bool
}

// DecodePollResponses parses every nonempty body entry. No partially
// decoded response is returned if any entry fails, including an unsupported file
// entry that follows otherwise valid records.
func DecodePollResponses(bodies [][]byte) ([]PollResponse, error) {
	// Reject files before decoding records in sibling entries. This examines the
	// response tag only; it does not parse file payloads or attempt file I/O.
	for _, body := range bodies {
		if len(body) < 2 {
			continue
		}
		kind := int16(binary.BigEndian.Uint16(body[:2]))
		if isSubscriptionFileResponse(kind) {
			return nil, fmt.Errorf("subscription: decode poll response type %d: %w", kind, subscription.ErrUnsupportedTsFile)
		}
	}
	var responses []PollResponse
	for index, body := range bodies {
		if len(body) == 0 {
			continue
		}
		response, err := DecodePollResponse(body)
		if err != nil {
			return nil, fmt.Errorf("subscription: decode poll body %d: %w", index, err)
		}
		responses = append(responses, response)
	}
	return responses, nil
}

// DecodePollResponse reads one complete event without changing consumer state.
func DecodePollResponse(body []byte) (PollResponse, error) {
	reader := internalwire.NewReader(body)
	response := PollResponse{Type: reader.ReadInt16(), TimeSelected: true}
	if reader.Err() != nil {
		return PollResponse{}, reader.Err()
	}
	switch response.Type {
	case ResponseTablets:
		databaseCount := reader.ReadCount(8, false)
		for databaseIndex := int32(0); databaseIndex < databaseCount && reader.Err() == nil; databaseIndex++ {
			database := reader.ReadString()
			tabletCount := reader.ReadCount(12, false)
			for tabletIndex := int32(0); tabletIndex < tabletCount && reader.Err() == nil; tabletIndex++ {
				tablet, err := decodeSubscriptionTablet(reader)
				if err != nil {
					return PollResponse{}, err
				}
				response.Records = append(response.Records, PollBatch{Database: database, Tablet: tablet})
			}
		}
		response.NextOffset = reader.ReadInt32()
	case ResponseError:
		response.ErrorMessage = reader.ReadString()
		response.Critical = reader.ReadBool()
	case ResponseTermination:
		// The termination payload is empty; its topic comes from the context.
	case ResponseWatermark:
		response.Watermark = reader.ReadInt64()
		response.DataNodeID = reader.ReadInt32()
	case ResponseFileInit, ResponseFilePiece, ResponseFileSeal:
		return PollResponse{}, fmt.Errorf("subscription: decode poll response type %d: %w", response.Type, subscription.ErrUnsupportedTsFile)
	default:
		return PollResponse{}, fmt.Errorf("subscription: unsupported poll response type %d", response.Type)
	}
	if reader.Err() != nil {
		return PollResponse{}, reader.Err()
	}
	response.CommitContext = readSubscriptionCommitContext(reader)
	// Older responses end after the context or after the global projection flag.
	if reader.Remaining() > 0 {
		response.TimeSelected = reader.ReadBool()
	}
	if reader.Remaining() > 0 {
		response.TimeSelectedByTable = readSubscriptionTimeSelection(reader)
	}
	if reader.Err() != nil {
		return PollResponse{}, reader.Err()
	}
	if reader.Remaining() != 0 {
		return PollResponse{}, fmt.Errorf("subscription: poll response has %d trailing bytes", reader.Remaining())
	}
	return response, nil
}

func isSubscriptionFileResponse(kind int16) bool {
	return kind == ResponseFileInit || kind == ResponseFilePiece || kind == ResponseFileSeal
}

func readSubscriptionTimeSelection(reader *subscriptionWireReader) map[string]map[string]bool {
	count := reader.ReadCount(8, false)
	selection := make(map[string]map[string]bool)
	for databaseIndex := int32(0); databaseIndex < count && reader.Err() == nil; databaseIndex++ {
		database, hasDatabase := readSubscriptionProjectionKey(reader)
		tableCount := reader.ReadCount(5, false)
		tables := make(map[string]bool)
		for tableIndex := int32(0); tableIndex < tableCount && reader.Err() == nil; tableIndex++ {
			table, hasTable := readSubscriptionProjectionKey(reader)
			selected := reader.ReadBool()
			if hasTable {
				tables[strings.ToLower(strings.TrimSpace(table))] = selected
			}
		}
		if hasDatabase && len(tables) != 0 {
			selection[strings.ToLower(strings.TrimSpace(database))] = tables
		}
	}
	return selection
}

// Projection maps discard null keys but preserve empty keys, unlike ordinary
// Go string protocol fields where null and empty both map to the zero value.
func readSubscriptionProjectionKey(reader *subscriptionWireReader) (string, bool) {
	length := reader.ReadInt32()
	if reader.Err() != nil || length == -1 {
		return "", false
	}
	if length < -1 {
		reader.SetError(fmt.Errorf("subscription: invalid projection key length %d", length))
		return "", false
	}
	key := reader.Take(int(length))
	return string(key), reader.Err() == nil
}

// AppendTablets merges a continuation page only after validating its delivery
// identity. Global time projection is ANDed; table overrides use the latest page.
func (response *PollResponse) AppendTablets(next PollResponse) error {
	if response.Type != ResponseTablets || response.NextOffset <= 0 {
		return fmt.Errorf("subscription: record response does not need a continuation")
	}
	if next.Type != ResponseTablets {
		return fmt.Errorf("subscription: expected tablet continuation, received type %d", next.Type)
	}
	if !sameSubscriptionCommitContext(response.CommitContext, next.CommitContext) {
		return fmt.Errorf("subscription: tablet continuation has a different commit context")
	}
	response.Records = append(response.Records, next.Records...)
	response.NextOffset = next.NextOffset
	response.TimeSelected = response.TimeSelected && next.TimeSelected
	if response.TimeSelectedByTable == nil {
		response.TimeSelectedByTable = make(map[string]map[string]bool)
	}
	for database, tables := range next.TimeSelectedByTable {
		if response.TimeSelectedByTable[database] == nil {
			response.TimeSelectedByTable[database] = make(map[string]bool)
		}
		for table, selected := range tables {
			response.TimeSelectedByTable[database][table] = selected
		}
	}
	return nil
}

// ToMessage creates a complete record view or a processor watermark. A complete
// zero-tablet delivery returns nil; its context remains available for the future
// consumer's empty-delivery ACK. No protocol state changes occur here.
func (response *PollResponse) ToMessage(dialect subscription.Dialect) (payload.SubscriptionMessage, error) {
	if dialect != subscription.TreeDialect && dialect != subscription.TableDialect {
		return nil, fmt.Errorf("subscription: unsupported dialect %q", dialect)
	}
	switch response.Type {
	case ResponseTablets:
		if response.NextOffset > 0 {
			return nil, fmt.Errorf("subscription: record message requires tablet continuation at offset %d", response.NextOffset)
		}
		if int64(len(response.Records)) != -int64(response.NextOffset) {
			return nil, fmt.Errorf("subscription: tablet count %d does not match final offset %d", len(response.Records), response.NextOffset)
		}
		if len(response.Records) == 0 {
			return nil, nil
		}
		batches := make([]payload.RecordBatch, 0, len(response.Records))
		for _, batch := range response.Records {
			selected := response.TimeSelected
			if tables := response.TimeSelectedByTable[strings.ToLower(strings.TrimSpace(batch.Database))]; tables != nil {
				if value, ok := tables[strings.ToLower(strings.TrimSpace(batch.Tablet.GetTargetName()))]; ok {
					selected = value
				}
			}
			batches = append(batches, record.NewRecordBatch(dialect, batch.Database, batch.Tablet, selected))
		}
		return record.NewRecordMessage(response.CommitContext, batches), nil
	case ResponseWatermark:
		return record.NewWatermarkMessage(response.CommitContext, response.Watermark), nil
	case ResponseError:
		return nil, &PollError{Message: response.ErrorMessage, Critical: response.Critical}
	case ResponseTermination:
		return nil, nil
	default:
		return nil, fmt.Errorf("subscription: poll type %d cannot produce a record message", response.Type)
	}
}

func sameSubscriptionCommitContext(left, right payload.CommitContext) bool {
	leftWriter, rightWriter := left.Writer, right.Writer
	left.Writer, right.Writer = nil, nil
	if left != right {
		return false
	}
	if leftWriter == nil || rightWriter == nil {
		return leftWriter == nil && rightWriter == nil
	}
	return *leftWriter == *rightWriter
}

// PollError preserves the server payload's critical flag separately
// from transport status errors so the consumer can apply the right retry policy.
type PollError struct {
	Message  string
	Critical bool
}

func (failure *PollError) Error() string {
	return "subscription: poll payload: " + failure.Message
}
