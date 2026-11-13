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
	"encoding/binary"
	"fmt"
	"strings"
)

const (
	subscriptionResponseError int16 = 0
	subscriptionResponseTablets int16 = 1
	subscriptionResponseFileInit int16 = 2
	subscriptionResponseFilePiece int16 = 3
	subscriptionResponseFileSeal int16 = 4
	subscriptionResponseTermination int16 = 5
	subscriptionResponseWatermark int16 = 7
)

type subscriptionPollBatch struct {
	Database string
	Tablet decodedSubscriptionTablet
}

// subscriptionPollResponse retains transport events separately from deliverable
// messages. In particular, termination and empty record deliveries still need
// consumer-level unsubscribe or confirmation; decoding does neither.
type subscriptionPollResponse struct {
	Type int16
	CommitContext CommitContext
	Records []subscriptionPollBatch
	NextOffset int32
	TimeSelected bool
	TimeSelectedByTable map[string]map[string]bool
	Watermark int64
	DataNodeID int32
	ErrorMessage string
	Critical bool
}

// decodeSubscriptionPollResponses parses every nonempty body entry. No partially
// decoded response is returned if any entry fails, including an unsupported file
// entry that follows otherwise valid records.
func decodeSubscriptionPollResponses(bodies [][]byte) ([]subscriptionPollResponse, error) {
	// Reject files before decoding records in sibling entries. This examines the
	// response tag only; it does not parse file payloads or attempt file I/O.
	for _, body := range bodies {
		if len(body) < 2 {
			continue
		}
		kind := int16(binary.BigEndian.Uint16(body[:2]))
		if isSubscriptionFileResponse(kind) {
			return nil, fmt.Errorf("subscription: decode poll response type %d: %w", kind, ErrUnsupportedTsFile)
		}
	}
	var responses []subscriptionPollResponse
	for index, body := range bodies {
		if len(body) == 0 {
			continue
		}
		response, err := decodeSubscriptionPollResponse(body)
		if err != nil {
			return nil, fmt.Errorf("subscription: decode poll body %d: %w", index, err)
		}
		responses = append(responses, response)
	}
	return responses, nil
}

func decodeSubscriptionPollResponse(body []byte) (subscriptionPollResponse, error) {
	reader := newSubscriptionWireReader(body)
	response := subscriptionPollResponse{Type: reader.readInt16(), TimeSelected: true}
	if reader.err != nil {
		return subscriptionPollResponse{}, reader.err
	}
	switch response.Type {
	case subscriptionResponseTablets:
		databaseCount := reader.readCount(8, false)
		for databaseIndex := int32(0); databaseIndex < databaseCount && reader.err == nil; databaseIndex++ {
			database := reader.readString()
			tabletCount := reader.readCount(12, false)
			for tabletIndex := int32(0); tabletIndex < tabletCount && reader.err == nil; tabletIndex++ {
				tablet, err := decodeSubscriptionTablet(reader)
				if err != nil {
					return subscriptionPollResponse{}, err
				}
				response.Records = append(response.Records, subscriptionPollBatch{Database: database, Tablet: tablet})
			}
		}
		response.NextOffset = reader.readInt32()
	case subscriptionResponseError:
		response.ErrorMessage = reader.readString()
		response.Critical = reader.readBool()
	case subscriptionResponseTermination:
		// The termination payload is empty; its topic comes from the context.
	case subscriptionResponseWatermark:
		response.Watermark = reader.readInt64()
		response.DataNodeID = reader.readInt32()
	case subscriptionResponseFileInit, subscriptionResponseFilePiece, subscriptionResponseFileSeal:
		return subscriptionPollResponse{}, fmt.Errorf("subscription: decode poll response type %d: %w", response.Type, ErrUnsupportedTsFile)
	default:
		return subscriptionPollResponse{}, fmt.Errorf("subscription: unsupported poll response type %d", response.Type)
	}
	if reader.err != nil {
		return subscriptionPollResponse{}, reader.err
	}
	response.CommitContext = readSubscriptionCommitContext(reader)
	// Older responses end after the context or after the global projection flag.
	if reader.remaining() > 0 {
		response.TimeSelected = reader.readBool()
	}
	if reader.remaining() > 0 {
		response.TimeSelectedByTable = readSubscriptionTimeSelection(reader)
	}
	if reader.err != nil {
		return subscriptionPollResponse{}, reader.err
	}
	if reader.remaining() != 0 {
		return subscriptionPollResponse{}, fmt.Errorf("subscription: poll response has %d trailing bytes", reader.remaining())
	}
	return response, nil
}

func isSubscriptionFileResponse(kind int16) bool {
	return kind == subscriptionResponseFileInit || kind == subscriptionResponseFilePiece || kind == subscriptionResponseFileSeal
}

func readSubscriptionTimeSelection(reader *subscriptionWireReader) map[string]map[string]bool {
	count := reader.readCount(8, false)
	selection := make(map[string]map[string]bool)
	for databaseIndex := int32(0); databaseIndex < count && reader.err == nil; databaseIndex++ {
		database, hasDatabase := readSubscriptionProjectionKey(reader)
		tableCount := reader.readCount(5, false)
		tables := make(map[string]bool)
		for tableIndex := int32(0); tableIndex < tableCount && reader.err == nil; tableIndex++ {
			table, hasTable := readSubscriptionProjectionKey(reader)
			selected := reader.readBool()
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
	length := reader.readInt32()
	if reader.err != nil || length == -1 {
		return "", false
	}
	if length < -1 {
		reader.err = fmt.Errorf("subscription: invalid projection key length %d", length)
		return "", false
	}
	key := reader.take(int(length))
	return string(key), reader.err == nil
}

// appendTablets merges a continuation page only after validating its delivery
// identity. Global time projection is ANDed; table overrides use the latest page.
func (response *subscriptionPollResponse) appendTablets(next subscriptionPollResponse) error {
	if response.Type != subscriptionResponseTablets || response.NextOffset <= 0 {
		return fmt.Errorf("subscription: record response does not need a continuation")
	}
	if next.Type != subscriptionResponseTablets {
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

// message creates a complete record view or a processor watermark. A complete
// zero-tablet delivery returns nil; its context remains available for the future
// consumer's empty-delivery ACK. No protocol state changes occur here.
func (response *subscriptionPollResponse) message(dialect Dialect) (SubscriptionMessage, error) {
	if dialect != TreeDialect && dialect != TableDialect {
		return nil, fmt.Errorf("subscription: unsupported dialect %q", dialect)
	}
	switch response.Type {
	case subscriptionResponseTablets:
		if response.NextOffset > 0 {
			return nil, fmt.Errorf("subscription: record message requires tablet continuation at offset %d", response.NextOffset)
		}
		if int64(len(response.Records)) != -int64(response.NextOffset) {
			return nil, fmt.Errorf("subscription: tablet count %d does not match final offset %d", len(response.Records), response.NextOffset)
		}
		if len(response.Records) == 0 {
			return nil, nil
		}
		batches := make([]RecordBatch, 0, len(response.Records))
		for _, record := range response.Records {
			selected := response.TimeSelected
			if tables := response.TimeSelectedByTable[strings.ToLower(strings.TrimSpace(record.Database))]; tables != nil {
				if value, ok := tables[strings.ToLower(strings.TrimSpace(record.Tablet.Target))]; ok {
					selected = value
				}
			}
			batches = append(batches, newSubscriptionRecordBatch(dialect, record.Database, record.Tablet, selected))
		}
		return newSubscriptionRecordMessage(response.CommitContext, batches), nil
	case subscriptionResponseWatermark:
		return newSubscriptionWatermarkMessage(response.CommitContext, response.Watermark), nil
	case subscriptionResponseError:
		return nil, &subscriptionPollError{Message: response.ErrorMessage, Critical: response.Critical}
	case subscriptionResponseTermination:
		return nil, nil
	default:
		return nil, fmt.Errorf("subscription: poll type %d cannot produce a record message", response.Type)
	}
}

func sameSubscriptionCommitContext(left, right CommitContext) bool {
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

// subscriptionPollError preserves the server payload's critical flag separately
// from transport status errors so the consumer can apply the right retry policy.
type subscriptionPollError struct {
	Message string
	Critical bool
}

func (failure *subscriptionPollError) Error() string {
	return "subscription: poll payload: " + failure.Message
}

