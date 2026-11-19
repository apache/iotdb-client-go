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
	"fmt"
	"net"
	"sort"
	"strconv"

	"github.com/apache/iotdb-client-go/v2/rpc"
	internalwire "github.com/apache/iotdb-client-go/v2/subscription/internal/wire"
	"github.com/apache/iotdb-client-go/v2/subscription/payload"
)

// RequestVersion is the subscription RPC request envelope version.
const RequestVersion int8 = 1

// Request types retain the Java subscription control protocol numbers.
const (
	RequestHandshake int16 = iota
	RequestHeartbeat
	RequestPoll
	RequestCommit
	RequestClose
	RequestSubscribe
	RequestUnsubscribe
	RequestSeek
	RequestSlice
)

// Seek modes distinguish WAL boundaries and writer-progress positioning.
const (
	SeekBeginning int16 = iota + 1
	SeekEnd
	SeekTimestamp
	SeekProgress
	SeekAfterProgress
)

const (
	subscriptionResponseVersion int8 = 1
	subscriptionResponseAck int16 = 0
	subscriptionStringObjectTag int8 = 6
	subscriptionPollInitial int16 = 0
	subscriptionPollNextTablets int16 = 2
)

// Handshake contains server-assigned connection identity.
type Handshake struct {
	DataNodeID int32
	ConsumerID string
	GroupID string
}

// Heartbeat carries subscription and endpoint coordination data.
type Heartbeat struct {
	Topics map[string]map[string]string
	Endpoints map[int32]string
	Unsubscribe []string
}

// CommitResult separates accepted contexts from confirmed recoverable progress.
type CommitResult struct {
	Accepted []payload.CommitContext
	Progress map[string]payload.TopicProgress
}

// ValidateResponse checks the envelope only. Providers interpret the
// status before decoding, including responses that carry partial commit results.
func ValidateResponse(response *rpc.TPipeSubscribeResp) error {
	if response == nil {
		return fmt.Errorf("subscription: no subscription response")
	}
	if response.Version != subscriptionResponseVersion {
		return fmt.Errorf("subscription: unsupported response version %d", response.Version)
	}
	if response.Type != subscriptionResponseAck {
		return fmt.Errorf("subscription: unsupported response type %d", response.Type)
	}
	return nil
}

// EncodeHandshake serializes connection identity and authentication attributes.
func EncodeHandshake(attributes map[string]string) (*rpc.TPipeSubscribeReq, error) {
	if attributes == nil {
		attributes = map[string]string{}
	}
	var writer subscriptionWireWriter
	writer.WriteStringMap(attributes)
	return subscriptionControlRequest(RequestHandshake, &writer)
}

// DecodeHandshake reads the server-assigned node and consumer identities.
func DecodeHandshake(response *rpc.TPipeSubscribeResp) (Handshake, error) {
	var result Handshake
	if err := ValidateResponse(response); err != nil {
		return result, err
	}
	if body := subscriptionControlBody(response); body != nil {
		reader := internalwire.NewReader(body)
		result.DataNodeID = reader.ReadInt32()
		result.ConsumerID = reader.ReadString()
		result.GroupID = reader.ReadString()
		if reader.Err() != nil {
			return Handshake{}, fmt.Errorf("subscription: decode handshake response: %w", reader.Err())
		}
	}
	return result, nil
}

// EncodeHeartbeat includes all retained delivery contexts for lease renewal.
func EncodeHeartbeat(contexts []payload.CommitContext) (*rpc.TPipeSubscribeReq, error) {
	var writer subscriptionWireWriter
	writeSubscriptionControlCount(&writer, len(contexts))
	for _, commitContext := range contexts {
		WriteCommitContext(&writer, commitContext)
	}
	return subscriptionControlRequest(RequestHeartbeat, &writer)
}

// DecodeHeartbeat reads current topics, endpoints, and unsubscribe hints.
func DecodeHeartbeat(response *rpc.TPipeSubscribeResp) (Heartbeat, error) {
	result := Heartbeat{
		Topics: make(map[string]map[string]string),
		Endpoints: make(map[int32]string),
	}
	if err := ValidateResponse(response); err != nil {
		return result, err
	}
	if body := subscriptionControlBody(response); body != nil {
		reader := internalwire.NewReader(body)
		result.Topics = readSubscriptionTopics(reader)
		count := readSubscriptionControlCount(reader, 12)
		for index := 0; index < count && reader.Err() == nil; index++ {
			nodeID := reader.ReadInt32()
			host := reader.ReadString()
			port := reader.ReadInt32()
			result.Endpoints[nodeID] = net.JoinHostPort(host, strconv.FormatInt(int64(port), 10))
		}
		result.Unsubscribe = reader.ReadStrings()
		if reader.Err() != nil {
			return Heartbeat{}, fmt.Errorf("subscription: decode heartbeat response: %w", reader.Err())
		}
	}
	return result, nil
}

// EncodeSubscribe encodes the tagged topic set for subscribe or unsubscribe.
func EncodeSubscribe(topics []string, unsubscribe bool) (*rpc.TPipeSubscribeReq, error) {
	var writer subscriptionWireWriter
	writeSubscriptionTopicSet(&writer, topics)
	requestType := RequestSubscribe
	if unsubscribe {
		requestType = RequestUnsubscribe
	}
	return subscriptionControlRequest(requestType, &writer)
}

// DecodeTopics reads the current subscription topic properties.
func DecodeTopics(response *rpc.TPipeSubscribeResp) (map[string]map[string]string, error) {
	if err := ValidateResponse(response); err != nil {
		return nil, err
	}
	if body := subscriptionControlBody(response); body != nil {
		reader := internalwire.NewReader(body)
		topics := readSubscriptionTopics(reader)
		if reader.Err() != nil {
			return nil, fmt.Errorf("subscription: decode topic response: %w", reader.Err())
		}
		return topics, nil
	}
	return make(map[string]map[string]string), nil
}

// EncodeClose creates the bodyless close request.
func EncodeClose() *rpc.TPipeSubscribeReq {
	return &rpc.TPipeSubscribeReq{
		Version: RequestVersion,
		Type: RequestClose,
	}
}

// EncodeCommit encodes acknowledgment or negative acknowledgment contexts.
func EncodeCommit(contexts []payload.CommitContext, nack bool) (*rpc.TPipeSubscribeReq, error) {
	var writer subscriptionWireWriter
	writeSubscriptionControlCount(&writer, len(contexts))
	for _, commitContext := range contexts {
		WriteCommitContext(&writer, commitContext)
	}
	writer.WriteBool(nack)
	return subscriptionControlRequest(RequestCommit, &writer)
}

// DecodeCommit preserves explicit acceptance and confirmed writer progress.
func DecodeCommit(response *rpc.TPipeSubscribeResp) (CommitResult, error) {
	result := CommitResult{Progress: make(map[string]payload.TopicProgress)}
	if err := ValidateResponse(response); err != nil {
		return result, err
	}
	if body := subscriptionControlBody(response); body != nil {
		reader := internalwire.NewReader(body)
		count := readSubscriptionControlCount(reader, 1)
		for index := 0; index < count && reader.Err() == nil; index++ {
			result.Accepted = append(result.Accepted, readSubscriptionCommitContext(reader))
		}
		count = readSubscriptionControlCount(reader, 8)
		for index := 0; index < count && reader.Err() == nil; index++ {
			topic := reader.ReadString()
			result.Progress[topic] = readSubscriptionTopicProgress(reader)
		}
		if reader.Err() != nil {
			return CommitResult{}, fmt.Errorf("subscription: decode commit response: %w", reader.Err())
		}
	}
	// A response without a body has no explicit acceptance or progress. Do not
	// infer that every requested context was accepted from the status alone.
	return result, nil
}

// EncodeSeek encodes a seek mode and its applicable timestamp or progress.
func EncodeSeek(topic string, kind int16, timestamp int64, progress payload.TopicProgress) (*rpc.TPipeSubscribeReq, error) {
	var writer subscriptionWireWriter
	writer.WriteString(topic)
	writer.WriteInt16(kind)
	switch kind {
	case SeekBeginning, SeekEnd:
	case SeekTimestamp:
		writer.WriteInt64(timestamp)
	case SeekProgress, SeekAfterProgress:
		writeSubscriptionTopicProgress(&writer, progress)
	default:
		return nil, fmt.Errorf("subscription: unsupported seek type %d", kind)
	}
	return subscriptionControlRequest(RequestSeek, &writer)
}

// EncodePoll encodes an initial poll with bounds and observed writer progress.
func EncodePoll(topics []string, timeoutMs, maxBytes int64, progress map[string]payload.TopicProgress) (*rpc.TPipeSubscribeReq, error) {
	var writer subscriptionWireWriter
	writer.WriteInt16(subscriptionPollInitial)
	writeSubscriptionTopicSet(&writer, topics)
	writeSubscriptionPollLimits(&writer, timeoutMs, maxBytes, progress)
	return subscriptionControlRequest(RequestPoll, &writer)
}

// EncodePollTablets requests another page for the same record delivery.
func EncodePollTablets(commitContext payload.CommitContext, offset int32, timeoutMs, maxBytes int64) (*rpc.TPipeSubscribeReq, error) {
	var writer subscriptionWireWriter
	writer.WriteInt16(subscriptionPollNextTablets)
	WriteCommitContext(&writer, commitContext)
	writer.WriteInt32(offset)
	writeSubscriptionPollLimits(&writer, timeoutMs, maxBytes, nil)
	return subscriptionControlRequest(RequestPoll, &writer)
}

// EncodeSlice wraps one oversized-request fragment with ordering metadata.
func EncodeSlice(request *rpc.TPipeSubscribeReq, orderID int32, index, count int32, piece []byte) (*rpc.TPipeSubscribeReq, error) {
	if request == nil {
		return nil, fmt.Errorf("subscription: cannot slice a nil request")
	}
	if count <= 0 || index < 0 || index >= count {
		return nil, fmt.Errorf("subscription: invalid slice index %d for %d slices", index, count)
	}
	if int64(len(request.Body)) > 1<<31-1 || len(piece) > len(request.Body) {
		return nil, fmt.Errorf("subscription: invalid subscription slice body size")
	}
	var writer subscriptionWireWriter
	writer.WriteInt32(orderID)
	writer.WriteByte(request.Version)
	writer.WriteInt16(request.Type)
	writer.WriteInt32(int32(len(request.Body)))
	writer.WriteInt32(index)
	writer.WriteInt32(count)
	writer.WriteInt32(int32(len(piece)))
	body, err := writer.Bytes()
	if err != nil {
		return nil, err
	}
	return &rpc.TPipeSubscribeReq{
		Version: RequestVersion,
		Type: RequestSlice,
		Body: append(body, piece...),
	}, nil
}

func subscriptionControlRequest(requestType int16, writer *subscriptionWireWriter) (*rpc.TPipeSubscribeReq, error) {
	body, err := writer.Bytes()
	if err != nil {
		return nil, err
	}
	return &rpc.TPipeSubscribeReq{
		Version: RequestVersion,
		Type: requestType,
		Body: body,
	}, nil
}

// Java reads the first nonempty response body and ignores later entries.
func subscriptionControlBody(response *rpc.TPipeSubscribeResp) []byte {
	for _, body := range response.Body {
		if len(body) != 0 {
			return body
		}
	}
	return nil
}

func readSubscriptionTopics(reader *subscriptionWireReader) map[string]map[string]string {
	topics := make(map[string]map[string]string)
	count := readSubscriptionControlCount(reader, 8)
	for index := 0; index < count && reader.Err() == nil; index++ {
		topic := reader.ReadString()
		topics[topic] = reader.ReadStringMap()
	}
	return topics
}

// Subscribe and Poll use writeObjectSet, whose string entries carry an object
// type tag. This is different from the untagged StringList used by heartbeat.
func writeSubscriptionTopicSet(writer *subscriptionWireWriter, topics []string) {
	unique := make(map[string]struct{}, len(topics))
	for _, topic := range topics {
		unique[topic] = struct{}{}
	}
	names := make([]string, 0, len(unique))
	for topic := range unique {
		names = append(names, topic)
	}
	sort.Strings(names)
	writeSubscriptionControlCount(writer, len(names))
	for _, topic := range names {
		writer.WriteByte(subscriptionStringObjectTag)
		writer.WriteString(topic)
	}
}

func writeSubscriptionPollLimits(writer *subscriptionWireWriter, timeoutMs, maxBytes int64, progress map[string]payload.TopicProgress) {
	writer.WriteInt64(timeoutMs)
	writer.WriteInt64(maxBytes)
	topics := make([]string, 0, len(progress))
	for topic := range progress {
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	writeSubscriptionControlCount(writer, len(topics))
	for _, topic := range topics {
		writer.WriteString(topic)
		writeSubscriptionTopicProgress(writer, progress[topic])
	}
}

func writeSubscriptionControlCount(writer *subscriptionWireWriter, count int) {
	if int64(count) > 1<<31-1 {
		if writer.Err() == nil {
			writer.SetError(fmt.Errorf("subscription: collection is too large for the wire format"))
		}
		return
	}
	writer.WriteInt32(int32(count))
}

func readSubscriptionControlCount(reader *subscriptionWireReader, minimumBytes int) int {
	count := reader.ReadInt32()
	if reader.Err() != nil {
		return 0
	}
	if count < 0 || int64(count)*int64(minimumBytes) > int64(reader.Remaining()) {
		reader.SetError(fmt.Errorf("subscription: invalid collection count %d", count))
		return 0
	}
	return int(count)
}
