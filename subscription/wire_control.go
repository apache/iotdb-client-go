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
	"fmt"
	"net"
	"sort"
	"strconv"

	"github.com/apache/iotdb-client-go/v2/rpc"
)

const subscriptionRequestVersion int8 = 1

const (
	subscriptionRequestHandshake int16 = iota
	subscriptionRequestHeartbeat
	subscriptionRequestPoll
	subscriptionRequestCommit
	subscriptionRequestClose
	subscriptionRequestSubscribe
	subscriptionRequestUnsubscribe
	subscriptionRequestSeek
	subscriptionRequestSlice
)

const (
	subscriptionSeekBeginning int16 = iota + 1
	subscriptionSeekEnd
	subscriptionSeekTimestamp
	subscriptionSeekProgress
	subscriptionSeekAfterProgress
)

const (
	subscriptionResponseVersion int8 = 1
	subscriptionResponseAck int16 = 0
	subscriptionStringObjectTag int8 = 6
	subscriptionPollInitial int16 = 0
	subscriptionPollNextTablets int16 = 2
)

type subscriptionHandshake struct {
	DataNodeID int32
	ConsumerID string
	GroupID string
}

type subscriptionHeartbeat struct {
	Topics map[string]map[string]string
	Endpoints map[int32]string
	Unsubscribe []string
}

type subscriptionCommitResult struct {
	Accepted []CommitContext
	Progress map[string]TopicProgress
}

// validateSubscriptionResponse checks the envelope only. Providers interpret the
// status before decoding, including responses that carry partial commit results.
func validateSubscriptionResponse(response *rpc.TPipeSubscribeResp) error {
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

func encodeSubscriptionHandshake(attributes map[string]string) (*rpc.TPipeSubscribeReq, error) {
	if attributes == nil {
		attributes = map[string]string{}
	}
	var writer subscriptionWireWriter
	writer.writeStringMap(attributes)
	return subscriptionControlRequest(subscriptionRequestHandshake, &writer)
}

func decodeSubscriptionHandshake(response *rpc.TPipeSubscribeResp) (subscriptionHandshake, error) {
	var result subscriptionHandshake
	if err := validateSubscriptionResponse(response); err != nil {
		return result, err
	}
	if body := subscriptionControlBody(response); body != nil {
		reader := newSubscriptionWireReader(body)
		result.DataNodeID = reader.readInt32()
		result.ConsumerID = reader.readString()
		result.GroupID = reader.readString()
		if reader.err != nil {
			return subscriptionHandshake{}, fmt.Errorf("subscription: decode handshake response: %w", reader.err)
		}
	}
	return result, nil
}

func encodeSubscriptionHeartbeat(contexts []CommitContext) (*rpc.TPipeSubscribeReq, error) {
	var writer subscriptionWireWriter
	writeSubscriptionControlCount(&writer, len(contexts))
	for _, commitContext := range contexts {
		writeSubscriptionCommitContext(&writer, commitContext)
	}
	return subscriptionControlRequest(subscriptionRequestHeartbeat, &writer)
}

func decodeSubscriptionHeartbeat(response *rpc.TPipeSubscribeResp) (subscriptionHeartbeat, error) {
	result := subscriptionHeartbeat{
		Topics: make(map[string]map[string]string),
		Endpoints: make(map[int32]string),
	}
	if err := validateSubscriptionResponse(response); err != nil {
		return result, err
	}
	if body := subscriptionControlBody(response); body != nil {
		reader := newSubscriptionWireReader(body)
		result.Topics = readSubscriptionTopics(reader)
		count := readSubscriptionControlCount(reader, 12)
		for index := 0; index < count && reader.err == nil; index++ {
			nodeID := reader.readInt32()
			host := reader.readString()
			port := reader.readInt32()
			result.Endpoints[nodeID] = net.JoinHostPort(host, strconv.FormatInt(int64(port), 10))
		}
		result.Unsubscribe = reader.readStrings()
		if reader.err != nil {
			return subscriptionHeartbeat{}, fmt.Errorf("subscription: decode heartbeat response: %w", reader.err)
		}
	}
	return result, nil
}

func encodeSubscriptionSubscribe(topics []string, unsubscribe bool) (*rpc.TPipeSubscribeReq, error) {
	var writer subscriptionWireWriter
	writeSubscriptionTopicSet(&writer, topics)
	requestType := subscriptionRequestSubscribe
	if unsubscribe {
		requestType = subscriptionRequestUnsubscribe
	}
	return subscriptionControlRequest(requestType, &writer)
}

func decodeSubscriptionTopics(response *rpc.TPipeSubscribeResp) (map[string]map[string]string, error) {
	if err := validateSubscriptionResponse(response); err != nil {
		return nil, err
	}
	if body := subscriptionControlBody(response); body != nil {
		reader := newSubscriptionWireReader(body)
		topics := readSubscriptionTopics(reader)
		if reader.err != nil {
			return nil, fmt.Errorf("subscription: decode topic response: %w", reader.err)
		}
		return topics, nil
	}
	return make(map[string]map[string]string), nil
}

func encodeSubscriptionClose() *rpc.TPipeSubscribeReq {
	return &rpc.TPipeSubscribeReq{
		Version: subscriptionRequestVersion,
		Type: subscriptionRequestClose,
	}
}

func encodeSubscriptionCommit(contexts []CommitContext, nack bool) (*rpc.TPipeSubscribeReq, error) {
	var writer subscriptionWireWriter
	writeSubscriptionControlCount(&writer, len(contexts))
	for _, commitContext := range contexts {
		writeSubscriptionCommitContext(&writer, commitContext)
	}
	writer.writeBool(nack)
	return subscriptionControlRequest(subscriptionRequestCommit, &writer)
}

func decodeSubscriptionCommit(response *rpc.TPipeSubscribeResp) (subscriptionCommitResult, error) {
	result := subscriptionCommitResult{Progress: make(map[string]TopicProgress)}
	if err := validateSubscriptionResponse(response); err != nil {
		return result, err
	}
	if body := subscriptionControlBody(response); body != nil {
		reader := newSubscriptionWireReader(body)
		count := readSubscriptionControlCount(reader, 1)
		for index := 0; index < count && reader.err == nil; index++ {
			result.Accepted = append(result.Accepted, readSubscriptionCommitContext(reader))
		}
		count = readSubscriptionControlCount(reader, 8)
		for index := 0; index < count && reader.err == nil; index++ {
			topic := reader.readString()
			result.Progress[topic] = readSubscriptionTopicProgress(reader)
		}
		if reader.err != nil {
			return subscriptionCommitResult{}, fmt.Errorf("subscription: decode commit response: %w", reader.err)
		}
	}
	// A response without a body has no explicit acceptance or progress. Do not
	// infer that every requested context was accepted from the status alone.
	return result, nil
}

func encodeSubscriptionSeek(topic string, kind int16, timestamp int64, progress TopicProgress) (*rpc.TPipeSubscribeReq, error) {
	var writer subscriptionWireWriter
	writer.writeString(topic)
	writer.writeInt16(kind)
	switch kind {
	case subscriptionSeekBeginning, subscriptionSeekEnd:
	case subscriptionSeekTimestamp:
		writer.writeInt64(timestamp)
	case subscriptionSeekProgress, subscriptionSeekAfterProgress:
		writeSubscriptionTopicProgress(&writer, progress)
	default:
		return nil, fmt.Errorf("subscription: unsupported seek type %d", kind)
	}
	return subscriptionControlRequest(subscriptionRequestSeek, &writer)
}

func encodeSubscriptionPoll(topics []string, timeoutMs, maxBytes int64, progress map[string]TopicProgress) (*rpc.TPipeSubscribeReq, error) {
	var writer subscriptionWireWriter
	writer.writeInt16(subscriptionPollInitial)
	writeSubscriptionTopicSet(&writer, topics)
	writeSubscriptionPollLimits(&writer, timeoutMs, maxBytes, progress)
	return subscriptionControlRequest(subscriptionRequestPoll, &writer)
}

func encodeSubscriptionPollTablets(commitContext CommitContext, offset int32, timeoutMs, maxBytes int64) (*rpc.TPipeSubscribeReq, error) {
	var writer subscriptionWireWriter
	writer.writeInt16(subscriptionPollNextTablets)
	writeSubscriptionCommitContext(&writer, commitContext)
	writer.writeInt32(offset)
	writeSubscriptionPollLimits(&writer, timeoutMs, maxBytes, nil)
	return subscriptionControlRequest(subscriptionRequestPoll, &writer)
}

func encodeSubscriptionSlice(request *rpc.TPipeSubscribeReq, orderID int32, index, count int32, piece []byte) (*rpc.TPipeSubscribeReq, error) {
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
	writer.writeInt32(orderID)
	writer.writeByte(request.Version)
	writer.writeInt16(request.Type)
	writer.writeInt32(int32(len(request.Body)))
	writer.writeInt32(index)
	writer.writeInt32(count)
	writer.writeInt32(int32(len(piece)))
	body, err := writer.bytes()
	if err != nil {
		return nil, err
	}
	return &rpc.TPipeSubscribeReq{
		Version: subscriptionRequestVersion,
		Type: subscriptionRequestSlice,
		Body: append(body, piece...),
	}, nil
}

func subscriptionControlRequest(requestType int16, writer *subscriptionWireWriter) (*rpc.TPipeSubscribeReq, error) {
	body, err := writer.bytes()
	if err != nil {
		return nil, err
	}
	return &rpc.TPipeSubscribeReq{
		Version: subscriptionRequestVersion,
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
	for index := 0; index < count && reader.err == nil; index++ {
		topic := reader.readString()
		topics[topic] = reader.readStringMap()
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
		writer.writeByte(subscriptionStringObjectTag)
		writer.writeString(topic)
	}
}

func writeSubscriptionPollLimits(writer *subscriptionWireWriter, timeoutMs, maxBytes int64, progress map[string]TopicProgress) {
	writer.writeInt64(timeoutMs)
	writer.writeInt64(maxBytes)
	topics := make([]string, 0, len(progress))
	for topic := range progress {
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	writeSubscriptionControlCount(writer, len(topics))
	for _, topic := range topics {
		writer.writeString(topic)
		writeSubscriptionTopicProgress(writer, progress[topic])
	}
}

func writeSubscriptionControlCount(writer *subscriptionWireWriter, count int) {
	if int64(count) > 1<<31-1 {
		if writer.err == nil {
			writer.err = fmt.Errorf("subscription: collection is too large for the wire format")
		}
		return
	}
	writer.writeInt32(int32(count))
}

func readSubscriptionControlCount(reader *subscriptionWireReader, minimumBytes int) int {
	count := reader.readInt32()
	if reader.err != nil {
		return 0
	}
	if count < 0 || int64(count)*int64(minimumBytes) > int64(reader.remaining()) {
		reader.err = fmt.Errorf("subscription: invalid collection count %d", count)
		return 0
	}
	return int(count)
}
