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

package processor

import (
	"github.com/apache/iotdb-client-go/subscription/payload"
)

// SubscriptionMessageProcessor transforms, filters, or enriches messages in the
// pull consumer pipeline. Processors run in their configured order on each poll,
// and the consumer invokes each processor serially. A processor can retain
// messages and return them during a later Process call or through Flush.
//
// A processor that retains messages must also implement BufferingProcessor so
// heartbeat renewal can protect undelivered records. A processor must not release
// messages it returns or retains.
type SubscriptionMessageProcessor interface {
	// Process receives messages from the previous pipeline stage, or the raw
	// poll when it is the first stage. It returns messages for the next stage
	// or application and may return fewer, more, or different messages than
	// it received. A non-nil error reports a processing failure.
	Process(messages []payload.SubscriptionMessage) ([]payload.SubscriptionMessage, error)
	// Flush returns all remaining buffered messages when the consumer drains
	// or closes. An empty result means no buffered output remains. A non-nil
	// error reports a failure to flush.
	//
	// Returned messages transfer to the consumer and continue through later
	// pipeline stages. The processor must not retain or subsequently modify
	// the returned messages.
	Flush() ([]payload.SubscriptionMessage, error)
	// Reset clears all state that must not survive a seek, including buffered
	// records, ordering progress, and cached alignment values. It applies to
	// all topics; TopicResetter provides a separate per-topic capability.
	Reset()
}

// BufferingProcessor is the optional capability of a processor that retains
// messages between calls. It reports count and renewable commit contexts together
// so retained, uncommitted records can be included in heartbeat renewal.
type BufferingProcessor interface {
	SubscriptionMessageProcessor
	// Buffered returns a snapshot of the messages currently held by this
	// processor. The snapshot must describe the same buffered state for both
	// its count and commit contexts.
	Buffered() ProcessorBuffer
}

// ProcessorBuffer is a snapshot of records retained by one processor. Contexts
// contains every committable buffered message's context, not just one per topic.
type ProcessorBuffer struct {
	// Count is the total number of messages currently retained by the processor.
	Count int
	// Contexts contains the commit context of every committable buffered
	// message so the consumer can renew those records before delivery.
	Contexts []payload.CommitContext
}

// TopicResetter is an optional processor capability. ResetTopic clears only the
// named topic, preserving every other topic. Its presence replaces Java's
// supportsTopicScopedReset flag.
type TopicResetter interface {
	// ResetTopic clears buffered records and processing state belonging to
	// topic after a topic-scoped seek, without touching any other topic.
	ResetTopic(topic string)
}

// WatermarkReporter is an optional capability for processors that calculate an
// ordering watermark. The boolean is false before a watermark is available.
type WatermarkReporter interface {
	// Watermark returns the processor's current ordering watermark. It is a
	// client-side processing result, distinct from the latest server watermark
	// observed by SubscriptionConsumer.LatestWatermark. The boolean is false
	// until this processor has a watermark to report.
	Watermark() (timestamp int64, ok bool)
}
