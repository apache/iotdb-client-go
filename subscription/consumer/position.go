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

package consumer

import (
	"context"

	"github.com/apache/iotdb-client-go/subscription/payload"
)

// Positioner separates fetched positions from confirmed, recoverable positions.
// Progress results are deep snapshots owned by the caller. Seek operations copy
// their inputs and reset state for the named topic only, including pending
// automatic commits and processor state. With multiple subscribed topics, every
// processor must implement TopicResetter; otherwise seek fails before changing
// server positions. With a single topic, SubscriptionMessageProcessor.Reset is a safe fallback.
type Positioner interface {
	// Positions returns the latest observed fetch positions for topic. They are
	// hints for subsequent polls, not durable checkpoints. No observed position
	// returns empty progress; a consumer that is not open or is fenced returns
	// an error. The returned map and its nested maps are independent snapshots.
	Positions(topic string) (payload.TopicProgress, error)
	// CommittedPositions returns the latest confirmed positions for topic.
	// Callers can persist this snapshot as a recovery checkpoint. Modifying it
	// does not modify the consumer's progress. An unopened or fenced consumer
	// returns an error.
	CommittedPositions(topic string) (payload.TopicProgress, error)
	// SeekToBeginning seeks to the earliest WAL position still retained for
	// topic. Reclaimed WAL segments cannot be recovered by this operation.
	SeekToBeginning(ctx context.Context, topic string) error
	// SeekToEnd seeks to the current WAL tail so subsequent consumption starts
	// with newly written data for topic.
	SeekToEnd(ctx context.Context, topic string) error
	// Seek replays topic from the supplied writer progress. It copies progress
	// and clears obsolete local acknowledgment and processor state for topic.
	// A failure across multiple providers does not imply atomic rollback.
	Seek(ctx context.Context, topic string, progress payload.TopicProgress) error
	// SeekAfter resumes topic after the supplied writer progress, with the same
	// snapshot, reset, and multi-provider failure semantics as Seek.
	SeekAfter(ctx context.Context, topic string, progress payload.TopicProgress) error
}

