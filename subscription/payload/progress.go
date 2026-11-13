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

package payload

// TopicProgress maps a region ID to that region's writer positions.
type TopicProgress map[string]RegionProgress

// RegionProgress preserves independent progress for each writer.
type RegionProgress map[WriterID]WriterProgress

// WriterID is comparable and can be used as a map key.
type WriterID struct {
	// RegionID identifies the region in which this writer produces records.
	RegionID string
	// NodeID identifies the writer node within RegionID. Both fields form the
	// identity; NodeID alone is not sufficient.
	NodeID int32
}

// WriterProgress contains protocol progress, not an application event timestamp.
type WriterProgress struct {
	// PhysicalTime is the physical-time component of protocol progress. It is
	// not the record's event timestamp and has no client-imposed time unit.
	PhysicalTime int64
	// LocalSeq orders writes for the writer; it is not a global stream offset.
	LocalSeq int64
}

// WriterPosition keeps optional writer identity and progress together.
type WriterPosition struct {
	// ID is the writer whose position is described.
	ID WriterID
	// Progress is the position associated with ID.
	Progress WriterProgress
}

// InvalidCommitID marks a context without a legacy commit ID. Writer progress,
// when present, can still identify a committable message.
const InvalidCommitID int64 = -1

// CommitContext preserves routing, restart, seek-generation, and writer metadata.
// A non-nil Writer selects writer progress; nil preserves legacy commit metadata.
// Values returned by SubscriptionMessage.CommitContext are caller-owned snapshots.
type CommitContext struct {
	// DataNodeID selects the node to which confirmation is routed.
	DataNodeID int32
	// RebootTimes distinguishes different restart instances of that node.
	RebootTimes int32
	// Topic identifies the topic that produced the message.
	Topic string
	// GroupID identifies the consumer group owning the delivery.
	GroupID string
	// CommitID is the legacy delivery identifier, or InvalidCommitID when absent.
	CommitID int64
	// SeekGeneration distinguishes deliveries from different seek generations.
	SeekGeneration int64
	// RegionID preserves the region component of legacy progress metadata.
	RegionID string
	// PhysicalTime preserves the physical-time component of legacy progress.
	PhysicalTime int64
	// Writer carries paired writer identity and progress when present.
	Writer *WriterPosition
}
