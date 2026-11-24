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
	"sort"

	internalwire "github.com/apache/iotdb-client-go/v2/subscription/internal/wire"
	"github.com/apache/iotdb-client-go/v2/subscription/payload"
)

const (
	subscriptionCommitContextVersion int8 = 2
	maxSubscriptionWireLength             = 1<<31 - 1
)

// Business codecs retain subscription-specific types in this package while
// sharing primitive encoding and decoding with the internal wire leaf package.
type subscriptionWireWriter = internalwire.Writer
type subscriptionWireReader = internalwire.Reader

// WriteCommitContext encodes delivery identity and optional writer progress.
func WriteCommitContext(w *internalwire.Writer, context payload.CommitContext) {
	w.WriteInt8(subscriptionCommitContextVersion)
	w.WriteInt32(context.DataNodeID)
	w.WriteInt32(context.RebootTimes)
	w.WriteString(context.Topic)
	w.WriteString(context.GroupID)
	w.WriteInt64(context.CommitID)
	w.WriteInt64(context.SeekGeneration)
	w.WriteString(context.RegionID)
	w.WriteInt64(context.PhysicalTime)
	w.WriteBool(context.Writer != nil)
	if context.Writer != nil {
		writeSubscriptionWriterPosition(w, *context.Writer)
	}
}

func readSubscriptionCommitContext(r *subscriptionWireReader) payload.CommitContext {
	version := r.ReadInt8()
	if r.Err() != nil {
		return payload.CommitContext{}
	}
	if version != subscriptionCommitContextVersion {
		r.SetError(fmt.Errorf("subscription: unsupported commit-context version %d", version))
		return payload.CommitContext{}
	}
	context := payload.CommitContext{
		DataNodeID:     r.ReadInt32(),
		RebootTimes:    r.ReadInt32(),
		Topic:          r.ReadString(),
		GroupID:        r.ReadString(),
		CommitID:       r.ReadInt64(),
		SeekGeneration: r.ReadInt64(),
		RegionID:       r.ReadString(),
		PhysicalTime:   r.ReadInt64(),
	}
	// Java reads this presence marker as a byte, not ReadWriteIOUtils.readBool.
	if r.ReadInt8() != 0 {
		position := readSubscriptionWriterPosition(r)
		context.Writer = &position
	}
	if r.Err() != nil {
		return payload.CommitContext{}
	}
	return context
}

func writeSubscriptionTopicProgress(w *subscriptionWireWriter, progress payload.TopicProgress) {
	if w.Err() != nil {
		return
	}
	if len(progress) > maxSubscriptionWireLength {
		w.SetError(fmt.Errorf("subscription: region count %d exceeds int32", len(progress)))
		return
	}
	regions := make([]string, 0, len(progress))
	for region := range progress {
		regions = append(regions, region)
	}
	sort.Strings(regions)
	w.WriteInt32(int32(len(regions)))
	for _, region := range regions {
		w.WriteString(region)
		if w.Err() != nil {
			return
		}
		positions := progress[region]
		if len(positions) > maxSubscriptionWireLength {
			w.SetError(fmt.Errorf("subscription: writer count %d exceeds int32", len(positions)))
			return
		}
		writers := make([]payload.WriterID, 0, len(positions))
		for writer := range positions {
			writers = append(writers, writer)
		}
		sort.Slice(writers, func(i, j int) bool {
			if writers[i].RegionID != writers[j].RegionID {
				return writers[i].RegionID < writers[j].RegionID
			}
			return writers[i].NodeID < writers[j].NodeID
		})
		w.WriteInt32(int32(len(writers)))
		for _, writer := range writers {
			writeSubscriptionWriterPosition(w, payload.WriterPosition{ID: writer, Progress: positions[writer]})
		}
	}
}

func readSubscriptionTopicProgress(r *subscriptionWireReader) payload.TopicProgress {
	regionCount := r.ReadCount(8, false)
	if r.Err() != nil {
		return nil
	}
	progress := make(payload.TopicProgress)
	for regionIndex := int32(0); regionIndex < regionCount && r.Err() == nil; regionIndex++ {
		region := r.ReadString()
		writerCount := r.ReadCount(24, false)
		positions := make(payload.RegionProgress)
		for writerIndex := int32(0); writerIndex < writerCount && r.Err() == nil; writerIndex++ {
			position := readSubscriptionWriterPosition(r)
			if r.Err() == nil {
				positions[position.ID] = position.Progress
			}
		}
		if r.Err() == nil {
			progress[region] = positions
		}
	}
	if r.Err() != nil {
		return nil
	}
	return progress
}

func writeSubscriptionWriterPosition(w *subscriptionWireWriter, position payload.WriterPosition) {
	w.WriteString(position.ID.RegionID)
	w.WriteInt32(position.ID.NodeID)
	w.WriteInt64(position.Progress.PhysicalTime)
	w.WriteInt64(position.Progress.LocalSeq)
}

func readSubscriptionWriterPosition(r *subscriptionWireReader) payload.WriterPosition {
	return payload.WriterPosition{
		ID:       payload.WriterID{RegionID: r.ReadString(), NodeID: r.ReadInt32()},
		Progress: payload.WriterProgress{PhysicalTime: r.ReadInt64(), LocalSeq: r.ReadInt64()},
	}
}
