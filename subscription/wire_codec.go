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
	"io"
	"sort"
)

const (
	subscriptionCommitContextVersion int8 = 2
	maxSubscriptionWireLength = 1<<31 - 1
)

// subscriptionWireWriter matches the big-endian primitives used by Java
// ReadWriteIOUtils. Its zero value is ready to use, and the first error is sticky.
type subscriptionWireWriter struct {
	data []byte
	err error
}

func (w *subscriptionWireWriter) writeByte(value int8) {
	if w.err == nil {
		w.data = append(w.data, byte(value))
	}
}

func (w *subscriptionWireWriter) writeInt16(value int16) {
	if w.err == nil {
		w.data = binary.BigEndian.AppendUint16(w.data, uint16(value))
	}
}

func (w *subscriptionWireWriter) writeInt32(value int32) {
	if w.err == nil {
		w.data = binary.BigEndian.AppendUint32(w.data, uint32(value))
	}
}

func (w *subscriptionWireWriter) writeInt64(value int64) {
	if w.err == nil {
		w.data = binary.BigEndian.AppendUint64(w.data, uint64(value))
	}
}

func (w *subscriptionWireWriter) writeBool(value bool) {
	if value {
		w.writeByte(1)
	} else {
		w.writeByte(0)
	}
}

// writeString always emits a non-null string; an empty string has length zero.
func (w *subscriptionWireWriter) writeString(value string) {
	if w.err != nil {
		return
	}
	if len(value) > maxSubscriptionWireLength {
		w.err = fmt.Errorf("subscription: string length %d exceeds int32", len(value))
		return
	}
	w.writeInt32(int32(len(value)))
	w.data = append(w.data, value...)
}

func (w *subscriptionWireWriter) writeStringMap(values map[string]string) {
	if w.err != nil {
		return
	}
	if values == nil {
		w.writeInt32(-1)
		return
	}
	if len(values) > maxSubscriptionWireLength {
		w.err = fmt.Errorf("subscription: map size %d exceeds int32", len(values))
		return
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	w.writeInt32(int32(len(keys)))
	for _, key := range keys {
		w.writeString(key)
		w.writeString(values[key])
	}
}

// writeStrings encodes a string list, not the tagged writeObjectSet format.
// List order is preserved and the caller's slice is never reordered.
func (w *subscriptionWireWriter) writeStrings(values []string) {
	if w.err != nil {
		return
	}
	if values == nil {
		w.writeInt32(-1)
		return
	}
	if len(values) > maxSubscriptionWireLength {
		w.err = fmt.Errorf("subscription: string list size %d exceeds int32", len(values))
		return
	}
	w.writeInt32(int32(len(values)))
	for _, value := range values {
		w.writeString(value)
	}
}

func (w *subscriptionWireWriter) bytes() ([]byte, error) {
	if w.err != nil {
		return nil, w.err
	}
	return w.data, nil
}

// subscriptionWireReader borrows the payload and records the first decoding
// error. Failed reads do not advance the offset or allocate from invalid lengths.
type subscriptionWireReader struct {
	data []byte
	offset int
	err error
}

func newSubscriptionWireReader(data []byte) *subscriptionWireReader {
	return &subscriptionWireReader{data: data}
}

func (r *subscriptionWireReader) remaining() int {
	if r.offset < 0 || r.offset > len(r.data) {
		return 0
	}
	return len(r.data) - r.offset
}

func (r *subscriptionWireReader) take(count int) []byte {
	if r.err != nil {
		return nil
	}
	if count < 0 || r.offset < 0 || r.offset > len(r.data) {
		r.err = fmt.Errorf("subscription: invalid payload read of %d bytes at offset %d", count, r.offset)
		return nil
	}
	if count > len(r.data)-r.offset {
		r.err = io.ErrUnexpectedEOF
		return nil
	}
	value := r.data[r.offset : r.offset+count]
	r.offset += count
	return value
}

func (r *subscriptionWireReader) readByte() int8 {
	value := r.take(1)
	if r.err != nil {
		return 0
	}
	return int8(value[0])
}

func (r *subscriptionWireReader) readInt16() int16 {
	value := r.take(2)
	if r.err != nil {
		return 0
	}
	return int16(binary.BigEndian.Uint16(value))
}

func (r *subscriptionWireReader) readInt32() int32 {
	value := r.take(4)
	if r.err != nil {
		return 0
	}
	return int32(binary.BigEndian.Uint32(value))
}

func (r *subscriptionWireReader) readInt64() int64 {
	value := r.take(8)
	if r.err != nil {
		return 0
	}
	return int64(binary.BigEndian.Uint64(value))
}

func (r *subscriptionWireReader) readBool() bool {
	return r.readByte() == 1
}

// readString maps Java's null marker to the Go string zero value. Other negative
// lengths are invalid; writeString distinguishes only empty and nonempty values.
func (r *subscriptionWireReader) readString() string {
	length := r.readInt32()
	if r.err != nil || length == -1 {
		return ""
	}
	if length < -1 {
		r.err = fmt.Errorf("subscription: invalid string length %d", length)
		return ""
	}
	return string(r.take(int(length)))
}

// readCount bounds iterations by the minimum encoded size of each entry without
// allocating a collection from an unchecked wire count.
func (r *subscriptionWireReader) readCount(minimumEntryBytes int, allowNull bool) int32 {
	count := r.readInt32()
	if r.err != nil || (allowNull && count == -1) {
		return count
	}
	if count < 0 {
		r.err = fmt.Errorf("subscription: invalid collection size %d", count)
		return 0
	}
	if int64(count) > int64(r.remaining()/minimumEntryBytes) {
		r.err = fmt.Errorf("subscription: collection size %d exceeds remaining payload: %w", count, io.ErrUnexpectedEOF)
		return 0
	}
	return count
}

func (r *subscriptionWireReader) readStringMap() map[string]string {
	count := r.readCount(8, true)
	if r.err != nil || count == -1 {
		return nil
	}
	values := make(map[string]string)
	for index := int32(0); index < count && r.err == nil; index++ {
		key := r.readString()
		value := r.readString()
		if r.err == nil {
			values[key] = value
		}
	}
	if r.err != nil {
		return nil
	}
	return values
}

func (r *subscriptionWireReader) readStrings() []string {
	count := r.readCount(4, true)
	if r.err != nil || count == -1 {
		return nil
	}
	values := make([]string, 0)
	for index := int32(0); index < count && r.err == nil; index++ {
		value := r.readString()
		if r.err == nil {
			values = append(values, value)
		}
	}
	if r.err != nil {
		return nil
	}
	return values
}

func writeSubscriptionCommitContext(w *subscriptionWireWriter, context CommitContext) {
	w.writeByte(subscriptionCommitContextVersion)
	w.writeInt32(context.DataNodeID)
	w.writeInt32(context.RebootTimes)
	w.writeString(context.Topic)
	w.writeString(context.GroupID)
	w.writeInt64(context.CommitID)
	w.writeInt64(context.SeekGeneration)
	w.writeString(context.RegionID)
	w.writeInt64(context.PhysicalTime)
	w.writeBool(context.Writer != nil)
	if context.Writer != nil {
		writeSubscriptionWriterPosition(w, *context.Writer)
	}
}

func readSubscriptionCommitContext(r *subscriptionWireReader) CommitContext {
	version := r.readByte()
	if r.err != nil {
		return CommitContext{}
	}
	if version != subscriptionCommitContextVersion {
		r.err = fmt.Errorf("subscription: unsupported commit-context version %d", version)
		return CommitContext{}
	}
	context := CommitContext{
		DataNodeID: r.readInt32(),
		RebootTimes: r.readInt32(),
		Topic: r.readString(),
		GroupID: r.readString(),
		CommitID: r.readInt64(),
		SeekGeneration: r.readInt64(),
		RegionID: r.readString(),
		PhysicalTime: r.readInt64(),
	}
	// Java reads this presence marker as a byte, not ReadWriteIOUtils.readBool.
	if r.readByte() != 0 {
		position := readSubscriptionWriterPosition(r)
		context.Writer = &position
	}
	if r.err != nil {
		return CommitContext{}
	}
	return context
}

func writeSubscriptionTopicProgress(w *subscriptionWireWriter, progress TopicProgress) {
	if w.err != nil {
		return
	}
	if len(progress) > maxSubscriptionWireLength {
		w.err = fmt.Errorf("subscription: region count %d exceeds int32", len(progress))
		return
	}
	regions := make([]string, 0, len(progress))
	for region := range progress {
		regions = append(regions, region)
	}
	sort.Strings(regions)
	w.writeInt32(int32(len(regions)))
	for _, region := range regions {
		w.writeString(region)
		if w.err != nil {
			return
		}
		positions := progress[region]
		if len(positions) > maxSubscriptionWireLength {
			w.err = fmt.Errorf("subscription: writer count %d exceeds int32", len(positions))
			return
		}
		writers := make([]WriterID, 0, len(positions))
		for writer := range positions {
			writers = append(writers, writer)
		}
		sort.Slice(writers, func(i, j int) bool {
			if writers[i].RegionID != writers[j].RegionID {
				return writers[i].RegionID < writers[j].RegionID
			}
			return writers[i].NodeID < writers[j].NodeID
		})
		w.writeInt32(int32(len(writers)))
		for _, writer := range writers {
			writeSubscriptionWriterPosition(w, WriterPosition{ID: writer, Progress: positions[writer]})
		}
	}
}

func readSubscriptionTopicProgress(r *subscriptionWireReader) TopicProgress {
	regionCount := r.readCount(8, false)
	if r.err != nil {
		return nil
	}
	progress := make(TopicProgress)
	for regionIndex := int32(0); regionIndex < regionCount && r.err == nil; regionIndex++ {
		region := r.readString()
		writerCount := r.readCount(24, false)
		positions := make(RegionProgress)
		for writerIndex := int32(0); writerIndex < writerCount && r.err == nil; writerIndex++ {
			position := readSubscriptionWriterPosition(r)
			if r.err == nil {
				positions[position.ID] = position.Progress
			}
		}
		if r.err == nil {
			progress[region] = positions
		}
	}
	if r.err != nil {
		return nil
	}
	return progress
}

func writeSubscriptionWriterPosition(w *subscriptionWireWriter, position WriterPosition) {
	w.writeString(position.ID.RegionID)
	w.writeInt32(position.ID.NodeID)
	w.writeInt64(position.Progress.PhysicalTime)
	w.writeInt64(position.Progress.LocalSeq)
}

func readSubscriptionWriterPosition(r *subscriptionWireReader) WriterPosition {
	return WriterPosition{
		ID: WriterID{RegionID: r.readString(), NodeID: r.readInt32()},
		Progress: WriterProgress{PhysicalTime: r.readInt64(), LocalSeq: r.readInt64()},
	}
}
