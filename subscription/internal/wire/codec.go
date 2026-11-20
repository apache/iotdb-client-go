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

// Package wire implements Java-compatible big-endian IoTDB wire primitives. It
// owns payload bounds and sticky errors without depending on message types or
// RPC transport.
package wire

import (
	"encoding/binary"
	"fmt"
	"io"
	"sort"
)

const maxLength = 1<<31 - 1

// Writer matches the big-endian primitives used by Java
// ReadWriteIOUtils. Its zero value is ready to use, and the first error is sticky.
type Writer struct {
	data []byte
	err error
}

// WriteInt8 appends the signed byte representation without conversion checks.
func (w *Writer) WriteInt8(value int8) {
	if w.err == nil {
		w.data = append(w.data, byte(value))
	}
}

// WriteInt16 appends a big-endian signed 16-bit integer.
func (w *Writer) WriteInt16(value int16) {
	if w.err == nil {
		w.data = binary.BigEndian.AppendUint16(w.data, uint16(value))
	}
}

// WriteInt32 appends a big-endian signed 32-bit integer.
func (w *Writer) WriteInt32(value int32) {
	if w.err == nil {
		w.data = binary.BigEndian.AppendUint32(w.data, uint32(value))
	}
}

// WriteInt64 appends a big-endian signed 64-bit integer.
func (w *Writer) WriteInt64(value int64) {
	if w.err == nil {
		w.data = binary.BigEndian.AppendUint64(w.data, uint64(value))
	}
}

// WriteBool writes one for true and zero for false.
func (w *Writer) WriteBool(value bool) {
	if value {
		w.WriteInt8(1)
	} else {
		w.WriteInt8(0)
	}
}

// WriteString always emits a non-null string; an empty string has length zero.
func (w *Writer) WriteString(value string) {
	if w.err != nil {
		return
	}
	if len(value) > maxLength {
		w.err = fmt.Errorf("wire: string length %d exceeds int32", len(value))
		return
	}
	w.WriteInt32(int32(len(value)))
	w.data = append(w.data, value...)
}

// WriteStringMap sorts keys for stable bytes and encodes a nil map as -1.
func (w *Writer) WriteStringMap(values map[string]string) {
	if w.err != nil {
		return
	}
	if values == nil {
		w.WriteInt32(-1)
		return
	}
	if len(values) > maxLength {
		w.err = fmt.Errorf("wire: map size %d exceeds int32", len(values))
		return
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	w.WriteInt32(int32(len(keys)))
	for _, key := range keys {
		w.WriteString(key)
		w.WriteString(values[key])
	}
}

// WriteStrings encodes a string list, not the tagged writeObjectSet format.
// List order is preserved and the caller's slice is never reordered.
func (w *Writer) WriteStrings(values []string) {
	if w.err != nil {
		return
	}
	if values == nil {
		w.WriteInt32(-1)
		return
	}
	if len(values) > maxLength {
		w.err = fmt.Errorf("wire: string list size %d exceeds int32", len(values))
		return
	}
	w.WriteInt32(int32(len(values)))
	for _, value := range values {
		w.WriteString(value)
	}
}

// Bytes returns the accumulated payload without copying, or the first error.
func (w *Writer) Bytes() ([]byte, error) {
	if w.err != nil {
		return nil, w.err
	}
	return w.data, nil
}

// Reader borrows the payload and records the first decoding
// error. Failed reads do not advance the offset or allocate from invalid lengths.
type Reader struct {
	data []byte
	offset int
	err error
}

// NewReader borrows data; it does not copy or modify the payload.
func NewReader(data []byte) *Reader {
	return &Reader{data: data}
}

// Remaining returns the unread payload length, including after a sticky error.
func (r *Reader) Remaining() int {
	if r.offset < 0 || r.offset > len(r.data) {
		return 0
	}
	return len(r.data) - r.offset
}

// RemainingBytes returns the unread payload without copying. The caller must
// not modify the returned slice.
func (r *Reader) RemainingBytes() []byte {
	if r.err != nil || r.offset < 0 || r.offset > len(r.data) {
		return nil
	}
	return r.data[r.offset:]
}

// Advance consumes count bytes after an external decoder has processed them.
func (r *Reader) Advance(count int) {
	if r.err != nil {
		return
	}
	if count < 0 || r.offset < 0 || r.offset > len(r.data) || count > len(r.data)-r.offset {
		r.err = fmt.Errorf("wire: invalid payload advance of %d bytes at offset %d", count, r.offset)
		return
	}
	r.offset += count
}

// Take borrows count bytes and advances only when the full read succeeds.
func (r *Reader) Take(count int) []byte {
	if r.err != nil {
		return nil
	}
	if count < 0 || r.offset < 0 || r.offset > len(r.data) {
		r.err = fmt.Errorf("wire: invalid payload read of %d bytes at offset %d", count, r.offset)
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

// ReadInt8 reads a signed byte, returning zero after an error.
func (r *Reader) ReadInt8() int8 {
	value := r.Take(1)
	if r.err != nil {
		return 0
	}
	return int8(value[0])
}

// ReadInt16 reads a big-endian signed 16-bit integer.
func (r *Reader) ReadInt16() int16 {
	value := r.Take(2)
	if r.err != nil {
		return 0
	}
	return int16(binary.BigEndian.Uint16(value))
}

// ReadInt32 reads a big-endian signed 32-bit integer.
func (r *Reader) ReadInt32() int32 {
	value := r.Take(4)
	if r.err != nil {
		return 0
	}
	return int32(binary.BigEndian.Uint32(value))
}

// ReadInt64 reads a big-endian signed 64-bit integer.
func (r *Reader) ReadInt64() int64 {
	value := r.Take(8)
	if r.err != nil {
		return 0
	}
	return int64(binary.BigEndian.Uint64(value))
}

// ReadBool matches Java: only the byte value one denotes true.
func (r *Reader) ReadBool() bool {
	return r.ReadInt8() == 1
}

// ReadString maps Java's null marker to the Go string zero value. Other negative
// lengths are invalid; WriteString distinguishes only empty and nonempty values.
func (r *Reader) ReadString() string {
	length := r.ReadInt32()
	if r.err != nil || length == -1 {
		return ""
	}
	if length < -1 {
		r.err = fmt.Errorf("wire: invalid string length %d", length)
		return ""
	}
	return string(r.Take(int(length)))
}

// ReadCount bounds iterations by the minimum encoded size of each entry without
// allocating a collection from an unchecked wire count. minimumEntryBytes must
// be positive; allowNull accepts -1 as the null marker.
func (r *Reader) ReadCount(minimumEntryBytes int, allowNull bool) int32 {
	count := r.ReadInt32()
	if r.err != nil || (allowNull && count == -1) {
		return count
	}
	if count < 0 {
		r.err = fmt.Errorf("wire: invalid collection size %d", count)
		return 0
	}
	if int64(count) > int64(r.Remaining()/minimumEntryBytes) {
		r.err = fmt.Errorf("wire: collection size %d exceeds remaining payload: %w", count, io.ErrUnexpectedEOF)
		return 0
	}
	return count
}

// ReadStringMap reads nullable string pairs; duplicate keys retain the last value.
func (r *Reader) ReadStringMap() map[string]string {
	count := r.ReadCount(8, true)
	if r.err != nil || count == -1 {
		return nil
	}
	values := make(map[string]string)
	for index := int32(0); index < count && r.err == nil; index++ {
		key := r.ReadString()
		value := r.ReadString()
		if r.err == nil {
			values[key] = value
		}
	}
	if r.err != nil {
		return nil
	}
	return values
}

// ReadStrings reads a nullable list in encoded order.
func (r *Reader) ReadStrings() []string {
	count := r.ReadCount(4, true)
	if r.err != nil || count == -1 {
		return nil
	}
	values := make([]string, 0)
	for index := int32(0); index < count && r.err == nil; index++ {
		value := r.ReadString()
		if r.err == nil {
			values = append(values, value)
		}
	}
	if r.err != nil {
		return nil
	}
	return values
}

// Err returns the first encoding error, or nil when writing can continue.
func (w *Writer) Err() error {
	return w.err
}

// SetError records a business-codec failure without replacing an earlier error.
// Subsequent writes leave the payload unchanged.
func (w *Writer) SetError(err error) {
	if w.err == nil {
		w.err = err
	}
}

// Err returns the first decoding error, or nil when reading can continue.
func (r *Reader) Err() error {
	return r.err
}

// SetError records a business-codec failure without replacing an earlier error.
// Subsequent reads leave the offset unchanged.
func (r *Reader) SetError(err error) {
	if r.err == nil {
		r.err = err
	}
}
