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

package client

type BitMap struct {
	size int
	bits []byte
}

var BitUtil = []byte{1, 2, 4, 8, 16, 32, 64, 128}

func NewBitMap(size int) *BitMap {
	bitMap := &BitMap{
		size: size,
		bits: make([]byte, (size+7)/8),
	}
	return bitMap
}

func (b *BitMap) Mark(position int) {
	b.bits[position/8] |= BitUtil[position%8]
}

func (b *BitMap) IsMarked(position int) bool {
	return (b.bits[position/8] & BitUtil[position%8]) != 0
}

func (b *BitMap) IsAllUnmarked() bool {
	for i := 0; i < b.size/8; i++ {
		if b.bits[i] != 0 {
			return false
		}
	}
	for i := 0; i < b.size%8; i++ {
		if (b.bits[b.size/8] & BitUtil[i]) != 0 {
			return false
		}
	}
	return true
}

func (b *BitMap) GetBits() []byte {
	return b.bits
}
