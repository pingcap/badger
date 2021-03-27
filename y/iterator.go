/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package y

import (
	"encoding/binary"
)

// ValueStruct represents the value info that can be associated with a key, but also the internal
// Meta field.
type ValueStruct struct {
	Meta     byte
	UserMeta []byte
	Value    []byte

	Version uint64 // This field is not serialized. Only for internal usage.
}

// EncodedSize is the size of the ValueStruct when encoded
func (v *ValueStruct) EncodedSize() uint32 {
	return uint32(len(v.Value) + len(v.UserMeta) + 2 + 8) // meta
}

// Decode uses the length of the slice to infer the length of the Value field.
func (v *ValueStruct) Decode(b []byte) {
	v.Version = binary.LittleEndian.Uint64(b)
	b = b[8:]
	v.Meta = b[0]
	v.UserMeta = nil
	userMetaEnd := 2 + b[1]
	if b[1] != 0 {
		v.UserMeta = b[2:userMetaEnd]
	}
	v.Value = b[userMetaEnd:]
}

// Encode expects a slice of length at least v.EncodedSize().
func (v *ValueStruct) Encode(b []byte) {
	binary.LittleEndian.PutUint64(b, v.Version)
	b = b[8:]
	b[0] = v.Meta
	b[1] = byte(len(v.UserMeta))
	copy(b[2:], v.UserMeta)
	copy(b[2+len(v.UserMeta):], v.Value)
}

// Valid checks if the ValueStruct is valid.
func (v *ValueStruct) Valid() bool {
	return v.Meta != 0 || v.Value != nil
}

// EncodeTo should be kept in sync with the Encode function above. The reason
// this function exists is to avoid creating byte arrays per key-value pair in
// table/builder.go.
func (v *ValueStruct) EncodeTo(buf []byte) []byte {
	tmp := make([]byte, 8)
	binary.LittleEndian.PutUint64(tmp, v.Version)
	buf = append(buf, tmp...)
	buf = append(buf, v.Meta, byte(len(v.UserMeta)))
	buf = append(buf, v.UserMeta...)
	buf = append(buf, v.Value...)
	return buf
}

// Iterator is an interface for a basic iterator.
type Iterator interface {
	// Next returns the next entry with different key on the latest version.
	// If old version is needed, call NextVersion.
	Next()
	// NextVersion set the current entry to an older version.
	// The iterator must be valid to call this method.
	// It returns true if there is an older version, returns false if there is no older version.
	// The iterator is still valid and on the same key.
	NextVersion() bool
	Rewind()
	Seek(key []byte)
	Key() Key
	Value() ValueStruct
	FillValue(vs *ValueStruct)
	Valid() bool
	Close() error
}

// SeekToVersion seeks a valid Iterator to the version that <= the given version.
func SeekToVersion(it Iterator, version uint64) bool {
	if version >= it.Key().Version {
		return true
	}
	for it.NextVersion() {
		if version >= it.Key().Version {
			return true
		}
	}
	return false
}

func NextAllVersion(it Iterator) {
	if !it.NextVersion() {
		it.Next()
	}
}
