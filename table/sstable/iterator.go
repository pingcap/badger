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

package sstable

import (
	"bytes"
	"github.com/pingcap/log"
	"io"
	"math"

	"encoding/binary"
	"github.com/pingcap/badger/surf"
	"github.com/pingcap/badger/y"
)

type singleKeyIterator struct {
	oldOffset uint32
	loaded    bool
	latestVal []byte
	oldVals   entrySlice
	idx       int
	oldBlock  []byte
}

func (ski *singleKeyIterator) set(oldOffset uint32, latestVal []byte) {
	ski.oldOffset = oldOffset
	ski.latestVal = latestVal
	ski.loaded = false
	ski.idx = 0
}

func (ski *singleKeyIterator) getVal() (val []byte) {
	if ski.idx == 0 {
		return ski.latestVal
	}
	oldEntry := ski.oldVals.getEntry(ski.idx - 1)
	return oldEntry
}

func (ski *singleKeyIterator) loadOld() {
	numEntries := bytesToU32(ski.oldBlock[ski.oldOffset:])
	endOffsStartIdx := ski.oldOffset + 4
	endOffsEndIdx := endOffsStartIdx + 4*numEntries
	ski.oldVals.endOffs = bytesToU32Slice(ski.oldBlock[endOffsStartIdx:endOffsEndIdx])
	valueEndOff := endOffsEndIdx + ski.oldVals.endOffs[numEntries-1]
	ski.oldVals.data = ski.oldBlock[endOffsEndIdx:valueEndOff]
	ski.loaded = true
}

func (ski *singleKeyIterator) length() int {
	return ski.oldVals.length() + 1
}

type blockIterator struct {
	entries entrySlice
	idx     int

	globalTsBytes [8]byte
	globalTs      uint64
	key           y.Key
	val           []byte

	baseLen uint16
	ski     singleKeyIterator

	keyDiffs []uint64
}

func (itr *blockIterator) setBlock(b block) {
	itr.idx = 0
	itr.key.Reset()
	itr.val = itr.val[:0]
	itr.loadEntries(b.data)
	itr.key.UserKey = append(itr.key.UserKey[:0], b.baseKey[:itr.baseLen]...)
}

// loadEntries loads the entryEndOffsets for binary searching for a key.
func (itr *blockIterator) loadEntries(data []byte) {
	// Get the number of entries from the end of `data` (and remove it).
	dataLen := len(data)
	itr.baseLen = binary.LittleEndian.Uint16(data[dataLen-2:])
	entriesNum := int(bytesToU32(data[dataLen-6:]))
	entriesEnd := dataLen - 6
	entriesStart := entriesEnd - entriesNum*4
	itr.entries.endOffs = bytesToU32Slice(data[entriesStart:entriesEnd])
	data = data[:entriesStart]
	keyDiffsOff := len(data) - itr.entries.length()*8
	itr.keyDiffs = bytesToU64Slice(data[keyDiffsOff:])
	data = data[:keyDiffsOff]
	itr.entries.data = data
}

// seekToFirst brings us to the first element. Valid should return true.
func (itr *blockIterator) seekToFirst() error {
	return itr.setIdx(0)
}

// seekToLast brings us to the last element. Valid should return true.
func (itr *blockIterator) seekToLast() error {
	return itr.setIdx(itr.entries.length() - 1)
}

// setIdx sets the iterator to the entry index and set the current key and value.
func (itr *blockIterator) setIdx(i int) error {
	itr.idx = i
	if i >= itr.entries.length() || i < 0 {
		return io.EOF
	}
	entryData := itr.entries.getEntry(i)
	keyDiff := itr.keyDiffs[i]
	var keyDiffBuf [8]byte
	binary.BigEndian.PutUint64(keyDiffBuf[:], keyDiff)
	itr.key.UserKey = append(itr.key.UserKey[:itr.baseLen], keyDiffBuf[:byte(keyDiff)]...)
	keySuffixLen := binary.LittleEndian.Uint16(entryData)
	entryData = entryData[2:]
	itr.key.UserKey = append(itr.key.UserKey, entryData[:keySuffixLen]...)
	entryData = entryData[keySuffixLen:]
	hasOld := entryData[0] != 0
	entryData = entryData[1:]
	var oldOffset uint32
	if hasOld {
		oldOffset = bytesToU32(entryData)
		entryData = entryData[4:]
	}
	if itr.globalTs != 0 {
		itr.key.Version = itr.globalTs
	} else {
		itr.key.Version = bytesToU64(entryData)
	}
	itr.val = entryData
	itr.ski.idx = 0
	if hasOld {
		itr.ski.set(oldOffset, itr.val)
	}
	return nil
}

func (itr *blockIterator) hasOldVersion() bool {
	return itr.ski.oldOffset != 0
}

func (itr *blockIterator) next() error {
	return itr.setIdx(itr.idx + 1)
}

func (itr *blockIterator) prev() error {
	return itr.setIdx(itr.idx - 1)
}

func (itr *blockIterator) seek(key []byte) error {
	keyDiff := extractKeyDiff(key, int(itr.baseLen))
	i, j := 0, len(itr.keyDiffs)
	k := j
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		// i ≤ h < j
		hKeyDiff := itr.keyDiffs[h]
		if keyDiff > hKeyDiff {
			i = h + 1
		} else {
			j = h
			if keyDiff < hKeyDiff {
				// ensure keyDiff < itr.keyDiffs[k]
				k = h
			}
		}
	}
	if i+1 < len(itr.keyDiffs) && keyDiff == itr.keyDiffs[i+1] {
		// The keyDiff is not unique, we need to fallback to suffix binary search.
		suffixStart := int(itr.baseLen + 7)
		if suffixStart > len(key) {
			suffixStart = len(key)
		}
		keySuffix := key[suffixStart:]
		j = k
		for i < j {
			h := int(uint(i+j) >> 1)
			if keyDiff != itr.keyDiffs[h] {
				j = h
				continue
			}
			// i ≤ h < j
			hKeySuffix := itr.getKeySuffix(h)
			cmp := bytes.Compare(keySuffix, hKeySuffix)
			if cmp > 0 {
				i = h + 1
			} else {
				j = h
			}
		}
	}
	err := itr.setIdx(i)
	if err != nil {
		return err
	}
	if i < len(itr.keyDiffs)-1 && itr.keyDiffs[i] == keyDiff {
		if bytes.Compare(itr.key.UserKey[itr.baseLen:], key[itr.baseLen:]) < 0 {
			err = itr.setIdx(i + 1)
		}
	}
	return err
}

func (itr *blockIterator) getKeySuffix(i int) []byte {
	entryData := itr.entries.getEntry(i)
	keySuffixLen := binary.LittleEndian.Uint16(entryData)
	entryData = entryData[2:]
	return entryData[:keySuffixLen]
}

func extractKeyDiff(key []byte, baseLen int) uint64 {
	buf := make([]byte, 8)
	n := copy(buf[:7], key[baseLen:])
	return binary.BigEndian.Uint64(buf) + uint64(n)
}

// Iterator is an iterator for a Table.
type Iterator struct {
	t    *Table
	tIdx *tableIndex
	surf *surf.Iterator
	bpos int
	bi   blockIterator
	err  error

	// Internally, Iterator is bidirectional. However, we only expose the
	// unidirectional functionality for now.
	reversed bool
}

// NewIterator returns a new iterator of the Table
func (t *Table) newIterator(reversed bool) *Iterator {
	idx, err := t.getIndex()
	if err != nil {
		return &Iterator{err: err}
	}
	return t.newIteratorWithIdx(reversed, idx)
}

func (t *Table) newIteratorWithIdx(reversed bool, index *tableIndex) *Iterator {
	it := &Iterator{t: t, reversed: reversed, tIdx: index}
	it.bi.globalTs = t.globalTs
	if t.oldBlockLen > 0 {
		y.Assert(len(t.oldBlock) > 0)
	}
	it.bi.ski.oldBlock = t.oldBlock
	binary.BigEndian.PutUint64(it.bi.globalTsBytes[:], math.MaxUint64-t.globalTs)
	if index.surf != nil {
		it.surf = index.surf.NewIterator()
	}
	return it
}

func (itr *Iterator) reset() {
	itr.bpos = 0
}

// Valid follows the y.Iterator interface
func (itr *Iterator) Valid() bool {
	return itr.err == nil
}

func (itr *Iterator) Error() error {
	if itr.err == io.EOF {
		return nil
	}
	return itr.err
}

func (itr *Iterator) seekToFirst() error {
	numBlocks := len(itr.tIdx.blockEndOffsets)
	if numBlocks == 0 {
		return io.EOF
	}
	itr.bpos = 0
	block, err := itr.t.block(itr.bpos, itr.tIdx)
	if err != nil {
		return err
	}
	itr.bi.setBlock(block)
	return itr.bi.seekToFirst()
}

func (itr *Iterator) seekToLast() error {
	numBlocks := len(itr.tIdx.blockEndOffsets)
	if numBlocks == 0 {
		return io.EOF
	}
	itr.bpos = numBlocks - 1
	block, err := itr.t.block(itr.bpos, itr.tIdx)
	if err != nil {
		return err
	}
	itr.bi.setBlock(block)
	return itr.bi.seekToLast()
}

func (itr *Iterator) seekInBlock(blockIdx int, key []byte) error {
	itr.bpos = blockIdx
	block, err := itr.t.block(blockIdx, itr.tIdx)
	if err != nil {
		return err
	}
	itr.bi.setBlock(block)
	return itr.bi.seek(key)
}

func (itr *Iterator) seekFromOffset(blockIdx int, offset int, key []byte) error {
	itr.bpos = blockIdx
	block, err := itr.t.block(blockIdx, itr.tIdx)
	if err != nil {
		return err
	}
	itr.bi.setBlock(block)
	if err = itr.bi.setIdx(offset); err != nil {
		return err
	}
	if bytes.Compare(itr.bi.key.UserKey, key) >= 0 {
		return nil
	}
	return itr.bi.seek(key)
}

func (itr *Iterator) seekBlock(key []byte) int {
	keyDiff := extractKeyDiff(key, itr.t.commonLen)
	baseKeyDiffs := itr.tIdx.baseKeyDiffs
	i, j := 0, len(baseKeyDiffs)
	k := j
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		// i ≤ h < j
		hBaseKeyDiff := baseKeyDiffs[h]
		if keyDiff > hBaseKeyDiff {
			i = h + 1
		} else {
			j = h
			if keyDiff < hBaseKeyDiff {
				// ensure keyDiff < baseKeyDiffs[k]
				k = h
			}
		}
	}
	if i+1 < len(baseKeyDiffs) && keyDiff == baseKeyDiffs[i+1] {
		// fallback to binary search on base key
		j = k
		for i < j {
			h := int(uint(i+j) >> 1)
			if baseKeyDiffs[h] != keyDiff {
				j = h
				continue
			}
			blockBaseKey := itr.tIdx.baseKeys.getEntry(h)
			cmp := bytes.Compare(key, blockBaseKey)
			if cmp > 0 {
				i = h + 1
			} else {
				j = h
			}
		}
	}
	if i < len(baseKeyDiffs) && keyDiff == baseKeyDiffs[i] {
		baseKey := itr.tIdx.baseKeys.getEntry(i)
		if bytes.Compare(baseKey, key) <= 0 {
			return i
		}
	}
	return i - 1
}

// seek brings us to a key that is >= input key.
func (itr *Iterator) seek(key []byte) (err error) {
	var idx int
	defer func() {
		if err != nil && len(key) > 30 {
			log.S().Infof("seek %x invalid idx %d", key, idx)
			if idx < len(itr.tIdx.baseKeyDiffs) {
				log.S().Infof("base key %d %x", idx, itr.tIdx.baseKeys.getEntry(idx))
				if idx > 0 {
					log.S().Infof("base key %d %x", idx-1, itr.tIdx.baseKeys.getEntry(idx-1))
				}
				if idx < len(itr.tIdx.baseKeyDiffs)-1 {
					log.S().Infof("base key %d %x", idx+1, itr.tIdx.baseKeys.getEntry(idx+1))
				}
			}
		}
	}()
	itr.reset()
	cmp := itr.comparePrefix(key)
	if cmp > 0 {
		if itr.reversed {
			return itr.seekToLast()
		}
		return io.EOF
	}
	if cmp < 0 {
		if itr.reversed {
			return io.EOF
		}
		return itr.seekToFirst()
	}
	idx = itr.seekBlock(key)
	if idx < 0 && !itr.reversed {
		return itr.seekToFirst()
	}
	err = itr.seekInBlock(idx, key)
	if err == io.EOF && !itr.reversed {
		return itr.next()
	}
	if itr.reversed && !bytes.Equal(itr.Key().UserKey, key) {
		return itr.prev()
	}
	return err
}

// comparePrefix compares the key prefix to the common key of the table.
func (itr *Iterator) comparePrefix(key []byte) int {
	var keyPrefix []byte
	if len(key) < itr.t.commonLen {
		keyPrefix = key
	} else {
		keyPrefix = key[:itr.t.commonLen]
	}
	commonKey := itr.t.smallest.UserKey[:itr.t.commonLen]
	return bytes.Compare(keyPrefix, commonKey)
}

func (itr *Iterator) seekSurf(key []byte) error {
	itr.reset()
	sit := itr.surf
	sit.Seek(key)
	if !sit.Valid() {
		return io.EOF
	}

	var pos entryPosition
	pos.decode(sit.Value())
	return itr.seekFromOffset(int(pos.blockIdx), int(pos.offset), key)
}

func (itr *Iterator) next() error {
	if itr.bpos >= len(itr.tIdx.blockEndOffsets) {
		return io.EOF
	}

	if itr.bi.entries.length() == 0 {
		block, err := itr.t.block(itr.bpos, itr.tIdx)
		if err != nil {
			return err
		}
		itr.bi.setBlock(block)
		return itr.bi.seekToFirst()
	}

	err := itr.bi.next()
	if err != nil {
		itr.bpos++
		itr.bi.entries.reset()
		return itr.next()
	}
	return nil
}

func (itr *Iterator) prev() error {
	if itr.bpos < 0 {
		return io.EOF
	}

	if itr.bi.entries.length() == 0 {
		block, err := itr.t.block(itr.bpos, itr.tIdx)
		if err != nil {
			return err
		}
		itr.bi.setBlock(block)
		return itr.bi.seekToLast()
	}

	err := itr.bi.prev()
	if err != nil {
		itr.bpos--
		itr.bi.entries.reset()
		return itr.prev()
	}
	return nil
}

// Key follows the y.Iterator interface
func (itr *Iterator) Key() y.Key {
	return itr.bi.key
}

// Value follows the y.Iterator interface
func (itr *Iterator) Value() (ret y.ValueStruct) {
	ret.Decode(itr.bi.val)
	return
}

// FillValue fill the value struct.
func (itr *Iterator) FillValue(vs *y.ValueStruct) {
	vs.Decode(itr.bi.val)
}

// Next follows the y.Iterator interface
func (itr *Iterator) Next() {
	if !itr.reversed {
		itr.err = itr.next()
	} else {
		itr.err = itr.prev()
	}
}

func (itr *Iterator) NextVersion() bool {
	if itr.bi.ski.oldOffset == 0 {
		return false
	}
	if !itr.bi.ski.loaded {
		itr.bi.ski.loadOld()
	}
	if itr.bi.ski.idx+1 < itr.bi.ski.length() {
		itr.bi.ski.idx++
		itr.bi.val = itr.bi.ski.getVal()
		itr.bi.key.Version = bytesToU64(itr.bi.val)
		return true
	}
	return false
}

// Rewind follows the y.Iterator interface
func (itr *Iterator) Rewind() {
	if !itr.reversed {
		itr.err = itr.seekToFirst()
	} else {
		itr.err = itr.seekToLast()
	}
}

// Seek follows the y.Iterator interface
func (itr *Iterator) Seek(key []byte) {
	if !itr.reversed && itr.surf != nil {
		itr.err = itr.seekSurf(key)
	} else {
		itr.err = itr.seek(key)
	}
}
