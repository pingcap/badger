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
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/coocood/bbloom"
	"github.com/pingcap/badger/buffer"
	"github.com/pingcap/badger/cache"
	"github.com/pingcap/badger/fileutil"
	"github.com/pingcap/badger/options"
	"github.com/pingcap/badger/surf"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
)

const (
	fileSuffix    = ".sst"
	idxFileSuffix = ".idx"

	intSize = int(unsafe.Sizeof(int(0)))
)

func IndexFilename(tableFilename string) string { return tableFilename + idxFileSuffix }

type tableIndex struct {
	blockEndOffsets []uint32
	baseKeys        entrySlice
	bf              *bbloom.Bloom
	hIdx            *hashIndex
	surf            *surf.SuRF
}

// Table represents a loaded table file with the info we have about it
type Table struct {
	sync.Mutex

	fd      *os.File // Own fd.
	indexFd *os.File

	globalTs          uint64
	tableSize         int64
	numBlocks         int
	smallest, biggest y.Key
	id                uint64

	blockCache *cache.Cache
	blocksData []byte

	indexCache *cache.Cache
	index      *tableIndex
	indexOnce  sync.Once
	indexData  []byte

	compacting int32

	compression options.CompressionType

	oldBlockLen int64
	oldBlock    []byte
}

// CompressionType returns the compression algorithm used for block compression.
func (t *Table) CompressionType() options.CompressionType {
	return t.compression
}

// Delete delete table's file from disk.
func (t *Table) Delete() error {
	if t.fd == nil {
		t.blocksData = nil
		t.indexData = nil
		return nil
	}
	if t.blockCache != nil {
		for blk := 0; blk < t.numBlocks; blk++ {
			key := t.blockCacheKey(blk)
			if v, ok := t.blockCache.Get(key); ok {
				if b, ok := v.(*block); ok {
					b.done()
				}
				t.blockCache.Del(key)
			}
		}
	}
	if t.indexCache != nil {
		t.indexCache.Del(t.id)
	}
	if len(t.blocksData) != 0 {
		y.Munmap(t.blocksData)
	}
	t.index = nil
	if len(t.indexData) != 0 {
		y.Munmap(t.indexData)
	}
	if err := t.fd.Truncate(0); err != nil {
		// This is very important to let the FS know that the file is deleted.
		return err
	}
	filename := t.fd.Name()
	if err := t.fd.Close(); err != nil {
		return err
	}
	if err := os.Remove(filename); err != nil {
		return err
	}
	return os.Remove(filename + idxFileSuffix)
}

// OpenTable assumes file has only one table and opens it.  Takes ownership of fd upon function
// entry.  Returns a table with one reference count on it (decrementing which may delete the file!
// -- consider t.Close() instead).  The fd has to writeable because we call Truncate on it before
// deleting.
func OpenTable(filename string, blockCache *cache.Cache, indexCache *cache.Cache) (*Table, error) {
	id, ok := ParseFileID(filename)
	if !ok {
		return nil, errors.Errorf("Invalid filename: %s", filename)
	}

	// TODO: after we support cache of L2 storage, we will open block data file in cache manager.
	fd, err := y.OpenExistingFile(filename, 0)
	if err != nil {
		return nil, err
	}

	indexFd, err := y.OpenExistingFile(filename+idxFileSuffix, 0)
	if err != nil {
		return nil, err
	}

	t := &Table{
		fd:         fd,
		indexFd:    indexFd,
		id:         id,
		blockCache: blockCache,
		indexCache: indexCache,
	}

	if err := t.initTableInfo(); err != nil {
		t.Close()
		return nil, err
	}
	if blockCache == nil || t.oldBlockLen > 0 {
		t.blocksData, err = y.Mmap(fd, false, t.Size())
		if err != nil {
			t.Close()
			return nil, y.Wrapf(err, "Unable to map file")
		}
		t.setOldBlock()
	}
	return t, nil
}

func (t *Table) setOldBlock() {
	t.oldBlock = t.blocksData[t.tableSize-t.oldBlockLen : t.tableSize]
}

// OpenInMemoryTable opens a table that has data in memory.
func OpenInMemoryTable(blockData, indexData []byte) (*Table, error) {
	t := &Table{
		blocksData: blockData,
		indexData:  indexData,
	}
	if err := t.initTableInfo(); err != nil {
		return nil, err
	}
	t.setOldBlock()
	return t, nil
}

// Close closes the open table.  (Releases resources back to the OS.)
func (t *Table) Close() error {
	if t.fd != nil {
		t.fd.Close()
	}
	if t.indexFd != nil {
		if len(t.indexData) != 0 {
			y.Munmap(t.indexData)
		}
		t.indexFd.Close()
	}
	return nil
}

func (t *Table) NewIterator(reversed bool) y.Iterator {
	return t.newIterator(reversed)
}

func (t *Table) Get(key y.Key, keyHash uint64) (y.ValueStruct, error) {
	resultKey, resultVs, ok, err := t.pointGet(key, keyHash)
	if err != nil {
		return y.ValueStruct{}, err
	}
	if !ok {
		it := t.NewIterator(false)
		defer it.Close()
		it.Seek(key.UserKey)
		if !it.Valid() {
			return y.ValueStruct{}, nil
		}
		if !key.SameUserKey(it.Key()) {
			return y.ValueStruct{}, nil
		}
		resultKey, resultVs = it.Key(), it.Value()
	} else if resultKey.IsEmpty() {
		return y.ValueStruct{}, nil
	}
	result := resultVs
	result.Version = resultKey.Version
	return result, nil
}

// pointGet try to lookup a key and its value by table's hash index.
// If it find an hash collision the last return value will be false,
// which means caller should fallback to seek search. Otherwise it value will be true.
// If the hash index does not contain such an element the returned key will be nil.
func (t *Table) pointGet(key y.Key, keyHash uint64) (y.Key, y.ValueStruct, bool, error) {
	idx, err := t.getIndex()
	if err != nil {
		return y.Key{}, y.ValueStruct{}, false, err
	}
	if idx.bf != nil && !idx.bf.Has(keyHash) {
		return y.Key{}, y.ValueStruct{}, true, err
	}

	blkIdx, offset := uint32(resultFallback), uint8(0)
	if idx.hIdx != nil {
		blkIdx, offset = idx.hIdx.lookup(keyHash)
	} else if idx.surf != nil {
		v, ok := idx.surf.Get(key.UserKey)
		if !ok {
			blkIdx = resultNoEntry
		} else {
			var pos entryPosition
			pos.decode(v)
			blkIdx, offset = uint32(pos.blockIdx), pos.offset
		}
	}
	if blkIdx == resultFallback {
		return y.Key{}, y.ValueStruct{}, false, nil
	}
	if blkIdx == resultNoEntry {
		return y.Key{}, y.ValueStruct{}, true, nil
	}

	it := t.newIterator(false)
	defer it.Close()
	it.seekFromOffset(int(blkIdx), int(offset), key.UserKey)

	if !it.Valid() || !key.SameUserKey(it.Key()) {
		return y.Key{}, y.ValueStruct{}, true, it.Error()
	}
	if !y.SeekToVersion(it, key.Version) {
		return y.Key{}, y.ValueStruct{}, true, it.Error()
	}
	return it.Key(), it.Value(), true, nil
}

func (t *Table) read(off int, sz int) ([]byte, error) {
	if len(t.blocksData) > 0 {
		if len(t.blocksData[off:]) < sz {
			return nil, y.ErrEOF
		}
		return t.blocksData[off : off+sz], nil
	}
	res := buffer.GetBuffer(sz)
	_, err := t.fd.ReadAt(res, int64(off))
	return res, err
}

func (t *Table) initTableInfo() error {
	d, err := t.loadIndexData(false)
	if err != nil {
		return err
	}

	t.compression = d.compression
	t.globalTs = d.globalTS

	for ; d.valid(); d.next() {
		switch d.currentId() {
		case idSmallest:
			if k := d.decode(); len(k) != 0 {
				t.smallest = y.KeyWithTs(y.Copy(k), math.MaxUint64)
			}
		case idBiggest:
			if k := d.decode(); len(k) != 0 {
				t.biggest = y.KeyWithTs(y.Copy(k), 0)
			}
		case idBlockEndOffsets:
			offsets := bytesToU32Slice(d.decode())
			t.tableSize = int64(offsets[len(offsets)-1])
			t.numBlocks = len(offsets)
		case idOldBlockLen:
			t.oldBlockLen = int64(bytesToU32(d.decode()))
			t.tableSize += t.oldBlockLen
		}
	}
	return nil
}

func (t *Table) readTableIndex(d *metaDecoder) *tableIndex {
	idx := new(tableIndex)
	for ; d.valid(); d.next() {
		switch d.currentId() {
		case idBaseKeysEndOffs:
			idx.baseKeys.endOffs = bytesToU32Slice(d.decode())
		case idBaseKeys:
			idx.baseKeys.data = d.decode()
		case idBlockEndOffsets:
			idx.blockEndOffsets = bytesToU32Slice(d.decode())
		case idBloomFilter:
			if d := d.decode(); len(d) != 0 {
				idx.bf = new(bbloom.Bloom)
				idx.bf.BinaryUnmarshal(d)
			}
		case idHashIndex:
			if d := d.decode(); len(d) != 0 {
				idx.hIdx = new(hashIndex)
				idx.hIdx.readIndex(d)
			}
		case idSuRFIndex:
			if d := d.decode(); len(d) != 0 {
				idx.surf = new(surf.SuRF)
				idx.surf.Unmarshal(d)
			}
		}
	}
	return idx
}

func (t *Table) getIndex() (*tableIndex, error) {
	if t.indexCache == nil {
		var err error
		t.indexOnce.Do(func() {
			var d *metaDecoder
			d, err = t.loadIndexData(true)
			if err != nil {
				return
			}
			t.index = t.readTableIndex(d)
		})
		return t.index, nil
	}

	index, err := t.indexCache.GetOrCompute(t.id, func() (interface{}, int64, error) {
		d, err := t.loadIndexData(false)
		if err != nil {
			return nil, 0, err
		}
		return t.readTableIndex(d), int64(len(d.buf)), nil
	})
	if err != nil {
		return nil, err
	}
	return index.(*tableIndex), nil
}

func (t *Table) loadIndexData(useMmap bool) (*metaDecoder, error) {
	if t.indexFd == nil {
		return newMetaDecoder(t.indexData)
	}
	fstat, err := t.indexFd.Stat()
	if err != nil {
		return nil, err
	}
	var idxData []byte

	if useMmap {
		idxData, err = y.Mmap(t.indexFd, false, fstat.Size())
		if err != nil {
			return nil, err
		}
		t.indexData = idxData
	} else {
		idxData = buffer.GetBuffer(int(fstat.Size()))
		if _, err = t.indexFd.ReadAt(idxData, 0); err != nil {
			return nil, err
		}
	}

	decoder, err := newMetaDecoder(idxData)
	if err != nil {
		return nil, err
	}
	if decoder.compression != options.None && useMmap {
		y.Munmap(idxData)
		t.indexData = nil
	}
	return decoder, nil
}

type block struct {
	offset  int
	data    []byte
	baseKey []byte

	reference int32
}

func OnEvict(key uint64, value interface{}) {
	if b, ok := value.(*block); ok {
		b.done()
	}
}

func (b *block) add() (ok bool) {
	for {
		old := atomic.LoadInt32(&b.reference)
		if old == 0 {
			return false
		}
		new := old + 1
		if atomic.CompareAndSwapInt32(&b.reference, old, new) {
			return true
		}
	}
}

func (b *block) done() {
	if b != nil && atomic.AddInt32(&b.reference, -1) == 0 {
		buffer.PutBuffer(b.data)
		b.data = nil
	}
}

func (b *block) size() int64 {
	return int64(intSize + len(b.data))
}

func (t *Table) block(idx int, index *tableIndex) (*block, error) {
	y.Assert(idx >= 0)

	if idx >= len(index.blockEndOffsets) {
		return &block{}, io.EOF
	}

	if t.blockCache == nil {
		return t.loadBlock(idx, index)
	}

	key := t.blockCacheKey(idx)
	blk, err := t.blockCache.GetOrCompute(key, func() (interface{}, int64, error) {
		b, e := t.loadBlock(idx, index)
		if e != nil {
			return nil, 0, e
		}
		b.reference = 1
		return b, int64(len(b.data)), nil
	})
	if err != nil {
		return &block{}, err
	}
	b := blk.(*block)
	if ok := b.add(); !ok {
		return &block{}, errors.Errorf("block is evicted")
	}
	return b, nil
}

func (t *Table) loadBlock(idx int, index *tableIndex) (*block, error) {
	var startOffset int
	if idx > 0 {
		startOffset = int(index.blockEndOffsets[idx-1])
	}
	blk := &block{
		offset: startOffset,
	}
	endOffset := int(index.blockEndOffsets[idx])
	dataLen := endOffset - startOffset
	var err error
	if blk.data, err = t.read(blk.offset, dataLen); err != nil {
		return &block{}, errors.Wrapf(err,
			"failed to read from file: %s at offset: %d, len: %d", t.fd.Name(), blk.offset, dataLen)
	}

	blk.data, err = t.compression.Decompress(blk.data)
	if err != nil {
		return &block{}, errors.Wrapf(err,
			"failed to decode compressed data in file: %s at offset: %d, len: %d",
			t.fd.Name(), blk.offset, dataLen)
	}
	blk.baseKey = index.baseKeys.getEntry(idx)
	return blk, nil
}

// HasGlobalTs returns table does set global ts.
func (t *Table) HasGlobalTs() bool {
	return t.globalTs != 0
}

// SetGlobalTs update the global ts of external ingested tables.
func (t *Table) SetGlobalTs(ts uint64) error {
	if _, err := t.indexFd.WriteAt(u64ToBytes(ts), 0); err != nil {
		return err
	}
	if err := fileutil.Fsync(t.indexFd); err != nil {
		return err
	}
	t.globalTs = ts
	return nil
}

func (t *Table) MarkCompacting(flag bool) {
	if flag {
		atomic.StoreInt32(&t.compacting, 1)
	}
	atomic.StoreInt32(&t.compacting, 0)
}

func (t *Table) IsCompacting() bool {
	return atomic.LoadInt32(&t.compacting) == 1
}

func (t *Table) blockCacheKey(idx int) uint64 {
	y.Assert(t.ID() < math.MaxUint32)
	y.Assert(idx < math.MaxUint32)
	return (t.ID() << 32) | uint64(idx)
}

// Size is its file size in bytes
func (t *Table) Size() int64 { return t.tableSize }

// Smallest is its smallest key, or nil if there are none
func (t *Table) Smallest() y.Key { return t.smallest }

// Biggest is its biggest key, or nil if there are none
func (t *Table) Biggest() y.Key { return t.biggest }

// Filename is NOT the file name.  Just kidding, it is.
func (t *Table) Filename() string { return t.fd.Name() }

// ID is the table's ID number (used to make the file name).
func (t *Table) ID() uint64 { return t.id }

func (t *Table) HasOverlap(start, end y.Key, includeEnd bool) bool {
	if start.Compare(t.Biggest()) > 0 {
		return false
	}

	if cmp := end.Compare(t.Smallest()); cmp < 0 {
		return false
	} else if cmp == 0 {
		return includeEnd
	}

	idx, err := t.getIndex()
	if err != nil {
		return true
	}

	if idx.surf != nil {
		return idx.surf.HasOverlap(start.UserKey, end.UserKey, includeEnd)
	}

	// If there are errors occurred during seeking,
	// we assume the table has overlapped with the range to prevent data loss.
	it := t.newIteratorWithIdx(false, idx)
	defer it.Close()
	it.Seek(start.UserKey)
	if !it.Valid() {
		return it.Error() != nil
	}
	if cmp := it.Key().Compare(end); cmp > 0 {
		return false
	} else if cmp == 0 {
		return includeEnd
	}
	return true
}

// ParseFileID reads the file id out of a filename.
func ParseFileID(name string) (uint64, bool) {
	name = path.Base(name)
	if !strings.HasSuffix(name, fileSuffix) {
		return 0, false
	}
	//	suffix := name[len(fileSuffix):]
	name = strings.TrimSuffix(name, fileSuffix)
	id, err := strconv.ParseUint(name, 16, 64)
	if err != nil {
		return 0, false
	}
	return id, true
}

// IDToFilename does the inverse of ParseFileID
func IDToFilename(id uint64) string {
	return fmt.Sprintf("%08x", id) + fileSuffix
}

// NewFilename should be named TableFilepath -- it combines the dir with the ID to make a table
// filepath.
func NewFilename(id uint64, dir string) string {
	return filepath.Join(dir, IDToFilename(id))
}
