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

package table

import (
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/coocood/badger/fileutil"
	"github.com/coocood/badger/l2"
	"github.com/coocood/badger/options"
	"github.com/coocood/badger/surf"
	"github.com/coocood/badger/y"
	"github.com/coocood/bbloom"
	"github.com/dgraph-io/ristretto"
	"github.com/golang/snappy"
	"github.com/pingcap/errors"
)

const (
	fileSuffix    = ".sst"
	idxFileSuffix = ".idx"

	intSize = int(unsafe.Sizeof(int(0)))
)

func IndexFilename(tableFilename string) string { return tableFilename + idxFileSuffix }

// Table represents a loaded table file with the info we have about it
type Table struct {
	sync.Mutex

	fd        *os.File // Own fd.
	filename  string
	indexFd   *os.File
	tableSize int // Initialized in OpenTable, using fd.Stat().

	globalTs        uint64
	blockEndOffsets []uint32
	baseKeys        []byte
	baseKeysEndOffs []uint32

	// The following are initialized once and const.
	smallest, biggest []byte // Smallest and largest keys.
	id                uint64 // file id, part of filename

	compacting int32

	bf   *bbloom.Bloom
	hIdx *hashIndex
	surf *surf.SuRF

	compression options.CompressionType

	l2Cache *l2.Cache
	cache   *ristretto.Cache
}

// CompressionType returns the compression algorithm used for block compression.
func (t *Table) CompressionType() options.CompressionType {
	return t.compression
}

// Delete delete table's file from disk.
func (t *Table) Delete() error {
	if err := deleteFile(t.indexFd); err != nil {
		return err
	}
	if t.fd != nil {
		return deleteFile(t.fd)
	}
	return t.l2Cache.Del(t.id, t.filename)
}

func deleteFile(fd *os.File) error {
	if err := fd.Truncate(0); err != nil {
		return err
	}
	filename := fd.Name()
	if err := fd.Close(); err != nil {
		return err
	}
	return os.Remove(filename)
}

type OpenTableConfig struct {
	ID          uint64
	Path        string
	Compression options.CompressionType
	Cache       *ristretto.Cache
	UseL2       bool
	L2Cache     *l2.Cache
}

// OpenTable assumes file has only one table and opens it.  Takes ownership of fd upon function
// entry.  Returns a table with one reference count on it (decrementing which may delete the file!
// -- consider t.Close() instead).  The fd has to writeable because we call Truncate on it before
// deleting.
func OpenTable(cfg *OpenTableConfig) (*Table, error) {
	filename := IDToFilename(cfg.ID)
	path := filepath.Join(cfg.Path, filename)

	t := &Table{
		id:          cfg.ID,
		filename:    filename,
		compression: cfg.Compression,
		cache:       cfg.Cache,
		l2Cache:     cfg.L2Cache,
	}

	var err error
	if !cfg.UseL2 {
		if t.fd, err = y.OpenExistingFile(path, 0); err != nil {
			return nil, err
		}
	}
	t.indexFd, err = y.OpenExistingFile(path+idxFileSuffix, 0)
	if err != nil {
		return nil, err
	}

	t.loadIndex()
	return t, nil
}

// Close closes the open table.  (Releases resources back to the OS.)
func (t *Table) Close() error {
	if t.fd != nil {
		t.fd.Close()
	}
	if t.indexFd != nil {
		t.indexFd.Close()
	}
	return nil
}

// PointGet try to lookup a key and its value by table's hash index.
// If it find an hash collision the last return value will be false,
// which means caller should fallback to seek search. Otherwise it value will be true.
// If the hash index does not contain such an element the returned key will be nil.
func (t *Table) PointGet(key []byte, keyHash uint64) ([]byte, y.ValueStruct, bool) {
	if t.bf != nil && !t.bf.Has(keyHash) {
		return nil, y.ValueStruct{}, true
	}

	blkIdx, offset := uint32(resultFallback), uint8(0)
	if t.hIdx != nil {
		blkIdx, offset = t.hIdx.lookup(keyHash)
	} else if t.surf != nil {
		v, ok := t.surf.Get(y.ParseKey(key))
		if !ok {
			blkIdx = resultNoEntry
		} else {
			var pos entryPosition
			pos.decode(v)
			blkIdx, offset = uint32(pos.blockIdx), pos.offset
		}
	}
	if blkIdx == resultFallback {
		return nil, y.ValueStruct{}, false
	}
	if blkIdx == resultNoEntry {
		return nil, y.ValueStruct{}, true
	}

	it := t.NewIterator(false)
	it.seekFromOffset(int(blkIdx), int(offset), key)

	if !it.Valid() || !y.SameKey(key, it.Key()) {
		return nil, y.ValueStruct{}, true
	}
	return it.Key(), it.Value(), true
}

func (t *Table) read(off int, sz int) (res []byte, err error) {
	res = make([]byte, sz)
	fd := t.fd
	if fd == nil {
		fd, err = t.l2Cache.Get(t.id, t.filename)
		if err != nil {
			return nil, err
		}
	}
	_, err = fd.ReadAt(res, int64(off))
	return res, err
}

func (t *Table) readNoFail(off int, sz int) []byte {
	res, err := t.read(off, sz)
	y.Check(err)
	return res
}

func (t *Table) loadIndex() error {
	// TODO: now we simply keep all index data in memory.
	// We can add a cache policy to evict unused index to avoid OOM later.
	idxData, err := ioutil.ReadAll(t.indexFd)
	if err != nil {
		return err
	}

	t.globalTs = bytesToU64(idxData[:8])
	idxData = idxData[8:]
	if t.compression != options.None {
		if idxData, err = t.decompressData(idxData); err != nil {
			return err
		}
	}

	decoder := metaDecoder{buf: idxData}
	for decoder.valid() {
		switch decoder.currentId() {
		case idSmallest:
			if k := decoder.decode(); len(k) != 0 {
				if !t.HasGlobalTs() {
					k = y.ParseKey(k)
				}
				t.smallest = y.KeyWithTs(k, math.MaxUint64)
			}
		case idBiggest:
			if k := decoder.decode(); len(k) != 0 {
				if !t.HasGlobalTs() {
					k = y.ParseKey(k)
				}
				t.biggest = y.KeyWithTs(k, 0)
			}
		case idBaseKeysEndOffs:
			t.baseKeysEndOffs = bytesToU32Slice(decoder.decode())
		case idBaseKeys:
			t.baseKeys = decoder.decode()
		case idBlockEndOffsets:
			t.blockEndOffsets = bytesToU32Slice(decoder.decode())
		case idBloomFilter:
			if d := decoder.decode(); len(d) != 0 {
				t.bf = new(bbloom.Bloom)
				t.bf.BinaryUnmarshal(d)
			}
		case idHashIndex:
			if d := decoder.decode(); len(d) != 0 {
				t.hIdx = new(hashIndex)
				t.hIdx.readIndex(d)
			}
		case idSuRFIndex:
			if d := decoder.decode(); len(d) != 0 {
				t.surf = new(surf.SuRF)
				t.surf.Unmarshal(d)
			}
		default:
			decoder.skip()
		}
	}
	return nil
}

type block struct {
	offset int
	data   []byte
}

func (b *block) size() int64 {
	return int64(intSize + len(b.data))
}

func (t *Table) block(idx int) (block, error) {
	y.Assert(idx >= 0)
	if idx >= len(t.blockEndOffsets) {
		return block{}, errors.New("block out of index")
	}

	if t.cache == nil {
		return t.loadBlock(idx)
	}

	key := t.blockCacheKey(idx)
	blk, err := t.cache.GetOrCompute(key, func() (interface{}, int64, error) {
		b, e := t.loadBlock(idx)
		if e != nil {
			return nil, 0, e
		}
		return b, int64(len(b.data)), nil
	})
	if err != nil {
		return block{}, err
	}
	return blk.(block), nil
}

func (t *Table) loadBlock(idx int) (block, error) {
	var startOffset int
	if idx > 0 {
		startOffset = int(t.blockEndOffsets[idx-1])
	}
	blk := block{
		offset: startOffset,
	}
	endOffset := int(t.blockEndOffsets[idx])
	dataLen := endOffset - startOffset
	var err error
	if blk.data, err = t.read(blk.offset, dataLen); err != nil {
		e := errors.Wrapf(err, "failed to read from file: %s at offset: %d, len: %d", t.filename, blk.offset, dataLen)
		return block{}, e
	}

	blk.data, err = t.decompressData(blk.data)
	if err != nil {
		e := errors.Wrapf(err, "failed to decode compressed data in file: %s at offset: %d, len: %d", t.filename, blk.offset, dataLen)
		return block{}, e
	}

	return blk, nil
}

func (t *Table) approximateOffset(it *Iterator, key []byte) int {
	if y.CompareKeysWithVer(t.Biggest(), key) < 0 {
		return int(t.blockEndOffsets[len(t.blockEndOffsets)-1])
	} else if y.CompareKeysWithVer(t.Smallest(), key) > 0 {
		return 0
	}
	blk := it.seekBlock(key)
	if blk != 0 {
		return int(t.blockEndOffsets[blk-1])
	}
	return 0
}

// HasGlobalTs returns table does set global ts.
func (t *Table) HasGlobalTs() bool {
	return t.globalTs != math.MaxUint64
}

// SetGlobalTs update the global ts of external ingested tables.
func (t *Table) SetGlobalTs(ts uint64) error {
	encodeTs := math.MaxUint64 - ts
	if _, err := t.indexFd.WriteAt(u64ToBytes(encodeTs), 0); err != nil {
		return err
	}
	if err := fileutil.Fsync(t.indexFd); err != nil {
		return err
	}
	t.globalTs = encodeTs
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
func (t *Table) Size() int64 { return int64(t.blockEndOffsets[len(t.blockEndOffsets)-1]) }

// Smallest is its smallest key, or nil if there are none
func (t *Table) Smallest() []byte { return t.smallest }

// Biggest is its biggest key, or nil if there are none
func (t *Table) Biggest() []byte { return t.biggest }

// Filename is NOT the file name.  Just kidding, it is.
func (t *Table) Filename() string { return t.fd.Name() }

// ID is the table's ID number (used to make the file name).
func (t *Table) ID() uint64 { return t.id }

func (t *Table) HasOverlap(start, end []byte, includeEnd bool) bool {
	if y.CompareKeysWithVer(start, t.Biggest()) > 0 {
		return false
	}

	if cmp := y.CompareKeysWithVer(end, t.Smallest()); cmp < 0 {
		return false
	} else if cmp == 0 {
		return includeEnd
	}

	if t.surf != nil {
		return t.surf.HasOverlap(start, end, includeEnd)
	}

	it := t.NewIterator(false)
	it.Seek(start)
	if !it.Valid() {
		return false
	}
	if cmp := y.CompareKeysWithVer(it.Key(), end); cmp > 0 {
		return false
	} else if cmp == 0 {
		return includeEnd
	}
	return true
}

// ParseFileID reads the file id out of a filename.
func ParseFileID(name string) (uint64, bool) {
	name = filepath.Base(name)
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

// decompressData decompresses the given data.
func (t *Table) decompressData(data []byte) ([]byte, error) {
	switch t.compression {
	case options.None:
		return data, nil
	case options.Snappy:
		return snappy.Decode(nil, data)
	case options.ZSTD:
		return decompress(data)
	}
	return nil, errors.New("Unsupported compression type")
}
