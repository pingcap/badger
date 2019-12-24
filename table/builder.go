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
	"bytes"
	"encoding/binary"
	"math"
	"os"
	"reflect"
	"unsafe"

	"github.com/DataDog/zstd"
	"github.com/coocood/badger/fileutil"
	"github.com/coocood/badger/options"
	"github.com/coocood/badger/surf"
	"github.com/coocood/badger/y"
	"github.com/coocood/bbloom"
	"github.com/dgryski/go-farm"
	"github.com/golang/snappy"
	"github.com/pingcap/errors"
	"golang.org/x/time/rate"
)

type header struct {
	baseLen uint16 // Overlap with base key.
	diffLen uint16 // Length of the diff.
}

// Encode encodes the header.
func (h header) Encode() []byte {
	var b [4]byte
	*(*header)(unsafe.Pointer(&b[0])) = h
	return b[:]
}

// Decode decodes the header.
func (h *header) Decode(buf []byte) {
	*h = *(*header)(unsafe.Pointer(&buf[0]))
}

const headerSize = 4

// Builder is used in building a table.
type Builder struct {
	counter int // Number of keys written for the current block.

	idxFileName   string
	w             *fileutil.DirectWriter
	buf           []byte
	writtenLen    int
	rawWrittenLen int
	compression   options.CompressionType

	baseKeysBuf     []byte
	baseKeysEndOffs []uint32

	blockBaseKey []byte // Base key for the current block.

	blockEndOffsets []uint32 // Base offsets of every block.

	// end offsets of every entry within the current block being built.
	// The offsets are relative to the start of the block.
	entryEndOffsets []uint32

	smallest []byte
	biggest  []byte

	hashEntries []hashEntry
	bloomFpr    float64
	isExternal  bool
	opt         options.TableBuilderOptions
	useSuRF     bool

	surfKeys [][]byte
	surfVals [][]byte
}

// NewTableBuilder makes a new TableBuilder.
// If the limiter is nil, the write speed during table build will not be limited.
func NewTableBuilder(f *os.File, limiter *rate.Limiter, level int, opt options.TableBuilderOptions) *Builder {
	t := float64(opt.LevelSizeMultiplier)
	fprBase := math.Pow(t, 1/(t-1)) * opt.LogicalBloomFPR * (t - 1)
	levelFactor := math.Pow(t, float64(opt.MaxLevels-level))

	return &Builder{
		idxFileName: f.Name() + idxFileSuffix,
		w:           fileutil.NewDirectWriter(f, opt.WriteBufferSize, limiter),
		buf:         make([]byte, 0, 4*1024),
		baseKeysBuf: make([]byte, 0, 4*1024),
		hashEntries: make([]hashEntry, 0, 4*1024),
		bloomFpr:    fprBase / levelFactor,
		compression: opt.CompressionPerLevel[level],
		opt:         opt,
		useSuRF:     level >= opt.SuRFStartLevel,
	}
}

func NewExternalTableBuilder(f *os.File, limiter *rate.Limiter, opt options.TableBuilderOptions, compression options.CompressionType) *Builder {
	return &Builder{
		idxFileName: f.Name() + idxFileSuffix,
		w:           fileutil.NewDirectWriter(f, opt.WriteBufferSize, limiter),
		buf:         make([]byte, 0, 4*1024),
		baseKeysBuf: make([]byte, 0, 4*1024),
		hashEntries: make([]hashEntry, 0, 4*1024),
		bloomFpr:    opt.LogicalBloomFPR,
		isExternal:  true,
		compression: compression,
		opt:         opt,
	}
}

// Reset this builder with new file.
func (b *Builder) Reset(f *os.File) {
	b.resetBuffers()
	b.w.Reset(f)
	b.idxFileName = f.Name() + idxFileSuffix
}

func (b *Builder) resetBuffers() {
	b.counter = 0
	b.buf = b.buf[:0]
	b.writtenLen = 0
	b.rawWrittenLen = 0
	b.baseKeysBuf = b.baseKeysBuf[:0]
	b.baseKeysEndOffs = b.baseKeysEndOffs[:0]
	b.blockBaseKey = b.blockBaseKey[:0]
	b.blockEndOffsets = b.blockEndOffsets[:0]
	b.entryEndOffsets = b.entryEndOffsets[:0]
	b.hashEntries = b.hashEntries[:0]
	b.surfKeys = nil
	b.surfVals = nil
	b.smallest = b.smallest[:0]
	b.biggest = b.biggest[:0]
}

// Close closes the TableBuilder.
func (b *Builder) Close() {}

// Empty returns whether it's empty.
func (b *Builder) Empty() bool { return b.writtenLen+len(b.buf) == 0 }

// keyDiff returns a suffix of newKey that is different from b.blockBaseKey.
func (b Builder) keyDiff(newKey []byte) []byte {
	for i := 0; i < len(newKey) && i < len(b.blockBaseKey); i++ {
		if newKey[i] != b.blockBaseKey[i] {
			return newKey[i:]
		}
	}
	return newKey
}

func (b *Builder) addIndex(key []byte) {
	keyNoTs := key
	if !b.isExternal {
		keyNoTs = y.ParseKey(key)
	}

	if len(b.smallest) == 0 {
		b.smallest = append(b.smallest, key...)
	}

	if len(b.biggest) > 0 {
		prev := b.biggest
		if !b.isExternal {
			prev = y.ParseKey(b.biggest)
		}
		cmp := bytes.Compare(keyNoTs, prev)
		y.Assert(cmp >= 0)
		if cmp == 0 {
			return
		}
	}
	b.biggest = y.SafeCopy(b.biggest, key)

	keyHash := farm.Fingerprint64(keyNoTs)
	// It is impossible that a single table contains 16 million keys.
	y.Assert(len(b.baseKeysEndOffs) < maxBlockCnt)

	pos := entryPosition{uint16(len(b.baseKeysEndOffs)), uint8(b.counter)}
	if b.useSuRF {
		b.surfKeys = append(b.surfKeys, y.SafeCopy(nil, keyNoTs))
		b.surfVals = append(b.surfVals, pos.encode())
	} else {
		b.hashEntries = append(b.hashEntries, hashEntry{pos, keyHash})
	}
}

func (b *Builder) addHelper(key []byte, v y.ValueStruct) {
	// Add key to bloom filter.
	if len(key) > 0 {
		b.addIndex(key)
	}

	// diffKey stores the difference of key with blockBaseKey.
	var diffKey []byte
	if len(b.blockBaseKey) == 0 {
		// Make a copy. Builder should not keep references. Otherwise, caller has to be very careful
		// and will have to make copies of keys every time they add to builder, which is even worse.
		b.blockBaseKey = append(b.blockBaseKey[:0], key...)
		diffKey = key
	} else {
		diffKey = b.keyDiff(key)
	}

	h := header{
		baseLen: uint16(len(key) - len(diffKey)),
		diffLen: uint16(len(diffKey)),
	}
	b.buf = append(b.buf, h.Encode()...)
	b.buf = append(b.buf, diffKey...) // We only need to store the key difference.
	b.buf = v.EncodeTo(b.buf)
	b.entryEndOffsets = append(b.entryEndOffsets, uint32(len(b.buf)))
	b.counter++ // Increment number of keys added for this current block.
}

func (b *Builder) finishBlock() error {
	b.buf = append(b.buf, u32SliceToBytes(b.entryEndOffsets)...)
	b.buf = append(b.buf, u32ToBytes(uint32(len(b.entryEndOffsets)))...)

	// Add base key.
	b.baseKeysBuf = append(b.baseKeysBuf, b.blockBaseKey...)
	b.baseKeysEndOffs = append(b.baseKeysEndOffs, uint32(len(b.baseKeysBuf)))

	data := b.buf
	if b.compression != options.None {
		var err error
		// TODO: Find a way to reuse buffers. Current implementation creates a
		// new buffer for each compressData call.
		data, err = b.compressData(b.buf)
		y.Check(err)
	}

	if err := b.w.Append(data); err != nil {
		return err
	}
	b.blockEndOffsets = append(b.blockEndOffsets, uint32(b.writtenLen+len(data)))
	b.writtenLen += len(data)
	b.rawWrittenLen += len(b.buf)

	// Reset the block for the next build.
	b.entryEndOffsets = b.entryEndOffsets[:0]
	b.counter = 0
	b.blockBaseKey = b.blockBaseKey[:0]
	b.buf = b.buf[:0]
	return nil
}

// Add adds a key-value pair to the block.
// If doNotRestart is true, we will not restart even if b.counter >= restartInterval.
func (b *Builder) Add(key []byte, value y.ValueStruct) error {
	if b.shouldFinishBlock(key, value) {
		if err := b.finishBlock(); err != nil {
			return err
		}
	}
	b.addHelper(key, value)
	return nil // Currently, there is no meaningful error.
}

func (b *Builder) shouldFinishBlock(key []byte, value y.ValueStruct) bool {
	// If there is no entry till now, we will return false.
	if len(b.entryEndOffsets) == 0 {
		return false
	}

	// We should include current entry also in size, that's why +1 to len(b.entryOffsets).
	entriesOffsetsSize := uint32((len(b.entryEndOffsets)+1)*4 + 4)
	estimatedSize := uint32(len(b.buf)) + uint32(6 /*header size for entry*/) +
		uint32(len(key)) + uint32(value.EncodedSize()) + entriesOffsetsSize

	return estimatedSize > uint32(b.opt.BlockSize)
}

// ReachedCapacity returns true if we... roughly (?) reached capacity?
func (b *Builder) ReachedCapacity(capacity int64) bool {
	estimateSz := b.rawWrittenLen + len(b.buf) +
		4*len(b.blockEndOffsets) +
		len(b.baseKeysBuf) +
		4*len(b.baseKeysEndOffs)
	return int64(estimateSz) > capacity
}

// EstimateSize returns the size of the SST to build.
func (b *Builder) EstimateSize() int {
	size := b.rawWrittenLen + len(b.buf) + 4*len(b.blockEndOffsets) + len(b.baseKeysBuf) + 4*len(b.baseKeysEndOffs)
	if !b.useSuRF {
		size += 3 * int(float32(len(b.hashEntries))/b.opt.HashUtilRatio)
	}
	return size
}

const (
	idSmallest byte = iota
	idBiggest
	idBaseKeysEndOffs
	idBaseKeys
	idBlockEndOffsets
	idBloomFilter
	idHashIndex
	idSuRFIndex
)

// Finish finishes the table by appending the index.
func (b *Builder) Finish() error {
	b.finishBlock() // This will never start a new block.
	if err := b.w.Finish(); err != nil {
		return err
	}
	idxFile, err := y.OpenTruncFile(b.idxFileName, false)
	if err != nil {
		return err
	}
	b.w.Reset(idxFile)

	// Don't compress the global ts, because it may be updated during ingest.
	ts := uint64(math.MaxUint64)
	if b.isExternal {
		// External builder doesn't append ts to the keys, the output sst should has a non-MaxUint64 global ts.
		ts = math.MaxUint64 - 1
	}
	if err := b.w.Append(u64ToBytes(ts)); err != nil {
		return err
	}

	encoder := metaEncoder{b.buf}

	encoder.append(b.smallest, idSmallest)
	encoder.append(b.biggest, idBiggest)
	encoder.append(u32SliceToBytes(b.baseKeysEndOffs), idBaseKeysEndOffs)
	encoder.append(b.baseKeysBuf, idBaseKeys)
	encoder.append(u32SliceToBytes(b.blockEndOffsets), idBlockEndOffsets)

	var bloomFilter []byte
	if !b.useSuRF {
		bf := bbloom.New(float64(len(b.hashEntries)), b.bloomFpr)
		for _, he := range b.hashEntries {
			bf.Add(he.hash)
		}
		bloomFilter = bf.BinaryMarshal()
	}
	encoder.append(bloomFilter, idBloomFilter)

	var hashIndex []byte
	if !b.useSuRF {
		hashIndex = buildHashIndex(b.hashEntries, b.opt.HashUtilRatio)
	}
	encoder.append(hashIndex, idHashIndex)

	var surfIndex []byte
	if b.useSuRF && len(b.surfKeys) > 0 {
		hl := uint32(b.opt.SuRFOptions.HashSuffixLen)
		rl := uint32(b.opt.SuRFOptions.RealSuffixLen)
		sb := surf.NewBuilder(3, hl, rl)
		sf := sb.Build(b.surfKeys, b.surfVals, b.opt.SuRFOptions.BitsPerKeyHint)
		surfIndex = sf.Marshal()
	}
	encoder.append(surfIndex, idSuRFIndex)

	idxData := encoder.buf
	if b.compression != options.None {
		if idxData, err = b.compressData(idxData); err != nil {
			return err
		}
	}
	if err := b.w.Append(idxData); err != nil {
		return err
	}

	return b.w.Finish()
}

func u32ToBytes(v uint32) []byte {
	var uBuf [4]byte
	binary.LittleEndian.PutUint32(uBuf[:], v)
	return uBuf[:]
}

func u64ToBytes(v uint64) []byte {
	var uBuf [8]byte
	binary.LittleEndian.PutUint64(uBuf[:], v)
	return uBuf[:]
}

func u32SliceToBytes(u32s []uint32) []byte {
	if len(u32s) == 0 {
		return nil
	}
	var b []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	hdr.Len = len(u32s) * 4
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&u32s[0]))
	return b
}

func bytesToU32Slice(b []byte) []uint32 {
	if len(b) == 0 {
		return nil
	}
	var u32s []uint32
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&u32s))
	hdr.Len = len(b) / 4
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&b[0]))
	return u32s
}

func bytesToU32(b []byte) uint32 {
	return binary.LittleEndian.Uint32(b)
}

func bytesToU64(b []byte) uint64 {
	return binary.LittleEndian.Uint64(b)
}

// compressData compresses the given data.
func (b *Builder) compressData(data []byte) ([]byte, error) {
	switch b.compression {
	case options.None:
		return data, nil
	case options.Snappy:
		return snappy.Encode(nil, data), nil
	case options.ZSTD:
		return zstd.Compress(nil, data)
	}
	return nil, errors.New("Unsupported compression type")
}

type metaEncoder struct {
	buf []byte
}

func (e *metaEncoder) append(d []byte, id byte) {
	e.buf = append(e.buf, id)
	e.buf = append(e.buf, u32ToBytes(uint32(len(d)))...)
	e.buf = append(e.buf, d...)
}

type metaDecoder struct {
	buf    []byte
	cursor int
}

func (e *metaDecoder) valid() bool {
	return e.cursor < len(e.buf)
}

func (e *metaDecoder) currentId() byte {
	return e.buf[e.cursor]
}

func (e *metaDecoder) decode() []byte {
	e.cursor++
	l := int(bytesToU32(e.buf[e.cursor:]))
	e.cursor += 4
	d := e.buf[e.cursor : e.cursor+l]
	e.cursor += l
	return d
}

func (e *metaDecoder) skip() {
	l := int(bytesToU32(e.buf[e.cursor+1:]))
	e.cursor += 1 + 4 + l
}
