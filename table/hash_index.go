package table

import (
	"encoding/binary"
)

const (
	resultNoEntry  = 65535
	resultFallback = 65534
	maxBlockCnt    = 65533
)

type hashIndexBuilder struct {
	entries       []indexEntry
	hashUtilRatio float32
	invalid       bool
}

type indexEntry struct {
	hash uint32

	// blockIdx is the index of block which contains this key.
	blockIdx uint16

	// offset is the index of this key in block.
	offset uint8
}

func newHashIndexBuilder(hashUtilRatio float32) hashIndexBuilder {
	return hashIndexBuilder{
		hashUtilRatio: hashUtilRatio,
	}
}

func (b *hashIndexBuilder) addKey(keyHash uint64, blkIdx uint32, offset uint8) {
	if blkIdx > maxBlockCnt {
		b.invalid = true
		b.entries = nil
		return
	}
	b.entries = append(b.entries, indexEntry{uint32(keyHash), uint16(blkIdx), offset})
}

func (b *hashIndexBuilder) finish(buf []byte) []byte {
	if b.invalid || len(b.entries) == 0 {
		return append(buf, u32ToBytes(0)...)
	}

	numBuckets := b.numBuckets()
	bufLen := len(buf)
	buf = append(buf, make([]byte, numBuckets*3+4)...)
	buckets := buf[bufLen:]
	for i := 0; i < int(numBuckets); i++ {
		binary.LittleEndian.PutUint16(buckets[i*3:], resultNoEntry)
	}

	for _, h := range b.entries {
		idx := h.hash % numBuckets
		bucket := buckets[idx*3 : (idx+1)*3]
		blkIdx := binary.LittleEndian.Uint16(bucket[:2])
		if blkIdx == resultNoEntry {
			binary.LittleEndian.PutUint16(bucket[:2], h.blockIdx)
			bucket[2] = h.offset
		} else if blkIdx != h.blockIdx {
			binary.LittleEndian.PutUint16(bucket[:2], resultFallback)
		}
	}
	copy(buckets[numBuckets*3:], u32ToBytes(numBuckets))

	return buf
}

func (b *hashIndexBuilder) numBuckets() uint32 {
	return uint32(float32(len(b.entries)) / b.hashUtilRatio)
}

func (b *hashIndexBuilder) reset() {
	b.entries = b.entries[:0]
	b.invalid = false
}

type hashIndex struct {
	buckets    []byte
	numBuckets int
}

func (i *hashIndex) readIndex(buf []byte, numBucket int) {
	i.buckets = buf
	i.numBuckets = numBucket
}

func (i *hashIndex) lookup(keyHash uint64) (uint32, uint8) {
	if i.buckets == nil {
		return resultFallback, 0
	}
	idx := int(keyHash) % i.numBuckets
	buf := i.buckets[idx*3:]
	blkIdx := binary.LittleEndian.Uint16(buf)
	return uint32(blkIdx), uint8(buf[2])
}
