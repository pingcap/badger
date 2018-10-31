package table

import (
	"encoding/binary"
	"github.com/dgryski/go-farm"
)

const (
	hashUtilRatio  = 0.75
	resultNoEntry  = 65535
	resultFallback = 65534
	maxRestart     = 65533
)

type hashIndexBuilder struct {
	pos []struct {
		hash    uint32
		restart uint16
		offset  uint8
	}
	estimatedNumBuckets float32
	invalid             bool
}

func (b *hashIndexBuilder) addKey(key []byte, restart uint32, offset uint8) {
	if restart > maxRestart || b.invalid {
		b.invalid = true
		return
	}
	b.estimatedNumBuckets += 1 / hashUtilRatio
	h := farm.Fingerprint32(key)
	b.pos = append(b.pos, struct {
		hash    uint32
		restart uint16
		offset  uint8
	}{h, uint16(restart), offset})
}

func (b *hashIndexBuilder) finish(buf []byte) []byte {
	if b.invalid || len(b.pos) == 0 {
		return append(buf, u32ToBytes(0)...)
	}
	numBucket := uint32(b.estimatedNumBuckets)
	buckets := make([]uint32, numBucket)
	for i := range buckets {
		buckets[i] = resultNoEntry << 8
	}

	for _, pair := range b.pos {
		idx := pair.hash % numBucket
		restart := buckets[idx] >> 8
		if restart == resultNoEntry {
			buckets[idx] = uint32(pair.restart)<<8 | uint32(pair.offset)
		} else if restart != uint32(pair.restart) {
			buckets[idx] = uint32(resultFallback) << 8
		}
	}

	encodeBuf := [2]byte{}
	for _, e := range buckets {
		binary.LittleEndian.PutUint16(encodeBuf[:], uint16(e>>8))
		buf = append(buf, encodeBuf[:]...)
		buf = append(buf, byte(e&0xff))
	}
	return append(buf, u32ToBytes(numBucket)...)
}

type hashIndex struct {
	buckets    []byte
	numBuckets int
}

func (i *hashIndex) readIndex(buf []byte, numBucket int) {
	i.buckets = buf
	i.numBuckets = numBucket
}

func (i *hashIndex) lookup(key []byte) (uint32, uint8) {
	if i.buckets == nil {
		return resultFallback, 0
	}
	h := farm.Fingerprint32(key)
	idx := int(h) % i.numBuckets
	buf := i.buckets[idx*3:]
	restart := binary.LittleEndian.Uint16(buf)
	return uint32(restart), uint8(buf[2])
}
