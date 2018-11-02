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

type position struct {
	restart uint16
	offset  uint8
}

type hashIndexBuilder struct {
	buf []struct {
		hash uint32
		pos  position
	}
	estimatedNumBuckets float32
	invalid             bool
}

func (b *hashIndexBuilder) addKey(key []byte, restart uint32, offset uint8) {
	if restart > maxRestart || b.invalid {
		b.invalid = true
		b.buf = nil
		return
	}
	b.estimatedNumBuckets += 1 / hashUtilRatio
	h := farm.Fingerprint32(key)
	b.buf = append(b.buf, struct {
		hash uint32
		pos  position
	}{h, position{uint16(restart), offset}})
}

func (b *hashIndexBuilder) finish(buf []byte) []byte {
	if b.invalid || len(b.buf) == 0 {
		return append(buf, u32ToBytes(0)...)
	}
	numBucket := uint32(b.estimatedNumBuckets)
	buckets := make([]position, numBucket)
	for i := range buckets {
		buckets[i].restart = resultNoEntry
	}

	for _, pair := range b.buf {
		idx := pair.hash % numBucket
		restart := buckets[idx].restart
		if restart == resultNoEntry {
			buckets[idx] = pair.pos
		} else if restart != pair.pos.restart {
			buckets[idx].restart = resultFallback
		}
	}

	cursor := len(buf)
	buf = append(buf, make([]byte, numBucket*3+4)...)
	for _, e := range buckets {
		binary.LittleEndian.PutUint16(buf[cursor:], e.restart)
		buf[cursor+2] = e.offset
		cursor += 3
	}
	return append(buf[:cursor], u32ToBytes(numBucket)...)
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
