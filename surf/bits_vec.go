package surf

import (
	"bytes"
	"io"
	"math/bits"
	"sort"
)

type bitVector struct {
	numBits uint32
	bits    []uint64
}

func (v *bitVector) numWords() uint32 {
	wordSz := v.numBits / wordSize
	if v.numBits%wordSize != 0 {
		wordSz++
	}
	return wordSz
}

func (v *bitVector) bitsSize() uint32 {
	return v.numWords() * 8
}

func (v *bitVector) Init(bitsPerLevel [][]uint64, numBitsPerLevel []uint32) {
	for _, n := range numBitsPerLevel {
		v.numBits += n
	}

	v.bits = make([]uint64, v.numWords())

	var wordID, bitShift uint32
	for level, bits := range bitsPerLevel {
		n := numBitsPerLevel[level]
		if n == 0 {
			continue
		}

		nCompleteWords := n / wordSize
		for word := 0; uint32(word) < nCompleteWords; word++ {
			v.bits[wordID] |= bits[word] << bitShift
			wordID++
			if bitShift > 0 {
				v.bits[wordID] |= bits[word] >> (wordSize - bitShift)
			}
		}

		remain := n % wordSize
		if remain > 0 {
			lastWord := bits[nCompleteWords]
			v.bits[wordID] |= lastWord << bitShift
			if bitShift+remain <= wordSize {
				bitShift = (bitShift + remain) % wordSize
				if bitShift == 0 {
					wordID++
				}
			} else {
				wordID++
				v.bits[wordID] |= lastWord >> (wordSize - bitShift)
				bitShift = bitShift + remain - wordSize
			}
		}
	}
}

func (v *bitVector) IsSet(pos uint32) bool {
	return readBit(v.bits, pos)
}

func (v *bitVector) DistanceToNextSetBit(pos uint32) uint32 {
	var distance uint32 = 1
	wordOff := (pos + 1) / wordSize
	bitsOff := (pos + 1) % wordSize

	if wordOff >= uint32(len(v.bits)) {
		return 0
	}

	testBits := v.bits[wordOff] >> bitsOff
	if testBits > 0 {
		return distance + uint32(bits.TrailingZeros64(testBits))
	}

	numWords := v.numWords()
	if wordOff == numWords-1 {
		return v.numBits - pos
	}
	distance += wordSize - bitsOff

	for wordOff < numWords-1 {
		wordOff++
		testBits = v.bits[wordOff]
		if testBits > 0 {
			return distance + uint32(bits.TrailingZeros64(testBits))
		}
		distance += wordSize
	}

	if wordOff == numWords-1 && v.numBits%64 != 0 {
		distance -= wordSize - v.numBits%64
	}

	return distance
}

func (v *bitVector) DistanceToPrevSetBit(pos uint32) uint32 {
	if pos == 0 {
		return 1
	}
	distance := uint32(1)
	wordOff := (pos - 1) / wordSize
	bitsOff := (pos - 1) % wordSize

	testBits := v.bits[wordOff] << (wordSize - 1 - bitsOff)
	if testBits > 0 {
		return distance + uint32(bits.LeadingZeros64(testBits))
	}
	distance += bitsOff + 1

	for wordOff > 0 {
		wordOff--
		testBits = v.bits[wordOff]
		if testBits > 0 {
			return distance + uint32(bits.LeadingZeros64(testBits))
		}
		distance += wordSize
	}
	return distance
}

type valueVector struct {
	bytes     []byte
	valueSize uint32
}

func (v *valueVector) Init(valuesPerLevel [][]byte, valueSize uint32) {
	var size int
	for l := range valuesPerLevel {
		size += len(valuesPerLevel[l])
	}
	v.valueSize = valueSize
	v.bytes = make([]byte, size)

	var pos uint32
	for _, val := range valuesPerLevel {
		copy(v.bytes[pos:], val)
		pos += uint32(len(val))
	}
}

func (v *valueVector) Get(pos uint32) []byte {
	off := pos * v.valueSize
	return v.bytes[off : off+v.valueSize]
}

func (v *valueVector) MarshalSize() int64 {
	return align(v.rawMarshalSize())
}

func (v *valueVector) rawMarshalSize() int64 {
	return 8 + int64(len(v.bytes))
}

func (v *valueVector) WriteTo(w io.Writer) error {
	var bs [4]byte
	endian.PutUint32(bs[:], uint32(len(v.bytes)))
	if _, err := w.Write(bs[:]); err != nil {
		return err
	}

	endian.PutUint32(bs[:], v.valueSize)
	if _, err := w.Write(bs[:]); err != nil {
		return err
	}

	if _, err := w.Write(v.bytes); err != nil {
		return err
	}

	var zeros [8]byte
	padding := v.MarshalSize() - v.rawMarshalSize()
	_, err := w.Write(zeros[:padding])
	return err
}

func (v *valueVector) Unmarshal(buf []byte) []byte {
	var cursor int64
	sz := int64(endian.Uint32(buf))
	cursor += 4

	v.valueSize = endian.Uint32(buf[cursor:])
	cursor += 4

	v.bytes = buf[cursor : cursor+sz]
	cursor = align(cursor + sz)

	return buf[cursor:]
}

const selectSampleInterval = 64

type selectVector struct {
	bitVector
	numOnes   uint32
	selectLut []uint32
}

func (v *selectVector) Init(bitsPerLevel [][]uint64, numBitsPerLevel []uint32) *selectVector {
	v.bitVector.Init(bitsPerLevel, numBitsPerLevel)
	lut := []uint32{0}
	sampledOnes := selectSampleInterval
	onesUptoWord := 0
	for i, w := range v.bits {
		ones := bits.OnesCount64(w)
		for sampledOnes <= onesUptoWord+ones {
			diff := sampledOnes - onesUptoWord
			targetPos := i*wordSize + int(select64(w, int64(diff)))
			lut = append(lut, uint32(targetPos))
			sampledOnes += selectSampleInterval
		}
		onesUptoWord += ones
	}

	v.numOnes = uint32(onesUptoWord)
	v.selectLut = make([]uint32, len(lut))
	for i := range v.selectLut {
		v.selectLut[i] = lut[i]
	}

	return v
}

func (v *selectVector) lutSize() uint32 {
	return (v.numOnes/selectSampleInterval + 1) * 4
}

// Select returns the postion of the rank-th 1 bit.
// position is zero-based; rank is one-based.
// E.g., for bitvector: 100101000, select(3) = 5
func (v *selectVector) Select(rank uint32) uint32 {
	lutIdx := rank / selectSampleInterval
	rankLeft := rank % selectSampleInterval
	if lutIdx == 0 {
		rankLeft--
	}

	pos := v.selectLut[lutIdx]
	if rankLeft == 0 {
		return pos
	}

	wordOff := pos / wordSize
	bitsOff := pos % wordSize
	if bitsOff == wordSize-1 {
		wordOff++
		bitsOff = 0
	} else {
		bitsOff++
	}

	w := v.bits[wordOff] >> bitsOff << bitsOff
	ones := uint32(bits.OnesCount64(w))
	for ones < rankLeft {
		wordOff++
		w = v.bits[wordOff]
		rankLeft -= ones
		ones = uint32(bits.OnesCount64(w))
	}

	return wordOff*wordSize + uint32(select64(w, int64(rankLeft)))
}

func (v *selectVector) MarshalSize() int64 {
	return align(v.rawMarshalSize())
}

func (v *selectVector) rawMarshalSize() int64 {
	return 4 + 4 + int64(v.bitsSize()) + int64(v.lutSize())
}

func (v *selectVector) WriteTo(w io.Writer) error {
	var buf [4]byte
	endian.PutUint32(buf[:], v.numBits)
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}
	endian.PutUint32(buf[:], v.numOnes)
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}
	if _, err := w.Write(u64SliceToBytes(v.bits)); err != nil {
		return err
	}
	if _, err := w.Write(u32SliceToBytes(v.selectLut)); err != nil {
		return err
	}

	var zeros [8]byte
	padding := v.MarshalSize() - v.rawMarshalSize()
	_, err := w.Write(zeros[:padding])
	return err
}

func (v *selectVector) Unmarshal(buf []byte) []byte {
	var cursor int64
	v.numBits = endian.Uint32(buf)
	cursor += 4
	v.numOnes = endian.Uint32(buf[cursor:])
	cursor += 4

	bitsSize := int64(v.bitsSize())
	v.bits = bytesToU64Slice(buf[cursor : cursor+bitsSize])
	cursor += bitsSize

	lutSize := int64(v.lutSize())
	v.selectLut = bytesToU32Slice(buf[cursor : cursor+lutSize])
	cursor = align(cursor + lutSize)
	return buf[cursor:]
}

const (
	rankDenseBlockSize  = 64
	rankSparseBlockSize = 512
)

type rankVector struct {
	bitVector
	blockSize uint32
	rankLut   []uint32
}

func (v *rankVector) init(blockSize uint32, bitsPerLevel [][]uint64, numBitsPerLevel []uint32) *rankVector {
	v.bitVector.Init(bitsPerLevel, numBitsPerLevel)
	v.blockSize = blockSize
	wordPerBlk := v.blockSize / wordSize
	nblks := v.numBits/v.blockSize + 1
	v.rankLut = make([]uint32, nblks)

	var totalRank, i uint32
	for i = 0; i < nblks-1; i++ {
		v.rankLut[i] = totalRank
		totalRank += popcountBlock(v.bits, i*wordPerBlk, v.blockSize)
	}
	v.rankLut[nblks-1] = totalRank
	return v
}

func (v *rankVector) lutSize() uint32 {
	return (v.numBits/v.blockSize + 1) * 4
}

func (v *rankVector) MarshalSize() int64 {
	return align(v.rawMarshalSize())
}

func (v *rankVector) rawMarshalSize() int64 {
	return 4 + 4 + int64(v.bitsSize()) + int64(v.lutSize())
}

func (v *rankVector) WriteTo(w io.Writer) error {
	var buf [4]byte
	endian.PutUint32(buf[:], v.numBits)
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}
	endian.PutUint32(buf[:], v.blockSize)
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}
	if _, err := w.Write(u64SliceToBytes(v.bits)); err != nil {
		return err
	}
	if _, err := w.Write(u32SliceToBytes(v.rankLut)); err != nil {
		return err
	}

	var zeros [8]byte
	padding := v.MarshalSize() - v.rawMarshalSize()
	_, err := w.Write(zeros[:padding])
	return err
}

func (v *rankVector) Unmarshal(buf []byte) []byte {
	var cursor int64
	v.numBits = endian.Uint32(buf)
	cursor += 4
	v.blockSize = endian.Uint32(buf[cursor:])
	cursor += 4

	bitsSize := int64(v.bitsSize())
	v.bits = bytesToU64Slice(buf[cursor : cursor+bitsSize])
	cursor += bitsSize

	lutSize := int64(v.lutSize())
	v.rankLut = bytesToU32Slice(buf[cursor : cursor+lutSize])
	cursor = align(cursor + lutSize)
	return buf[cursor:]
}

type rankVectorDense struct {
	rankVector
}

func (v *rankVectorDense) Init(bitsPerLevel [][]uint64, numBitsPerLevel []uint32) {
	v.rankVector.init(rankDenseBlockSize, bitsPerLevel, numBitsPerLevel)
}

func (v *rankVectorDense) Rank(pos uint32) uint32 {
	wordPreBlk := uint32(rankDenseBlockSize / wordSize)
	blockOff := pos / rankDenseBlockSize
	bitsOff := pos % rankDenseBlockSize

	return v.rankLut[blockOff] + popcountBlock(v.bits, blockOff*wordPreBlk, bitsOff+1)
}

type rankVectorSparse struct {
	rankVector
}

func (v *rankVectorSparse) Init(bitsPerLevel [][]uint64, numBitsPerLevel []uint32) {
	v.rankVector.init(rankSparseBlockSize, bitsPerLevel, numBitsPerLevel)
}

func (v *rankVectorSparse) Rank(pos uint32) uint32 {
	wordPreBlk := uint32(rankSparseBlockSize / wordSize)
	blockOff := pos / rankSparseBlockSize
	bitsOff := pos % rankSparseBlockSize

	return v.rankLut[blockOff] + popcountBlock(v.bits, blockOff*wordPreBlk, bitsOff+1)
}

const labelTerminator = 0xff

type labelVector struct {
	labels []byte
}

func (v *labelVector) Init(labelsPerLevel [][]byte, startLevel, endLevel uint32) {
	numBytes := 1
	for l := startLevel; l < endLevel; l++ {
		numBytes += len(labelsPerLevel[l])
	}
	v.labels = make([]byte, numBytes)

	var pos uint32
	for l := startLevel; l < endLevel; l++ {
		copy(v.labels[pos:], labelsPerLevel[l])
		pos += uint32(len(labelsPerLevel[l]))
	}
}

func (v *labelVector) GetLabel(pos uint32) byte {
	return v.labels[pos]
}

func (v *labelVector) Search(k byte, start, size uint32) (uint32, bool) {
	if size > 1 && v.labels[start] == labelTerminator {
		start++
		size--
	}

	end := start + size
	if end > uint32(len(v.labels)) {
		end = uint32(len(v.labels))
	}
	result := bytes.IndexByte(v.labels[start:end], k)
	if result < 0 {
		return start, false
	}
	return start + uint32(result), true
}

func (v *labelVector) SearchGreaterThan(label byte, pos, size uint32) (uint32, bool) {
	if size > 1 && v.labels[pos] == labelTerminator {
		pos++
		size--
	}

	result := sort.Search(int(size), func(i int) bool { return v.labels[pos+uint32(i)] > label })
	if uint32(result) == size {
		return pos + uint32(result) - 1, false
	}
	return pos + uint32(result), true
}

func (v *labelVector) MarshalSize() int64 {
	return align(v.rawMarshalSize())
}

func (v *labelVector) rawMarshalSize() int64 {
	return 4 + int64(len(v.labels))
}

func (v *labelVector) WriteTo(w io.Writer) error {
	var bs [4]byte
	endian.PutUint32(bs[:], uint32(len(v.labels)))
	if _, err := w.Write(bs[:]); err != nil {
		return err
	}
	if _, err := w.Write(v.labels); err != nil {
		return err
	}

	padding := v.MarshalSize() - v.rawMarshalSize()
	var zeros [8]byte
	_, err := w.Write(zeros[:padding])
	return err
}

func (v *labelVector) Unmarshal(buf []byte) []byte {
	l := endian.Uint32(buf)
	v.labels = buf[4 : 4+l]
	return buf[align(int64(4+l)):]
}
