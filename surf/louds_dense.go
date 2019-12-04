package surf

import "io"

const (
	denseFanout      = 256
	denseRankBlkSize = 512
)

type loudsDense struct {
	labelVec    rankVectorDense
	hasChildVec rankVectorDense
	isPrefixVec rankVectorDense
	suffixes    suffixVector
	values      valueVector
	prefixVec   prefixVector

	// height is dense end level.
	height uint32
}

func (ld *loudsDense) Init(builder *Builder) *loudsDense {
	ld.height = builder.sparseStartLevel

	numBitsPerLevel := make([]uint32, ld.height)
	for level := range numBitsPerLevel {
		numBitsPerLevel[level] = uint32(len(builder.ldLabels[level]) * wordSize)
	}

	ld.labelVec.Init(builder.ldLabels[:ld.height], numBitsPerLevel)
	ld.hasChildVec.Init(builder.ldHasChild[:ld.height], numBitsPerLevel)
	ld.isPrefixVec.Init(builder.ldIsPrefix[:ld.height], builder.nodeCounts)

	if builder.suffixLen() != 0 {
		hashLen := builder.hashSuffixLen
		realLen := builder.realSuffixLen
		suffixLen := hashLen + realLen
		numSuffixBitsPerLevel := make([]uint32, ld.height)
		for i := range numSuffixBitsPerLevel {
			numSuffixBitsPerLevel[i] = builder.suffixCounts[i] * suffixLen
		}
		ld.suffixes.Init(hashLen, realLen, builder.suffixes[:ld.height], numSuffixBitsPerLevel)
	}

	ld.values.Init(builder.values[:ld.height], builder.valueSize)
	ld.prefixVec.Init(builder.hasPrefix[:ld.height], builder.nodeCounts[:ld.height], builder.prefixes[:ld.height])

	return ld
}

func (ld *loudsDense) Get(key []byte) (sparseNode int64, depth uint32, value []byte, ok bool) {
	var nodeID, pos uint32
	for level := uint32(0); level < ld.height; level++ {
		prefixLen, ok := ld.prefixVec.CheckPrefix(key, depth, nodeID)
		if !ok {
			return -1, depth, nil, false
		}
		depth += prefixLen

		pos = nodeID * denseFanout
		if depth >= uint32(len(key)) {
			if ok = ld.isPrefixVec.IsSet(nodeID); ok {
				valPos := ld.suffixPos(pos, true)
				if ok = ld.suffixes.CheckEquality(valPos, key, depth+1); ok {
					value = ld.values.Get(valPos)
				}
			}
			return -1, depth, value, ok
		}
		pos += uint32(key[depth])

		if !ld.labelVec.IsSet(pos) {
			return -1, depth, nil, false
		}

		if !ld.hasChildVec.IsSet(pos) {
			valPos := ld.suffixPos(pos, false)
			if ok = ld.suffixes.CheckEquality(valPos, key, depth+1); ok {
				value = ld.values.Get(valPos)
			}
			return -1, depth, value, ok
		}

		nodeID = ld.childNodeID(pos)
		depth++
	}

	return int64(nodeID), depth, nil, true
}

func (ld *loudsDense) MarshalSize() int64 {
	return align(ld.rawMarshalSize())
}

func (ld *loudsDense) rawMarshalSize() int64 {
	return 4 + ld.labelVec.MarshalSize() + ld.hasChildVec.MarshalSize() + ld.isPrefixVec.MarshalSize() + ld.suffixes.MarshalSize() + ld.prefixVec.MarshalSize()
}

func (ld *loudsDense) WriteTo(w io.Writer) error {
	var bs [4]byte
	endian.PutUint32(bs[:], ld.height)

	if _, err := w.Write(bs[:]); err != nil {
		return err
	}
	if err := ld.labelVec.WriteTo(w); err != nil {
		return err
	}
	if err := ld.hasChildVec.WriteTo(w); err != nil {
		return err
	}
	if err := ld.isPrefixVec.WriteTo(w); err != nil {
		return err
	}
	if err := ld.suffixes.WriteTo(w); err != nil {
		return err
	}
	if err := ld.prefixVec.WriteTo(w); err != nil {
		return err
	}

	padding := ld.MarshalSize() - ld.rawMarshalSize()
	var zeros [8]byte
	_, err := w.Write(zeros[:padding])
	return err
}

func (ld *loudsDense) Unmarshal(buf []byte) []byte {
	ld.height = endian.Uint32(buf)
	buf1 := buf[4:]
	buf1 = ld.labelVec.Unmarshal(buf1)
	buf1 = ld.hasChildVec.Unmarshal(buf1)
	buf1 = ld.isPrefixVec.Unmarshal(buf1)
	buf1 = ld.suffixes.Unmarshal(buf1)
	buf1 = ld.prefixVec.Unmarshal(buf1)

	sz := align(int64(len(buf) - len(buf1)))
	return buf[sz:]
}

func (ld *loudsDense) childNodeID(pos uint32) uint32 {
	return ld.hasChildVec.Rank(pos)
}

func (ld *loudsDense) suffixPos(pos uint32, isPrefix bool) uint32 {
	nodeID := pos / denseFanout
	suffixPos := ld.labelVec.Rank(pos) - ld.hasChildVec.Rank(pos) + ld.isPrefixVec.Rank(nodeID) - 1

	// Correct off by one error when current have a leaf node at label 0.
	// Otherwise suffixPos will point to that leaf node's suffix.
	if isPrefix && ld.labelVec.IsSet(pos) && !ld.hasChildVec.IsSet(pos) {
		suffixPos--
	}
	return suffixPos
}

func (ld *loudsDense) nextPos(pos uint32) uint32 {
	return pos + ld.labelVec.DistanceToNextSetBit(pos)
}

func (ld *loudsDense) prevPos(pos uint32) (uint32, bool) {
	dist := ld.labelVec.DistanceToPrevSetBit(pos)
	if pos <= dist {
		return 0, true
	}
	return pos - dist, false
}
