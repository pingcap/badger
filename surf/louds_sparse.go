package surf

import "io"

type loudsSparse struct {
	height          uint32
	startLevel      uint32
	denseNodeCount  uint32
	denseChildCount uint32

	labelVec    labelVector
	hasChildVec rankVectorSparse
	loudsVec    selectVector
	suffixes    suffixVector
	values      valueVector
	prefixVec   prefixVector
}

func (ls *loudsSparse) Init(builder *Builder) *loudsSparse {
	ls.height = uint32(len(builder.lsLabels))
	ls.startLevel = builder.sparseStartLevel

	for l := 0; uint32(l) < ls.startLevel; l++ {
		ls.denseNodeCount += builder.nodeCounts[l]
	}

	if ls.startLevel != 0 {
		ls.denseChildCount = ls.denseNodeCount + builder.nodeCounts[ls.startLevel] - 1
	}

	ls.labelVec.Init(builder.lsLabels, ls.startLevel, ls.height)

	numItemsPerLevel := make([]uint32, ls.sparseLevels())
	for level := range numItemsPerLevel {
		numItemsPerLevel[level] = uint32(len(builder.lsLabels[int(ls.startLevel)+level]))
	}
	ls.hasChildVec.Init(builder.lsHasChild[ls.startLevel:], numItemsPerLevel)
	ls.loudsVec.Init(builder.lsLoudsBits[ls.startLevel:], numItemsPerLevel)

	if builder.suffixLen() != 0 {
		hashLen := builder.hashSuffixLen
		realLen := builder.realSuffixLen
		suffixLen := hashLen + realLen
		numSuffixBitsPerLevel := make([]uint32, ls.sparseLevels())
		for i := range numSuffixBitsPerLevel {
			numSuffixBitsPerLevel[i] = builder.suffixCounts[int(ls.startLevel)+i] * suffixLen
		}
		ls.suffixes.Init(hashLen, realLen, builder.suffixes[ls.startLevel:], numSuffixBitsPerLevel)
	}

	ls.values.Init(builder.values[ls.startLevel:], builder.valueSize)
	ls.prefixVec.Init(builder.hasPrefix[ls.startLevel:], builder.nodeCounts[ls.startLevel:], builder.prefixes[ls.startLevel:])

	return ls
}

func (ls *loudsSparse) Get(key []byte, startDepth, nodeID uint32) (value []byte, ok bool) {
	var (
		pos       = ls.firstLabelPos(nodeID)
		depth     uint32
		prefixLen uint32
	)
	for depth = startDepth; depth < uint32(len(key)); depth++ {
		prefixLen, ok = ls.prefixVec.CheckPrefix(key, depth, ls.prefixID(nodeID))
		if !ok {
			return nil, false
		}
		depth += prefixLen

		if depth >= uint32(len(key)) {
			break
		}

		if pos, ok = ls.labelVec.Search(key[depth], pos, ls.nodeSize(pos)); !ok {
			return nil, false
		}

		if !ls.hasChildVec.IsSet(pos) {
			valPos := ls.suffixPos(pos)
			if ok = ls.suffixes.CheckEquality(valPos, key, depth+1); ok {
				value = ls.values.Get(valPos)
			}
			return value, ok
		}

		nodeID = ls.childNodeID(pos)
		pos = ls.firstLabelPos(nodeID)
	}

	if ls.labelVec.GetLabel(pos) == labelTerminator && !ls.hasChildVec.IsSet(pos) {
		valPos := ls.suffixPos(pos)
		if ok = ls.suffixes.CheckEquality(valPos, key, depth+1); ok {
			value = ls.values.Get(valPos)
		}
		return value, ok
	}

	return nil, false
}

func (ls *loudsSparse) MarshalSize() int64 {
	return align(ls.rawMarshalSize())
}

func (ls *loudsSparse) rawMarshalSize() int64 {
	return 4*4 + ls.labelVec.MarshalSize() + ls.hasChildVec.MarshalSize() + ls.loudsVec.MarshalSize() +
		ls.suffixes.MarshalSize() + ls.prefixVec.MarshalSize()
}

func (ls *loudsSparse) WriteTo(w io.Writer) error {
	var bs [4]byte
	endian.PutUint32(bs[:], ls.height)
	if _, err := w.Write(bs[:]); err != nil {
		return err
	}
	endian.PutUint32(bs[:], ls.startLevel)
	if _, err := w.Write(bs[:]); err != nil {
		return err
	}
	endian.PutUint32(bs[:], ls.denseNodeCount)
	if _, err := w.Write(bs[:]); err != nil {
		return err
	}
	endian.PutUint32(bs[:], ls.denseChildCount)
	if _, err := w.Write(bs[:]); err != nil {
		return err
	}
	if err := ls.labelVec.WriteTo(w); err != nil {
		return err
	}
	if err := ls.hasChildVec.WriteTo(w); err != nil {
		return err
	}
	if err := ls.loudsVec.WriteTo(w); err != nil {
		return err
	}
	if err := ls.suffixes.WriteTo(w); err != nil {
		return err
	}
	if err := ls.prefixVec.WriteTo(w); err != nil {
		return err
	}

	padding := ls.MarshalSize() - ls.rawMarshalSize()
	var zeros [8]byte
	_, err := w.Write(zeros[:padding])
	return err
}

func (ls *loudsSparse) Unmarshal(buf []byte) []byte {
	buf1 := buf
	ls.height = endian.Uint32(buf1)
	buf1 = buf1[4:]
	ls.startLevel = endian.Uint32(buf1)
	buf1 = buf1[4:]
	ls.denseNodeCount = endian.Uint32(buf1)
	buf1 = buf1[4:]
	ls.denseChildCount = endian.Uint32(buf1)
	buf1 = buf1[4:]

	buf1 = ls.labelVec.Unmarshal(buf1)
	buf1 = ls.hasChildVec.Unmarshal(buf1)
	buf1 = ls.loudsVec.Unmarshal(buf1)
	buf1 = ls.suffixes.Unmarshal(buf1)
	buf1 = ls.prefixVec.Unmarshal(buf1)

	sz := align(int64(len(buf) - len(buf1)))
	return buf[sz:]
}

func (ls *loudsSparse) suffixPos(pos uint32) uint32 {
	return pos - ls.hasChildVec.Rank(pos)
}

func (ls *loudsSparse) firstLabelPos(nodeID uint32) uint32 {
	return ls.loudsVec.Select(nodeID + 1 - ls.denseNodeCount)
}

func (ls *loudsSparse) sparseLevels() uint32 {
	return ls.height - ls.startLevel
}
func (ls *loudsSparse) prefixID(nodeID uint32) uint32 {
	return nodeID - ls.denseNodeCount
}

func (ls *loudsSparse) lastLabelPos(nodeID uint32) uint32 {
	nextRank := nodeID + 2 - ls.denseNodeCount
	if nextRank > ls.loudsVec.numOnes {
		return ls.loudsVec.numBits - 1
	}
	return ls.loudsVec.Select(nextRank) - 1
}

func (ls *loudsSparse) childNodeID(pos uint32) uint32 {
	return ls.hasChildVec.Rank(pos) + ls.denseChildCount
}

func (ls *loudsSparse) nodeSize(pos uint32) uint32 {
	return ls.loudsVec.DistanceToNextSetBit(pos)
}

func (ls *loudsSparse) isEndOfNode(pos uint32) bool {
	return pos == ls.loudsVec.numBits-1 || ls.loudsVec.IsSet(pos+1)
}
