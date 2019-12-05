package surf

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildPrefixKeys(t *testing.T) {
	buildAndCheckSuRF(t, [][]byte{
		{1},
		{1, 1},
		{1, 1, 1},
		{1, 1, 1, 1},
		{2},
		{2, 2},
		{2, 2, 2},
	})
}

func TestBuildCompressPath(t *testing.T) {
	buildAndCheckSuRF(t, [][]byte{
		{1, 1, 1},
		{1, 1, 1, 2, 2},
		{1, 1, 1, 2, 2, 2},
		{1, 1, 1, 2, 2, 3},
		{2, 1, 3},
		{2, 2, 3},
		{2, 3, 1, 1, 1, 1, 1, 1, 1},
		{2, 3, 1, 1, 1, 2, 2, 2, 2},
	})
}

func TestRandomKeysSparse(t *testing.T) {
	buildAndCheckSuRF(t, randomKeys(2000000, 60, 0))
}

func TestRandomKeysPrefixGrowth(t *testing.T) {
	buildAndCheckSuRF(t, randomKeys(100, 10, 200))
}

func TestBuildSuffixKeys(t *testing.T) {
	buildAndCheckSuRF(t, [][]byte{
		bytes.Repeat([]byte{1}, 30),
		bytes.Repeat([]byte{2}, 30),
		bytes.Repeat([]byte{3}, 30),
		bytes.Repeat([]byte{4}, 30),
	})
}

func TestMarshal(t *testing.T) {
	keys := randomKeys(30, 20, 300)
	vals := make([][]byte, len(keys))
	for i := range keys {
		vals[i] = make([]byte, 4)
		endian.PutUint32(vals[i], uint32(i))
	}
	b := NewBuilder(4, 13, 13)
	s1 := b.Build(keys, vals, 60)
	var s2 SuRF
	buf := s1.Marshal()
	s2.Unmarshal(buf)
	s1.checkEquals(t, &s2)
	checkSuRF(t, &s2, keys, vals)
}

// max key length is `initLen * (round + 1)`
// max result size is (initSize + initSize * (round + 1)) * (round + 1) / 2
// you can use small round (0 is allowed) to generate a sparse key set,
// or use a large round to generate a key set which has many common prefixes.
func randomKeys(initSize, initLen, round int) [][]byte {
	keys := make([][]byte, initSize)
	rand := rand.New(rand.NewSource(0xdeadbeaf))
	for i := range keys {
		keys[i] = make([]byte, rand.Intn(initLen)+1)
		rand.Read(keys[i])
	}

	for r := 1; r <= round; r++ {
		for i := 0; i < initSize*r; i++ {
			k := make([]byte, len(keys[i])+rand.Intn(initLen)+1)
			copy(k, keys[i])
			rand.Read(k[len(keys[i]):])
			keys = append(keys, k)
		}
	}

	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})

	var prev []byte
	result := keys[:0]
	for _, k := range keys {
		if bytes.Equal(prev, k) {
			continue
		}
		prev = k
		result = append(result, k)
	}
	for i := len(result); i < len(keys); i++ {
		keys[i] = nil
	}

	return result
}

func buildAndCheckSuRF(t *testing.T, keys [][]byte) {
	suffixLens := [][]uint32{
		{0, 0},
		{4, 0},
		{13, 0},
		{32, 0},
		{0, 4},
		{0, 13},
		{0, 32},
		{3, 3},
		{8, 8},
	}

	for _, sl := range suffixLens {
		b := NewBuilder(4, sl[0], sl[1])
		vals := make([][]byte, len(keys))
		for i := range keys {
			vals[i] = make([]byte, 4)
			endian.PutUint32(vals[i], uint32(i))
		}

		b.totalCount = len(keys)
		b.buildNodes(keys, vals, 0, 0, 0)
		for i := 0; i < b.treeHeight(); i++ {
			b.sparseStartLevel = uint32(i)
			b.ldLabels = b.ldLabels[:0]
			b.ldHasChild = b.ldHasChild[:0]
			b.ldIsPrefix = b.ldIsPrefix[:0]
			b.buildDense()

			surf := new(SuRF)
			surf.ld.Init(b)
			surf.ls.Init(b)

			t.Run(fmt.Sprintf("cutoff=%d,hashLen=%d,realLen=%d", i, sl[0], sl[1]), func(t *testing.T) {
				t.Parallel()
				checkSuRF(t, surf, keys, vals)
			})
		}
	}
}

func checkSuRF(t *testing.T, surf *SuRF, keys, vals [][]byte) {
	for i, k := range keys {
		val, ok := surf.Get(k)
		require.True(t, ok)
		require.EqualValues(t, vals[i], val)
	}

	// TODO: add iterator check
}

func (v *rankVector) checkEquals(t *testing.T, o *rankVector) {
	require.Equal(t, v.numBits, o.numBits)
	require.Equal(t, v.lutSize(), o.lutSize())
	if v.numBits != 0 {
		require.Equal(t, v.bits, o.bits)
	}
	require.Equal(t, v.rankLut, o.rankLut)
}

func (v *selectVector) checkEquals(t *testing.T, o *selectVector) {
	require.Equal(t, v.numBits, o.numBits)
	require.Equal(t, v.numOnes, o.numOnes)
	require.Equal(t, v.lutSize(), o.lutSize())
	require.Equal(t, v.bits, o.bits)
	require.Equal(t, v.selectLut, o.selectLut)
}

func (v *suffixVector) checkEquals(t *testing.T, o *suffixVector) {
	require.Equal(t, v.numBits, o.numBits)
	if v.numBits != 0 {
		require.Equal(t, v.bits, o.bits)
	}
	require.Equal(t, v.hashSuffixLen, o.hashSuffixLen)
	require.Equal(t, v.realSuffixLen, o.realSuffixLen)
}

func (v *valueVector) checkEquals(t *testing.T, o *valueVector) {
	require.Equal(t, v.bytes, o.bytes)
	require.Equal(t, v.valueSize, o.valueSize)
}

func (v *labelVector) checkEquals(t *testing.T, o *labelVector) {
	require.Equal(t, v.labels, o.labels)
}

func (ld *loudsDense) checkEquals(t *testing.T, o *loudsDense) {
	require.Equal(t, ld.height, o.height)
	ld.labelVec.checkEquals(t, &o.labelVec.rankVector)
	ld.hasChildVec.checkEquals(t, &o.hasChildVec.rankVector)
	ld.isPrefixVec.checkEquals(t, &o.isPrefixVec.rankVector)
	ld.suffixes.checkEquals(t, &o.suffixes)
	ld.values.checkEquals(t, &o.values)
}

func (ls *loudsSparse) checkEquals(t *testing.T, o *loudsSparse) {
	require.Equal(t, ls.height, o.height)
	require.Equal(t, ls.startLevel, o.startLevel)
	require.Equal(t, ls.denseChildCount, o.denseChildCount)
	require.Equal(t, ls.denseNodeCount, o.denseNodeCount)
	ls.labelVec.checkEquals(t, &o.labelVec)
	ls.hasChildVec.checkEquals(t, &o.hasChildVec.rankVector)
	ls.loudsVec.checkEquals(t, &o.loudsVec)
	ls.suffixes.checkEquals(t, &o.suffixes)
	ls.values.checkEquals(t, &o.values)
}

func (s *SuRF) checkEquals(t *testing.T, o *SuRF) {
	s.ld.checkEquals(t, &o.ld)
	s.ls.checkEquals(t, &o.ls)
}
