package surf

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type BitsVecTestSuite struct {
	suite.Suite
}

func TestBitsVecTestSuite(t *testing.T) {
	suite.Run(t, new(BitsVecTestSuite))
}

func TestSelect64(t *testing.T) {
	cases := [][]uint64{
		{0, 3, 1},
		{33, 3 << 32, 2},
		{63, 1 << 63, 1},
		{9, 0b11101011001010101, 5},
	}
	t.Run("broadword", func(t *testing.T) {
		for _, c := range cases {
			require.EqualValues(t, c[0], select64Broadword(c[1], int64(c[2])))
		}
	})
	t.Run("bmi2", func(t *testing.T) {
		for _, c := range cases {
			require.EqualValues(t, c[0], select64(c[1], int64(c[2])))
		}
	})
	t.Run("fallback", func(t *testing.T) {
		hasBMI2 = false
		for _, c := range cases {
			require.EqualValues(t, c[0], select64(c[1], int64(c[2])))
		}
		hasBMI2 = true
	})
}

func TestBitSetAndRead(t *testing.T) {
	for i := 0; i < 128; i++ {
		var bits [2]uint64
		setBit(bits[:], uint32(i))
		require.True(t, readBit(bits[:], uint32(i)))
	}
}

func TestPopCount(t *testing.T) {
	cases := [][]int{
		{0, 1, 2, 3, 4},
		{0, 2, 16, 17, 33, 62},
		{63, 64, 65, 66},
		{63, 127},
		{64},
	}

	for _, c := range cases {
		bits, nbits := constructBits(c)
		require.EqualValues(t, len(c), popcountBlock(bits, 0, nbits))
		require.EqualValues(t, len(c)-1, popcountBlock(bits, 0, nbits-1))
	}
}

func TestBitVector(t *testing.T) {
	cases := [][][]int{
		{
			{0, 1, 24, 60},
			{0, 31, 127},
			{4},
		},
		{
			{23, 44},
			{0, 122, 123, 456},
			{0, 1, 2, 3, 4, 5, 62, 63},
			{127, 128, 129, 255, 257},
		},
	}

	for _, c := range cases {
		var vec bitVector
		numBitsPerLevel := make([]uint32, len(c))
		bitsPerLevel := make([][]uint64, len(c))
		for l, p := range c {
			bitsPerLevel[l], numBitsPerLevel[l] = constructBits(p)
		}
		vec.Init(bitsPerLevel, numBitsPerLevel)

		off := uint32(0)
		for l, p := range c {
			for i, pos := range p {
				idx := off + uint32(pos)

				require.True(t, vec.IsSet(idx))

				dist := vec.DistanceToNextSetBit(idx)
				var expected int
				if i == len(p)-1 {
					if l < len(c)-1 {
						expected = c[l+1][0] + 1
					} else {
						expected = 1
					}
				} else {
					expected = p[i+1] - pos
				}
				require.EqualValues(t, expected, dist)

				dist = vec.DistanceToPrevSetBit(idx)
				if i == 0 {
					expected = pos + 1
				} else {
					expected = pos - p[i-1]
				}
				require.EqualValuesf(t, expected, dist, "level %d, pos %d", l, pos)
			}
			off += numBitsPerLevel[l]
		}
	}
}

func TestSelectVector(t *testing.T) {
	cases := [][][]int{
		{
			{0, 1, 24, 60},
			{0, 31, 127},
			{4},
		},
		{
			{0, 23, 44},
			{0, 122, 123, 456},
			{0, 1, 2, 3, 4, 5, 62, 63},
			{127, 128, 129, 255, 257},
		},
	}

	for _, c := range cases {
		var vec selectVector
		numBitsPerLevel := make([]uint32, len(c))
		bitsPerLevel := make([][]uint64, len(c))
		for l, p := range c {
			bitsPerLevel[l], numBitsPerLevel[l] = constructBits(p)
		}
		vec.Init(bitsPerLevel, numBitsPerLevel)

		off, rank := uint32(0), uint32(1)
		for l, p := range c {
			for _, pos := range p {
				idx := off + uint32(pos)
				sr := vec.Select(rank)

				require.EqualValuesf(t, idx, sr, "level: %d, pos: %d, rank: %d", l, pos, rank)
				rank++
			}
			off += numBitsPerLevel[l]
		}
	}
}

func constructBits(sets []int) ([]uint64, uint32) {
	nbits := sets[len(sets)-1] + 1
	words := nbits / wordSize
	if nbits%wordSize != 0 {
		words++
	}
	bits := make([]uint64, words)
	for _, i := range sets {
		setBit(bits, uint32(i))
	}
	return bits, uint32(nbits)
}

func BenchmarkSelect64(b *testing.B) {
	b.Run("BMI2", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			select64(0b01101010101011110111101011001010101, 10)
		}
	})

	b.Run("fallback", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			select64Broadword(0b01101010101011110111101011001010101, 10)
		}
	})
}

func TestLabelVecSearch(t *testing.T) {
	labels := [][]byte{
		{1},
		{2, 3},
		{4, 5, 6},
		{labelTerminator, 7, 8, 9},
	}
	v := new(labelVector)
	v.Init(labels, 0, uint32(len(labels)))
	labelShouldExist := func(k byte, start, size, pos uint32) {
		r, ok := v.Search(k, start, size)
		require.True(t, ok)
		require.Equal(t, pos, r)
	}
	labelShouldExist(1, 0, 1, 0)
	labelShouldExist(3, 0, 5, 2)
	labelShouldExist(5, 3, 7, 4)
	labelShouldExist(7, 6, 8, 7)
}
