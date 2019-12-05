package surf

import (
	"bytes"
	"fmt"
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

func TestBuildSuffixKeys(t *testing.T) {
	buildAndCheckSuRF(t, [][]byte{
		bytes.Repeat([]byte{1}, 30),
		bytes.Repeat([]byte{2}, 30),
		bytes.Repeat([]byte{3}, 30),
		bytes.Repeat([]byte{4}, 30),
	})
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
				for i, k := range keys {
					val, ok := surf.Get(k)
					require.True(t, ok)
					require.EqualValues(t, vals[i], val)
				}

				// TODO: add iterator check
			})
		}
	}
}
