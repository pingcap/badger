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
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"sort"
	"testing"

	"github.com/coocood/badger/options"
	"github.com/coocood/badger/y"
	"github.com/dgryski/go-farm"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func key(prefix string, i int) string {
	return prefix + fmt.Sprintf("%04d", i)
}

var defaultBuilderOpt = options.TableBuilderOptions{
	EnableHashIndex:     true,
	HashUtilRatio:       0.75,
	WriteBufferSize:     1024 * 1024,
	MaxLevels:           7,
	LevelSizeMultiplier: 10,
	LogicalBloomFPR:     0.01,
}

func generateKeyValues(prefix string, n int) [][]string {
	y.Assert(n <= 10000)
	keyValues := make([][]string, n)
	for i := 0; i < n; i++ {
		k := key(prefix, i)
		v := fmt.Sprintf("%d", i)
		keyValues[i] = []string{k, v}
	}
	return keyValues
}

func buildTestTable(t *testing.T, prefix string, n int) *os.File {
	return buildTable(t, generateKeyValues(prefix, n))
}

// keyValues is n by 2 where n is number of pairs.
func buildTable(t *testing.T, keyValues [][]string) *os.File {
	// TODO: Add test for file garbage collection here. No files should be left after the tests here.

	filename := fmt.Sprintf("%s%s%x.sst", os.TempDir(), string(os.PathSeparator), rand.Int63())
	f, err := y.OpenSyncedFile(filename, true)
	if t != nil {
		require.NoError(t, err)
	} else {
		y.Check(err)
	}
	b := NewTableBuilder(f, rate.NewLimiter(rate.Inf, math.MaxInt32), 0, defaultBuilderOpt)

	sort.Slice(keyValues, func(i, j int) bool {
		return keyValues[i][0] < keyValues[j][0]
	})
	for _, kv := range keyValues {
		y.Assert(len(kv) == 2)
		err := b.Add(y.KeyWithTs([]byte(kv[0]), 0), y.ValueStruct{Value: []byte(kv[1]), Meta: 'A', UserMeta: []byte{0}})
		if t != nil {
			require.NoError(t, err)
		} else {
			y.Check(err)
		}
	}
	y.Check(b.Finish())
	f.Close()
	f, _ = y.OpenSyncedFile(filename, true)
	return f
}

func TestTableIterator(t *testing.T) {
	for _, n := range []int{99, 100, 101} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			f := buildTestTable(t, "key", n)
			table, err := OpenTable(f, options.MemoryMap)
			require.NoError(t, err)
			defer table.Delete()
			it := table.NewIterator(false)
			defer it.Close()
			count := 0
			for it.Rewind(); it.Valid(); it.Next() {
				v := it.Value()
				k := y.KeyWithTs([]byte(key("key", count)), 0)
				require.EqualValues(t, k, it.Key())
				require.EqualValues(t, fmt.Sprintf("%d", count), string(v.Value))
				count++
			}
			require.Equal(t, count, n)
		})
	}
}

func TestHashIndexTS(t *testing.T) {
	filename := fmt.Sprintf("%s%s%x.sst", os.TempDir(), string(os.PathSeparator), rand.Int63())
	f, err := y.OpenSyncedFile(filename, true)
	if t != nil {
		require.NoError(t, err)
	} else {
		y.Check(err)
	}
	b := NewTableBuilder(f, nil, 0, defaultBuilderOpt)
	keys := [][]byte{
		y.KeyWithTs([]byte("key"), 9),
		y.KeyWithTs([]byte("key"), 7),
		y.KeyWithTs([]byte("key"), 5),
		y.KeyWithTs([]byte("key"), 3),
		y.KeyWithTs([]byte("key"), 1),
	}
	for _, k := range keys {
		b.Add(k, y.ValueStruct{Value: k, Meta: 'A', UserMeta: []byte{0}})
	}
	y.Check(b.Finish())
	f.Close()
	f, _ = y.OpenSyncedFile(filename, true)
	table, err := OpenTable(f, options.MemoryMap)
	keyHash := farm.Fingerprint64([]byte("key"))

	rk, _, ok := table.PointGet(y.KeyWithTs([]byte("key"), 10), keyHash)
	require.True(t, ok)
	require.True(t, bytes.Compare(rk, keys[0]) == 0)

	rk, _, ok = table.PointGet(y.KeyWithTs([]byte("key"), 6), keyHash)
	require.True(t, ok)
	require.True(t, bytes.Compare(rk, keys[2]) == 0)

	rk, _, ok = table.PointGet(y.KeyWithTs([]byte("key"), 2), keyHash)
	require.True(t, ok)
	require.True(t, bytes.Compare(rk, keys[4]) == 0)
}

func TestPointGet(t *testing.T) {
	f := buildTestTable(t, "key", 8000)
	table, err := OpenTable(f, options.MemoryMap)
	require.NoError(t, err)
	defer table.Delete()

	for i := 0; i < 8000; i++ {
		k := y.KeyWithTs([]byte(key("key", i)), math.MaxUint64)
		keyHash := farm.Fingerprint64(y.ParseKey(k))
		k, _, ok := table.PointGet(k, keyHash)
		if !ok {
			// will fallback to seek
			continue
		}
		require.NotNil(t, k, key("key", i)+" not find")
		require.True(t, y.SameKey(k, k), "point get not point to correct key")
	}

	for i := 8000; i < 10000; i++ {
		k := y.KeyWithTs([]byte(key("key", i)), math.MaxUint64)
		keyHash := farm.Fingerprint64(y.ParseKey(k))
		rk, _, ok := table.PointGet(k, keyHash)
		if !ok {
			// will fallback to seek
			continue
		}
		if rk == nil {
			// hash table says no entry, fast return
			continue
		}
		require.False(t, y.SameKey(k, rk), "point get not point to correct key")
	}
}

func TestExternalTable(t *testing.T) {
	filename := fmt.Sprintf("%s%s%x.sst", os.TempDir(), string(os.PathSeparator), rand.Int63())
	f, err := y.OpenSyncedFile(filename, true)
	if t != nil {
		require.NoError(t, err)
	} else {
		y.Check(err)
	}

	n := 200
	b := NewExternalTableBuilder(f, rate.NewLimiter(rate.Inf, math.MaxInt32), defaultBuilderOpt)
	kvs := generateKeyValues("key", n)
	for _, kv := range kvs {
		y.Assert(len(kv) == 2)
		err := b.Add([]byte(kv[0]), y.ValueStruct{Value: []byte(kv[1]), Meta: 'A', UserMeta: []byte{0}})
		if t != nil {
			require.NoError(t, err)
		} else {
			y.Check(err)
		}
	}
	y.Check(b.Finish())
	f.Close()
	f, _ = y.OpenSyncedFile(filename, true)
	table, err := OpenTable(f, options.MemoryMap)
	require.NoError(t, err)
	require.NoError(t, table.SetGlobalTs(10))

	require.NoError(t, f.Close())
	f, _ = y.OpenSyncedFile(filename, true)
	table, err = OpenTable(f, options.MemoryMap)
	require.NoError(t, err)
	defer table.Delete()

	it := table.NewIterator(false)
	defer it.Close()
	count := 0
	for it.Rewind(); it.Valid(); it.Next() {
		v := it.Value()
		k := y.KeyWithTs([]byte(key("key", count)), 10)
		require.EqualValues(t, k, it.Key())
		require.EqualValues(t, fmt.Sprintf("%d", count), string(v.Value))
		count++
	}
	require.Equal(t, count, n)
}

func TestSeekToFirst(t *testing.T) {
	for _, n := range []int{99, 100, 101, 199, 200, 250, 9999, 10000} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			f := buildTestTable(t, "key", n)
			table, err := OpenTable(f, options.MemoryMap)
			require.NoError(t, err)
			defer table.Delete()
			it := table.NewIterator(false)
			defer it.Close()
			it.seekToFirst()
			require.True(t, it.Valid())
			v := it.Value()
			require.EqualValues(t, "0", string(v.Value))
			require.EqualValues(t, 'A', v.Meta)
		})
	}
}

func TestSeekToLast(t *testing.T) {
	for _, n := range []int{99, 100, 101, 199, 200, 250, 9999, 10000} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			f := buildTestTable(t, "key", n)
			table, err := OpenTable(f, options.MemoryMap)
			require.NoError(t, err)
			defer table.Delete()
			it := table.NewIterator(false)
			defer it.Close()
			it.seekToLast()
			require.True(t, it.Valid())
			v := it.Value()
			require.EqualValues(t, fmt.Sprintf("%d", n-1), string(v.Value))
			require.EqualValues(t, 'A', v.Meta)
			it.prev()
			require.True(t, it.Valid())
			v = it.Value()
			require.EqualValues(t, fmt.Sprintf("%d", n-2), string(v.Value))
			require.EqualValues(t, 'A', v.Meta)
		})
	}
}

func TestSeek(t *testing.T) {
	f := buildTestTable(t, "k", 10000)
	table, err := OpenTable(f, options.MemoryMap)
	require.NoError(t, err)
	defer table.Delete()

	it := table.NewIterator(false)
	defer it.Close()

	var data = []struct {
		in    string
		valid bool
		out   string
	}{
		{"abc", true, "k0000"},
		{"k0100", true, "k0100"},
		{"k0100b", true, "k0101"}, // Test case where we jump to next block.
		{"k1234", true, "k1234"},
		{"k1234b", true, "k1235"},
		{"k9999", true, "k9999"},
		{"z", false, ""},
	}

	for _, tt := range data {
		it.seek(y.KeyWithTs([]byte(tt.in), 0))
		if !tt.valid {
			require.False(t, it.Valid())
			continue
		}
		require.True(t, it.Valid())
		k := it.Key()
		require.EqualValues(t, tt.out, string(y.ParseKey(k)))
	}
}

func TestSeekForPrev(t *testing.T) {
	f := buildTestTable(t, "k", 10000)
	table, err := OpenTable(f, options.MemoryMap)
	require.NoError(t, err)
	defer table.Delete()

	it := table.NewIterator(false)
	defer it.Close()

	var data = []struct {
		in    string
		valid bool
		out   string
	}{
		{"abc", false, ""},
		{"k0100", true, "k0100"},
		{"k0100b", true, "k0100"}, // Test case where we jump to next block.
		{"k1234", true, "k1234"},
		{"k1234b", true, "k1234"},
		{"k9999", true, "k9999"},
		{"z", true, "k9999"},
	}

	for _, tt := range data {
		it.seekForPrev(y.KeyWithTs([]byte(tt.in), 0))
		if !tt.valid {
			require.False(t, it.Valid())
			continue
		}
		require.True(t, it.Valid())
		k := it.Key()
		require.EqualValues(t, tt.out, string(y.ParseKey(k)))
	}
}

func TestIterateFromStart(t *testing.T) {
	// Vary the number of elements added.
	for _, n := range []int{99, 100, 101, 199, 200, 250, 9999, 10000} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			f := buildTestTable(t, "key", n)
			table, err := OpenTable(f, options.MemoryMap)
			require.NoError(t, err)
			defer table.Delete()
			ti := table.NewIterator(false)
			defer ti.Close()
			ti.reset()
			ti.seekToFirst()
			require.True(t, ti.Valid())
			// No need to do a Next.
			// ti.Seek brings us to the first key >= "". Essentially a SeekToFirst.
			var count int
			for ; ti.Valid(); ti.next() {
				v := ti.Value()
				require.EqualValues(t, fmt.Sprintf("%d", count), string(v.Value))
				require.EqualValues(t, 'A', v.Meta)
				count++
			}
			require.EqualValues(t, n, count)
		})
	}
}

func TestIterateFromEnd(t *testing.T) {
	// Vary the number of elements added.
	for _, n := range []int{99, 100, 101, 199, 200, 250, 9999, 10000} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			f := buildTestTable(t, "key", n)
			table, err := OpenTable(f, options.FileIO)
			require.NoError(t, err)
			defer table.Delete()
			ti := table.NewIterator(false)
			defer ti.Close()
			ti.reset()
			ti.seek(y.KeyWithTs([]byte("zzzzzz"), 0)) // Seek to end, an invalid element.
			require.False(t, ti.Valid())
			for i := n - 1; i >= 0; i-- {
				ti.prev()
				require.True(t, ti.Valid())
				v := ti.Value()
				require.EqualValues(t, fmt.Sprintf("%d", i), string(v.Value))
				require.EqualValues(t, 'A', v.Meta)
			}
			ti.prev()
			require.False(t, ti.Valid())
		})
	}
}

func TestTable(t *testing.T) {
	f := buildTestTable(t, "key", 10000)
	table, err := OpenTable(f, options.FileIO)
	require.NoError(t, err)
	defer table.Delete()
	ti := table.NewIterator(false)
	defer ti.Close()
	kid := 1010
	seek := y.KeyWithTs([]byte(key("key", kid)), 0)
	for ti.seek(seek); ti.Valid(); ti.next() {
		k := ti.Key()
		require.EqualValues(t, string(y.ParseKey(k)), key("key", kid))
		kid++
	}
	if kid != 10000 {
		t.Errorf("Expected kid: 10000. Got: %v", kid)
	}

	ti.seek(y.KeyWithTs([]byte(key("key", 99999)), 0))
	require.False(t, ti.Valid())

	ti.seek(y.KeyWithTs([]byte(key("key", -1)), 0))
	require.True(t, ti.Valid())
	k := ti.Key()
	require.EqualValues(t, string(y.ParseKey(k)), key("key", 0))
}

func TestIterateBackAndForth(t *testing.T) {
	f := buildTestTable(t, "key", 10000)
	table, err := OpenTable(f, options.MemoryMap)
	require.NoError(t, err)
	defer table.Delete()

	seek := y.KeyWithTs([]byte(key("key", 1010)), 0)
	it := table.NewIterator(false)
	defer it.Close()
	it.seek(seek)
	require.True(t, it.Valid())
	k := it.Key()
	require.EqualValues(t, seek, k)

	it.prev()
	it.prev()
	require.True(t, it.Valid())
	k = it.Key()
	require.EqualValues(t, key("key", 1008), string(y.ParseKey(k)))

	it.next()
	it.next()
	require.True(t, it.Valid())
	k = it.Key()
	require.EqualValues(t, key("key", 1010), y.ParseKey(k))

	it.seek(y.KeyWithTs([]byte(key("key", 2000)), 0))
	require.True(t, it.Valid())
	k = it.Key()
	require.EqualValues(t, key("key", 2000), y.ParseKey(k))

	it.prev()
	require.True(t, it.Valid())
	k = it.Key()
	require.EqualValues(t, key("key", 1999), y.ParseKey(k))

	it.seekToFirst()
	k = it.Key()
	require.EqualValues(t, key("key", 0), y.ParseKey(k))
}

func TestUniIterator(t *testing.T) {
	f := buildTestTable(t, "key", 10000)
	table, err := OpenTable(f, options.MemoryMap)
	require.NoError(t, err)
	defer table.Delete()
	{
		it := table.NewIterator(false)
		defer it.Close()
		var count int
		for it.Rewind(); it.Valid(); it.Next() {
			v := it.Value()
			require.EqualValues(t, fmt.Sprintf("%d", count), string(v.Value))
			require.EqualValues(t, 'A', v.Meta)
			count++
		}
		require.EqualValues(t, 10000, count)
	}
	{
		it := table.NewIterator(true)
		defer it.Close()
		var count int
		for it.Rewind(); it.Valid(); it.Next() {
			v := it.Value()
			require.EqualValues(t, fmt.Sprintf("%d", 10000-1-count), string(v.Value))
			require.EqualValues(t, 'A', v.Meta)
			count++
		}
		require.EqualValues(t, 10000, count)
	}
}

// Try having only one table.
func TestConcatIteratorOneTable(t *testing.T) {
	f := buildTable(t, [][]string{
		{"k1", "a1"},
		{"k2", "a2"},
	})

	tbl, err := OpenTable(f, options.MemoryMap)
	require.NoError(t, err)
	defer tbl.Delete()

	it := NewConcatIterator([]*Table{tbl}, false)
	defer it.Close()

	it.Rewind()
	require.True(t, it.Valid())
	k := it.Key()
	require.EqualValues(t, "k1", string(y.ParseKey(k)))
	vs := it.Value()
	require.EqualValues(t, "a1", string(vs.Value))
	require.EqualValues(t, 'A', vs.Meta)
}

func TestConcatIterator(t *testing.T) {
	f := buildTestTable(t, "keya", 10000)
	f2 := buildTestTable(t, "keyb", 10000)
	f3 := buildTestTable(t, "keyc", 10000)
	tbl, err := OpenTable(f, options.MemoryMap)
	require.NoError(t, err)
	defer tbl.Delete()
	tbl2, err := OpenTable(f2, options.LoadToRAM)
	require.NoError(t, err)
	defer tbl2.Delete()
	tbl3, err := OpenTable(f3, options.LoadToRAM)
	require.NoError(t, err)
	defer tbl3.Delete()

	{
		it := NewConcatIterator([]*Table{tbl, tbl2, tbl3}, false)
		defer it.Close()
		it.Rewind()
		require.True(t, it.Valid())
		var count int
		for ; it.Valid(); it.Next() {
			vs := it.Value()
			require.EqualValues(t, fmt.Sprintf("%d", count%10000), string(vs.Value))
			require.EqualValues(t, 'A', vs.Meta)
			count++
		}
		require.EqualValues(t, 30000, count)

		it.Seek(y.KeyWithTs([]byte("a"), 0))
		require.EqualValues(t, "keya0000", string(y.ParseKey(it.Key())))
		vs := it.Value()
		require.EqualValues(t, "0", string(vs.Value))

		it.Seek(y.KeyWithTs([]byte("keyb"), 0))
		require.EqualValues(t, "keyb0000", string(y.ParseKey(it.Key())))
		vs = it.Value()
		require.EqualValues(t, "0", string(vs.Value))

		it.Seek(y.KeyWithTs([]byte("keyb9999b"), 0))
		require.EqualValues(t, "keyc0000", string(y.ParseKey(it.Key())))
		vs = it.Value()
		require.EqualValues(t, "0", string(vs.Value))

		it.Seek(y.KeyWithTs([]byte("keyd"), 0))
		require.False(t, it.Valid())
	}
	{
		it := NewConcatIterator([]*Table{tbl, tbl2, tbl3}, true)
		defer it.Close()
		it.Rewind()
		require.True(t, it.Valid())
		var count int
		for ; it.Valid(); it.Next() {
			vs := it.Value()
			require.EqualValues(t, fmt.Sprintf("%d", 10000-(count%10000)-1), string(vs.Value))
			require.EqualValues(t, 'A', vs.Meta)
			count++
		}
		require.EqualValues(t, 30000, count)

		it.Seek(y.KeyWithTs([]byte("a"), 0))
		require.False(t, it.Valid())

		it.Seek(y.KeyWithTs([]byte("keyb"), 0))
		require.EqualValues(t, "keya9999", string(y.ParseKey(it.Key())))
		vs := it.Value()
		require.EqualValues(t, "9999", string(vs.Value))

		it.Seek(y.KeyWithTs([]byte("keyb9999b"), 0))
		require.EqualValues(t, "keyb9999", string(y.ParseKey(it.Key())))
		vs = it.Value()
		require.EqualValues(t, "9999", string(vs.Value))

		it.Seek(y.KeyWithTs([]byte("keyd"), 0))
		require.EqualValues(t, "keyc9999", string(y.ParseKey(it.Key())))
		vs = it.Value()
		require.EqualValues(t, "9999", string(vs.Value))
	}
}

func TestMergingIterator(t *testing.T) {
	f1 := buildTable(t, [][]string{
		{"k1", "a1"},
		{"k2", "a2"},
	})
	f2 := buildTable(t, [][]string{
		{"k1", "b1"},
		{"k2", "b2"},
	})
	tbl1, err := OpenTable(f1, options.LoadToRAM)
	require.NoError(t, err)
	defer tbl1.Delete()
	tbl2, err := OpenTable(f2, options.LoadToRAM)
	require.NoError(t, err)
	defer tbl2.Delete()
	it1 := tbl1.NewIterator(false)
	it2 := NewConcatIterator([]*Table{tbl2}, false)
	it := NewMergeIterator([]y.Iterator{it1, it2}, false)
	defer it.Close()

	it.Rewind()
	require.True(t, it.Valid())
	k := it.Key()
	require.EqualValues(t, "k1", string(y.ParseKey(k)))
	vs := it.Value()
	require.EqualValues(t, "a1", string(vs.Value))
	require.EqualValues(t, 'A', vs.Meta)
	it.Next()

	require.True(t, it.Valid())
	k = it.Key()
	require.EqualValues(t, "k2", string(y.ParseKey(k)))
	vs = it.Value()
	require.EqualValues(t, "a2", string(vs.Value))
	require.EqualValues(t, 'A', vs.Meta)
	it.Next()

	require.False(t, it.Valid())
}

func TestMergingIteratorReversed(t *testing.T) {
	f1 := buildTable(t, [][]string{
		{"k1", "a1"},
		{"k2", "a2"},
	})
	f2 := buildTable(t, [][]string{
		{"k1", "b1"},
		{"k2", "b2"},
	})
	tbl1, err := OpenTable(f1, options.LoadToRAM)
	require.NoError(t, err)
	defer tbl1.Delete()
	tbl2, err := OpenTable(f2, options.LoadToRAM)
	require.NoError(t, err)
	defer tbl2.Delete()
	it1 := tbl1.NewIterator(true)
	it2 := NewConcatIterator([]*Table{tbl2}, true)
	it := NewMergeIterator([]y.Iterator{it1, it2}, true)
	defer it.Close()

	it.Rewind()
	require.True(t, it.Valid())
	k := it.Key()
	require.EqualValues(t, "k2", string(y.ParseKey(k)))
	vs := it.Value()
	require.EqualValues(t, "a2", string(vs.Value))
	require.EqualValues(t, 'A', vs.Meta)
	it.Next()

	require.True(t, it.Valid())
	k = it.Key()
	require.EqualValues(t, "k1", string(y.ParseKey(k)))
	vs = it.Value()
	require.EqualValues(t, "a1", string(vs.Value))
	require.EqualValues(t, 'A', vs.Meta)
	it.Next()

	require.False(t, it.Valid())
}

// Take only the first iterator.
func TestMergingIteratorTakeOne(t *testing.T) {
	f1 := buildTable(t, [][]string{
		{"k1", "a1"},
		{"k2", "a2"},
	})
	f2 := buildTable(t, [][]string{})

	t1, err := OpenTable(f1, options.LoadToRAM)
	require.NoError(t, err)
	defer t1.Delete()
	t2, err := OpenTable(f2, options.LoadToRAM)
	require.NoError(t, err)
	defer t2.Delete()

	it1 := NewConcatIterator([]*Table{t1}, false)
	it2 := NewConcatIterator([]*Table{t2}, false)
	it := NewMergeIterator([]y.Iterator{it1, it2}, false)
	defer it.Close()

	it.Rewind()
	require.True(t, it.Valid())
	k := it.Key()
	require.EqualValues(t, "k1", string(y.ParseKey(k)))
	vs := it.Value()
	require.EqualValues(t, "a1", string(vs.Value))
	require.EqualValues(t, 'A', vs.Meta)
	it.Next()

	require.True(t, it.Valid())
	k = it.Key()
	require.EqualValues(t, "k2", string(y.ParseKey(k)))
	vs = it.Value()
	require.EqualValues(t, "a2", string(vs.Value))
	require.EqualValues(t, 'A', vs.Meta)
	it.Next()

	require.False(t, it.Valid())
}

// Take only the second iterator.
func TestMergingIteratorTakeTwo(t *testing.T) {
	f1 := buildTable(t, [][]string{})
	f2 := buildTable(t, [][]string{
		{"k1", "a1"},
		{"k2", "a2"},
	})

	t1, err := OpenTable(f1, options.LoadToRAM)
	require.NoError(t, err)
	defer t1.Delete()
	t2, err := OpenTable(f2, options.LoadToRAM)
	require.NoError(t, err)
	defer t2.Delete()

	it1 := NewConcatIterator([]*Table{t1}, false)
	it2 := NewConcatIterator([]*Table{t2}, false)
	it := NewMergeIterator([]y.Iterator{it1, it2}, false)
	defer it.Close()

	it.Rewind()
	require.True(t, it.Valid())
	k := it.Key()
	require.EqualValues(t, "k1", string(y.ParseKey(k)))
	vs := it.Value()
	require.EqualValues(t, "a1", string(vs.Value))
	require.EqualValues(t, 'A', vs.Meta)
	it.Next()

	require.True(t, it.Valid())
	k = it.Key()
	require.EqualValues(t, "k2", string(y.ParseKey(k)))
	vs = it.Value()
	require.EqualValues(t, "a2", string(vs.Value))
	require.EqualValues(t, 'A', vs.Meta)
	it.Next()

	require.False(t, it.Valid())
}

func BenchmarkRead(b *testing.B) {
	n := 5 << 20
	filename := fmt.Sprintf("%s%s%d.sst", os.TempDir(), string(os.PathSeparator), rand.Int63())
	f, err := y.OpenSyncedFile(filename, true)
	y.Check(err)
	builder := NewTableBuilder(f, nil, 0, defaultBuilderOpt)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("%016x", i)
		v := fmt.Sprintf("%d", i)
		y.Check(builder.Add([]byte(k), y.ValueStruct{Value: []byte(v), Meta: 123, UserMeta: []byte{0}}))
	}

	y.Check(builder.Finish())
	tbl, err := OpenTable(f, options.MemoryMap)
	y.Check(err)
	defer tbl.Delete()

	//	y.Printf("Size of table: %d\n", tbl.Size())
	b.ResetTimer()
	// Iterate b.N times over the entire table.
	for i := 0; i < b.N; i++ {
		func() {
			it := tbl.NewIterator(false)
			defer it.Close()
			for it.seekToFirst(); it.Valid(); it.next() {
			}
		}()
	}
}

func BenchmarkBuildTable(b *testing.B) {
	ns := []int{1000, 10000, 100000, 1000000, 5000000, 10000000, 15000000}
	for _, n := range ns {
		kvs := make([]struct{ k, v []byte }, n)
		for i := 0; i < n; i++ {
			kvs[i].k = y.KeyWithTs([]byte(fmt.Sprintf("%016x", i)), 0)
			kvs[i].v = []byte(fmt.Sprintf("%d", i))
		}
		b.ResetTimer()

		b.Run(fmt.Sprintf("NoHash_%d", n), func(b *testing.B) {
			filename := fmt.Sprintf("%s%s%d.sst", os.TempDir(), string(os.PathSeparator), rand.Int63())
			f, err := y.OpenSyncedFile(filename, false)
			y.Check(err)
			opt := defaultBuilderOpt
			opt.EnableHashIndex = false
			for bn := 0; bn < b.N; bn++ {
				builder := NewTableBuilder(f, nil, 0, opt)
				for i := 0; i < n; i++ {
					y.Check(builder.Add(kvs[i].k, y.ValueStruct{Value: kvs[i].v, Meta: 123, UserMeta: []byte{0}}))
				}
				y.Check(builder.Finish())
				_, err := f.Seek(0, io.SeekStart)
				y.Check(err)
			}
		})

		b.Run(fmt.Sprintf("Hash_%d", n), func(b *testing.B) {
			filename := fmt.Sprintf("%s%s%d.sst", os.TempDir(), string(os.PathSeparator), rand.Int63())
			f, err := y.OpenSyncedFile(filename, false)
			y.Check(err)
			for bn := 0; bn < b.N; bn++ {
				builder := NewTableBuilder(f, nil, 0, defaultBuilderOpt)
				for i := 0; i < n; i++ {
					y.Check(builder.Add(kvs[i].k, y.ValueStruct{Value: kvs[i].v, Meta: 123, UserMeta: []byte{0}}))
				}
				y.Check(builder.Finish())
				_, err := f.Seek(0, io.SeekStart)
				y.Check(err)
			}
		})
	}
}

func BenchmarkPointGet(b *testing.B) {
	ns := []int{1000, 10000, 100000, 1000000, 5000000, 10000000, 15000000}
	for _, n := range ns {
		filename := fmt.Sprintf("%s%s%d.sst", os.TempDir(), string(os.PathSeparator), rand.Int63())
		f, err := y.OpenSyncedFile(filename, true)
		builder := NewTableBuilder(f, nil, 0, defaultBuilderOpt)
		keys := make([][]byte, n)
		y.Check(err)
		for i := 0; i < n; i++ {
			k := y.KeyWithTs([]byte(fmt.Sprintf("%016x", i)), 0)
			v := fmt.Sprintf("%d", i)
			keys[i] = k
			y.Check(builder.Add([]byte(k), y.ValueStruct{Value: []byte(v), Meta: 123, UserMeta: []byte{0}}))
		}

		y.Check(builder.Finish())
		tbl, err := OpenTable(f, options.MemoryMap)
		y.Check(err)
		b.ResetTimer()

		b.Run(fmt.Sprintf("Seek_%d", n), func(b *testing.B) {
			var vs y.ValueStruct
			rand := rand.New(rand.NewSource(0))
			for bn := 0; bn < b.N; bn++ {
				rand.Seed(0)
				for i := 0; i < n; i++ {
					k := keys[rand.Intn(n)]
					it := tbl.NewIterator(false)
					it.Seek(k)
					if !it.Valid() {
						continue
					}
					if !y.SameKey(k, it.Key()) {
						continue
					}
					vs = it.Value()
				}
			}
			_ = vs
		})

		b.Run(fmt.Sprintf("Hash_%d", n), func(b *testing.B) {
			var (
				resultKey []byte
				resultVs  y.ValueStruct
				ok        bool
			)
			rand := rand.New(rand.NewSource(0))
			for bn := 0; bn < b.N; bn++ {
				rand.Seed(0)
				for i := 0; i < n; i++ {
					k := keys[rand.Intn(n)]
					keyHash := farm.Fingerprint64(y.ParseKey(k))
					resultKey, resultVs, ok = tbl.PointGet(k, keyHash)
					if !ok {
						it := tbl.NewIterator(false)
						it.Seek(k)
						if !it.Valid() {
							continue
						}
						if !y.SameKey(k, it.Key()) {
							continue
						}
						resultKey, resultVs = it.Key(), it.Value()
					}
				}
			}
			_, _ = resultKey, resultVs
		})

		tbl.Delete()
	}
}

func BenchmarkReadAndBuild(b *testing.B) {
	n := 5 << 20
	filename := fmt.Sprintf("%s%s%d.sst", os.TempDir(), string(os.PathSeparator), rand.Int63())
	f, err := y.OpenSyncedFile(filename, false)
	builder := NewTableBuilder(f, nil, 0, defaultBuilderOpt)
	y.Check(err)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("%016x", i)
		v := fmt.Sprintf("%d", i)
		y.Check(builder.Add([]byte(k), y.ValueStruct{Value: []byte(v), Meta: 123, UserMeta: []byte{0}}))
	}

	y.Check(builder.Finish())
	tbl, err := OpenTable(f, options.MemoryMap)
	y.Check(err)
	defer tbl.Delete()

	//	y.Printf("Size of table: %d\n", tbl.Size())
	b.ResetTimer()
	// Iterate b.N times over the entire table.
	filename = fmt.Sprintf("%s%s%d.sst", os.TempDir(), string(os.PathSeparator), rand.Int63())
	f, err = y.OpenSyncedFile(filename, false)
	y.Check(err)
	for i := 0; i < b.N; i++ {
		func() {
			newBuilder := NewTableBuilder(f, nil, 0, options.TableBuilderOptions{})
			it := tbl.NewIterator(false)
			defer it.Close()
			for it.seekToFirst(); it.Valid(); it.next() {
				vs := it.Value()
				newBuilder.Add(it.Key(), vs)
			}
			y.Check(newBuilder.Finish())
			_, err := f.Seek(0, io.SeekStart)
			y.Check(err)
		}()
	}
}

func BenchmarkReadMerged(b *testing.B) {
	n := 5 << 20
	m := 5 // Number of tables.
	y.Assert((n % m) == 0)
	tableSize := n / m
	var tables []*Table
	for i := 0; i < m; i++ {
		filename := fmt.Sprintf("%s%s%d.sst", os.TempDir(), string(os.PathSeparator), rand.Int63())
		f, err := y.OpenSyncedFile(filename, true)
		y.Check(err)
		builder := NewTableBuilder(f, nil, 0, defaultBuilderOpt)
		for j := 0; j < tableSize; j++ {
			id := j*m + i // Arrays are interleaved.
			// id := i*tableSize+j (not interleaved)
			k := fmt.Sprintf("%016x", id)
			v := fmt.Sprintf("%d", id)
			y.Check(builder.Add([]byte(k), y.ValueStruct{Value: []byte(v), Meta: 123, UserMeta: []byte{0}}))
		}
		y.Check(builder.Finish())
		tbl, err := OpenTable(f, options.MemoryMap)
		y.Check(err)
		tables = append(tables, tbl)
		defer tbl.Delete()
	}

	b.ResetTimer()
	// Iterate b.N times over the entire table.
	for i := 0; i < b.N; i++ {
		func() {
			var iters []y.Iterator
			for _, tbl := range tables {
				iters = append(iters, tbl.NewIterator(false))
			}
			it := NewMergeIterator(iters, false)
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
			}
		}()
	}
}
