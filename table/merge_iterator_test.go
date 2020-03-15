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
	"fmt"
	"sort"
	"testing"

	"github.com/coocood/badger/y"
	"github.com/stretchr/testify/require"
)

type SimpleIterator struct {
	keys     []y.Key
	vals     [][]byte
	idx      int
	reversed bool
}

var (
	closeCount int
)

func (s *SimpleIterator) Next() {
	if !s.reversed {
		s.idx++
	} else {
		s.idx--
	}
}

func (s *SimpleIterator) Rewind() {
	if !s.reversed {
		s.idx = 0
	} else {
		s.idx = len(s.keys) - 1
	}
}

func (s *SimpleIterator) Seek(key y.Key) {
	if !s.reversed {
		s.idx = sort.Search(len(s.keys), func(i int) bool {
			return s.keys[i].Compare(key) >= 0
		})
	} else {
		n := len(s.keys)
		s.idx = n - 1 - sort.Search(n, func(i int) bool {
			return s.keys[n-1-i].Compare(key) <= 0
		})
	}
}

func (s *SimpleIterator) Key() y.Key { return s.keys[s.idx] }
func (s *SimpleIterator) Value() y.ValueStruct {
	return y.ValueStruct{
		Value:    s.vals[s.idx],
		UserMeta: []byte{55},
		Meta:     0,
	}
}
func (s *SimpleIterator) FillValue(vs *y.ValueStruct) {
	vs.Value = s.vals[s.idx]
	vs.UserMeta = []byte{55} // arbitrary value for test
	vs.Meta = 0
}
func (s *SimpleIterator) Valid() bool {
	return s.idx >= 0 && s.idx < len(s.keys)
}

func newSimpleIterator(keys []string, vals []string, reversed bool) *SimpleIterator {
	k := make([]y.Key, len(keys))
	v := make([][]byte, len(vals))
	y.Assert(len(keys) == len(vals))
	for i := 0; i < len(keys); i++ {
		k[i] = y.KeyWithTs([]byte(keys[i]), 0)
		v[i] = []byte(vals[i])
	}
	return &SimpleIterator{
		keys:     k,
		vals:     v,
		idx:      -1,
		reversed: reversed,
	}
}

func getAll(it y.Iterator) ([]string, []string) {
	var keys, vals []string
	for ; it.Valid(); it.Next() {
		k := it.Key()
		keys = append(keys, string(k.UserKey))
		v := it.Value()
		vals = append(vals, string(v.Value))
	}
	return keys, vals
}

func TestSimpleIterator(t *testing.T) {
	keys := []string{"1", "2", "3"}
	vals := []string{"v1", "v2", "v3"}
	it := newSimpleIterator(keys, vals, false)
	it.Rewind()
	k, v := getAll(it)
	require.EqualValues(t, keys, k)
	require.EqualValues(t, vals, v)
}

func reversed(a []string) []string {
	var out []string
	for i := len(a) - 1; i >= 0; i-- {
		out = append(out, a[i])
	}
	return out
}

func TestMergeSingle(t *testing.T) {
	keys := []string{"1", "2", "3"}
	vals := []string{"v1", "v2", "v3"}
	it := newSimpleIterator(keys, vals, false)
	mergeIt := NewMergeIterator([]y.Iterator{it}, false)
	mergeIt.Rewind()
	k, v := getAll(mergeIt)
	require.EqualValues(t, keys, k)
	require.EqualValues(t, vals, v)
}

func TestMergeSingleReversed(t *testing.T) {
	keys := []string{"1", "2", "3"}
	vals := []string{"v1", "v2", "v3"}
	it := newSimpleIterator(keys, vals, true)
	mergeIt := NewMergeIterator([]y.Iterator{it}, true)
	mergeIt.Rewind()
	k, v := getAll(mergeIt)
	require.EqualValues(t, reversed(keys), k)
	require.EqualValues(t, reversed(vals), v)
}

func TestMergeMore(t *testing.T) {
	it1 := newSimpleIterator([]string{"1", "3", "7"}, []string{"a1", "a3", "a7"}, false)
	it2 := newSimpleIterator([]string{"2", "3", "5"}, []string{"b2", "b3", "b5"}, false)
	it3 := newSimpleIterator([]string{"1"}, []string{"c1"}, false)
	it4 := newSimpleIterator([]string{"1", "7", "9"}, []string{"d1", "d7", "d9"}, false)

	mergeIt := NewMergeIterator([]y.Iterator{it1, it2, it3, it4}, false)
	expectedKeys := []string{"1", "2", "3", "5", "7", "9"}
	expectedVals := []string{"a1", "b2", "a3", "b5", "a7", "d9"}
	mergeIt.Rewind()
	k, v := getAll(mergeIt)
	require.EqualValues(t, expectedKeys, k)
	require.EqualValues(t, expectedVals, v)
}

// Ensure MergeIterator satisfies the Iterator interface
func TestMergeIteratorNested(t *testing.T) {
	keys := []string{"1", "2", "3"}
	vals := []string{"v1", "v2", "v3"}
	it := newSimpleIterator(keys, vals, false)
	mergeIt := NewMergeIterator([]y.Iterator{it}, false)
	mergeIt2 := NewMergeIterator([]y.Iterator{mergeIt}, false)
	mergeIt2.Rewind()
	k, v := getAll(mergeIt2)
	require.EqualValues(t, keys, k)
	require.EqualValues(t, vals, v)
}

func TestMergeIteratorSeek(t *testing.T) {
	it := newSimpleIterator([]string{"1", "3", "7"}, []string{"a1", "a3", "a7"}, false)
	it2 := newSimpleIterator([]string{"2", "3", "5"}, []string{"b2", "b3", "b5"}, false)
	it3 := newSimpleIterator([]string{"1"}, []string{"c1"}, false)
	it4 := newSimpleIterator([]string{"1", "7", "9"}, []string{"d1", "d7", "d9"}, false)
	mergeIt := NewMergeIterator([]y.Iterator{it, it2, it3, it4}, false)
	mergeIt.Seek(y.KeyWithTs([]byte("4"), 0))
	k, v := getAll(mergeIt)
	require.EqualValues(t, []string{"5", "7", "9"}, k)
	require.EqualValues(t, []string{"b5", "a7", "d9"}, v)
}

func TestMergeIteratorSeekReversed(t *testing.T) {
	it := newSimpleIterator([]string{"1", "3", "7"}, []string{"a1", "a3", "a7"}, true)
	it2 := newSimpleIterator([]string{"2", "3", "5"}, []string{"b2", "b3", "b5"}, true)
	it3 := newSimpleIterator([]string{"1"}, []string{"c1"}, true)
	it4 := newSimpleIterator([]string{"1", "7", "9"}, []string{"d1", "d7", "d9"}, true)
	mergeIt := NewMergeIterator([]y.Iterator{it, it2, it3, it4}, true)
	mergeIt.Seek(y.KeyWithTs([]byte("5"), 0))
	k, v := getAll(mergeIt)
	require.EqualValues(t, []string{"5", "3", "2", "1"}, k)
	require.EqualValues(t, []string{"b5", "a3", "b2", "a1"}, v)
}

func TestMergeIteratorSeekInvalid(t *testing.T) {
	it := newSimpleIterator([]string{"1", "3", "7"}, []string{"a1", "a3", "a7"}, false)
	it2 := newSimpleIterator([]string{"2", "3", "5"}, []string{"b2", "b3", "b5"}, false)
	it3 := newSimpleIterator([]string{"1"}, []string{"c1"}, false)
	it4 := newSimpleIterator([]string{"1", "7", "9"}, []string{"d1", "d7", "d9"}, false)
	mergeIt := NewMergeIterator([]y.Iterator{it, it2, it3, it4}, false)
	mergeIt.Seek(y.KeyWithTs([]byte("f"), 0))
	require.False(t, mergeIt.Valid())
}

func TestMergeIteratorSeekInvalidReversed(t *testing.T) {
	it := newSimpleIterator([]string{"1", "3", "7"}, []string{"a1", "a3", "a7"}, true)
	it2 := newSimpleIterator([]string{"2", "3", "5"}, []string{"b2", "b3", "b5"}, true)
	it3 := newSimpleIterator([]string{"1"}, []string{"c1"}, true)
	it4 := newSimpleIterator([]string{"1", "7", "9"}, []string{"d1", "d7", "d9"}, true)
	mergeIt := NewMergeIterator([]y.Iterator{it, it2, it3, it4}, true)
	mergeIt.Seek(y.KeyWithTs([]byte("0"), 0))
	require.False(t, mergeIt.Valid())
}

func TestMergeIteratorDuplicate(t *testing.T) {
	it1 := newSimpleIterator([]string{"0", "1", "2"}, []string{"0", "1", "2"}, false)
	it2 := newSimpleIterator([]string{"1"}, []string{"1"}, false)
	it3 := newSimpleIterator([]string{"2"}, []string{"2"}, false)
	it := NewMergeIterator([]y.Iterator{it3, it2, it1}, false)

	var cnt int
	for it.Rewind(); it.Valid(); it.Next() {
		require.EqualValues(t, cnt+48, it.Key().UserKey[0])
		cnt++
	}
	require.Equal(t, 3, cnt)
}

func BenchmarkMergeIterator(b *testing.B) {
	num := 2
	simpleIters := make([]y.Iterator, num)
	for i := 0; i < num; i++ {
		simpleIters[i] = new(SimpleIterator)
	}
	for i := 0; i < num*100; i += num {
		for j := 0; j < num; j++ {
			iter := simpleIters[j].(*SimpleIterator)
			iter.keys = append(iter.keys, y.KeyWithTs([]byte(fmt.Sprintf("key%08d", i+j)), 0))
		}
	}
	mergeIter := NewMergeIterator(simpleIters, false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mergeIter.Rewind()
		for mergeIter.Valid() {
			mergeIter.Key()
			mergeIter.Next()
		}
	}
}
