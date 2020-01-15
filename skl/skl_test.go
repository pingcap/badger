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

package skl

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/coocood/badger/y"
	"github.com/stretchr/testify/require"
)

const arenaSize = 1 << 20

func newValue(v int) []byte {
	return []byte(fmt.Sprintf("%05d", v))
}

// length iterates over skiplist to give exact size.
func length(s *Skiplist) int {
	x := s.getNext(s.head, 0)
	count := 0
	for x != nil {
		count++
		x = s.getNext(x, 0)
	}
	return count
}

func TestEmpty(t *testing.T) {
	key := []byte("aaa")
	l := NewSkiplist(arenaSize)

	v := l.Get(key)
	require.True(t, v.Value == nil) // Cannot use require.Nil for unsafe.Pointer nil.

	for _, less := range []bool{true, false} {
		for _, allowEqual := range []bool{true, false} {
			n, found := l.findNear(key, less, allowEqual)
			require.Nil(t, n)
			require.False(t, found)
		}
	}

	it := l.NewIterator()
	require.False(t, it.Valid())

	it.SeekToFirst()
	require.False(t, it.Valid())

	it.SeekToLast()
	require.False(t, it.Valid())

	it.Seek(key)
	require.False(t, it.Valid())
}

// TestBasic tests single-threaded inserts and updates and gets.
func TestBasic(t *testing.T) {
	l := NewSkiplist(arenaSize)
	val1 := newValue(42)
	val2 := newValue(52)
	val3 := newValue(62)
	val4 := newValue(72)

	// Try inserting values.
	// Somehow require.Nil doesn't work when checking for unsafe.Pointer(nil).
	l.Put(y.KeyWithTs([]byte("key1"), 0), y.ValueStruct{Value: val1, Meta: 55, UserMeta: []byte{0}})
	l.Put(y.KeyWithTs([]byte("key2"), 2), y.ValueStruct{Value: val2, Meta: 56, UserMeta: []byte{0}})
	l.Put(y.KeyWithTs([]byte("key3"), 0), y.ValueStruct{Value: val3, Meta: 57, UserMeta: []byte{0}})

	v := l.Get(y.KeyWithTs([]byte("key"), 0))
	require.True(t, v.Value == nil)

	v = l.Get(y.KeyWithTs([]byte("key1"), 0))
	require.True(t, v.Value != nil)
	require.EqualValues(t, "00042", string(v.Value))
	require.EqualValues(t, 55, v.Meta)

	v = l.Get(y.KeyWithTs([]byte("key2"), 0))
	require.True(t, v.Value == nil)

	v = l.Get(y.KeyWithTs([]byte("key3"), 0))
	require.True(t, v.Value != nil)
	require.EqualValues(t, "00062", string(v.Value))
	require.EqualValues(t, 57, v.Meta)

	l.Put(y.KeyWithTs([]byte("key3"), 1), y.ValueStruct{Value: val4, Meta: 12, UserMeta: []byte{0}})
	v = l.Get(y.KeyWithTs([]byte("key3"), 1))
	require.True(t, v.Value != nil)
	require.EqualValues(t, "00072", string(v.Value))
	require.EqualValues(t, 12, v.Meta)
}

// TestConcurrentBasic tests concurrent writes followed by concurrent reads.
func TestConcurrentBasic(t *testing.T) {
	const n = 1000
	l := NewSkiplist(arenaSize)
	var wg sync.WaitGroup
	key := func(i int) []byte {
		return y.KeyWithTs([]byte(fmt.Sprintf("%05d", i)), 0)
	}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			l.Put(key(i),
				y.ValueStruct{Value: newValue(i), Meta: 0, UserMeta: []byte{0}})
		}(i)
	}
	wg.Wait()
	// Check values. Concurrent reads.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v := l.Get(key(i))
			require.True(t, v.Value != nil)
			require.EqualValues(t, newValue(i), v.Value)
		}(i)
	}
	wg.Wait()
	require.EqualValues(t, n, length(l))
}

func TestFindNear(t *testing.T) {
	l := NewSkiplist(arenaSize)
	defer l.Delete()
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("%05d", i*10+5)
		l.Put(y.KeyWithTs([]byte(key), 0), y.ValueStruct{Value: newValue(i), Meta: 0, UserMeta: []byte{0}})
	}

	n, eq := l.findNear(y.KeyWithTs([]byte("00001"), 0), false, false)
	require.NotNil(t, n)
	require.EqualValues(t, y.KeyWithTs([]byte("00005"), 0), string(n.key(l.arena)))
	require.False(t, eq)
	n, eq = l.findNear(y.KeyWithTs([]byte("00001"), 0), false, true)
	require.NotNil(t, n)
	require.EqualValues(t, y.KeyWithTs([]byte("00005"), 0), string(n.key(l.arena)))
	require.False(t, eq)
	n, eq = l.findNear(y.KeyWithTs([]byte("00001"), 0), true, false)
	require.Nil(t, n)
	require.False(t, eq)
	n, eq = l.findNear(y.KeyWithTs([]byte("00001"), 0), true, true)
	require.Nil(t, n)
	require.False(t, eq)

	n, eq = l.findNear(y.KeyWithTs([]byte("00005"), 0), false, false)
	require.NotNil(t, n)
	require.EqualValues(t, y.KeyWithTs([]byte("00015"), 0), string(n.key(l.arena)))
	require.False(t, eq)
	n, eq = l.findNear(y.KeyWithTs([]byte("00005"), 0), false, true)
	require.NotNil(t, n)
	require.EqualValues(t, y.KeyWithTs([]byte("00005"), 0), string(n.key(l.arena)))
	require.True(t, eq)
	n, eq = l.findNear(y.KeyWithTs([]byte("00005"), 0), true, false)
	require.Nil(t, n)
	require.False(t, eq)
	n, eq = l.findNear(y.KeyWithTs([]byte("00005"), 0), true, true)
	require.NotNil(t, n)
	require.EqualValues(t, y.KeyWithTs([]byte("00005"), 0), string(n.key(l.arena)))
	require.True(t, eq)

	n, eq = l.findNear(y.KeyWithTs([]byte("05555"), 0), false, false)
	require.NotNil(t, n)
	require.EqualValues(t, y.KeyWithTs([]byte("05565"), 0), string(n.key(l.arena)))
	require.False(t, eq)
	n, eq = l.findNear(y.KeyWithTs([]byte("05555"), 0), false, true)
	require.NotNil(t, n)
	require.EqualValues(t, y.KeyWithTs([]byte("05555"), 0), string(n.key(l.arena)))
	require.True(t, eq)
	n, eq = l.findNear(y.KeyWithTs([]byte("05555"), 0), true, false)
	require.NotNil(t, n)
	require.EqualValues(t, y.KeyWithTs([]byte("05545"), 0), string(n.key(l.arena)))
	require.False(t, eq)
	n, eq = l.findNear(y.KeyWithTs([]byte("05555"), 0), true, true)
	require.NotNil(t, n)
	require.EqualValues(t, y.KeyWithTs([]byte("05555"), 0), string(n.key(l.arena)))
	require.True(t, eq)

	n, eq = l.findNear(y.KeyWithTs([]byte("05558"), 0), false, false)
	require.NotNil(t, n)
	require.EqualValues(t, y.KeyWithTs([]byte("05565"), 0), string(n.key(l.arena)))
	require.False(t, eq)
	n, eq = l.findNear(y.KeyWithTs([]byte("05558"), 0), false, true)
	require.NotNil(t, n)
	require.EqualValues(t, y.KeyWithTs([]byte("05565"), 0), string(n.key(l.arena)))
	require.False(t, eq)
	n, eq = l.findNear(y.KeyWithTs([]byte("05558"), 0), true, false)
	require.NotNil(t, n)
	require.EqualValues(t, y.KeyWithTs([]byte("05555"), 0), string(n.key(l.arena)))
	require.False(t, eq)
	n, eq = l.findNear(y.KeyWithTs([]byte("05558"), 0), true, true)
	require.NotNil(t, n)
	require.EqualValues(t, y.KeyWithTs([]byte("05555"), 0), string(n.key(l.arena)))
	require.False(t, eq)

	n, eq = l.findNear(y.KeyWithTs([]byte("09995"), 0), false, false)
	require.Nil(t, n)
	require.False(t, eq)
	n, eq = l.findNear(y.KeyWithTs([]byte("09995"), 0), false, true)
	require.NotNil(t, n)
	require.EqualValues(t, y.KeyWithTs([]byte("09995"), 0), string(n.key(l.arena)))
	require.True(t, eq)
	n, eq = l.findNear(y.KeyWithTs([]byte("09995"), 0), true, false)
	require.NotNil(t, n)
	require.EqualValues(t, y.KeyWithTs([]byte("09985"), 0), string(n.key(l.arena)))
	require.False(t, eq)
	n, eq = l.findNear(y.KeyWithTs([]byte("09995"), 0), true, true)
	require.NotNil(t, n)
	require.EqualValues(t, y.KeyWithTs([]byte("09995"), 0), string(n.key(l.arena)))
	require.True(t, eq)

	n, eq = l.findNear(y.KeyWithTs([]byte("59995"), 0), false, false)
	require.Nil(t, n)
	require.False(t, eq)
	n, eq = l.findNear(y.KeyWithTs([]byte("59995"), 0), false, true)
	require.Nil(t, n)
	require.False(t, eq)
	n, eq = l.findNear(y.KeyWithTs([]byte("59995"), 0), true, false)
	require.NotNil(t, n)
	require.EqualValues(t, y.KeyWithTs([]byte("09995"), 0), string(n.key(l.arena)))
	require.False(t, eq)
	n, eq = l.findNear(y.KeyWithTs([]byte("59995"), 0), true, true)
	require.NotNil(t, n)
	require.EqualValues(t, y.KeyWithTs([]byte("09995"), 0), string(n.key(l.arena)))
	require.False(t, eq)
}

// TestIteratorNext tests a basic iteration over all nodes from the beginning.
func TestIteratorNext(t *testing.T) {
	const n = 100
	l := NewSkiplist(arenaSize)
	defer l.Delete()
	it := l.NewIterator()
	require.False(t, it.Valid())
	it.SeekToFirst()
	require.False(t, it.Valid())
	for i := n - 1; i >= 0; i-- {
		l.Put(y.KeyWithTs([]byte(fmt.Sprintf("%05d", i)), 0),
			y.ValueStruct{Value: newValue(i), Meta: 0, UserMeta: []byte{0}})
	}
	it.SeekToFirst()
	for i := 0; i < n; i++ {
		require.True(t, it.Valid())
		v := it.Value()
		require.EqualValues(t, newValue(i), v.Value)
		it.Next()
	}
	require.False(t, it.Valid())
}

// TestIteratorPrev tests a basic iteration over all nodes from the end.
func TestIteratorPrev(t *testing.T) {
	const n = 100
	l := NewSkiplist(arenaSize)
	defer l.Delete()
	it := l.NewIterator()
	require.False(t, it.Valid())
	it.SeekToFirst()
	require.False(t, it.Valid())
	for i := 0; i < n; i++ {
		l.Put(y.KeyWithTs([]byte(fmt.Sprintf("%05d", i)), 0),
			y.ValueStruct{Value: newValue(i), Meta: 0, UserMeta: []byte{0}})
	}
	it.SeekToLast()
	for i := n - 1; i >= 0; i-- {
		require.True(t, it.Valid())
		v := it.Value()
		require.EqualValues(t, newValue(i), v.Value)
		it.Prev()
	}
	require.False(t, it.Valid())
}

// TestIteratorSeek tests Seek and SeekForPrev.
func TestIteratorSeek(t *testing.T) {
	const n = 100
	l := NewSkiplist(arenaSize)
	defer l.Delete()

	it := l.NewIterator()

	require.False(t, it.Valid())
	it.SeekToFirst()
	require.False(t, it.Valid())
	// 1000, 1010, 1020, ..., 1990.
	for i := n - 1; i >= 0; i-- {
		v := i*10 + 1000
		l.Put(y.KeyWithTs([]byte(fmt.Sprintf("%05d", i*10+1000)), 0),
			y.ValueStruct{Value: newValue(v), Meta: 0, UserMeta: []byte{0}})
	}
	it.SeekToFirst()
	require.True(t, it.Valid())
	v := it.Value()
	require.EqualValues(t, "01000", v.Value)

	it.Seek(y.KeyWithTs([]byte("01000"), 0))
	require.True(t, it.Valid())
	v = it.Value()
	require.EqualValues(t, "01000", v.Value)

	it.Seek(y.KeyWithTs([]byte("01005"), 0))
	require.True(t, it.Valid())
	v = it.Value()
	require.EqualValues(t, "01010", v.Value)

	it.Seek(y.KeyWithTs([]byte("01010"), 0))
	require.True(t, it.Valid())
	v = it.Value()
	require.EqualValues(t, "01010", v.Value)

	it.Seek(y.KeyWithTs([]byte("99999"), 0))
	require.False(t, it.Valid())

	// Try SeekForPrev.
	it.SeekForPrev(y.KeyWithTs([]byte("00"), 0))
	require.False(t, it.Valid())

	it.SeekForPrev(y.KeyWithTs([]byte("01000"), 0))
	require.True(t, it.Valid())
	v = it.Value()
	require.EqualValues(t, "01000", v.Value)

	it.SeekForPrev(y.KeyWithTs([]byte("01005"), 0))
	require.True(t, it.Valid())
	v = it.Value()
	require.EqualValues(t, "01000", v.Value)

	it.SeekForPrev(y.KeyWithTs([]byte("01010"), 0))
	require.True(t, it.Valid())
	v = it.Value()
	require.EqualValues(t, "01010", v.Value)

	it.SeekForPrev(y.KeyWithTs([]byte("99999"), 0))
	require.True(t, it.Valid())
	v = it.Value()
	require.EqualValues(t, "01990", v.Value)
}

func TestPutWithHint(t *testing.T) {
	l := NewSkiplist(arenaSize)
	sp := new(Hint)
	cnt := 0
	for {
		if l.arena.size() > arenaSize-256 {
			break
		}
		key := randomKey()
		l.PutWithHint(key, y.ValueStruct{Value: key}, sp)
		cnt++
	}
	it := l.NewIterator()
	var lastKey []byte
	cntGot := 0
	for it.SeekToFirst(); it.Valid(); it.Next() {
		require.True(t, bytes.Compare(lastKey, it.Key()) <= 0)
		require.True(t, bytes.Compare(it.Key(), it.Value().Value) == 0)
		cntGot++
	}
	require.True(t, cntGot == cnt)
}

func TestGetWithHint(t *testing.T) {
	rand.Seed(time.Now().Unix())
	l := NewSkiplist(arenaSize)
	var keys [][]byte
	sp := new(Hint)
	for {
		if l.arena.size() > arenaSize-256 {
			break
		}
		key := randomKey()
		keys = append(keys, key)
		l.PutWithHint(key, y.ValueStruct{Value: key}, sp)
	}
	hint := new(Hint)
	for _, key := range keys {
		bytes.Equal(l.GetWithHint(key, hint).Value, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})
	for _, key := range keys {
		bytes.Equal(l.GetWithHint(key, hint).Value, key)
	}
}

func TestPutLargeValue(t *testing.T) {
	l := NewSkiplist(arenaSize)
	key := randomKey()
	val := make([]byte, 128*1024)
	l.Put(key, y.ValueStruct{Value: val})
	result := l.Get(key)
	require.Equal(t, val, result.Value)
}

func randomKey() []byte {
	b := make([]byte, 8)
	key := rand.Uint32()
	key2 := rand.Uint32()
	binary.LittleEndian.PutUint32(b, key)
	binary.LittleEndian.PutUint32(b[4:], key2)
	return y.KeyWithTs(b, 0)
}

// Standard test. Some fraction is read. Some fraction is write. Writes have
// to go through mutex lock.
func BenchmarkReadWrite(b *testing.B) {
	value := newValue(123)
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("frac_%d", i), func(b *testing.B) {
			l := NewSkiplist(int64((b.N + 1) * MaxNodeSize))
			defer l.Delete()
			b.ResetTimer()
			var count int
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				for pb.Next() {
					if rng.Float32() < readFrac {
						v := l.Get(randomKey())
						if v.Value != nil {
							count++
						}
					} else {
						l.Put(randomKey(), y.ValueStruct{Value: value, Meta: 0, UserMeta: []byte{0}})
					}
				}
			})
		})
	}
}

// Standard test. Some fraction is read. Some fraction is write. Writes have
// to go through mutex lock.
func BenchmarkReadWriteMap(b *testing.B) {
	value := newValue(123)
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("frac_%d", i), func(b *testing.B) {
			m := make(map[string][]byte)
			var mutex sync.RWMutex
			b.ResetTimer()
			var count int
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if rand.Float32() < readFrac {
						mutex.RLock()
						_, ok := m[string(randomKey())]
						mutex.RUnlock()
						if ok {
							count++
						}
					} else {
						mutex.Lock()
						m[string(randomKey())] = value
						mutex.Unlock()
					}
				}
			})
		})
	}
}

// Standard test. Some fraction is read. Some fraction is write. Writes have
// to go through mutex lock.
func BenchmarkGetSequential(b *testing.B) {
	size := 300000
	keys, l, _ := buildKeysAndList(size)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := keys[i%size]
		l.Get(key)
	}
}

func BenchmarkGetWithHintSequential(b *testing.B) {
	size := 300000
	keys, l, hint := buildKeysAndList(size)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := keys[i%size]
		l.GetWithHint(key, hint)
	}
}

func buildKeysAndList(size int) ([][]byte, *Skiplist, *Hint) {
	l := NewSkiplist(32 * 1024 * 1024)
	keys := make([][]byte, size)
	hint := new(Hint)
	for i := 0; i < size; i++ {
		keys[i] = y.KeyWithTs([]byte(fmt.Sprintf("%05d", i)), 0)
	}
	for i := 0; i < size; i++ {
		key := keys[i]
		l.PutWithHint(key, y.ValueStruct{Value: []byte{byte(i)}}, hint)
	}
	return keys, l, hint
}

func BenchmarkGetRandom(b *testing.B) {
	size := 300000
	keys, l, _ := buildKeysAndList(size)
	b.ResetTimer()
	r := rand.New(rand.NewSource(1))
	for i := 0; i < b.N; i++ {
		key := keys[r.Int()%size]
		l.Get(key)
	}
}

func BenchmarkGetWithHintRandom(b *testing.B) {
	size := 300000
	keys, l, hint := buildKeysAndList(size)
	b.ResetTimer()
	r := rand.New(rand.NewSource(1))
	for i := 0; i < b.N; i++ {
		key := keys[r.Int()%size]
		l.GetWithHint(key, hint)
	}
}

func BenchmarkPutWithHint(b *testing.B) {
	l := NewSkiplist(16 * 1024 * 1024)
	size := 100000
	keys := make([][]byte, size)
	for i := 0; i < size; i++ {
		keys[i] = y.KeyWithTs([]byte(fmt.Sprintf("%05d", i)), 0)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hint := new(Hint)
		l = NewSkiplist(16 * 1024 * 1024)
		for j := 0; j < size; j++ {
			key := keys[j]
			l.PutWithHint(key, y.ValueStruct{Value: []byte{byte(j)}}, hint)
		}
	}
}
