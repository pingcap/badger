package badger

import (
	"bytes"
	"math"
	"sort"

	"github.com/pingcap/badger/table"
	"github.com/pingcap/badger/y"
)

func (s *Snapshot) NewIterator(cf byte, reversed, allVersions bool) *Iterator {
	iters := make([]y.Iterator, 0, 12)
	for _, memTbl := range s.memTbls.tables {
		iter := memTbl.NewIterator(cf, reversed)
		if iter != nil {
			iters = append(iters, iter)
		}
	}
	for _, l0Tbl := range s.l0Tbls.tables {
		iter := l0Tbl.newIterator(cf, reversed, s.startKey, s.endKey)
		if iter != nil {
			iters = append(iters, iter)
		}
	}
	if len(s.shards) == 1 {
		iters = append(iters, s.shards[0].GetIterators(cf, reversed)...)
	} else {
		iters = append(iters, newShardConcatIterator(cf, s.shards, reversed))
	}

	iter := &Iterator{
		iitr: table.NewMergeIterator(iters, reversed),
		opt:  IteratorOptions{Reverse: reversed, AllVersions: allVersions},
	}
	if s.cfs[cf].Managed {
		iter.readTs = math.MaxUint64
	} else {
		iter.readTs = s.readTS
	}
	return iter
}

func (st *Shard) GetIterators(cf byte, reverse bool) []y.Iterator {
	iters := make([]y.Iterator, 0, 8)
	scf := st.cfs[cf]
	l0 := scf.getLevelHandler(0)
	for _, tbl := range l0.tables {
		iters = append(iters, tbl.NewIterator(reverse))
	}
	for i := 1; i < shardMaxLevel; i++ {
		h := scf.getLevelHandler(i)
		if len(h.tables) == 0 {
			continue
		}
		iters = append(iters, table.NewConcatIterator(h.tables, reverse))
	}
	return iters
}

type shardConcatIterator struct {
	shards   []*Shard
	idx      int // Which iterator is active now.
	cur      y.Iterator
	iters    []y.Iterator // Corresponds to tables.
	reversed bool
	cf       byte
}

func newShardConcatIterator(cf byte, shards []*Shard, reversed bool) y.Iterator {
	iter := &shardConcatIterator{
		shards:   shards,
		idx:      0,
		reversed: reversed,
		cf:       cf,
	}
	iter.iters = make([]y.Iterator, 0, len(shards))
	for _, shard := range shards {
		iter.iters = append(iter.iters, table.NewMergeIterator(shard.GetIterators(cf, reversed), reversed))
	}
	return iter
}

func (s *shardConcatIterator) setIdx(idx int) {
	s.idx = idx
	if idx < 0 || idx >= len(s.iters) {
		s.cur = nil
	} else {
		if s.iters[s.idx] == nil {
			ti := table.NewMergeIterator(s.shards[s.idx].GetIterators(s.cf, s.reversed), s.reversed)
			ti.Rewind()
			s.iters[s.idx] = ti
		}
		s.cur = s.iters[s.idx]
	}
}

// Rewind implements y.Interface
func (s *shardConcatIterator) Rewind() {
	if len(s.iters) == 0 {
		return
	}
	if !s.reversed {
		s.setIdx(0)
	} else {
		s.setIdx(len(s.iters) - 1)
	}
	s.cur.Rewind()
}

// Valid implements y.Interface
func (s *shardConcatIterator) Valid() bool {
	return s.cur != nil && s.cur.Valid()
}

// Key implements y.Interface
func (s *shardConcatIterator) Key() y.Key {
	return s.cur.Key()
}

// Value implements y.Interface
func (s *shardConcatIterator) Value() y.ValueStruct {
	return s.cur.Value()
}

func (s *shardConcatIterator) FillValue(vs *y.ValueStruct) {
	s.cur.FillValue(vs)
}

// Seek brings us to element >= key if reversed is false. Otherwise, <= key.
func (s *shardConcatIterator) Seek(key []byte) {
	var idx int
	if !s.reversed {
		idx = sort.Search(len(s.shards), func(i int) bool {
			return bytes.Compare(s.shards[i].End, key) >= 0
		})
	} else {
		n := len(s.shards)
		idx = n - 1 - sort.Search(n, func(i int) bool {
			return bytes.Compare(s.shards[n-1-i].Start, key) <= 0
		})
	}
	if idx >= len(s.shards) || idx < 0 {
		s.setIdx(-1)
		return
	}
	// For reversed=false, we know s.tables[i-1].Biggest() < key. Thus, the
	// previous table cannot possibly contain key.
	s.setIdx(idx)
	s.cur.Seek(key)
}

// Next advances our concat iterator.
func (s *shardConcatIterator) Next() {
	s.cur.Next()
	if s.cur.Valid() {
		// Nothing to do. Just stay with the current table.
		return
	}
	for { // In case there are empty tables.
		if !s.reversed {
			s.setIdx(s.idx + 1)
		} else {
			s.setIdx(s.idx - 1)
		}
		if s.cur == nil {
			// End of list. Valid will become false.
			return
		}
		s.cur.Rewind()
		if s.cur.Valid() {
			break
		}
	}
}

func (s *shardConcatIterator) NextVersion() bool {
	return s.cur.NextVersion()
}
