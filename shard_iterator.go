package badger

import (
	"bytes"
	"math"
	"sort"

	"github.com/pingcap/badger/table"
	"github.com/pingcap/badger/y"
)

func (s *Snapshot) NewIterator(cf byte, reversed, allVersions bool) *Iterator {
	iter := &Iterator{
		iitr: newShardConcatIterator(cf, s, reversed),
		opt:  IteratorOptions{Reverse: reversed, AllVersions: allVersions},
	}
	if s.cfs[cf].Managed {
		iter.readTs = math.MaxUint64
	} else {
		iter.readTs = s.readTS
	}
	return iter
}

func (s *Snapshot) newIteratorForShard(cf byte, reversed bool, shard *Shard) y.Iterator {
	minGlobalL0 := shard.loadMinGlobalL0()
	iters := make([]y.Iterator, 0, 12)
	for _, memTbl := range s.memTbls.tables {
		if minGlobalL0 > memTbl.ID() {
			continue
		}
		iter := memTbl.NewIterator(cf, reversed)
		if iter != nil && validInRange(iter, reversed, shard.Start, shard.End) {
			iters = append(iters, iter)
		}
	}
	for _, l0 := range s.l0Tbls.tables {
		if minGlobalL0 > l0.fid {
			continue
		}
		iter := l0.newIterator(cf, reversed, shard.Start, shard.End)
		if iter != nil {
			iters = append(iters, iter)
		}
	}
	iters = shard.AppendIterators(iters, cf, reversed)
	return y.NewBoundedIterator(table.NewMergeIterator(iters, reversed), shard.Start, shard.End, reversed)
}

func validInRange(iter y.Iterator, reversed bool, start, end []byte) bool {
	if reversed {
		iter.Seek(end)
		return iter.Valid() && bytes.Compare(start, iter.Key().UserKey) <= 0
	}
	iter.Seek(start)
	return iter.Valid() && bytes.Compare(iter.Key().UserKey, end) < 0
}

func (st *Shard) AppendIterators(iters []y.Iterator, cf byte, reverse bool) []y.Iterator {
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
	snap     *Snapshot
	idx      int // Which iterator is active now.
	cur      y.Iterator
	iters    []y.Iterator // Corresponds to tables.
	reversed bool
	cf       byte
}

func newShardConcatIterator(cf byte, snap *Snapshot, reversed bool) y.Iterator {
	iter := &shardConcatIterator{
		snap:     snap,
		idx:      0,
		reversed: reversed,
		cf:       cf,
	}
	iter.iters = make([]y.Iterator, len(snap.shards))
	return iter
}

func (s *shardConcatIterator) setIdx(idx int) {
	s.idx = idx
	if idx < 0 || idx >= len(s.iters) {
		s.cur = nil
	} else {
		if s.iters[s.idx] == nil {
			ti := s.snap.newIteratorForShard(s.cf, s.reversed, s.snap.shards[idx])
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
		idx = sort.Search(len(s.snap.shards), func(i int) bool {
			return bytes.Compare(s.snap.shards[i].End, key) >= 0
		})
	} else {
		n := len(s.snap.shards)
		idx = n - 1 - sort.Search(n, func(i int) bool {
			return bytes.Compare(s.snap.shards[n-1-i].Start, key) <= 0
		})
	}
	if idx >= len(s.snap.shards) || idx < 0 {
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
