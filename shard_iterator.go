package badger

import (
	"bytes"
	"github.com/pingcap/badger/table"
	"github.com/pingcap/badger/y"
	"math"
	"sort"
)

func (s *Snapshot) NewIterator(cf int, reversed, allVersions bool) *Iterator {
	iter := &Iterator{
		iitr: newShardConcatIterator(cf, s, reversed, 0),
		opt:  IteratorOptions{Reverse: reversed, AllVersions: allVersions},
	}
	if s.cfs[cf].Managed {
		if s.managedReadTS != 0 {
			iter.readTs = s.managedReadTS
		} else {
			iter.readTs = math.MaxUint64
		}
	} else {
		iter.readTs = s.readTS
	}
	return iter
}

func (s *Snapshot) newIteratorForShard(cf int, reverse bool, shard *Shard, minL0ID uint64) y.Iterator {
	iters := make([]y.Iterator, 0, 12)
	if shard.isSplitting() {
		for i := 0; i < len(shard.splittingMemTbls); i++ {
			memTbls := shard.loadSplittingMemTables(i)
			iters = s.appendMemTblIters(iters, memTbls, cf, reverse)
		}
		for i := 0; i < len(shard.splittingL0s); i++ {
			l0s := shard.loadSplittingL0Tables(i)
			iters = s.appendL0Iters(iters, l0s, cf, reverse, minL0ID)
		}
	}
	memTbls := shard.loadMemTables()
	iters = s.appendMemTblIters(iters, memTbls, cf, reverse)
	l0s := shard.loadL0Tables()
	iters = s.appendL0Iters(iters, l0s, cf, reverse, minL0ID)
	if minL0ID > 0 {
		// This is a delta iterator, we don't read data >= L1.
		return table.NewMergeIterator(iters, reverse)
	}
	scf := shard.cfs[cf]
	for i := 1; i <= ShardMaxLevel; i++ {
		h := scf.getLevelHandler(i)
		if len(h.tables) == 0 {
			continue
		}
		iters = append(iters, table.NewConcatIterator(h.tables, reverse))
	}
	return table.NewMergeIterator(iters, reverse)
}

func (s *Snapshot) appendMemTblIters(iters []y.Iterator, memTbls *shardingMemTables, cf int, reverse bool) []y.Iterator {
	for _, tbl := range memTbls.tables {
		it := tbl.NewIterator(cf, reverse)
		if it != nil {
			iters = append(iters, it)
		}
	}
	return iters
}

func (s *Snapshot) appendL0Iters(iters []y.Iterator, l0s *shardL0Tables, cf int, reverse bool, minID uint64) []y.Iterator {
	for _, tbl := range l0s.tables {
		if tbl.fid <= minID {
			continue
		}
		it := tbl.newIterator(cf, reverse)
		if it != nil {
			iters = append(iters, it)
		}
	}
	return iters
}

type shardConcatIterator struct {
	snap      *Snapshot
	idx       int // Which iterator is active now.
	cur       y.Iterator
	iters     []y.Iterator // Corresponds to tables.
	reversed  bool
	cf        int
	sinceL0ID uint64
}

func newShardConcatIterator(cf int, snap *Snapshot, reversed bool, sinceL0ID uint64) y.Iterator {
	iter := &shardConcatIterator{
		snap:      snap,
		idx:       0,
		reversed:  reversed,
		cf:        cf,
		sinceL0ID: sinceL0ID,
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
			ti := s.snap.newIteratorForShard(s.cf, s.reversed, s.snap.shards[idx], s.sinceL0ID)
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

// NewDeltaIterator creates an iterator that iterates mem-tables and all the l0 tables that has ID greater than minL0FileID.
// It is used to collect new data that doesn't exist in the file meta tree.
func (s *Snapshot) NewDeltaIterator(cf int, sinceL0FileID uint64) *Iterator {
	iter := &Iterator{
		iitr: newShardConcatIterator(cf, s, false, sinceL0FileID),
	}
	if s.cfs[cf].Managed {
		// get all versions for managed CF.
		iter.readTs = math.MaxUint64
		iter.opt.AllVersions = true
	} else {
		// Get the snapshot version for unmanaged cf
		iter.readTs = s.readTS
	}
	return iter
}
