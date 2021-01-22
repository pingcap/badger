package badger

import (
	"github.com/pingcap/badger/table"
	"github.com/pingcap/badger/y"
	"math"
)

func (s *Snapshot) NewIterator(cf int, reversed, allVersions bool) *Iterator {
	iter := &Iterator{
		iitr: s.newShardIterator(cf, reversed, 0),
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

func (s *Snapshot) newShardIterator(cf int, reverse bool, minL0ID uint64) y.Iterator {
	iters := make([]y.Iterator, 0, 12)
	if s.shard.isSplitting() {
		for i := 0; i < len(s.shard.splittingMemTbls); i++ {
			memTbls := s.shard.loadSplittingMemTables(i)
			iters = s.appendMemTblIters(iters, memTbls, cf, reverse)
		}
		for i := 0; i < len(s.shard.splittingL0s); i++ {
			l0s := s.shard.loadSplittingL0Tables(i)
			iters = s.appendL0Iters(iters, l0s, cf, reverse, minL0ID)
		}
	}
	memTbls := s.shard.loadMemTables()
	iters = s.appendMemTblIters(iters, memTbls, cf, reverse)
	l0s := s.shard.loadL0Tables()
	iters = s.appendL0Iters(iters, l0s, cf, reverse, minL0ID)
	if minL0ID > 0 {
		// This is a delta iterator, we don't read data >= L1.
		return table.NewMergeIterator(iters, reverse)
	}
	scf := s.shard.cfs[cf]
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

// NewDeltaIterator creates an iterator that iterates mem-tables and all the l0 tables that has ID greater than minL0FileID.
// It is used to collect new data that doesn't exist in the file meta tree.
func (s *Snapshot) NewDeltaIterator(cf int, sinceL0FileID uint64) *Iterator {
	iter := &Iterator{
		iitr: s.newShardIterator(cf, false, sinceL0FileID),
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
