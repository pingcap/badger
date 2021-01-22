package badger

import (
	"bytes"
	"github.com/dgryski/go-farm"
	"github.com/pingcap/badger/epoch"
	"github.com/pingcap/badger/protos"
	"github.com/pingcap/badger/table/memtable"
	"github.com/pingcap/badger/y"
	"sync"
	"sync/atomic"
	"unsafe"
)

const ShardMaxLevel = 4

// Shard split can be performed by the following steps:
// 1. set the splitKeys and mark the state to splitting.
// 2. a splitting Shard will separate all the files by the SplitKeys.
// 3. incoming SST is also split by SplitKeys.
// 4. After all existing files are split by the split keys, the state is changed to SplitDone.
// 5. After SplitDone, the shard map replace the old shard two new shardsByID.
type Shard struct {
	ID    uint64
	Ver   uint64
	Start []byte
	End   []byte
	cfs   []*shardCF
	lock  sync.Mutex

	memTbls *unsafe.Pointer
	l0s     *unsafe.Pointer
	flushCh chan *shardFlushTask

	// split state transition: initial(0) -> set keys (1) -> splitting (2) -> splitDone (3)
	splitState       uint32
	splitKeys        [][]byte
	splittingMemTbls []*unsafe.Pointer
	splittingL0s     []*unsafe.Pointer
	estimatedSize    int64
	removeFilesOnDel bool

	// If the shard is not active, flush mem table and do compaction will ignore this shard.
	active int32
}

const (
	splitStateInitial   uint32 = 0
	splitStateSetKeys   uint32 = 1
	splitStateSplitting uint32 = 2
	splitStateSplitDone uint32 = 3
)

func newShard(id, ver uint64, start, end []byte, opt Options, metrics *y.MetricsSet) *Shard {
	shard := &Shard{
		ID:    id,
		Ver:   ver,
		Start: start,
		End:   end,
		cfs:   make([]*shardCF, len(opt.CFs)),
	}
	shard.memTbls = new(unsafe.Pointer)
	atomic.StorePointer(shard.memTbls, unsafe.Pointer(&shardingMemTables{}))
	shard.l0s = new(unsafe.Pointer)
	atomic.StorePointer(shard.l0s, unsafe.Pointer(&shardingMemTables{}))
	for i := 0; i < len(opt.CFs); i++ {
		sCF := &shardCF{
			levels: make([]unsafe.Pointer, ShardMaxLevel),
		}
		for j := 1; j <= ShardMaxLevel; j++ {
			sCF.casLevelHandler(j, nil, newLevelHandler(opt.NumLevelZeroTablesStall, j, metrics))
		}
		shard.cfs[i] = sCF
	}
	return shard
}

func (s *Shard) tableIDs() []uint64 {
	var ids []uint64
	l0s := s.loadL0Tables()
	for _, tbl := range l0s.tables {
		ids = append(ids, tbl.fid)
	}
	if s.isSplitting() {
		for i := range s.splittingL0s {
			splittingL0s := s.loadSplittingL0Tables(i)
			for _, tbl := range splittingL0s.tables {
				ids = append(ids, tbl.fid)
			}
		}
	}
	s.foreachLevel(func(cf int, level *levelHandler) (stop bool) {
		for _, tbl := range level.tables {
			ids = append(ids, tbl.ID())
		}
		return false
	})
	return ids
}

func (s *Shard) isSplitting() bool {
	return atomic.LoadUint32(&s.splitState) >= splitStateSplitting
}

func (s *Shard) GetEstimatedSize() int64 {
	return atomic.LoadInt64(&s.estimatedSize)
}

func (s *Shard) addEstimatedSize(size int64) int64 {
	return atomic.AddInt64(&s.estimatedSize, size)
}

func (s *Shard) setSplitKeys(keys [][]byte) bool {
	if atomic.CompareAndSwapUint32(&s.splitState, splitStateInitial, splitStateSetKeys) {
		s.splitKeys = keys
		s.splittingMemTbls = make([]*unsafe.Pointer, len(keys)+1)
		s.splittingL0s = make([]*unsafe.Pointer, len(keys)+1)
		for i := range s.splittingMemTbls {
			memPtr := new(unsafe.Pointer)
			*memPtr = unsafe.Pointer(&shardingMemTables{})
			s.splittingMemTbls[i] = memPtr
			l0Ptr := new(unsafe.Pointer)
			*l0Ptr = unsafe.Pointer(&shardL0Tables{})
			s.splittingL0s[i] = l0Ptr
		}
		y.Assert(atomic.CompareAndSwapUint32(&s.splitState, splitStateSetKeys, splitStateSplitting))
		return true
	}
	return false
}

func (s *Shard) setSplitDone() {
	y.Assert(atomic.CompareAndSwapUint32(&s.splitState, splitStateSplitting, splitStateSplitDone))
}

func (s *Shard) foreachLevel(f func(cf int, level *levelHandler) (stop bool)) {
	for cf, scf := range s.cfs {
		for i := 1; i <= ShardMaxLevel; i++ {
			l := scf.getLevelHandler(i)
			if stop := f(cf, l); stop {
				return
			}
		}
	}
}

func (s *Shard) Get(cf int, key y.Key) y.ValueStruct {
	keyHash := farm.Fingerprint64(key.UserKey)
	if s.isSplitting() {
		idx := s.getSplittingIndex(key.UserKey)
		memTbls := s.loadSplittingMemTables(idx)
		for _, tbl := range memTbls.tables {
			v := tbl.Get(cf, key.UserKey, key.Version)
			if v.Valid() {
				return v
			}
		}
		l0Tbls := s.loadSplittingL0Tables(idx)
		for _, tbl := range l0Tbls.tables {
			v := tbl.Get(cf, key, keyHash)
			if v.Valid() {
				return v
			}
		}
	}
	memTbls := s.loadMemTables()
	for _, tbl := range memTbls.tables {
		v := tbl.Get(cf, key.UserKey, key.Version)
		if v.Valid() {
			return v
		}
	}
	l0Tbls := s.loadL0Tables()
	for _, tbl := range l0Tbls.tables {
		v := tbl.Get(cf, key, keyHash)
		if v.Valid() {
			return v
		}
	}
	scf := s.cfs[cf]
	for i := 1; i <= ShardMaxLevel; i++ {
		level := scf.getLevelHandler(i)
		if len(level.tables) == 0 {
			continue
		}
		v := level.get(key, keyHash)
		if v.Valid() {
			return v
		}
	}
	return y.ValueStruct{}
}

func (s *Shard) getSplittingIndex(key []byte) int {
	var i int
	for ; i < len(s.splitKeys); i++ {
		if bytes.Compare(key, s.splitKeys[i]) < 0 {
			break
		}
	}
	return i
}

func (s *Shard) loadSplittingL0Tables(i int) *shardL0Tables {
	return (*shardL0Tables)(atomic.LoadPointer(s.splittingL0s[i]))
}

func (s *Shard) casSplittingL0Tables(i int, old, new *shardL0Tables) bool {
	return atomic.CompareAndSwapPointer(s.splittingL0s[i], unsafe.Pointer(old), unsafe.Pointer(new))
}

func (s *Shard) loadSplittingMemTables(i int) *shardingMemTables {
	return (*shardingMemTables)(atomic.LoadPointer(s.splittingMemTbls[i]))
}

func (s *Shard) loadSplittingWritableMemTable(i int) *memtable.CFTable {
	tbls := s.loadSplittingMemTables(i)
	if tbls != nil && len(tbls.tables) > 0 {
		return tbls.tables[0]
	}
	return nil
}

func (s *Shard) loadMemTables() *shardingMemTables {
	return (*shardingMemTables)(atomic.LoadPointer(s.memTbls))
}

func (s *Shard) loadWritableMemTable() *memtable.CFTable {
	tbls := s.loadMemTables()
	if len(tbls.tables) > 0 {
		return tbls.tables[0]
	}
	return nil
}

func (s *Shard) loadL0Tables() *shardL0Tables {
	return (*shardL0Tables)(atomic.LoadPointer(s.l0s))
}

func (s *Shard) getSplitKeys(targetSize int64) [][]byte {
	if s.GetEstimatedSize() < targetSize {
		return nil
	}
	var maxLevel *levelHandler
	s.foreachLevel(func(cf int, level *levelHandler) (stop bool) {
		if maxLevel == nil {
			maxLevel = level
		}
		if maxLevel.totalSize < level.totalSize {
			maxLevel = level
		}
		return false
	})
	levelTargetSize := int64(float64(targetSize) * (float64(maxLevel.totalSize) / float64(s.GetEstimatedSize())))
	var keys [][]byte
	var currentSize int64
	for i, tbl := range maxLevel.tables {
		currentSize += tbl.Size()
		if i != 0 && currentSize > levelTargetSize {
			keys = append(keys, tbl.Smallest().UserKey)
			currentSize = 0
		}
	}
	return keys
}

func (s *Shard) Delete() error {
	s.foreachLevel(func(cf int, level *levelHandler) (stop bool) {
		for _, tbl := range level.tables {
			tbl.Close()
			if s.removeFilesOnDel {
				tbl.Delete()
			}
		}
		return false
	})
	return nil
}

func (s *Shard) GetFullMeta() *protos.MetaChangeEvent {
	e := new(protos.MetaChangeEvent)
	e.StartKey = s.Start
	e.EndKey = s.End
	if s.isSplitting() {
		for i := 0; i < len(s.splittingMemTbls); i++ {
			startKey, endKey := getSplittingStartEnd(s.Start, s.End, s.splitKeys, i)
			l0Tbls := s.loadSplittingL0Tables(i)
			for _, l0Tbl := range l0Tbls.tables {
				e.AddedL0Files = append(e.AddedL0Files, &protos.L0FileMeta{
					ID:       l0Tbl.fid,
					CommitTS: l0Tbl.commitTS,
					StartKey: startKey,
					EndKey:   endKey,
				})
			}
		}
	}
	l0s := s.loadL0Tables()
	for _, tbl := range l0s.tables {
		e.AddedL0Files = append(e.AddedL0Files, &protos.L0FileMeta{
			ID:       tbl.fid,
			CommitTS: tbl.commitTS,
			StartKey: s.Start,
			EndKey:   s.End,
		})
	}
	s.foreachLevel(func(cf int, level *levelHandler) (stop bool) {
		for _, tbl := range level.tables {
			e.AddedFiles = append(e.AddedFiles, &protos.FileMeta{
				ID:       tbl.ID(),
				CF:       int32(cf),
				Level:    uint32(level.level),
				Smallest: tbl.Smallest().UserKey,
				Biggest:  tbl.Biggest().UserKey,
			})
		}
		return false
	})
	return e
}

func getSplittingStartEnd(oldStart, oldEnd []byte, splitKeys [][]byte, i int) (startKey, endKey []byte) {
	if i != 0 {
		startKey = splitKeys[i-1]
	} else {
		startKey = oldStart
	}
	if i == len(splitKeys) {
		endKey = oldEnd
	} else {
		endKey = splitKeys[i]
	}
	return
}

func (s *Shard) OverlapRange(startKey, endKey []byte) bool {
	return bytes.Compare(s.Start, endKey) < 0 && bytes.Compare(startKey, s.End) < 0
}

func (s *Shard) OverlapKey(key []byte) bool {
	return bytes.Compare(s.Start, key) <= 0 && bytes.Compare(key, s.End) < 0
}

type shardCF struct {
	levels []unsafe.Pointer
}

func (scf *shardCF) getLevelHandler(level int) *levelHandler {
	return (*levelHandler)(atomic.LoadPointer(&scf.levels[level-1]))
}

func (scf *shardCF) casLevelHandler(level int, oldH, newH *levelHandler) bool {
	return atomic.CompareAndSwapPointer(&scf.levels[level-1], unsafe.Pointer(oldH), unsafe.Pointer(newH))
}

func (scf *shardCF) setHasOverlapping(cd *CompactDef) {
	if cd.moveDown() {
		return
	}
	kr := getKeyRange(cd.Top)
	for lvl := cd.Level + 2; lvl < len(scf.levels); lvl++ {
		lh := scf.getLevelHandler(lvl)
		left, right := lh.overlappingTables(levelHandlerRLocked{}, kr)
		if right-left > 0 {
			cd.HasOverlap = true
			return
		}
	}
	return
}

type deletions struct {
	resources []epoch.Resource
}

func (d *deletions) Append(res epoch.Resource) {
	d.resources = append(d.resources, res)
}
