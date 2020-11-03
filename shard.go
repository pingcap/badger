package badger

import (
	"bytes"
	"encoding/binary"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/ncw/directio"
	"github.com/pingcap/badger/epoch"
	"github.com/pingcap/badger/options"
	"github.com/pingcap/badger/protos"
	"github.com/pingcap/badger/table"
	"github.com/pingcap/badger/table/sstable"
	"github.com/pingcap/badger/y"
)

const shardMaxLevel = 4

func newShardTree(manifest *ShardingManifest, opt Options, metrics *y.MetricsSet) (*shardTree, error) {
	tree := &shardTree{
		shards: make([]*Shard, 0, len(manifest.shards)),
	}
	for _, mShard := range manifest.shards {
		shard := newShard(mShard.ID, mShard.Start, mShard.End, opt, metrics)
		for fid := range mShard.files {
			cfLevel := manifest.globalFiles[fid]
			cf := cfLevel.cf
			level := cfLevel.level
			scf := shard.cfs[cf]
			handler := scf.getLevelHandler(int(level))
			filename := sstable.NewFilename(uint64(fid), opt.Dir)
			reader, err := sstable.NewMMapFile(filename)
			if err != nil {
				return nil, err
			}
			tbl, err := sstable.OpenTable(filename, reader)
			if err != nil {
				return nil, err
			}
			handler.tables = append(handler.tables, tbl)
		}
		for i := 0; i < len(opt.CFs); i++ {
			scf := shard.cfs[i]
			for i := range scf.levels {
				handler := scf.getLevelHandler(i)
				sortTables(handler.tables)
			}
		}
		tree.shards = append(tree.shards, shard)
	}
	sort.Slice(tree.shards, func(i, j int) bool {
		return bytes.Compare(tree.shards[i].Start, tree.shards[j].Start) < 0
	})
	return tree, nil
}

// This data structure is rarely written, so we can make it immutable.
type shardTree struct {
	shards []*Shard
}

func (tree *shardTree) last() *Shard {
	return tree.shards[len(tree.shards)-1]
}

// the removes and adds has the same total range, so we can append before and after.
func (st *shardTree) replace(removes []*Shard, adds []*Shard) *shardTree {
	newShards := make([]*Shard, 0, len(st.shards)+len(adds)-len(removes))
	left, right := getShardRange(st.shards, removes[0].Start, removes[len(removes)-1].End)
	newShards = append(newShards, st.shards[:left]...)
	newShards = append(newShards, adds...)
	newShards = append(newShards, st.shards[right:]...)
	return &shardTree{shards: newShards}
}

func (st *shardTree) get(key []byte) *Shard {
	return getShard(st.shards, key)
}

func getShard(shards []*Shard, key []byte) *Shard {
	idx := sort.Search(len(shards), func(i int) bool {
		return bytes.Compare(key, shards[i].End) < 0
	})
	return shards[idx]
}

func getShardRange(shards []*Shard, start, end []byte) (left, right int) {
	left = sort.Search(len(shards), func(i int) bool {
		return bytes.Compare(start, shards[i].End) < 0
	})
	right = sort.Search(len(shards), func(i int) bool {
		return bytes.Compare(end, shards[i].Start) <= 0
	})
	return
}

func (st *shardTree) getShards(start, end []byte) []*Shard {
	left, right := getShardRange(st.shards, start, end)
	return st.shards[left:right]
}

func (sdb *ShardingDB) allocShardID() uint32 {
	return atomic.AddUint32(&sdb.lastShardID, 1)
}

// Shard split can be performed by the following steps:
// 1. set the splitKeys and mark the state to splitting.
// 2. a splitting Shard will separate all the files by the SplitKeys.
// 3. incoming SST is also split by SplitKeys.
// 4. After all existing files are split by the split keys, the state is changed to SplitDone.
// 5. After SplitDone, the shard map replace the old shard two new shardsByID.
type Shard struct {
	ID    uint32
	Start []byte
	End   []byte
	cfs   []*shardCF
	lock  sync.Mutex

	// minGlobalL0 is used to ignore data in global l0 files that has been range deleted.
	minGlobalL0 uint32
}

func newShard(id uint32, start, end []byte, opt Options, metrics *y.MetricsSet) *Shard {
	shard := &Shard{
		ID:    id,
		Start: start,
		End:   end,
		cfs:   make([]*shardCF, len(opt.CFs)),
	}
	for i := 0; i < len(opt.CFs); i++ {
		sCF := &shardCF{
			levels: make([]unsafe.Pointer, shardMaxLevel),
		}
		for j := 0; j < shardMaxLevel; j++ {
			sCF.casLevelHandler(j, nil, newLevelHandler(opt.NumLevelZeroTablesStall, j, metrics))
		}
		shard.cfs[i] = sCF
	}
	return shard
}

func (s *Shard) tableIDs() []uint32 {
	var ids []uint32
	s.foreachLevel(func(cf int, level *levelHandler) (stop bool) {
		for _, tbl := range level.tables {
			ids = append(ids, uint32(tbl.ID()))
		}
		return false
	})
	return ids
}

func (s *Shard) foreachLevel(f func(cf int, level *levelHandler) (stop bool)) {
	for cf, scf := range s.cfs {
		for i := 0; i < shardMaxLevel; i++ {
			l := scf.getLevelHandler(i)
			if stop := f(cf, l); stop {
				return
			}
		}
	}
}

func (s *Shard) loadMinGlobalL0() uint32 {
	return atomic.LoadUint32(&s.minGlobalL0)
}

type shardCF struct {
	levels []unsafe.Pointer
}

func (scf *shardCF) getLevelHandler(i int) *levelHandler {
	return (*levelHandler)(atomic.LoadPointer(&scf.levels[i]))
}

func (scf *shardCF) casLevelHandler(i int, oldH, newH *levelHandler) bool {
	return atomic.CompareAndSwapPointer(&scf.levels[i], unsafe.Pointer(oldH), unsafe.Pointer(newH))
}

func (scf *shardCF) setHasOverlapping(cd *CompactDef) {
	if cd.moveDown() {
		return
	}
	kr := getKeyRange(cd.Top)
	for i := cd.Level + 2; i < len(scf.levels); i++ {
		lh := scf.getLevelHandler(i)
		left, right := lh.overlappingTables(levelHandlerRLocked{}, kr)
		if right-left > 0 {
			cd.HasOverlap = true
			return
		}
	}
	return
}

type Level struct {
	tables []table.Table
}

// preSplit blocks all compactions
func (sdb *ShardingDB) preSplit(s *Shard, keys [][]byte, guard *epoch.Guard, change *protos.ManifestChangeSet) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	// Set split keys so any compaction would fail.
	// We can safely split our tables.
	for cf := 0; cf < sdb.numCFs; cf++ {
		for lvl := 0; lvl < shardMaxLevel; lvl++ {
			if err := sdb.splitTables(s, cf, lvl, keys, guard, change); err != nil {
				return err
			}
		}
	}
	return nil
}

func (sdb *ShardingDB) splitTables(shard *Shard, cf int, level int, keys [][]byte, guard *epoch.Guard, changeSet *protos.ManifestChangeSet) error {
	scf := shard.cfs[cf]
	oldHandler := scf.getLevelHandler(level)
	oldTables := oldHandler.tables
	newTbls := make([]table.Table, 0, len(oldTables)+len(keys))
	toDeleteIDs := make(map[uint64]struct{})
	var relatedKeys [][]byte
	for _, tbl := range oldTables {
		relatedKeys = relatedKeys[:0]
		for _, key := range keys {
			if bytes.Compare(tbl.Smallest().UserKey, key) < 0 &&
				bytes.Compare(key, tbl.Biggest().UserKey) <= 0 {
				relatedKeys = append(relatedKeys, key)
			}
		}
		if len(relatedKeys) == 0 {
			newTbls = append(newTbls, tbl)
			continue
		}
		toDeleteIDs[tbl.ID()] = struct{}{}
		// append an end key to build the last table.
		relatedKeys = append(relatedKeys, globalShardEndKey)
		itr := tbl.NewIterator(false)
		itr.Rewind()
		for _, relatedKey := range relatedKeys {
			ntbl, err := sdb.buildTableBeforeKey(itr, relatedKey, level, sdb.opt.TableBuilderOptions)
			if err != nil {
				return err
			}
			if ntbl != nil {
				changeSet.Changes = append(changeSet.Changes, newShardCreateChange(shard.ID, ntbl.ID(), cf, level))
				newTbls = append(newTbls, ntbl)
			}
		}
		changeSet.Changes = append(changeSet.Changes, newShardDeleteChange(shard.ID, tbl.ID(), cf))
	}
	newHandler := newLevelHandler(sdb.opt.NumLevelZeroTablesStall, level, sdb.metrics)
	newHandler.totalSize = oldHandler.totalSize
	newHandler.tables = newTbls
	y.Assert(scf.casLevelHandler(level, oldHandler, newHandler))
	toDelete := make([]epoch.Resource, 0, len(toDeleteIDs))
	for _, oldTbl := range oldTables {
		if _, ok := toDeleteIDs[oldTbl.ID()]; ok {
			toDelete = append(toDelete, oldTbl)
		}
	}
	guard.Delete(toDelete)
	return nil
}

func (sdb *ShardingDB) buildTableBeforeKey(itr y.Iterator, key []byte, level int, opt options.TableBuilderOptions) (table.Table, error) {
	filename := sstable.NewFilename(uint64(sdb.allocFid("splitTable")), sdb.opt.Dir)
	fd, err := directio.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	b := sstable.NewTableBuilder(fd, nil, level, opt)
	for itr.Valid() {
		if len(key) > 0 && bytes.Compare(itr.Key().UserKey, key) >= 0 {
			return builderToTable(b)
		}
		err = b.Add(itr.Key(), itr.Value())
		if err != nil {
			return nil, err
		}
		y.NextAllVersion(itr)
	}
	return builderToTable(b)
}

func builderToTable(b *sstable.Builder) (table.Table, error) {
	if b.Empty() {
		return nil, nil
	}
	result, err1 := b.Finish()
	if err1 != nil {
		return nil, err1
	}
	mmapFile, err1 := sstable.NewMMapFile(result.FileName)
	if err1 != nil {
		return nil, err1
	}
	return sstable.OpenTable(result.FileName, mmapFile)
}

func (sdb *ShardingDB) Split(keys [][]byte) error {
	guard := sdb.resourceMgr.Acquire()
	defer guard.Done()
	sdb.splitLock.Lock()
	defer sdb.splitLock.Unlock()
	return sdb.splitInLock(keys, guard)
}

func (sdb *ShardingDB) splitInLock(keys [][]byte, guard *epoch.Guard) error {
	tree := sdb.loadShardTree()
	changeSet := &protos.ManifestChangeSet{}
	shardTasks := map[uint32]*shardSplitTask{}
	for _, key := range keys {
		shard := tree.get(key)
		task, ok := shardTasks[shard.ID]
		if !ok {
			task = &shardSplitTask{
				shard: shard,
			}
			shardTasks[shard.ID] = task
		}
		task.keys = append(task.keys, key)
	}
	for _, shardTask := range shardTasks {
		if err := sdb.preSplit(shardTask.shard, shardTask.keys, guard, changeSet); err != nil {
			return err
		}
	}
	task := &splitTask{
		shards: shardTasks,
		notify: make(chan error, 1),
		change: changeSet,
	}
	sdb.writeCh <- engineTask{splitTask: task}
	return <-task.notify
}

// At this stage, all files are split by splitKeys, it is a fast operation.
// This is done after preSplit is done, so we don't need to acquire any lock, just atomic CAS will do.
func (sdb *ShardingDB) split(s *Shard, splitKeys [][]byte) []*Shard {
	newShards := make([]*Shard, 0, len(splitKeys)+1)
	firstShard := newShard(sdb.allocShardID(), s.Start, splitKeys[0], sdb.opt, sdb.metrics)
	newShards = append(newShards, firstShard)
	for i, splitKey := range splitKeys {
		var endKey []byte
		if i == len(splitKeys)-1 {
			endKey = s.End
		} else {
			endKey = splitKeys[i+1]
		}
		newShards = append(newShards, newShard(sdb.allocShardID(), splitKey, endKey, sdb.opt, sdb.metrics))
	}
	for i, scf := range s.cfs {
		for j := range scf.levels {
			level := scf.getLevelHandler(j)
			for _, t := range level.tables {
				sdb.insertTableToNewShard(t, i, level.level, newShards)
			}
		}
	}
	return newShards
}

func (sdb *ShardingDB) insertTableToNewShard(t table.Table, cf, level int, shards []*Shard) {
	shard := getShard(shards, t.Smallest().UserKey)
	sCF := shard.cfs[cf]
	handler := sCF.getLevelHandler(level)
	handler.tables = append(handler.tables, t)
}

type shardDataBuilder struct {
	shard    *Shard
	builders []*sstable.Builder
}

func newShardDataBuilder(shard *Shard, numCFs int, opt options.TableBuilderOptions) *shardDataBuilder {
	sdb := &shardDataBuilder{
		shard:    shard,
		builders: make([]*sstable.Builder, numCFs),
	}
	for i := 0; i < numCFs; i++ {
		sdb.builders[i] = sstable.NewTableBuilder(nil, nil, 0, opt)
	}
	return sdb
}

func (e *shardDataBuilder) Add(cf byte, key y.Key, value y.ValueStruct) {
	e.builders[cf].Add(key, value)
}

/*
shard Data format:
 | CF0 Data | CF0 index | CF1 Data | CF1 index | cfs index
*/
func (e *shardDataBuilder) Finish() []byte {
	cfDatas := make([][]byte, 0, len(e.builders)*2)
	cfsIndex := make([]byte, len(e.builders)*8)
	var fileSize int
	for i, builder := range e.builders {
		result, _ := builder.Finish()
		cfDatas = append(cfDatas, result.FileData)
		fileSize += len(result.FileData)
		binary.LittleEndian.PutUint32(cfsIndex[i*8:], uint32(fileSize))
		cfDatas = append(cfDatas, result.IndexData)
		fileSize += len(result.IndexData)
		binary.LittleEndian.PutUint32(cfsIndex[i*8+4:], uint32(fileSize))
	}
	result := make([]byte, 0, fileSize+len(cfsIndex)+1)
	result = append(result)
	for _, cfData := range cfDatas {
		result = append(result, cfData...)
	}
	result = append(result, cfsIndex...)
	result = append(result, byte(len(e.builders))) // number of CF
	return result
}
