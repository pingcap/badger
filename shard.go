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
	"github.com/pingcap/badger/table"
	"github.com/pingcap/badger/table/sstable"
	"github.com/pingcap/badger/y"
)

type ShardManager struct {
	lastShardID uint32
	lastFid     *uint32
	opt         Options
	metrics     *y.MetricsSet
	guard       *epoch.Guard
}

func newShardTree(manifest *ShardingManifest, opt Options, metrics *y.MetricsSet) (*shardTree, error) {
	tree := &shardTree{
		shards: make([]*Shard, 0, len(manifest.shards)),
	}
	for _, mShard := range manifest.shards {
		shard := &Shard{
			ID:    mShard.ID,
			Start: mShard.Start,
			End:   mShard.End,
			cfs:   make([]*shardCF, len(opt.CFs)),
		}
		for i := 0; i < len(opt.CFs); i++ {
			sCF := &shardCF{
				levels: make([]unsafe.Pointer, opt.TableBuilderOptions.MaxLevels),
			}
			sCF.setLevelHandler(0, newLevelHandler(opt.NumLevelZeroTablesStall, 0, metrics))
			shard.cfs[i] = sCF
		}

		for fid, cfLevel := range mShard.files {
			cf := cfLevel.cf()
			level := cfLevel.level()
			scf := shard.cfs[cf]
			handler := scf.getLevelHandler(int(level))
			if handler == nil {
				handler = newLevelHandler(opt.NumLevelZeroTablesStall, int(level), metrics)
				scf.setLevelHandler(int(level), handler)
			}
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
				if handler != nil {
					sortTables(handler.tables)
				}
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
	idx := sort.Search(len(st.shards), func(i int) bool {
		return bytes.Compare(st.shards[i].Start, removes[0].Start) < 0
	})
	newShards = append(newShards, st.shards[:idx]...)
	newShards = append(newShards, adds...)
	newShards = append(newShards, st.shards[idx+len(removes):]...)
	return &shardTree{shards: newShards}
}

func (st *shardTree) get(key []byte) *Shard {
	idx := sort.Search(len(st.shards), func(i int) bool {
		return bytes.Compare(key, st.shards[i].End) < 0
	})
	return st.shards[idx]
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
	ID        uint32
	Start     []byte
	End       []byte
	splitKeys [][]byte
	cfs       []*shardCF
	lock      sync.Mutex
}

type shardCF struct {
	levels []unsafe.Pointer
}

func (scf *shardCF) getLevelHandler(i int) *levelHandler {
	return (*levelHandler)(atomic.LoadPointer(&scf.levels[i]))
}

func (scf *shardCF) setLevelHandler(i int, h *levelHandler) {
	atomic.StorePointer(&scf.levels[i], unsafe.Pointer(h))
}

func (st *Shard) Get(cf byte, key y.Key, keyHash uint64) y.ValueStruct {
	scf := st.cfs[cf]
	for i := range scf.levels {
		level := scf.getLevelHandler(i)
		if i == 0 {
			y.Assert(level != nil)
		}
		if level == nil {
			return y.ValueStruct{}
		}
		v := level.get(key, keyHash)
		if v.Valid() {
			return v
		}
	}
	return y.ValueStruct{}
}

type Level struct {
	tables []table.Table
}

// preSplit blocks all compactions
func (sdb *ShardingDB) preSplit(s *Shard, keys [][]byte, guard *epoch.Guard) error {
	s.lock.Lock()
	s.splitKeys = keys
	s.lock.Unlock()
	// Set split keys so any compaction would fail.
	// We can safely split our tables.
	for _, scf := range s.cfs {
		for i := range scf.levels {
			handler := scf.getLevelHandler(i)
			if handler == nil {
				continue
			}
			if err := sdb.splitTables(handler, keys, guard); err != nil {
				return err
			}
		}
	}
	return nil
}

func (sdb *ShardingDB) splitTables(handler *levelHandler, keys [][]byte, guard *epoch.Guard) error {
	handler.Lock()
	oldTables := handler.tables
	handler.Unlock()
	newTbls := make([]table.Table, 0, len(oldTables)+len(keys))
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
		// append an end key to build the last table.
		relatedKeys = append(relatedKeys, nil)
		itr := tbl.NewIterator(false)
		itr.Rewind()
		for _, relatedKey := range relatedKeys {
			tbl, err := sdb.buildTableBeforeKey(itr, relatedKey, handler.level, sdb.opt.TableBuilderOptions)
			if err != nil {
				return err
			}
			if tbl != nil {
				newTbls = append(newTbls, tbl)
			}
		}
	}
	handler.Lock()
	handler.tables = newTbls
	handler.Unlock()
	toDelete := make([]epoch.Resource, len(oldTables))
	for i, oldTbl := range oldTables {
		toDelete[i] = oldTbl
	}
	guard.Delete(toDelete)
	return nil
}

func (sdb *ShardingDB) buildTableBeforeKey(itr y.Iterator, key []byte, level int, opt options.TableBuilderOptions) (table.Table, error) {
	filename := sstable.NewFilename(uint64(sdb.allocShardID()), sdb.opt.Dir)
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
	shardByKey := sdb.loadShardTree()
	var shardTasks []shardSplitTask
	for _, key := range keys {
		if len(shardTasks) > 0 {
			shardTask := shardTasks[len(shardTasks)-1]
			if bytes.Compare(shardTask.shard.End, key) > 0 {
				shardTask.keys = append(shardTask.keys, key)
				continue
			}
		}
		shardTask := shardSplitTask{
			shard: shardByKey.get(key),
			keys:  [][]byte{key},
		}
		shardTasks = append(shardTasks, shardTask)
	}
	for _, shardTask := range shardTasks {
		if err := sdb.preSplit(shardTask.shard, shardTask.keys, guard); err != nil {
			return err
		}
	}
	task := &splitTask{
		shards: shardTasks,
		notify: make(chan error, 1),
	}
	sdb.writeCh <- engineTask{splitTask: task}
	return <-task.notify
}

// At this stage, all files are split by splitKeys, it is a fast operation.
// This is done after preSplit is done, so we don't need to acquire any lock, just atomic CAS will do.
func (sdb *ShardingDB) split(s *Shard) []*Shard {
	newShards := make([]*Shard, 0, len(s.splitKeys)+1)
	firstShard := &Shard{
		ID:    sdb.allocShardID(),
		Start: s.Start,
		End:   s.splitKeys[0],
		cfs:   make([]*shardCF, len(s.cfs)),
	}
	newShards = append(newShards, firstShard)
	for i, splitKey := range s.splitKeys {
		var endKey []byte
		if i == len(s.splitKeys)-1 {
			endKey = s.End
		} else {
			endKey = s.splitKeys[i+1]
		}
		newShards = append(newShards, &Shard{
			ID:    sdb.allocShardID(),
			Start: splitKey,
			End:   endKey,
			cfs:   make([]*shardCF, len(s.cfs)),
		})
	}
	for i, scf := range s.cfs {
		for j := range scf.levels {
			level := scf.getLevelHandler(j)
			if level == nil {
				continue
			}
			for _, t := range level.tables {
				sdb.insertTableToNewShard(t, i, level.level, newShards)
			}
		}
	}
	return newShards
}

func (sdb *ShardingDB) insertTableToNewShard(t table.Table, cf, level int, shards []*Shard) {
	for _, shard := range shards {
		if bytes.Compare(shard.Start, t.Smallest().UserKey) <= 0 {
			sCF := shard.cfs[cf]
			if sCF == nil {
				sCF = &shardCF{
					levels: make([]unsafe.Pointer, sdb.opt.TableBuilderOptions.MaxLevels),
				}
				shard.cfs[cf] = sCF
			}
			handler := sCF.getLevelHandler(level)
			if handler == nil {
				handler = newLevelHandler(sdb.opt.NumLevelZeroTablesStall, level, sdb.metrics)
				sCF.setLevelHandler(level, handler)
			}
			handler.tables = append(handler.tables, t)
			break
		}
	}
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
