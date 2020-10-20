package badger

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/ncw/directio"
	"github.com/pingcap/badger/epoch"
	"os"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/google/btree"
	"github.com/pingcap/badger/options"
	"github.com/pingcap/badger/table"
	"github.com/pingcap/badger/table/sstable"
	"github.com/pingcap/badger/y"
)

type ShardManager struct {
	// shardByID is a very frequent operation, we need to use plain map without lock to access.
	// It is only used in write thread.
	shardsByID map[uint32]*Shard
	oldShards  map[uint32]*Shard
	// for infrequent usage out of the write thread we use a sync.Map to access.
	shardsByIDSafe sync.Map

	shardsByKey *shardByKeyTree
	lastShardID uint32
	lastFid     *uint32
	opt         Options
	metrics     *y.MetricsSet
	guard       *epoch.Guard
}

func NewShardManager(manifest *ShardingManifest, opt Options, metrics *y.MetricsSet) (*ShardManager, error) {
	sm := &ShardManager{
		shardsByID:  map[uint32]*Shard{},
		shardsByKey: &shardByKeyTree{tree: btree.New(8)},
		opt:         opt,
		metrics:     metrics,
	}
	for _, mShard := range manifest.shards {
		shard := &Shard{
			ID:       mShard.ID,
			Start:    mShard.Start,
			End:      mShard.End,
			cfLevels: make([]unsafe.Pointer, opt.NumCFs),
		}
		for i := 0; i < opt.NumCFs; i++ {
			shardLevels := &ShardLevels{
				levels: make([]*levelHandler, opt.TableBuilderOptions.MaxLevels),
			}
			shard.cfLevels[i] = unsafe.Pointer(shardLevels)
		}

		for fid, cfLevel := range mShard.files {
			cf := cfLevel.cf()
			level := cfLevel.level()
			shardLevels := (*ShardLevels)(shard.cfLevels[cf])
			handler := shardLevels.levels[level]
			if handler == nil {
				handler = newLevelHandler(opt.NumLevelZeroTablesStall, int(level), metrics)
				shardLevels.levels[level] = handler
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
		for i := 0; i < opt.NumCFs; i++ {
			shardLevels := (*ShardLevels)(shard.cfLevels[i])
			for _, handler := range shardLevels.levels {
				if handler != nil {
					sortTables(handler.tables)
				}
			}
		}
		sm.shardsByID[shard.ID] = shard
	}
	for _, oldShard := range manifest.oldShards {
		sm.oldShards[oldShard.ID] = &Shard{
			ID:    oldShard.ID,
			Start: oldShard.Start,
			End:   oldShard.End,
		}
	}
	return sm, nil
}

type shardByKeyTree struct {
	mu   sync.RWMutex
	tree *btree.BTree
}

func (st *shardByKeyTree) replace(removes []*Shard, adds []*Shard) {
	st.mu.Lock()
	defer st.mu.Unlock()
	for _, remove := range removes {
		st.tree.Delete(shardItem{
			startKey: remove.Start,
		})
	}
	for _, add := range adds {
		st.tree.ReplaceOrInsert(shardItem{
			startKey: add.Start,
			shardID:  add.ID,
		})
	}
}

type shardItem struct {
	startKey []byte
	shardID  uint32
}

func (si shardItem) Less(i btree.Item) bool {
	return bytes.Compare(si.startKey, i.(shardItem).startKey) < 0
}

func (st *shardByKeyTree) get(key []byte) uint32 {
	st.mu.RLock()
	defer st.mu.RUnlock()
	var shardID uint32
	st.tree.DescendLessOrEqual(shardItem{
		startKey: key,
	}, func(i btree.Item) bool {
		shardID = i.(shardItem).shardID
		return false
	})
	return shardID
}

func (sm *ShardManager) getShardByIDSafe(shardID uint32) *Shard {
	val, ok := sm.shardsByIDSafe.Load(shardID)
	if !ok {
		return nil
	}
	return val.(*Shard)
}

func (sm *ShardManager) split(shardID uint32) error {
	oldShard := sm.shardsByID[shardID]
	newShards := sm.Split(oldShard)
	for _, ns := range newShards {
		sm.shardsByIDSafe.Store(ns.ID, ns)
		sm.shardsByID[ns.ID] = ns
	}
	sm.shardsByKey.replace([]*Shard{oldShard}, newShards)
	sm.shardsByIDSafe.Delete(shardID)
	delete(sm.shardsByID, shardID)
	return nil
}

func (sm *ShardManager) allocShardID() uint32 {
	return atomic.AddUint32(&sm.lastShardID, 1)
}

func (sm *ShardManager) allocFileD() uint32 {
	return atomic.AddUint32(sm.lastFid, 1)
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
	cfLevels  []unsafe.Pointer
	lock      sync.Mutex
}

type ShardLevels struct {
	levels []*levelHandler
}

func (st *ShardLevels) Clone() *ShardLevels {
	return nil // TODO
}

func (st *Shard) LoadShardLevels(cf int) *ShardLevels {
	ptr := atomic.LoadPointer(&st.cfLevels[cf])
	return (*ShardLevels)(ptr)
}

func (s *Shard) CASLevels(cf int, old, new *ShardLevels) bool {
	return atomic.CompareAndSwapPointer(&s.cfLevels[cf], unsafe.Pointer(old), unsafe.Pointer(new))
}

type Level struct {
	tables []table.Table
}

// PreSplit blocks all compactions
func (sm *ShardManager) PreSplit(s *Shard, keys [][]byte) error {
	s.lock.Lock()
	s.splitKeys = keys
	s.lock.Unlock()
	// Set split keys so any compaction would fail.
	// We can safely split our tables.
	for cf := range s.cfLevels {
		shardLevels := s.LoadShardLevels(cf)
		for _, handler := range shardLevels.levels {
			if handler == nil {
				continue
			}
			if err := sm.splitTables(handler, keys); err != nil {
				return err
			}
		}
	}
	return nil
}

func (sm *ShardManager) splitTables(handler *levelHandler, keys [][]byte) error {
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
			tbl, err := sm.buildTableBeforeKey(itr, relatedKey, handler.level, sm.opt.TableBuilderOptions)
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
	sm.guard.Delete(toDelete)
	return nil
}

func (sm *ShardManager) buildTableBeforeKey(itr y.Iterator, key []byte, level int, opt options.TableBuilderOptions) (table.Table, error) {
	filename := sstable.NewFilename(uint64(sm.allocFileD()), sm.opt.Dir)
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

// At this stage, all files are split by splitKeys, it is a fast operation.
// This is done after PreSplit is done, so we don't need to acquire any lock, just atomic CAS will do.
func (sm *ShardManager) Split(s *Shard) []*Shard {
	newShards := make([]*Shard, 0, len(s.splitKeys)+1)
	firstShard := &Shard{
		ID:       sm.allocShardID(),
		Start:    s.Start,
		End:      s.splitKeys[0],
		cfLevels: make([]unsafe.Pointer, len(s.cfLevels)),
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
			ID:       sm.allocShardID(),
			Start:    splitKey,
			End:      endKey,
			cfLevels: make([]unsafe.Pointer, len(s.cfLevels)),
		})
	}
	for i := 0; i < len(s.cfLevels); i++ {
		oldCFLevel := s.LoadShardLevels(i)
		for _, level := range oldCFLevel.levels {
			if level == nil {
				continue
			}
			for _, t := range level.tables {
				sm.insertTableToNewShard(t, i, level.level, newShards)
			}
		}
	}
	sm.shardsByKey.replace([]*Shard{s}, newShards)

	return newShards
}

func (sm *ShardManager) insertTableToNewShard(t table.Table, cf, level int, shards []*Shard) {
	for _, shard := range shards {
		if bytes.Compare(shard.Start, t.Smallest().UserKey) <= 0 {
			shardLevels := shard.LoadShardLevels(cf)
			if shardLevels == nil {
				shardLevels = &ShardLevels{
					levels: make([]*levelHandler, sm.opt.TableBuilderOptions.MaxLevels),
				}
				shard.cfLevels[cf] = unsafe.Pointer(shardLevels)
			}
			handler := shardLevels.levels[level]
			if handler == nil {
				handler = newLevelHandler(sm.opt.NumLevelZeroTablesStall, level, sm.metrics)
				shardLevels.levels[level] = handler
			}
			handler.tables = append(handler.tables, t)
			break
		}
	}
}

// AddTables has the highest priority, it should not be blocked.
// The ShardingEngine should execute AddTables concurrently for different shard because it may take time to split
// the newly added table.
// It returns error is the shard is deleted.
func (s *Shard) AddTables(tbls []table.Table) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	for cf, tbl := range tbls {
		splitTbls := []table.Table{tbl}
		if s.splitKeys != nil {
			splitTbls = splitTable(tbl, s.splitKeys)
		}
		for {
			oldLevels := s.LoadShardLevels(cf)
			if oldLevels == nil {
				return errors.New("shard is deleted")
			}
			newLevels := oldLevels.Clone()
			l0 := newLevels.levels[0]
			l0.tables = append(l0.tables, splitTbls...)
			if s.CASLevels(cf, oldLevels, newLevels) {
				break
			}
		}
	}
	return nil
}

func splitTable(tbl table.Table, keys [][]byte) []table.Table {
	return nil
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
 | CF0 Data | CF 0 index | CF1 Data | CF1 index | cfs index | shard meta | shard meta len
*/

func (e *shardDataBuilder) Finish(meta []byte) []byte {
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
	fileSize += len(meta) + 4
	result := make([]byte, 0, fileSize)
	for _, cfData := range cfDatas {
		result = append(result, cfData...)
	}
	result = append(result, meta...)
	metaLen := make([]byte, 4)
	binary.LittleEndian.PutUint32(metaLen, uint32(len(meta)))
	result = append(result, metaLen...)
	return result
}
