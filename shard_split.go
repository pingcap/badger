package badger

import (
	"bytes"
	"fmt"
	"github.com/pingcap/badger/table/memtable"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/badger/fileutil"
	"github.com/pingcap/badger/options"
	"github.com/pingcap/badger/protos"
	"github.com/pingcap/badger/table"
	"github.com/pingcap/badger/table/sstable"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

/*
Shard split can be performed in 3 steps:
1. PreSplit
	Set the split keys of the shard, then all new entries are written to separated mem-tables.
    This operation also
*/

// PreSplit sets the split keys, then all new entries are written to separated mem-tables.
func (sdb *ShardingDB) PreSplit(shardID, ver uint64, keys [][]byte) error {
	guard := sdb.resourceMgr.Acquire()
	defer guard.Done()
	shard := sdb.GetShard(shardID)
	if shard == nil {
		return errShardNotFound
	}
	if shard.Ver != ver {
		log.Info("shard not match", zap.Uint64("current", shard.Ver), zap.Uint64("request", ver))
		return errShardNotMatch
	}
	notify := make(chan error, 1)
	task := &preSplitTask{
		shard: shard,
		keys:  keys,
	}
	sdb.writeCh <- engineTask{preSplitTask: task, notify: notify}
	return <-notify
}

// executePreSplitTask is executed in the write thread.
func (sdb *ShardingDB) executePreSplitTask(eTask engineTask) {
	task := eTask.preSplitTask
	shard := task.shard
	if !shard.setSplitKeys(task.keys) {
		eTask.notify <- errors.New("failed to set split keys")
		return
	}
	change := newShardChangeSet(shard)
	change.State = protos.SplitState_PRE_SPLIT
	change.PreSplit = &protos.ShardPreSplit{
		Keys:     task.keys,
		MemProps: shard.properties.toPB(shard.ID),
	}
	err := sdb.manifest.writeChangeSet(change, false)
	if err != nil {
		eTask.notify <- err
		return
	}
	if !shard.IsPassive() {
		sdb.switchMemTable(shard, sdb.opt.MaxMemTableSize, sdb.orc.readTs(), true)
	}
	eTask.notify <- nil
}

// SplitShardFiles splits the files that overlaps the split keys.
func (sdb *ShardingDB) SplitShardFiles(shardID, ver uint64) error {
	guard := sdb.resourceMgr.Acquire()
	defer guard.Done()
	shard := sdb.GetShard(shardID)
	if shard == nil {
		return errShardNotFound
	}
	if shard.Ver != ver {
		log.Info("shard not match", zap.Uint64("current", shard.Ver), zap.Uint64("request", ver))
		return errShardNotMatch
	}
	if !shard.isSplitting() {
		log.S().Infof("wrong splitting state %s", shard.GetSplitState())
		return errShardWrongSplittingState
	}
	d := new(deletions)
	change := newShardChangeSet(shard)
	change.SplitFiles = &protos.ShardSplitFiles{}
	change.State = protos.SplitState_SPLIT_FILE_DONE
	shard.lock.Lock()
	defer shard.lock.Unlock()
	keys := shard.splitKeys
	sdb.waitForAllMemTablesFlushed(shard)
	err := sdb.splitShardL0Tables(shard, d, change.SplitFiles)
	if err != nil {
		return err
	}
	for cf := 0; cf < sdb.numCFs; cf++ {
		for lvl := 1; lvl <= ShardMaxLevel; lvl++ {
			if err := sdb.splitTables(shard, cf, lvl, keys, d, change.SplitFiles); err != nil {
				return err
			}
		}
	}
	err = sdb.manifest.writeChangeSet(change, true)
	if err != nil {
		return err
	}
	shard.setSplitState(protos.SplitState_SPLIT_FILE_DONE)
	guard.Delete(d.resources)
	return nil
}

func (sdb *ShardingDB) waitForAllMemTablesFlushed(shard *Shard) {
	for {
		memTbls := shard.loadMemTables()
		if len(memTbls.tables) == 0 || (len(memTbls.tables) == 1 && memTbls.tables[0].Empty()) {
			break
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func (sdb *ShardingDB) splitShardL0Tables(shard *Shard, d *deletions, splitFiles *protos.ShardSplitFiles) error {
	keys := shard.splitKeys
	l0s := shard.loadL0Tables()
	var allNewL0s []*shardL0Table
	for i := 0; i < len(l0s.tables); i++ {
		l0Tbl := l0s.tables[i]
		newL0s, err := sdb.splitShardL0Table(shard, l0Tbl)
		if err != nil {
			return err
		}
		for splitIdx, newL0 := range newL0s {
			if newL0 != nil {
				start, end := getSplittingStartEnd(shard.Start, shard.End, keys, splitIdx)
				splitFiles.L0Creates = append(splitFiles.L0Creates, newL0Create(newL0, nil, start, end))
				allNewL0s = append(allNewL0s, newL0)
			}
		}
		d.Append(l0Tbl)
		splitFiles.TableDeletes = append(splitFiles.TableDeletes, l0Tbl.fid)
	}
	y.Assert(atomic.CompareAndSwapPointer(shard.l0s, unsafe.Pointer(l0s), unsafe.Pointer(&shardL0Tables{tables: allNewL0s})))
	return nil
}

func (sdb *ShardingDB) splitShardL0Table(shard *Shard, l0 *shardL0Table) ([]*shardL0Table, error) {
	iters := make([]y.Iterator, sdb.numCFs)
	for cf := 0; cf < sdb.numCFs; cf++ {
		iters[cf] = l0.newIterator(cf, false)
		if iters[cf] != nil {
			it := iters[cf]
			for it.Rewind(); it.Valid(); it.Next() {
			}
			it.Rewind()
		}
	}
	var newL0s []*shardL0Table
	for _, key := range shard.splitKeys {
		newL0, err := sdb.buildShardL0BeforeKey(iters, key, l0.commitTS)
		if err != nil {
			return nil, err
		}
		newL0s = append(newL0s, newL0)
	}
	lastL0, err := sdb.buildShardL0BeforeKey(iters, globalShardEndKey, l0.commitTS)
	if err != nil {
		return nil, err
	}
	newL0s = append(newL0s, lastL0)
	return newL0s, nil
}

func (sdb *ShardingDB) buildShardL0BeforeKey(iters []y.Iterator, key []byte, commitTS uint64) (*shardL0Table, error) {
	builder := newShardL0Builder(sdb.numCFs, sdb.opt.TableBuilderOptions, commitTS)
	var hasData bool
	for cf := 0; cf < sdb.numCFs; cf++ {
		iter := iters[cf]
		if iter == nil {
			continue
		}
		for ; iter.Valid(); y.NextAllVersion(iter) {
			if bytes.Compare(iter.Key().UserKey, key) >= 0 {
				break
			}
			builder.Add(cf, iter.Key(), iter.Value())
			hasData = true
		}
	}
	if !hasData {
		return nil, nil
	}
	shardL0Data := builder.Finish()
	fid := sdb.idAlloc.AllocID()
	fd, err := sdb.createL0File(fid)
	if err != nil {
		panic(err)
	}
	writer := fileutil.NewBufferedWriter(fd, sdb.opt.TableBuilderOptions.WriteBufferSize, nil)
	_, err = writer.Write(shardL0Data)
	if err != nil {
		return nil, err
	}
	err = writer.Finish()
	if err != nil {
		return nil, err
	}
	if sdb.s3c != nil {
		err = putSSTBuildResultToS3(sdb.s3c, &sstable.BuildResult{FileName: fd.Name(), FileData: shardL0Data})
		if err != nil {
			return nil, err
		}
	}
	tbl, err := openShardL0Table(fd.Name(), fid)
	if err != nil {
		return nil, err
	}
	return tbl, nil
}

func (sdb *ShardingDB) splitTables(shard *Shard, cf int, level int, keys [][]byte, d *deletions, splitFiles *protos.ShardSplitFiles) error {
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
				splitFiles.TableCreates = append(splitFiles.TableCreates, newTableCreate(tbl, cf, level))
				newTbls = append(newTbls, ntbl)
			}
		}
		splitFiles.TableDeletes = append(splitFiles.TableDeletes, tbl.ID())
	}
	newHandler := newLevelHandler(sdb.opt.NumLevelZeroTablesStall, level, sdb.metrics)
	newHandler.totalSize = oldHandler.totalSize
	newHandler.tables = newTbls
	y.Assert(scf.casLevelHandler(level, oldHandler, newHandler))
	for _, oldTbl := range oldTables {
		if _, ok := toDeleteIDs[oldTbl.ID()]; ok {
			d.Append(oldTbl)
		}
	}
	return nil
}

func (sdb *ShardingDB) buildTableBeforeKey(itr y.Iterator, key []byte, level int, opt options.TableBuilderOptions) (table.Table, error) {
	filename := sstable.NewFilename(sdb.idAlloc.AllocID(), sdb.opt.Dir)
	fd, err := y.OpenSyncedFile(filename, false)
	if err != nil {
		return nil, err
	}
	b := sstable.NewTableBuilder(fd, nil, level, opt)
	for itr.Valid() {
		if len(key) > 0 && bytes.Compare(itr.Key().UserKey, key) >= 0 {
			break
		}
		err = b.Add(itr.Key(), itr.Value())
		if err != nil {
			return nil, err
		}
		y.NextAllVersion(itr)
	}
	if b.Empty() {
		return nil, nil
	}
	result, err1 := b.Finish()
	if err1 != nil {
		return nil, err1
	}
	if sdb.s3c != nil {
		err = putSSTBuildResultToS3(sdb.s3c, &sstable.BuildResult{FileName: filename})
		if err != nil {
			return nil, err
		}
	}
	mmapFile, err1 := sstable.NewMMapFile(result.FileName)
	if err1 != nil {
		return nil, err1
	}
	return sstable.OpenTable(result.FileName, mmapFile)
}

// FinishSplit finishes the Split process on a Shard in PreSplitState.
// This is done after preSplit is done, so we don't need to acquire any lock, just atomic CAS will do.
func (sdb *ShardingDB) FinishSplit(oldShardID, ver uint64, newShardsProps []*protos.ShardProperties) (newShards []*Shard, err error) {
	oldShard := sdb.GetShard(oldShardID)
	if oldShard.Ver != ver {
		return nil, errShardNotMatch
	}
	if !oldShard.isSplitting() {
		return nil, errors.New("shard is not in splitting state")
	}
	if len(newShardsProps) != len(oldShard.splittingMemTbls) {
		return nil, fmt.Errorf("newShardsProps length %d is not equals to splittingMemTbls length %d", len(newShardsProps), len(oldShard.splittingMemTbls))
	}
	notify := make(chan error, 1)
	task := &finishSplitTask{
		shard:    oldShard,
		newProps: newShardsProps,
	}
	sdb.writeCh <- engineTask{finishSplitTask: task, notify: notify}
	err = <-notify
	if err != nil {
		return nil, err
	}
	return task.newShards, nil
}

// executeFinishSplitTask write the last entry and finish the WAL.
// It is executed in the write thread.
func (sdb *ShardingDB) executeFinishSplitTask(eTask engineTask) {
	task := eTask.finishSplitTask
	oldShard := task.shard
	latest := sdb.GetShard(oldShard.ID)
	if latest.Ver != oldShard.Ver {
		eTask.notify <- errShardNotMatch
		return
	}
	changeSet := newShardChangeSet(task.shard)
	changeSet.Split = &protos.ShardSplit{
		NewShards: task.newProps,
		Keys:      oldShard.splitKeys,
		MemProps:  oldShard.properties.toPB(oldShard.ID),
	}
	err := sdb.manifest.writeChangeSet(changeSet, false)
	if err != nil {
		eTask.notify <- err
		return
	}
	newShards, flushTask := sdb.buildSplitShards(oldShard, task.newProps)
	// All the mem tables are not flushed, we need to flush them all.
	if flushTask != nil {
		sdb.flushCh <- flushTask
	}
	task.newShards = newShards
	eTask.notify <- nil
	return
}

func (sdb *ShardingDB) buildSplitShards(oldShard *Shard, newShardsProps []*protos.ShardProperties) (newShards []*Shard, flushTask *shardFlushTask) {
	newShards = make([]*Shard, len(oldShard.splittingMemTbls))
	newVer := oldShard.Ver + uint64(len(newShardsProps)) - 1
	for i := range oldShard.splittingMemTbls {
		startKey, endKey := getSplittingStartEnd(oldShard.Start, oldShard.End, oldShard.splitKeys, i)
		shard := newShard(newShardsProps[i], newVer, startKey, endKey, sdb.opt, sdb.metrics)
		if oldShard.IsPassive() {
			shard.SetPassive(true)
		}
		log.S().Infof("new shard %d:%d", shard.ID, shard.Ver)
		shard.memTbls = oldShard.splittingMemTbls[i]
		shard.l0s = new(unsafe.Pointer)
		atomic.StorePointer(shard.l0s, unsafe.Pointer(new(shardL0Tables)))
		newShards[i] = shard
	}
	l0s := oldShard.loadL0Tables()
	for _, l0 := range l0s.tables {
		idx := l0.getSplitIndex(oldShard.splitKeys)
		nShard := newShards[idx]
		nL0s := nShard.loadL0Tables()
		nL0s.tables = append(nL0s.tables, l0)
	}
	for cf, scf := range oldShard.cfs {
		for l := 1; l <= ShardMaxLevel; l++ {
			level := scf.getLevelHandler(l)
			for _, t := range level.tables {
				sdb.insertTableToNewShard(t, cf, level.level, newShards, oldShard.splitKeys)
			}
		}
	}
	for _, nShard := range newShards {
		sdb.shardMap.Store(nShard.ID, nShard)
	}
	commitTS := sdb.orc.allocTs()
	sdb.orc.doneCommit(commitTS)
	if !oldShard.IsPassive() {
		flushTask = &shardFlushTask{
			finishSplitOldShard: oldShard,
			finishSplitShards:   newShards,
			finishSplitProps:    newShardsProps,
			finishSplitMemTbls:  make([]*shardingMemTables, len(newShards)),
			commitTS:            commitTS,
		}
	}
	for i, nShard := range newShards {
		memTbls := nShard.loadMemTables()
		if len(memTbls.tables) > 0 {
			// The old mem-tables already set commitTS, we only need to set commitTS for the latest one.
			memTbls.tables[0].SetVersion(commitTS)
		}
		if flushTask != nil {
			flushTask.finishSplitMemTbls[i] = memTbls
		}
		newMemTable := memtable.NewCFTable(sdb.opt.MaxMemTableSize, sdb.numCFs)
		newMemTbls := &shardingMemTables{}
		newMemTbls.tables = append(newMemTbls.tables, newMemTable)
		newMemTbls.tables = append(newMemTbls.tables, memTbls.tables...)
		atomic.StorePointer(nShard.memTbls, unsafe.Pointer(newMemTbls))
	}
	log.S().Infof("shard %d split to %s", oldShard.ID, newShardsProps)
	return
}

func (sdb *ShardingDB) insertTableToNewShard(t table.Table, cf, level int, shards []*Shard, splitKeys [][]byte) {
	idx := getSplitShardIndex(splitKeys, t.Smallest().UserKey)
	shard := shards[idx]
	y.Assert(shard.OverlapKey(t.Smallest().UserKey))
	y.Assert(shard.OverlapKey(t.Biggest().UserKey))
	sCF := shard.cfs[cf]
	handler := sCF.getLevelHandler(level)
	handler.tables = append(handler.tables, t)
}

func getSplitShardIndex(splitKeys [][]byte, key []byte) int {
	for i := 0; i < len(splitKeys); i++ {
		if bytes.Compare(key, splitKeys[i]) < 0 {
			return i
		}
	}
	return len(splitKeys)
}
