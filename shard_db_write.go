package badger

import (
	"errors"
	"os"
	"sync/atomic"
	"unsafe"

	"github.com/pingcap/badger/protos"
	"github.com/pingcap/badger/table/memtable"
	"github.com/pingcap/badger/table/sstable"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/log"
)

type shardingMemTables struct {
	tables []*memtable.CFTable // tables from new to old, the first one is mutable.
}

type engineTask struct {
	writeTask       *WriteBatch
	preSplitTask    *preSplitTask
	finishSplitTask *finishSplitTask
	getProperties   *getPropertyTask
	notify          chan error
}

type preSplitTask struct {
	shard *Shard
	keys  [][]byte
}

type finishSplitTask struct {
	shard     *Shard
	newProps  []*protos.ShardProperties
	newShards []*Shard
}

type getPropertyTask struct {
	shard  *Shard
	keys   []string
	snap   *Snapshot
	values [][]byte
}

func (sdb *ShardingDB) runWriteLoop(closer *y.Closer) {
	defer closer.Done()
	for {
		tasks := sdb.collectTasks(closer)
		if len(tasks) == 0 {
			return
		}
		for _, task := range tasks {
			if task.writeTask != nil {
				sdb.executeWriteTask(task)
			}
			if task.preSplitTask != nil {
				sdb.executePreSplitTask(task)
			}
			if task.finishSplitTask != nil {
				sdb.executeFinishSplitTask(task)
			}
			if task.getProperties != nil {
				sdb.executeGetPropertiesTask(task)
			}
		}
	}
}

func (sdb *ShardingDB) collectTasks(c *y.Closer) []engineTask {
	var engineTasks []engineTask
	select {
	case x := <-sdb.writeCh:
		engineTasks = append(engineTasks, x)
		l := len(sdb.writeCh)
		for i := 0; i < l; i++ {
			engineTasks = append(engineTasks, <-sdb.writeCh)
		}
	case <-c.HasBeenClosed():
		return nil
	}
	return engineTasks
}

func (sdb *ShardingDB) switchMemTable(shard *Shard, minSize int64, commitTS uint64, preSplitFlush bool) {
	writableMemTbl := shard.loadWritableMemTable()
	if writableMemTbl != nil && writableMemTbl.Empty() {
		return
	}
	newTableSize := sdb.opt.MaxMemTableSize
	if newTableSize < minSize {
		newTableSize = minSize
	}
	newMemTable := memtable.NewCFTable(newTableSize, sdb.numCFs)
	atomicAddMemTable(shard.memTbls, newMemTable)
	if writableMemTbl == nil && !preSplitFlush {
		return
	}
	if shard.IsPassive() {
		return
	}
	shard.properties.set(commitTSKey, sstable.U64ToBytes(commitTS))
	writableMemTbl.SetVersion(commitTS)
	sdb.flushCh <- &shardFlushTask{
		shard:         shard,
		tbl:           writableMemTbl,
		preSplitFlush: preSplitFlush,
		properties:    shard.properties.toPB(shard.ID),
	}
}

func (sdb *ShardingDB) switchSplittingMemTable(shard *Shard, idx int, minSize int64) {
	newTableSize := sdb.opt.MaxMemTableSize
	if newTableSize < minSize {
		newTableSize = minSize
	}
	newMemTable := memtable.NewCFTable(newTableSize, sdb.numCFs)
	atomicAddMemTable(shard.splittingMemTbls[idx], newMemTable)
	// Splitting MemTable is never flushed, we will flush the mem tables after finish split.
}

func (sdb *ShardingDB) executeWriteTask(eTask engineTask) {
	task := eTask.writeTask
	commitTS := sdb.orc.allocTs()
	defer func() {
		sdb.orc.doneCommit(commitTS)
		if len(eTask.notify) == 0 {
			eTask.notify <- nil
		}
	}()
	shard := task.shard
	latest := sdb.GetShard(shard.ID)
	if latest.Ver != shard.Ver {
		eTask.notify <- errShardNotMatch
		return
	}
	if shard.isSplitting() {
		commitTS = sdb.writeSplitting(task, commitTS)
		return
	}
	memTbl := shard.loadWritableMemTable()
	if memTbl == nil || memTbl.Size()+task.estimatedSize > sdb.opt.MaxMemTableSize {
		sdb.switchMemTable(shard, task.estimatedSize, commitTS, false)
		memTbl = shard.loadWritableMemTable()
		// Update the commitTS so that the new memTable has a new commitTS, then
		// the old commitTS can be used as a snapshot at the memTable-switching time.
		sdb.orc.doneCommit(commitTS)
		commitTS = sdb.orc.allocTs()
	}
	for cf, entries := range task.entries {
		if !sdb.opt.CFs[cf].Managed {
			for _, entry := range entries {
				entry.Value.Version = commitTS
			}
		}
		memTbl.PutEntries(cf, entries)
	}
	for key, val := range task.properties {
		shard.properties.set(key, val)
	}
}

func (sdb *ShardingDB) writeSplitting(batch *WriteBatch, commitTS uint64) uint64 {
	for cf, entries := range batch.entries {
		if !sdb.opt.CFs[cf].Managed {
			for _, entry := range entries {
				entry.Value.Version = commitTS
			}
		}
		for _, entry := range entries {
			idx := getSplitShardIndex(batch.shard.splitKeys, entry.Key)
			memTbl := batch.shard.loadSplittingWritableMemTable(idx)
			if memTbl == nil || memTbl.Size()+int64(batch.estimatedSize) > sdb.opt.MaxMemTableSize {
				sdb.switchSplittingMemTable(batch.shard, idx, int64(batch.estimatedSize))
				sdb.orc.doneCommit(commitTS)
				commitTS = sdb.orc.allocTs()
				memTbl = batch.shard.loadSplittingWritableMemTable(idx)
			}
			memTbl.Put(cf, entry.Key, entry.Value)
			batch.shard.splittingCnt++
		}
	}
	for key, val := range batch.properties {
		batch.shard.properties.set(key, val)
	}
	return commitTS
}

func (sdb *ShardingDB) createL0File(fid uint64) (fd *os.File, err error) {
	filename := sstable.NewFilename(fid, sdb.opt.Dir)
	return y.OpenSyncedFile(filename, false)
}

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
		Keys:                 task.keys,
		MemProps:             shard.properties.toPB(shard.ID),
	}
	err := sdb.manifest.writeChangeSet(change, false)
	if err != nil {
		eTask.notify <- err
		return
	}
	log.Info("pre-split switch memtable")
	sdb.switchMemTable(shard, sdb.opt.MaxMemTableSize, sdb.orc.readTs(), true)
	eTask.notify <- nil
}

// executeFinishSplitTask write the last entry and finish the WAL.
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
	sdb.flushCh <- flushTask
	task.newShards = newShards
	eTask.notify <- nil
	return
}

func (sdb *ShardingDB) executeGetPropertiesTask(eTask engineTask) {
	task := eTask.getProperties
	for _, key := range task.keys {
		val, _ := task.shard.properties.get(key)
		task.values = append(task.values, val)
	}
	task.snap = sdb.NewSnapshot(task.shard)
	eTask.notify <- nil
}

func (sdb *ShardingDB) buildSplitShards(oldShard *Shard, newShardsProps []*protos.ShardProperties) (newShards []*Shard, flushTask *shardFlushTask) {
	newShards = make([]*Shard, len(oldShard.splittingMemTbls))
	for i := range oldShard.splittingMemTbls {
		startKey, endKey := getSplittingStartEnd(oldShard.Start, oldShard.End, oldShard.splitKeys, i)
		ver := uint64(1)
		if newShardsProps[i].ShardID == oldShard.ID {
			ver = oldShard.Ver + uint64(len(newShards)) - 1
		}
		shard := newShard(newShardsProps[i], ver, startKey, endKey, sdb.opt, sdb.metrics)
		shard.memTbls = oldShard.splittingMemTbls[i]
		shard.l0s = new(unsafe.Pointer)
		atomic.StorePointer(shard.l0s, unsafe.Pointer(oldShard.splittingL0Tbls[i]))
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
	flushTask = &shardFlushTask{
		finishSplitOldShard: oldShard,
		finishSplitShards:   newShards,
		finishSplitProps:    newShardsProps,
		finishSplitMemTbls:  make([]*shardingMemTables, len(newShards)),
	}
	commitTS := sdb.orc.allocTs()
	sdb.orc.doneCommit(commitTS)
	for i, nShard := range newShards {
		memTbls := nShard.loadMemTables()
		if len(memTbls.tables) > 0 {
			// The old mem-tables already set commitTS, we only need to set commitTS for the latest one.
			memTbls.tables[0].SetVersion(commitTS)
		}
		flushTask.finishSplitMemTbls[i] = memTbls
		newMemTable := memtable.NewCFTable(sdb.opt.MaxMemTableSize, sdb.numCFs)
		newMemTbls := &shardingMemTables{}
		newMemTbls.tables = append(newMemTbls.tables, newMemTable)
		newMemTbls.tables = append(newMemTbls.tables, memTbls.tables...)
		atomic.StorePointer(nShard.memTbls, unsafe.Pointer(newMemTbls))
	}
	log.S().Infof("shard %d split to %s", oldShard.ID, newShardsProps)
	return
}
