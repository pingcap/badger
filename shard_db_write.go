package badger

import (
	"github.com/pingcap/badger/protos"
	"github.com/pingcap/badger/table/memtable"
	"github.com/pingcap/badger/table/sstable"
	"github.com/pingcap/badger/y"
	"os"
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
	if writableMemTbl != nil {
		writableMemTbl.SetVersion(commitTS)
	}
	sdb.flushCh <- &shardFlushTask{
		shard:         shard,
		tbl:           writableMemTbl,
		preSplitFlush: preSplitFlush,
		properties:    shard.properties.toPB(shard.ID),
		commitTS:      commitTS,
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

func (sdb *ShardingDB) executeGetPropertiesTask(eTask engineTask) {
	task := eTask.getProperties
	for _, key := range task.keys {
		val, _ := task.shard.properties.get(key)
		task.values = append(task.values, val)
	}
	task.snap = sdb.NewSnapshot(task.shard)
	eTask.notify <- nil
}
