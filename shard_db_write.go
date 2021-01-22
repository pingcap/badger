package badger

import (
	"errors"
	"github.com/pingcap/badger/table/memtable"
	"github.com/pingcap/badger/table/sstable"
	"github.com/pingcap/badger/y"
	"os"
	"sync/atomic"
	"unsafe"
)

type shardingMemTables struct {
	tables []*memtable.CFTable // tables from new to old, the first one is mutable.
}

type engineTask struct {
	writeTask    *WriteBatch
	preSplitTask *preSplitTask
}

type preSplitTask struct {
	shard *Shard
	keys  [][]byte
	// L0 files is ordered by fileID, we need to make sure split old file's ID is smaller than the newly flushed file ID.
	// So we pre-allocate the file IDs to be used for Split old L0 files.
	reservedIDs []uint64
	notify      chan error
}

func (sdb *ShardingDB) runWriteLoop(closer *y.Closer) {
	defer closer.Done()
	for {
		writeTasks, splitTask := sdb.collectTasks(closer)
		if len(writeTasks) == 0 && splitTask == nil {
			return
		}
		if len(writeTasks) > 0 {
			sdb.executeWriteTasks(writeTasks)
		}
		if splitTask != nil {
			sdb.executePreSplitTask(splitTask)
		}
	}
}

func (sdb *ShardingDB) collectTasks(c *y.Closer) ([]*WriteBatch, *preSplitTask) {
	var writeTasks []*WriteBatch
	var splitTask *preSplitTask
	select {
	case x := <-sdb.writeCh:
		if x.writeTask != nil {
			writeTasks = append(writeTasks, x.writeTask)
		} else {
			splitTask = x.preSplitTask
		}
		l := len(sdb.writeCh)
		for i := 0; i < l; i++ {
			x = <-sdb.writeCh
			if x.writeTask != nil {
				writeTasks = append(writeTasks, x.writeTask)
			} else {
				// There is only one split tasks at a time.
				splitTask = x.preSplitTask
			}
		}
	case <-c.HasBeenClosed():
		return nil, nil
	}
	return writeTasks, splitTask
}

func (sdb *ShardingDB) switchMemTable(shard *Shard, minSize int64, commitTS uint64) {
	writableMemTbl := shard.loadWritableMemTable()
	if writableMemTbl != nil && writableMemTbl.Empty() {
		return
	}
	newTableSize := sdb.opt.MaxMemTableSize
	if newTableSize < minSize {
		newTableSize = minSize
	}
	newMemTable := memtable.NewCFTable(newTableSize, sdb.numCFs)
	for {
		oldMemTbls := shard.loadMemTables()
		newMemTbls := &shardingMemTables{}
		newMemTbls.tables = append(newMemTbls.tables, newMemTable)
		newMemTbls.tables = append(newMemTbls.tables, oldMemTbls.tables...)
		if atomic.CompareAndSwapPointer(shard.memTbls, unsafe.Pointer(oldMemTbls), unsafe.Pointer(newMemTbls)) {
			break
		}
	}
	if writableMemTbl == nil {
		return
	}
	sdb.flushCh <- &shardFlushTask{
		shard:    shard,
		tbl:      writableMemTbl,
		commitTS: commitTS,
	}
}

func (sdb *ShardingDB) switchSplittingMemTable(shard *Shard, idx int, minSize int64, commitTS uint64) {
	writableMemTbl := shard.loadSplittingWritableMemTable(idx)
	newTableSize := sdb.opt.MaxMemTableSize
	if newTableSize < minSize {
		newTableSize = minSize
	}
	newMemTable := memtable.NewCFTable(newTableSize, sdb.numCFs)
	for {
		oldMemTbls := shard.loadSplittingMemTables(idx)
		newMemTbls := &shardingMemTables{}
		newMemTbls.tables = append(newMemTbls.tables, newMemTable)
		newMemTbls.tables = append(newMemTbls.tables, oldMemTbls.tables...)
		if atomic.CompareAndSwapPointer(shard.splittingMemTbls[idx], unsafe.Pointer(oldMemTbls), unsafe.Pointer(newMemTbls)) {
			break
		}
	}
	if writableMemTbl != nil && !writableMemTbl.Empty() {
		sdb.flushCh <- &shardFlushTask{
			shard:        shard,
			tbl:          writableMemTbl,
			splittingIdx: idx,
			splitting:    true,
			commitTS:     commitTS,
		}
	}
}

func (sdb *ShardingDB) executeWriteTasks(tasks []*WriteBatch) {
	commitTS := sdb.orc.allocTs()
	for _, task := range tasks {
		shard := task.shard
		latest := sdb.GetShard(shard.ID)
		if latest.Ver != shard.Ver {
			task.notify <- errShardNotMatch
			continue
		}
		if shard.isSplitting() {
			commitTS = sdb.writeSplitting(task, commitTS)
			continue
		}
		memTbl := shard.loadWritableMemTable()
		if memTbl == nil || memTbl.Size()+task.estimatedSize > sdb.opt.MaxMemTableSize {
			sdb.switchMemTable(shard, task.estimatedSize, commitTS)
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
	}
	sdb.orc.doneCommit(commitTS)
	for _, task := range tasks {
		if len(task.notify) == 0 {
			task.notify <- nil
		}
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
				sdb.switchSplittingMemTable(batch.shard, idx, int64(batch.estimatedSize), commitTS)
				sdb.orc.doneCommit(commitTS)
				commitTS = sdb.orc.allocTs()
				memTbl = batch.shard.loadSplittingWritableMemTable(idx)
			}
			memTbl.Put(cf, entry.Key, entry.Value)
		}
	}
	return commitTS
}

func (sdb *ShardingDB) createL0File(fid uint64) (fd *os.File, err error) {
	filename := sstable.NewFilename(fid, sdb.opt.Dir)
	return y.OpenSyncedFile(filename, false)
}

func (sdb *ShardingDB) executePreSplitTask(task *preSplitTask) {
	shard := task.shard
	if !shard.setSplitKeys(task.keys) {
		task.notify <- errors.New("failed to set split keys")
		return
	}
	sdb.switchMemTable(shard, sdb.opt.MaxMemTableSize, sdb.orc.readTs())
	idCnt := (len(shard.loadL0Tables().tables) + len(shard.loadMemTables().tables)) * (len(task.keys) + 1)
	task.reservedIDs = make([]uint64, 0, idCnt)
	for i := 0; i < idCnt; i++ {
		task.reservedIDs = append(task.reservedIDs, sdb.idAlloc.AllocID())
	}
	task.notify <- nil
}
