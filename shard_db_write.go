package badger

import (
	"errors"
	"github.com/pingcap/badger/table/memtable"
	"github.com/pingcap/badger/table/sstable"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"os"
	"sync/atomic"
	"unsafe"
)

type shardingMemTables struct {
	tables []*memtable.CFTable // tables from new to old, the first one is mutable.
}

type engineTask struct {
	writeTask *WriteBatch
	splitTask *splitTask
}

type splitTask struct {
	shards map[uint64]*shardSplitTask
	notify chan error
}

type shardSplitTask struct {
	shard *Shard
	keys  [][]byte
	// reservedIDs is used to get smaller file IDs to use for split old l0 files.
	reservedIDs []uint64
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
			sdb.executeSplitTask(splitTask)
		}
	}
}

func (sdb *ShardingDB) collectTasks(c *y.Closer) ([]*WriteBatch, *splitTask) {
	var writeTasks []*WriteBatch
	var splitTask *splitTask
	select {
	case x := <-sdb.writeCh:
		if x.writeTask != nil {
			writeTasks = append(writeTasks, x.writeTask)
		} else {
			splitTask = x.splitTask
		}
		l := len(sdb.writeCh)
		for i := 0; i < l; i++ {
			x = <-sdb.writeCh
			if x.writeTask != nil {
				writeTasks = append(writeTasks, x.writeTask)
			} else {
				// There is only one split tasks at a time.
				splitTask = x.splitTask
			}
		}
	case <-c.HasBeenClosed():
		return nil, nil
	}
	return writeTasks, splitTask
}

func (sdb *ShardingDB) switchMemTable(shard *Shard, minSize int64) {
	writableMemTbl := shard.loadWritableMemTable()
	newTableSize := sdb.opt.MaxMemTableSize
	if newTableSize < minSize {
		newTableSize = minSize
	}
	newMemTable := memtable.NewCFTable(newTableSize, sdb.numCFs, sdb.idAlloc.AllocID())
	for {
		oldMemTbls := shard.loadMemTables()
		newMemTbls := &shardingMemTables{}
		newMemTbls.tables = append(newMemTbls.tables, newMemTable)
		newMemTbls.tables = append(newMemTbls.tables, oldMemTbls.tables...)
		if atomic.CompareAndSwapPointer(shard.memTbls, unsafe.Pointer(oldMemTbls), unsafe.Pointer(newMemTbls)) {
			break
		}
	}
	if writableMemTbl != nil && !writableMemTbl.Empty() {
		sdb.flushCh <- &shardFlushTask{
			shard: shard,
			tbl:   writableMemTbl,
		}
	}
}

func (sdb *ShardingDB) switchSplittingMemTable(shard *Shard, idx int, minSize int64) {
	writableMemTbl := shard.loadSplittingWritableMemTable(idx)
	newTableSize := sdb.opt.MaxMemTableSize
	if newTableSize < minSize {
		newTableSize = minSize
	}
	newMemTable := memtable.NewCFTable(newTableSize, sdb.numCFs, sdb.idAlloc.AllocID())
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
		oldMemTbls := shard.loadSplittingMemTables(idx)
		log.Warn("send flush splitting task", zap.Int("mem cnt", len(oldMemTbls.tables)), zap.Int("shard", int(shard.ID)), zap.Int("split index", idx))
		sdb.flushCh <- &shardFlushTask{
			shard:        shard,
			tbl:          writableMemTbl,
			splittingIdx: idx,
			splitting:    true,
		}
	}
}

func (sdb *ShardingDB) executeWriteTasks(tasks []*WriteBatch) {
	commitTS := sdb.orc.allocTs()
	for _, task := range tasks {
		for _, batch := range task.shardBatches {
			if batch.isSplitting() {
				sdb.writeSplitting(batch, commitTS)
				continue
			}
			memTbl := batch.loadWritableMemTable()
			if memTbl == nil || memTbl.Size()+int64(batch.estimatedSize) > sdb.opt.MaxMemTableSize {
				sdb.switchMemTable(batch.Shard, int64(batch.estimatedSize))
				memTbl = batch.loadWritableMemTable()
			}
			for cf, entries := range batch.entries {
				if !sdb.opt.CFs[cf].Managed {
					for _, entry := range entries {
						entry.Value.Version = commitTS
					}
				}
				memTbl.PutEntries(cf, entries)
			}
		}
	}
	sdb.orc.doneCommit(commitTS)
	for _, task := range tasks {
		task.notify <- nil
	}
}

func (sdb *ShardingDB) writeSplitting(batch *shardBatch, commitTS uint64) {
	for cf, entries := range batch.entries {
		if !sdb.opt.CFs[cf].Managed {
			for _, entry := range entries {
				entry.Value.Version = commitTS
			}
		}
		for _, entry := range entries {
			idx := batch.getSplittingIndex(entry.Key)
			memTbl := batch.loadSplittingWritableMemTable(idx)
			if memTbl == nil || memTbl.Size()+int64(batch.estimatedSize) > sdb.opt.MaxMemTableSize {
				sdb.switchSplittingMemTable(batch.Shard, idx, int64(batch.estimatedSize))
				memTbl = batch.loadSplittingWritableMemTable(idx)
			}
			memTbl.Put(cf, entry.Key, entry.Value)
		}
	}
}

func (sdb *ShardingDB) createL0File(fid uint64) (fd *os.File, err error) {
	filename := sstable.NewFilename(fid, sdb.opt.Dir)
	return y.OpenSyncedFile(filename, false)
}

func (sdb *ShardingDB) executeSplitTask(task *splitTask) {
	for _, shardTask := range task.shards {
		shard := shardTask.shard
		if !shard.setSplitKeys(shardTask.keys) {
			task.notify <- errors.New("failed to set split keys")
			return
		}
		sdb.switchMemTable(shard, sdb.opt.MaxMemTableSize)
		idCnt := (len(shard.loadL0Tables().tables) + len(shard.loadMemTables().tables)) * (len(shardTask.keys) + 1)
		shardTask.reservedIDs = make([]uint64, 0, idCnt)
		for i := 0; i < idCnt; i++ {
			shardTask.reservedIDs = append(shardTask.reservedIDs, sdb.idAlloc.AllocID())
		}
	}
	task.notify <- nil
}
