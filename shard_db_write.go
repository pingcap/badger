package badger

import (
	"bytes"
	"github.com/ncw/directio"
	"github.com/pingcap/badger/epoch"
	"github.com/pingcap/badger/protos"
	"github.com/pingcap/badger/table/memtable"
	"github.com/pingcap/badger/table/sstable"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/log"
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
	truncates *truncateTask
}

type splitTask struct {
	shards map[uint32]*shardSplitTask
	change *protos.ManifestChangeSet
	notify chan error
}

type shardSplitTask struct {
	shard *Shard
	keys  [][]byte
}

type truncateTask struct {
	guard  *epoch.Guard
	shards []*Shard
	notify chan error
}

func (sdb *ShardingDB) runWriteLoop(closer *y.Closer) {
	defer closer.Done()
	for {
		writeTasks, splitTask, truncate := sdb.collectTasks(closer)
		if len(writeTasks) == 0 && splitTask == nil && truncate == nil {
			return
		}
		if len(writeTasks) > 0 {
			sdb.executeWriteTasks(writeTasks)
		}
		if splitTask != nil {
			sdb.executeSplitTask(splitTask)
		}
		if truncate != nil {
			// TODO
		}
	}
}

func (sdb *ShardingDB) collectTasks(c *y.Closer) ([]*WriteBatch, *splitTask, *truncateTask) {
	var writeTasks []*WriteBatch
	var splitTask *splitTask
	var truncateTask *truncateTask
	select {
	case x := <-sdb.writeCh:
		if x.writeTask != nil {
			writeTasks = append(writeTasks, x.writeTask)
		} else if x.splitTask != nil {
			splitTask = x.splitTask
		} else {
			truncateTask = x.truncates
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
		return nil, nil, nil
	}
	return writeTasks, splitTask, truncateTask
}

func (sdb *ShardingDB) switchMemTable(shard *Shard, minSize int64) {
	writableMemTbl := shard.loadWritableMemTable()
	newTableSize := sdb.opt.MaxMemTableSize
	if newTableSize < minSize {
		newTableSize = minSize
	}
	newMemTable := memtable.NewCFTable(newTableSize, sdb.numCFs, sdb.allocFid("l0"))
	for {
		oldMemTbls := shard.loadMemTables()
		newMemTbls := &shardingMemTables{
			tables: make([]*memtable.CFTable, 0, len(oldMemTbls.tables)+1),
		}
		newMemTbls.tables = append(newMemTbls.tables, newMemTable)
		newMemTbls.tables = append(newMemTbls.tables, oldMemTbls.tables...)
		if atomic.CompareAndSwapPointer(&shard.memTbls, unsafe.Pointer(oldMemTbls), unsafe.Pointer(newMemTbls)) {
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

func (sdb *ShardingDB) executeWriteTasks(tasks []*WriteBatch) {
	cts := sdb.orc.allocTs()
	for _, task := range tasks {
		for _, batch := range task.shardBatches {
			memTbl := batch.loadWritableMemTable()
			if memTbl == nil || memTbl.Size()+int64(batch.estimatedSize) > sdb.opt.MaxMemTableSize {
				sdb.switchMemTable(batch.Shard, int64(batch.estimatedSize))
				memTbl = batch.loadWritableMemTable()
			}
			for cf, entries := range batch.entries {
				if !sdb.opt.CFs[cf].Managed {
					for _, entry := range entries {
						entry.Value.Version = cts
					}
				}
				memTbl.PutEntries(cf, entries)
			}
		}
	}
	sdb.orc.doneCommit(cts)
	for _, task := range tasks {
		task.notify <- nil
	}
}

func (sdb *ShardingDB) createL0File(fid uint32) (fd *os.File, err error) {
	filename := sstable.NewFilename(uint64(fid), sdb.opt.Dir)
	return directio.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
}

func (sdb *ShardingDB) allocFid(allocFor string) uint32 {
	fid := atomic.AddUint32(&sdb.lastFID, 1)
	log.S().Debugf("alloc fid %d for %s", fid, allocFor)
	return fid
}

func (sdb *ShardingDB) allocFidForCompaction() uint64 {
	return uint64(sdb.allocFid("compaction"))
}

func (sdb *ShardingDB) executeSplitTask(task *splitTask) {
	shardByKey := sdb.loadShardTree()
	var changes []*protos.ShardChange
	for _, shard := range task.shards {
		newShards := sdb.split(shard.shard, shard.keys)
		for _, newShard := range newShards {
			changes = append(changes, &protos.ShardChange{
				ShardID:  newShard.ID,
				Op:       protos.ShardChange_CREATE,
				StartKey: newShard.Start,
				EndKey:   newShard.End,
				TableIDs: newShard.tableIDs(),
			})
		}
		changes = append(changes, &protos.ShardChange{
			ShardID:  shard.shard.ID,
			Op:       protos.ShardChange_DELETE,
			StartKey: shard.shard.Start,
			EndKey:   shard.shard.End,
		})
		shardByKey = shardByKey.replace([]*Shard{shard.shard}, newShards)
	}
	task.change.ShardChange = changes
	task.change.Head = &protos.HeadInfo{Version: sdb.orc.commitTs()}
	err := sdb.manifest.writeChangeSet(task.change)
	if err == nil {
		atomic.StorePointer(&sdb.shardTree, unsafe.Pointer(shardByKey))
	}
	task.notify <- err
}

func (sdb *ShardingDB) removeShardTables(s *Shard, guard *epoch.Guard) {
	// Lock the shard so compaction will not add new tables after truncate.
	s.lock.Lock()
	defer s.lock.Unlock()
	var toBeDelete []epoch.Resource
	for _, scf := range s.cfs {
		for i := 1; i <= shardMaxLevel; i++ {
			l := scf.getLevelHandler(i)
			if len(l.tables) == 0 {
				continue
			}
			scf.casLevelHandler(i, l, newLevelHandler(sdb.opt.NumLevelZeroTablesStall, l.level, sdb.metrics))
			for _, t := range l.tables {
				toBeDelete = append(toBeDelete, t)
			}
		}
	}
	guard.Delete(toBeDelete)
}

func (sdb *ShardingDB) removeRangeInMemTable(memTbl *memtable.CFTable, start, end []byte) {
	var keys [][]byte
	for cf := 0; cf < sdb.numCFs; cf++ {
		keys = keys[:0]
		itr := memTbl.NewIterator(cf, false)
		if itr == nil {
			continue
		}
		for itr.Seek(start); itr.Valid(); itr.Next() {
			if bytes.Compare(itr.Key().UserKey, end) >= 0 {
				break
			}
			keys = append(keys, y.SafeCopy(nil, itr.Key().UserKey))
		}
		for _, key := range keys {
			memTbl.DeleteKey(byte(cf), key)
		}
	}
}
