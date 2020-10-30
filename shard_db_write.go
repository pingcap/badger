package badger

import (
	"bytes"
	"os"
	"sort"
	"sync/atomic"
	"unsafe"

	"github.com/ncw/directio"
	"github.com/pingcap/badger/fileutil"
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
	writeTask *WriteBatch
	splitTask *splitTask
}

type splitTask struct {
	shards []shardSplitTask
	change *protos.ManifestChangeSet
	notify chan error
}

type shardSplitTask struct {
	shard *Shard
	keys  [][]byte
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

func (sdb *ShardingDB) switchMemTable(minSize int64) {
	writableMemTbl := sdb.loadWritableMemTable()
	newTableSize := sdb.opt.MaxMemTableSize
	if newTableSize < minSize {
		newTableSize = minSize
	}
	newMemTable := memtable.NewCFTable(newTableSize, sdb.numCFs)
	for {
		oldMemTbls := sdb.loadMemTables()
		newMemTbls := &shardingMemTables{
			tables: make([]*memtable.CFTable, 0, len(oldMemTbls.tables)+1),
		}
		newMemTbls.tables = append(newMemTbls.tables, newMemTable)
		newMemTbls.tables = append(newMemTbls.tables, oldMemTbls.tables...)
		if atomic.CompareAndSwapPointer(&sdb.memTbls, unsafe.Pointer(oldMemTbls), unsafe.Pointer(newMemTbls)) {
			break
		}
	}
	if !writableMemTbl.Empty() {
		sdb.flushCh <- writableMemTbl
	}
}

func (sdb *ShardingDB) executeWriteTasks(tasks []*WriteBatch) {
	entries, estimatedSize := sdb.buildMemEntries(tasks)
	memTable := sdb.loadWritableMemTable()
	if memTable.Size()+estimatedSize > sdb.opt.MaxMemTableSize {
		sdb.switchMemTable(estimatedSize)
		memTable = sdb.loadWritableMemTable()
	}
	cts := sdb.orc.allocTs()
	for cf, cfEntries := range entries {
		if !sdb.opt.CFs[cf].Managed {
			for _, entry := range cfEntries {
				entry.Value.Version = cts
			}
		}
		memTable.PutEntries(byte(cf), cfEntries)
	}
	sdb.orc.doneCommit(cts)
	for _, task := range tasks {
		task.notify <- nil
	}
}

func (sdb *ShardingDB) buildMemEntries(tasks []*WriteBatch) (entries [][]*memtable.Entry, estimateSize int64) {
	entries = make([][]*memtable.Entry, sdb.numCFs)
	for _, task := range tasks {
		for _, entry := range task.entries {
			memEntry := &memtable.Entry{Key: entry.key, Value: entry.val}
			entries[entry.cf] = append(entries[entry.cf], memEntry)
			estimateSize += memEntry.EstimateSize()
		}
	}
	for cf := 0; cf < sdb.numCFs; cf++ {
		cfEntries := entries[cf]
		sort.Slice(cfEntries, func(i, j int) bool {
			return bytes.Compare(cfEntries[i].Key, cfEntries[j].Key) < 0
		})
	}
	return
}

func (sdb *ShardingDB) createL0File(fid uint32) (fd, idxFD *os.File, err error) {
	filename := sstable.NewFilename(uint64(fid), sdb.opt.Dir)
	fd, err = directio.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, nil, err
	}
	idxFilename := sstable.IndexFilename(filename)
	idxFD, err = directio.OpenFile(idxFilename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, nil, err
	}
	return fd, idxFD, nil
}

func (sdb *ShardingDB) allocFid(allocFor string) uint32 {
	fid := atomic.AddUint32(&sdb.lastFID, 1)
	log.S().Debugf("alloc fid %d for %s", fid, allocFor)
	return fid
}

func (sdb *ShardingDB) allocFidForCompaction() uint64 {
	return uint64(sdb.allocFid("compaction"))
}

func (sdb *ShardingDB) runFlushMemTable(c *y.Closer) {
	defer c.Done()
	for m := range sdb.flushCh {
		fid := sdb.allocFid("flushMemTable")
		fd, idxFD, err := sdb.createL0File(fid)
		if err != nil {
			panic(err)
		}
		err = sdb.flushMemTable(m, fd, idxFD)
		if err != nil {
			panic(err)
		}
		filename := fd.Name()
		fd.Close()
		idxFD.Close()
		l0Table, err := openGlobalL0Table(filename, fid)
		if err != nil {
			panic(err)
		}
		err = sdb.addL0Table(l0Table)
		if err != nil {
			panic(err)
		}
	}
}

func (sdb *ShardingDB) addL0Table(l0Table *globalL0Table) error {
	err := sdb.manifest.addChanges(sdb.orc.commitTs(), &protos.ManifestChange{
		Id:    uint64(l0Table.fid),
		Op:    protos.ManifestChange_CREATE,
		Level: 0,
	})
	if err != nil {
		return err
	}
	oldL0Tables := sdb.loadGlobalL0Tables()
	newL0Tables := &globalL0Tables{tables: make([]*globalL0Table, 0, len(oldL0Tables.tables)+1)}
	newL0Tables.tables = append(newL0Tables.tables, l0Table)
	newL0Tables.tables = append(newL0Tables.tables, oldL0Tables.tables...)
	atomic.StorePointer(&sdb.l0Tbls, unsafe.Pointer(newL0Tables))
	for {
		oldMemTbls := sdb.loadMemTables()
		newMemTbls := &shardingMemTables{tables: make([]*memtable.CFTable, len(oldMemTbls.tables)-1)}
		copy(newMemTbls.tables, oldMemTbls.tables)
		if atomic.CompareAndSwapPointer(&sdb.memTbls, unsafe.Pointer(oldMemTbls), unsafe.Pointer(newMemTbls)) {
			break
		}
	}
	return nil
}

func (sdb *ShardingDB) flushMemTable(m *memtable.CFTable, fd, idxFD *os.File) error {
	log.S().Info("flush memtable")
	writer := fileutil.NewDirectWriter(fd, sdb.opt.TableBuilderOptions.WriteBufferSize, nil)
	builders := map[uint32]*shardDataBuilder{}
	shardByKey := sdb.loadShardTree()
	for cf := 0; cf < sdb.numCFs; cf++ {
		it := m.NewIterator(byte(cf), false)
		if it == nil {
			continue
		}
		var lastShardBuilder *shardDataBuilder
		for it.Rewind(); it.Valid(); y.NextAllVersion(it) {
			var shardBuilder *shardDataBuilder
			if lastShardBuilder != nil && bytes.Compare(it.Key().UserKey, lastShardBuilder.shard.End) < 0 {
				shardBuilder = lastShardBuilder
			} else {
				shard := shardByKey.get(it.Key().UserKey)
				shardBuilder = builders[shard.ID]
				if shardBuilder == nil {
					shardBuilder = newShardDataBuilder(shard, sdb.numCFs, sdb.opt.TableBuilderOptions)
					builders[shard.ID] = shardBuilder
				}
				lastShardBuilder = shardBuilder
			}
			shardBuilder.Add(byte(cf), it.Key(), it.Value())
		}
	}
	shardDatas := make([][]byte, len(builders))
	shardIndex := &l0ShardIndex{
		startKeys:  make([][]byte, len(builders)),
		endOffsets: make([]uint32, len(builders)),
	}
	sortedBuilders := make([]*shardDataBuilder, 0, len(builders))
	for _, builder := range builders {
		sortedBuilders = append(sortedBuilders, builder)
	}
	sort.Slice(sortedBuilders, func(i, j int) bool {
		return bytes.Compare(sortedBuilders[i].shard.Start, sortedBuilders[j].shard.Start) < 0
	})
	endOffset := uint32(0)
	for i, builder := range sortedBuilders {
		shardData := builder.Finish()
		_, err := writer.Write(shardData)
		if err != nil {
			return err
		}
		shardDatas[i] = shardData
		endOffset += uint32(len(shardData))
		shardIndex.endOffsets[i] = endOffset
		shardIndex.startKeys[i] = builder.shard.Start
		if i == len(builders)-1 {
			shardIndex.endKey = sortedBuilders[len(sortedBuilders)-1].shard.End
		}
	}
	for _, shardData := range shardDatas {
		_, err := writer.Write(shardData)
		if err != nil {
			return err
		}
	}
	err := writer.Finish()
	if err != nil {
		return err
	}
	writer.Reset(idxFD)
	_, err = writer.Write(shardIndex.encode())
	if err != nil {
		return err
	}
	return writer.Finish()
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
