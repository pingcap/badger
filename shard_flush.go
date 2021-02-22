package badger

import (
	"github.com/pingcap/badger/fileutil"
	"github.com/pingcap/badger/protos"
	"github.com/pingcap/badger/table/memtable"
	"github.com/pingcap/badger/table/sstable"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/log"
	"os"
	"sync/atomic"
	"unsafe"
)

type shardFlushTask struct {
	shard        *Shard
	tbl          *memtable.CFTable
	preSplitKeys [][]byte
	properties   *protos.ShardProperties

	finishSplitOldShard *Shard
	finishSplitShards   []*Shard
	finishSplitMemTbls  []*shardingMemTables
	finishSplitProps    []*protos.ShardProperties
}

func (sdb *ShardingDB) runFlushMemTable(c *y.Closer) {
	defer c.Done()
	for task := range sdb.flushCh {
		if len(task.finishSplitShards) > 0 {
			err := sdb.flushFinishSplit(task)
			if err != nil {
				panic(err)
			}
			continue
		}
		if task.tbl == nil {
			y.Assert(len(task.preSplitKeys) > 0)
			err := sdb.addShardL0Table(task, nil)
			if err != nil {
				panic(err)
			}
			continue
		}
		l0Table, err := sdb.flushMemTable(task.tbl)
		if err != nil {
			// TODO: handle S3 error by queue the failed operation and retry.
			panic(err)
		}
		err = sdb.addShardL0Table(task, l0Table)
		if err != nil {
			panic(err)
		}
	}
}

func (sdb *ShardingDB) flushFinishSplit(task *shardFlushTask) error {
	log.S().Info("flush finish split")
	if atomic.LoadUint32(&sdb.closed) == 1 {
		return nil
	}
	allL0s := make([]*shardL0Tables, len(task.finishSplitMemTbls))
	for idx, memTbls := range task.finishSplitMemTbls {
		l0s := &shardL0Tables{tables: make([]*shardL0Table, len(memTbls.tables))}
		for j, memTbl := range memTbls.tables {
			l0Table, err := sdb.flushMemTable(memTbl)
			if err != nil {
				// TODO: handle s3 error by queue the failed operation and retry.
				panic(err)
			}
			l0s.tables[j] = l0Table
		}
		allL0s[idx] = l0s
	}
	oldShard := task.finishSplitOldShard
	splitChangeSet := &protos.ShardChangeSet{
		ShardID:  oldShard.ID,
		ShardVer: oldShard.Ver,
		Split: &protos.ShardSplit{
			NewShards: task.finishSplitProps,
			Keys:      oldShard.splitKeys,
		},
	}
	l0ChangeSets := make([]*protos.ShardChangeSet, len(task.finishSplitMemTbls))
	for i := range allL0s {
		nShard := task.finishSplitShards[i]
		l0s := allL0s[i]
		l0ChangeSet := &protos.ShardChangeSet{
			ShardID:  nShard.ID,
			ShardVer: nShard.Ver,
		}
		for _, l0 := range l0s.tables {
			l0ChangeSet.L0Creates = append(l0ChangeSet.L0Creates, &protos.L0Create{
				ID:    l0.fid,
				Start: nShard.Start,
				End:   nShard.End,
			})
		}
		l0ChangeSets[i] = l0ChangeSet
	}
	for idx, nShard := range task.finishSplitShards {
		newL0s := allL0s[idx]
		atomicAddL0(nShard.l0s, newL0s.tables...)
		atomicRemoveMemTable(nShard.memTbls, len(newL0s.tables))
	}
	err := sdb.manifest.writeFinishSplitChangeSet(task.finishSplitShards, splitChangeSet, l0ChangeSets, true)
	if err != nil {
		return err
	}
	os.Remove(oldShard.walFilename)
	return nil
}

func (sdb *ShardingDB) flushMemTable(m *memtable.CFTable) (*shardL0Table, error) {
	y.Assert(sdb.idAlloc != nil)
	id := sdb.idAlloc.AllocID()
	log.S().Infof("flush memtable %d", id)
	fd, err := sdb.createL0File(id)
	if err != nil {
		return nil, err
	}
	writer := fileutil.NewBufferedWriter(fd, sdb.opt.TableBuilderOptions.WriteBufferSize, nil)
	builder := newShardL0Builder(sdb.numCFs, sdb.opt.TableBuilderOptions)
	for cf := 0; cf < sdb.numCFs; cf++ {
		it := m.NewIterator(cf, false)
		if it == nil {
			continue
		}
		for it.Rewind(); it.Valid(); y.NextAllVersion(it) {
			builder.Add(cf, it.Key(), it.Value())
		}
	}
	shardL0Data := builder.Finish()
	_, err = writer.Write(shardL0Data)
	if err != nil {
		return nil, err
	}
	err = writer.Finish()
	if err != nil {
		return nil, err
	}
	filename := fd.Name()
	_ = fd.Close()
	if sdb.s3c != nil {
		err = putSSTBuildResultToS3(sdb.s3c, &sstable.BuildResult{FileName: filename})
		if err != nil {
			// TODO: handle this error by queue the failed operation and retry.
			return nil, err
		}
	}
	return openShardL0Table(filename, id)
}

func (sdb *ShardingDB) addShardL0Table(task *shardFlushTask, l0 *shardL0Table) error {
	shard := task.shard
	var creates []*protos.L0Create
	if l0 != nil {
		creates = []*protos.L0Create{newL0Create(l0, task.properties, shard.Start, shard.End)}
	}
	var preSplit *protos.ShardPreSplit
	if len(task.preSplitKeys) > 0 {
		preSplit = &protos.ShardPreSplit{Keys: task.preSplitKeys}
	}
	changeSet := &protos.ShardChangeSet{
		L0Creates: creates,
		PreSplit:  preSplit,
	}
	err := sdb.manifest.writeChangeSet(shard, changeSet, true)
	if err != nil {
		return err
	}
	if l0 == nil {
		return nil
	}
	atomicAddL0(shard.l0s, l0)
	shard.addEstimatedSize(l0.size)
	atomicRemoveMemTable(shard.memTbls, 1)
	return nil
}

func atomicAddMemTable(pointer *unsafe.Pointer, memTbl *memtable.CFTable) {
	for {
		oldMemTbls := (*shardingMemTables)(atomic.LoadPointer(pointer))
		newMemTbls := &shardingMemTables{make([]*memtable.CFTable, 0, len(oldMemTbls.tables)+1)}
		newMemTbls.tables = append(newMemTbls.tables, memTbl)
		newMemTbls.tables = append(newMemTbls.tables, oldMemTbls.tables...)
		if atomic.CompareAndSwapPointer(pointer, unsafe.Pointer(oldMemTbls), unsafe.Pointer(newMemTbls)) {
			break
		}
	}
}

func atomicRemoveMemTable(pointer *unsafe.Pointer, cnt int) {
	for {
		oldMemTbls := (*shardingMemTables)(atomic.LoadPointer(pointer))
		newMemTbls := &shardingMemTables{make([]*memtable.CFTable, len(oldMemTbls.tables)-cnt)}
		copy(newMemTbls.tables, oldMemTbls.tables)
		if atomic.CompareAndSwapPointer(pointer, unsafe.Pointer(oldMemTbls), unsafe.Pointer(newMemTbls)) {
			break
		}
	}
}

func atomicAddL0(pointer *unsafe.Pointer, l0Tbls ...*shardL0Table) {
	for {
		oldL0Tbls := (*shardL0Tables)(atomic.LoadPointer(pointer))
		newL0Tbls := &shardL0Tables{make([]*shardL0Table, 0, len(oldL0Tbls.tables)+1)}
		newL0Tbls.tables = append(newL0Tbls.tables, l0Tbls...)
		newL0Tbls.tables = append(newL0Tbls.tables, oldL0Tbls.tables...)
		if atomic.CompareAndSwapPointer(pointer, unsafe.Pointer(oldL0Tbls), unsafe.Pointer(newL0Tbls)) {
			break
		}
	}
}

func atomicRemoveL0(pointer *unsafe.Pointer, cnt int) {
	log.S().Infof("atomic remove l0 %p", pointer)
	for {
		oldL0Tbls := (*shardL0Tables)(atomic.LoadPointer(pointer))
		newL0Tbls := &shardL0Tables{make([]*shardL0Table, len(oldL0Tbls.tables)-cnt)}
		copy(newL0Tbls.tables, oldL0Tbls.tables)
		if atomic.CompareAndSwapPointer(pointer, unsafe.Pointer(oldL0Tbls), unsafe.Pointer(newL0Tbls)) {
			break
		}
	}
}
