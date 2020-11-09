package badger

import (
	"github.com/pingcap/badger/fileutil"
	"github.com/pingcap/badger/protos"
	"github.com/pingcap/badger/table/memtable"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/log"
	"os"
	"sync/atomic"
	"unsafe"
)

type shardFlushTask struct {
	shard        *Shard
	tbl          *memtable.CFTable
	splittingIdx int
	splitting    bool
}

func (sdb *ShardingDB) runFlushMemTable(c *y.Closer) {
	defer c.Done()
	for task := range sdb.flushCh {
		fd, err := sdb.createL0File(task.tbl.ID())
		if err != nil {
			panic(err)
		}
		err = sdb.flushMemTable(task, fd)
		if err != nil {
			panic(err)
		}
		filename := fd.Name()
		fd.Close()
		l0Table, err := openShardL0Table(filename, task.tbl.ID())
		if err != nil {
			panic(err)
		}
		err = sdb.addShardL0Table(task, l0Table)
		if err != nil {
			panic(err)
		}
	}
}

func (sdb *ShardingDB) flushMemTable(task *shardFlushTask, fd *os.File) error {
	m := task.tbl
	log.S().Info("flush memtable")
	writer := fileutil.NewDirectWriter(fd, sdb.opt.TableBuilderOptions.WriteBufferSize, nil)
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
	_, err := writer.Write(shardL0Data)
	if err != nil {
		return err
	}
	return writer.Finish()
}

func (sdb *ShardingDB) addShardL0Table(task *shardFlushTask, l0 *shardL0Table) error {
	shard := task.shard
	change := sdb.newManifestChange(l0.fid, shard.ID, -1, 0, protos.ManifestChange_CREATE)
	err := sdb.manifest.addChanges(change)
	if err != nil {
		if err != errShardNotFound {
			return err
		}
		var shardStartKey []byte
		if task.splittingIdx == 0 {
			shardStartKey = task.shard.Start
		} else {
			shardStartKey = task.shard.splitKeys[task.splittingIdx-1]
		}
		shardID := sdb.loadShardTree().get(shardStartKey).ID
		change = sdb.newManifestChange(l0.fid, shardID, -1, 0, protos.ManifestChange_CREATE)
	}
	oldL0Ptr := shard.l0s
	oldMemTblsPtr := shard.memTbls
	if task.splitting {
		oldL0Ptr = shard.splittingL0s[task.splittingIdx]
		oldMemTblsPtr = shard.splittingMemTbls[task.splittingIdx]
	}
	oldL0Tbls := (*shardL0Tables)(atomic.LoadPointer(oldL0Ptr))
	newL0Tbls := &shardL0Tables{}
	newL0Tbls.tables = append(newL0Tbls.tables, l0)
	if oldL0Tbls != nil {
		newL0Tbls.tables = append(newL0Tbls.tables, oldL0Tbls.tables...)
	}
	y.Assert(atomic.CompareAndSwapPointer(oldL0Ptr, unsafe.Pointer(oldL0Tbls), unsafe.Pointer(newL0Tbls)))
	for {
		oldMemTbls := (*shardingMemTables)(atomic.LoadPointer(oldMemTblsPtr))
		newMemTbls := &shardingMemTables{tables: make([]*memtable.CFTable, len(oldMemTbls.tables)-1)}
		copy(newMemTbls.tables, oldMemTbls.tables)
		if atomic.CompareAndSwapPointer(oldMemTblsPtr, unsafe.Pointer(oldMemTbls), unsafe.Pointer(newMemTbls)) {
			break
		}
	}
	return nil
}
