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
	shard *Shard
	tbl   *memtable.CFTable
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
		err = sdb.addShardL0Table(task.shard, l0Table)
		if err != nil {
			panic(err)
		}
	}
}

func (sdb *ShardingDB) flushMemTable(task *shardFlushTask, fd *os.File) error {
	m := task.tbl
	log.S().Info("flush memtable")
	writer := fileutil.NewDirectWriter(fd, sdb.opt.TableBuilderOptions.WriteBufferSize, nil)
	builder := newShardDataBuilder(task.shard, sdb.numCFs, sdb.opt.TableBuilderOptions)
	for cf := 0; cf < sdb.numCFs; cf++ {
		it := m.NewIterator(cf, false)
		if it == nil {
			continue
		}
		for it.Rewind(); it.Valid(); y.NextAllVersion(it) {
			builder.Add(byte(cf), it.Key(), it.Value())
		}
	}
	shardData := builder.Finish()
	_, err := writer.Write(shardData)
	if err != nil {
		return err
	}
	return writer.Finish()
}

func (sdb *ShardingDB) addShardL0Table(shard *Shard, l0 *shardL0Table) error {
	change := sdb.newManifestChange(l0.fid, shard.ID, -1, 0, protos.ManifestChange_CREATE)
	err := sdb.manifest.addChanges(sdb.orc.commitTs(), change)
	if err != nil {
		return err
	}
	oldL0Tbls := shard.loadL0Tables()
	newL0Tbls := &shardL0Tables{tables: make([]*shardL0Table, 0, len(oldL0Tbls.tables)+1)}
	newL0Tbls.tables = append(newL0Tbls.tables, l0)
	newL0Tbls.tables = append(newL0Tbls.tables, oldL0Tbls.tables...)
	y.Assert(atomic.CompareAndSwapPointer(&shard.l0s, unsafe.Pointer(oldL0Tbls), unsafe.Pointer(newL0Tbls)))
	for {
		oldMemTbls := shard.loadMemTables()
		newMemTbls := &shardingMemTables{tables: make([]*memtable.CFTable, len(oldMemTbls.tables)-1)}
		copy(newMemTbls.tables, oldMemTbls.tables)
		if atomic.CompareAndSwapPointer(&shard.memTbls, unsafe.Pointer(oldMemTbls), unsafe.Pointer(newMemTbls)) {
			break
		}
	}
	return nil
}
