package badger

import (
	"bytes"
	"encoding/binary"
	"os"
	"sort"
	"sync/atomic"
	"unsafe"

	"github.com/pingcap/badger/table/memtable"
	"github.com/pingcap/badger/y"
)

type ShardingMemTable struct {
	*memtable.CFTable
	metas map[uint32][]byte
}

func NewShardingMemTable(arenaSize int64, numCFs int) *ShardingMemTable {
	cfTable := memtable.NewCFTable(arenaSize, numCFs)
	return &ShardingMemTable{
		CFTable: cfTable,
		metas:   map[uint32][]byte{},
	}
}

type WriteTask struct {
	Shard *Shard
	// The entries must be in sorted order.
	Entries   [][]*memtable.Entry
	CFs       []byte
	ShardMeta []byte
	NotifyCh  chan error
}

type engineTask struct {
	writeTask *WriteTask
	splitTask *splitTask
}

type splitTask struct {
	shard  *Shard
	keys   [][]byte
	notify chan error
}

func (sdb *ShardingDB) runWriteLoop(closer *y.Closer) {
	defer closer.Done()
	for {
		writeTasks, splitTasks := sdb.collectTasks(closer)
		if len(writeTasks) == 0 && len(splitTasks) == 0 {
			return
		}
		sdb.executeWriteTasks(writeTasks)
		sdb.executeSplitTasks(splitTasks)
	}
}

func (sdb *ShardingDB) collectTasks(c *y.Closer) ([]*WriteTask, []*splitTask) {
	var writeTasks []*WriteTask
	var splitTasks []*splitTask
	select {
	case x := <-sdb.writeCh:
		if x.writeTask != nil {
			writeTasks = append(writeTasks, x.writeTask)
		} else {
			splitTasks = append(splitTasks, x.splitTask)
		}
		l := len(sdb.writeCh)
		for i := 0; i < l; i++ {
			x = <-sdb.writeCh
			if x.writeTask != nil {
				writeTasks = append(writeTasks, x.writeTask)
			} else {
				splitTasks = append(splitTasks, x.splitTask)
			}
		}
	case <-c.HasBeenClosed():
		return nil, nil
	}
	return writeTasks, splitTasks
}

func (sdb *ShardingDB) splitWriteTask(task *WriteTask) []*WriteTask {
	// TODO
	return nil
}

func (sdb *ShardingDB) checkWriteTasks(tasks []*WriteTask) []*WriteTask {
	validTasks := tasks[:0]
	for _, task := range tasks {
		if _, ok := sdb.shards.shardsByID[task.Shard.ID]; !ok {
			validTasks = append(validTasks, sdb.splitWriteTask(task)...)
		} else {
			validTasks = append(validTasks, task)
		}
	}
	return validTasks
}

func (sdb *ShardingDB) loadMemTable() *ShardingMemTable {
	return (*ShardingMemTable)(atomic.LoadPointer(&sdb.memTable))
}

func (sdb *ShardingDB) switchMemTable() {
	newMemTable := NewShardingMemTable(sdb.opt.MaxMemTableSize, sdb.opt.NumCFs)
	old := atomic.SwapPointer(&sdb.memTable, unsafe.Pointer(newMemTable))
	sdb.flushCh <- (*ShardingMemTable)(old)
}

func (sdb *ShardingDB) executeWriteTasks(tasks []*WriteTask) {
	tasks = sdb.checkWriteTasks(tasks)
	sort.Slice(tasks, func(i, j int) bool {
		return bytes.Compare(tasks[i].Shard.Start, tasks[i].Shard.Start) < 0
	})
	memTable := sdb.loadMemTable()
	entries := sdb.batchTasks(tasks)
	for cf, cfEntries := range entries {
		memTable.PutEntries(byte(cf), cfEntries)
	}
	for _, task := range tasks {
		memTable.metas[task.Shard.ID] = task.ShardMeta
	}
	if memTable.Size() > sdb.opt.MaxMemTableSize {
		sdb.switchMemTable()
	}
}

func (sdb *ShardingDB) batchTasks(tasks []*WriteTask) (entries [][]*memtable.Entry) {
	entries = make([][]*memtable.Entry, sdb.opt.NumCFs)
	for cf := 0; cf < sdb.opt.NumCFs; cf++ {
		cnt := 0
		for _, task := range tasks {
			cnt += len(task.Entries[cf])
			entries[cf] = append(entries[cf], task.Entries[cf]...)
		}
		cfEntries := make([]*memtable.Entry, 0, cnt)
		for _, task := range tasks {
			cfEntries = append(cfEntries, task.Entries[cf]...)
		}
		entries[cf] = cfEntries
	}
	return entries
}

func (sdb *ShardingDB) allocFile() (*os.File, error) {
	return nil, nil
}

func (sdb *ShardingDB) runFlushMemTable(c *y.Closer) {
	defer c.Done()
	for m := range sdb.flushCh {
		fd, err := sdb.allocFile()
		if err != nil {
			panic(err)
		}
		err = sdb.flushMemTable(m, fd)
		if err != nil {
			panic(err)
		}
	}
}

func (sdb *ShardingDB) flushMemTable(m *ShardingMemTable, fd *os.File) error {
	builders := map[uint32]*shardDataBuilder{}
	for cf := 0; cf < sdb.opt.NumCFs; cf++ {
		it := m.NewIterator(byte(cf))
		var lastShardBuilder *shardDataBuilder
		for it.SeekToFirst(); it.Valid(); it.Next() {
			var shardBuilder *shardDataBuilder
			if lastShardBuilder != nil && bytes.Compare(it.Key().UserKey, lastShardBuilder.shard.End) <= 0 {
				shardBuilder = lastShardBuilder
			} else {
				shard := sdb.getShard(it.Key().UserKey)
				shardBuilder = builders[shard.ID]
				if shardBuilder == nil {
					shardBuilder = newShardDataBuilder(shard, sdb.opt.NumCFs, sdb.opt.TableBuilderOptions)
				}
				lastShardBuilder = shardBuilder
			}
			shardBuilder.Add(byte(cf), it.Key(), it.Value())
		}
	}
	sorted := make([]*shardDataBuilder, 0, len(builders))
	for _, builder := range builders {
		sorted = append(sorted, builder)
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].shard.ID < sorted[j].shard.ID
	})
	// flushed file format
	// shardIndex is sorted by shard ID.
	//	numShards(4)|shardIndex/*id and endOffset */(8)...|shardData(var)...
	shardDatas := make([][]byte, len(sorted))
	shardIndex := make([]byte, len(sorted)*8+4)
	endOffset := uint32(0)
	for i, builder := range sorted {
		shardData := builder.Finish(m.metas[builder.shard.ID])
		_, err := fd.Write(shardData)
		if err != nil {
			return err
		}
		shardDatas[i] = shardData
		endOffset += uint32(len(shardData))
		binary.LittleEndian.PutUint32(shardIndex[i*8:], builder.shard.ID)
		binary.LittleEndian.PutUint32(shardIndex[i*8+4:], endOffset)
	}
	_, err := fd.Write(shardIndex)
	return err
}

func (sdb *ShardingDB) getShard(key []byte) *Shard {
	for i := 0; i < 2; i++ {
		shardID := sdb.shards.shardsByKey.get(key)
		shard := sdb.shards.getShardByIDSafe(shardID)
		if shard != nil {
			return shard
		}
	}
	panic("shard not found")
}

func (sdb *ShardingDB) executeSplitTasks(tasks []*splitTask) {

}
