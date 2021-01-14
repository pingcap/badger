package badger

import (
	"bytes"
	"github.com/pingcap/badger/protos"
	"github.com/pingcap/badger/table/memtable"
	"github.com/pingcap/badger/table/sstable"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"os"
	"sync/atomic"
	"unsafe"
)

type IngestTree struct {
	MaxTS uint64
	Delta []*memtable.CFTable
	Meta  *protos.MetaChangeEvent
}

func (sdb *ShardingDB) Ingest(ingestTree *IngestTree) error {
	start, end := ingestTree.Meta.StartKey, ingestTree.Meta.EndKey
	if len(end) == 0 {
		end = globalShardEndKey
	}
	sdb.orc.Lock()
	if sdb.orc.nextCommit < ingestTree.MaxTS {
		sdb.orc.nextCommit = ingestTree.MaxTS
	}
	sdb.orc.Unlock()
	defer sdb.orc.doneCommit(ingestTree.MaxTS)
	if err := sdb.Split([][]byte{start, end}); err != nil {
		return err
	}
	// TODO: support merge multiple shards. now assert single shard.
	guard := sdb.resourceMgr.Acquire()
	defer guard.Done()
	tree := sdb.loadShardTree()
	shards := tree.getShards(start, end)
	if len(shards) != 1 {
		log.Error("shard length is not 1", zap.Int("len", len(shards)))
		log.S().Errorf("%v %v", start, end)
		for _, shd := range shards {
			log.S().Errorf("shard:%d %v %v", shd.ID, shd.Start, shd.End)
		}
		panic("not handled yet")
	}
	shard := shards[0]
	shard.lock.Lock()
	defer shard.lock.Unlock()
	y.Assert(bytes.Equal(shard.Start, start))
	y.Assert(bytes.Equal(shard.End, end))
	y.Assert(!shard.isSplitting())

	err := sdb.loadFilesFromS3(ingestTree.Meta)
	if err != nil {
		return err
	}
	change, d := sdb.createIngestTreeManifestChange(ingestTree, shard)
	l0s, levelHandlers, err := sdb.createIngestTreeLevelHandlers(ingestTree)
	if err != nil {
		return err
	}
	// Ingest is manually triggered with meta change, so we don't need to notify meta listener.
	if err = sdb.manifest.writeChangeSet(nil, change, false); err != nil {
		return err
	}
	atomic.StorePointer(shard.memTbls, unsafe.Pointer(&shardingMemTables{tables: ingestTree.Delta}))
	atomic.StorePointer(shard.l0s, unsafe.Pointer(l0s))
	shard.foreachLevel(func(cf int, level *levelHandler) (stop bool) {
		scf := shard.cfs[cf]
		y.Assert(scf.casLevelHandler(level.level, level, levelHandlers[cf][level.level-1]))
		return false
	})
	guard.Delete(d.resources)
	return nil
}

func (sdb *ShardingDB) IngestBuffer(startKey []byte, buffer *memtable.CFTable) {
	tree := sdb.loadShardTree()
	shard := tree.get(startKey)
	memTbls := (*shardingMemTables)(atomic.LoadPointer(shard.memTbls))
	memTbls.tables = append([]*memtable.CFTable{buffer}, memTbls.tables...)
}

func (sdb *ShardingDB) loadFilesFromS3(meta *protos.MetaChangeEvent) error {
	for _, l0File := range meta.AddedL0Files {
		localFileName := sstable.NewFilename(l0File.ID, sdb.opt.Dir)
		_, err := os.Stat(localFileName)
		if err == nil {
			// File already exists, no need to load.
			continue
		}
		key := sdb.s3c.BlockKey(l0File.ID)
		tmpFileName := localFileName + ".tmp"
		err = sdb.s3c.GetToFile(key, tmpFileName)
		if err != nil {
			return err
		}
		err = os.Rename(tmpFileName, localFileName)
		if err != nil {
			return err
		}
	}
	for _, fMeta := range meta.AddedFiles {
		localFileName := sstable.NewFilename(fMeta.ID, sdb.opt.Dir)
		_, err := os.Stat(localFileName)
		if err == nil {
			continue
		}
		blockKey := sdb.s3c.BlockKey(fMeta.ID)
		tmpBlockFileName := localFileName + ".tmp"
		err = sdb.s3c.GetToFile(blockKey, tmpBlockFileName)
		if err != nil {
			return err
		}
		idxKey := sdb.s3c.IndexKey(fMeta.ID)
		idxFileName := sstable.IndexFilename(localFileName)
		err = sdb.s3c.GetToFile(idxKey, idxFileName)
		if err != nil {
			return err
		}
		err = os.Rename(tmpBlockFileName, localFileName)
		if err != nil {
			return err
		}
	}
	return nil
}
