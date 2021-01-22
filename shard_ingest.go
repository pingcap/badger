package badger

import (
	"github.com/pingcap/badger/protos"
	"github.com/pingcap/badger/table/memtable"
	"github.com/pingcap/badger/table/sstable"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"os"
	"sort"
	"sync/atomic"
	"unsafe"
)

type IngestTree struct {
	MaxTS    uint64
	ShardID  uint64
	ShardVer uint64
	Delta    []*memtable.CFTable
	Meta     *protos.MetaChangeEvent
}

func (sdb *ShardingDB) Ingest(ingestTree *IngestTree) error {
	if shd := sdb.GetShard(ingestTree.ShardID); shd != nil {
		return errors.New("shard already exists")
	}
	sdb.orc.Lock()
	if sdb.orc.nextCommit < ingestTree.MaxTS {
		sdb.orc.nextCommit = ingestTree.MaxTS
	}
	sdb.orc.Unlock()
	defer sdb.orc.doneCommit(ingestTree.MaxTS)
	guard := sdb.resourceMgr.Acquire()
	defer guard.Done()

	err := sdb.loadFilesFromS3(ingestTree.Meta)
	if err != nil {
		return err
	}
	change := sdb.createIngestTreeManifestChange(ingestTree)
	l0s, levelHandlers, err := sdb.createIngestTreeLevelHandlers(ingestTree)
	if err != nil {
		return err
	}
	// Ingest is manually triggered with meta change, so we don't need to notify meta listener.
	if err = sdb.manifest.writeChangeSet(nil, change, false); err != nil {
		return err
	}
	shard := newShard(ingestTree.ShardID, ingestTree.ShardVer, ingestTree.Meta.StartKey, ingestTree.Meta.EndKey, sdb.opt, sdb.metrics)
	atomic.StorePointer(shard.memTbls, unsafe.Pointer(&shardingMemTables{tables: ingestTree.Delta}))
	atomic.StorePointer(shard.l0s, unsafe.Pointer(l0s))
	shard.foreachLevel(func(cf int, level *levelHandler) (stop bool) {
		scf := shard.cfs[cf]
		y.Assert(scf.casLevelHandler(level.level, level, levelHandlers[cf][level.level-1]))
		return false
	})
	sdb.shardMap.Store(ingestTree.ShardID, shard)
	return nil
}

func (sdb *ShardingDB) createIngestTreeManifestChange(ingestTree *IngestTree) (change *protos.ManifestChangeSet) {
	change = &protos.ManifestChangeSet{}
	for _, fm := range ingestTree.Meta.AddedL0Files {
		change.Changes = append(change.Changes, newManifestChange(fm.ID, ingestTree.ShardID, -1, 0, protos.ManifestChange_CREATE))
	}
	for _, fm := range ingestTree.Meta.AddedFiles {
		change.Changes = append(change.Changes, newManifestChange(fm.ID, ingestTree.ShardID, int(fm.CF), int(fm.Level), protos.ManifestChange_CREATE))
	}
	change.ShardChange = []*protos.ShardChange{{
		Op:       protos.ShardChange_CREATE,
		ShardID:  ingestTree.ShardID,
		ShardVer: ingestTree.ShardVer,
		StartKey: ingestTree.Meta.StartKey,
		EndKey:   ingestTree.Meta.EndKey,
	}}
	return
}

func (sdb *ShardingDB) createIngestTreeLevelHandlers(ingestTree *IngestTree) (*shardL0Tables, [][]*levelHandler, error) {
	l0s := &shardL0Tables{}
	newHandlers := make([][]*levelHandler, sdb.numCFs)
	for cf := 0; cf < sdb.numCFs; cf++ {
		for l := 1; l <= ShardMaxLevel; l++ {
			newHandler := newLevelHandler(sdb.opt.NumLevelZeroTablesStall, l, sdb.metrics)
			newHandlers[cf] = append(newHandlers[cf], newHandler)
		}
	}
	for _, fm := range ingestTree.Meta.AddedFiles {
		if fm.Level == 0 {
			l0Tbl, err := openShardL0Table(sstable.NewFilename(fm.ID, sdb.opt.Dir), fm.ID)
			if err != nil {
				return nil, nil, err
			}
			l0s.tables = append(l0s.tables, l0Tbl)
		} else {
			handler := newHandlers[fm.CF][fm.Level-1]
			filename := sstable.NewFilename(fm.ID, sdb.opt.Dir)
			file, err := sstable.NewMMapFile(filename)
			if err != nil {
				return nil, nil, err
			}
			tbl, err := sstable.OpenTable(filename, file)
			if err != nil {
				return nil, nil, err
			}
			handler.tables = append(handler.tables, tbl)
		}
	}
	sort.Slice(l0s.tables, func(i, j int) bool {
		return l0s.tables[i].fid > l0s.tables[j].fid
	})
	for cf := 0; cf < sdb.numCFs; cf++ {
		for l := 1; l <= ShardMaxLevel; l++ {
			handler := newHandlers[cf][l-1]
			sort.Slice(handler.tables, func(i, j int) bool {
				return handler.tables[i].Smallest().Compare(handler.tables[j].Smallest()) < 0
			})
		}
	}
	return l0s, newHandlers, nil
}

func (sdb *ShardingDB) IngestBuffer(shardID, shardVer uint64, buffer *memtable.CFTable) error {
	shard := sdb.GetShard(shardID)
	if shard == nil {
		return errShardNotFound
	}
	if shard.Ver != shardVer {
		return errShardNotMatch
	}
	memTbls := (*shardingMemTables)(atomic.LoadPointer(shard.memTbls))
	memTbls.tables = append([]*memtable.CFTable{buffer}, memTbls.tables...)
	return nil
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
