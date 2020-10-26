package badger

import (
	"bytes"
	"fmt"
	"os"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/ncw/directio"
	"github.com/pingcap/badger/protos"
	"github.com/pingcap/badger/table"
	"github.com/pingcap/badger/table/sstable"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

func (sdb *ShardingDB) runL0CompactionLoop(c *y.Closer) {
	defer c.Done()
	for {
		l0Tbls := sdb.loadGlobalL0Tables()
		if len(l0Tbls.tables) < sdb.opt.NumLevelZeroTables {
			select {
			case <-time.After(time.Second):
				continue
			case <-c.HasBeenClosed():
				return
			}
		}
		log.S().Info("start l0 compaction")
		err := sdb.doL0Compaction()
		if err != nil {
			log.Error("failed to compact L0", zap.Error(err))
		} else {
			log.S().Info("finish l0 compaction")
		}
	}
}

func (sdb *ShardingDB) doL0Compaction() error {
	// prevent split run concurrently with compact L0.
	sdb.splitLock.Lock()
	defer sdb.splitLock.Unlock()
	l0Tbls := sdb.loadGlobalL0Tables()
	allResults := make([][]*sstable.BuildResult, sdb.numCFs)
	for i := 0; i < sdb.numCFs; i++ {
		cfResults, err := sdb.buildShardTablesForCF(l0Tbls, byte(i))
		if err != nil {
			return err
		}
		allResults[i] = cfResults
	}
	err := sdb.addToShards(l0Tbls, allResults)
	if err != nil {
		return err
	}
	for {
		latestL0Tbls := sdb.loadGlobalL0Tables()
		newL0Tbls := &globalL0Tables{
			tables: make([]*globalL0Table, len(latestL0Tbls.tables)-len(l0Tbls.tables)),
		}
		copy(newL0Tbls.tables, latestL0Tbls.tables)
		if atomic.CompareAndSwapPointer(&sdb.l0Tbls, unsafe.Pointer(latestL0Tbls), unsafe.Pointer(newL0Tbls)) {
			break
		}
	}
	return nil
}

func (sdb *ShardingDB) addToShards(l0Tbls *globalL0Tables, allResults [][]*sstable.BuildResult) error {
	tree := sdb.loadShardTree()
	shardMap := make(map[uint32]*shardL0Result)
	for cf, cfResults := range allResults {
		for _, result := range cfResults {
			if result == nil {
				continue
			}
			shard := tree.get(result.Smallest.UserKey)
			shardL1Res, ok := shardMap[shard.ID]
			if !ok {
				shardL1Res = &shardL0Result{
					cfResults: make([]*sstable.BuildResult, sdb.numCFs),
					shard:     shard,
				}
				shardMap[shard.ID] = shardL1Res
			}
			shardL1Res.cfResults[cf] = result
		}
	}
	var changes []*protos.ManifestChange
	for _, sl0Result := range shardMap {
		for cf, result := range sl0Result.cfResults {
			if result == nil {
				continue
			}
			fid, ok := sstable.ParseFileID(result.FileName)
			if !ok {
				return fmt.Errorf("failed to parse file id %s cf %d", result.FileName, cf)
			}
			changes = append(changes, sdb.newManifestChange(uint32(fid), sl0Result.shard.ID, byte(cf), 0, protos.ManifestChange_CREATE))
		}
	}
	for _, l0Tbl := range l0Tbls.tables {
		changes = append(changes, sdb.newManifestChange(l0Tbl.fid, 0, 0, 0, protos.ManifestChange_DELETE))
	}
	err := sdb.manifest.addChanges(&protos.HeadInfo{Version: sdb.orc.commitTs()}, changes...)
	if err != nil {
		return err
	}
	for _, l1Result := range shardMap {
		err = sdb.addToShard(l1Result)
		if err != nil {
			return err
		}
	}
	return nil
}

func (sdb *ShardingDB) newManifestChange(fid, shardID uint32, cf byte, level int, op protos.ManifestChange_Operation) *protos.ManifestChange {
	log.S().Infof("new manifest change fid %d, shard %d, cf %d, level %d %s", fid, shardID, cf, level, op.String())
	return &protos.ManifestChange{
		Id:    uint64(shardID)<<32 + uint64(fid),
		Op:    op,
		Level: uint32(cf)<<16 + uint32(level),
	}
}

func (sdb *ShardingDB) addToShard(sl0Result *shardL0Result) error {
	for i, cfResult := range sl0Result.cfResults {
		if cfResult == nil {
			continue
		}
		var tbl table.Table
		var err error
		if cfResult.FileName != "" {
			// must
			var file sstable.TableFile
			file, err = sstable.NewMMapFile(cfResult.FileName)
			if err != nil {
				return err
			}
			tbl, err = sstable.OpenTable(cfResult.FileName, file)
			if err != nil {
				return err
			}
		} else {
			file := sstable.NewInMemFile(cfResult.FileData, cfResult.IndexData)
			tbl, err = sstable.OpenInMemoryTable(file)
			if err != nil {
				return err
			}
		}
		debugPrintTable(tbl, "addToShard")
		sdb.addTableToShardCF(sl0Result.shard.cfs[i], tbl)
	}
	return nil
}

func debugPrintTable(tbl table.Table, label string) {
	it := tbl.NewIterator(false)
	rowCnt := 0
	for it.Rewind(); it.Valid(); it.Next() {
		rowCnt++
	}
	log.S().Infof("%s table row count %d", label, rowCnt)
}

func (sdb *ShardingDB) addTableToShardCF(scf *shardCF, tbl table.Table) {
	for {
		oldSubLevel0 := (*levelHandler)(atomic.LoadPointer(&scf.levels[0]))
		newSubLevel0 := newLevelHandler(sdb.opt.NumLevelZeroTablesStall, 0, sdb.metrics)
		newSubLevel0.tables = append(newSubLevel0.tables, tbl)
		newSubLevel0.tables = append(newSubLevel0.tables, oldSubLevel0.tables...)
		if atomic.CompareAndSwapPointer(&scf.levels[0], unsafe.Pointer(oldSubLevel0), unsafe.Pointer(newSubLevel0)) {
			break
		}
	}
}

type shardL0Result struct {
	cfResults []*sstable.BuildResult
	cfConfs   []CFConfig
	shard     *Shard
}

func (sdb *ShardingDB) UpdateMangedSafeTs(ts uint64) {
	for {
		old := atomic.LoadUint64(&sdb.mangedSafeTS)
		if old < ts {
			if !atomic.CompareAndSwapUint64(&sdb.mangedSafeTS, old, ts) {
				continue
			}
		}
		break
	}
}

func (sdb *ShardingDB) getManagedSafeTS() uint64 {
	return atomic.LoadUint64(&sdb.mangedSafeTS)
}

func (sdb *ShardingDB) getUnmanagedSafeTS() uint64 {
	return atomic.LoadUint64(&sdb.safeTsTracker.safeTs)
}

func (sdb *ShardingDB) buildShardTablesForCF(l0Tbls *globalL0Tables, cf byte) ([]*sstable.BuildResult, error) {
	log.S().Infof("build shard tables for CF %d", cf)
	iters := make([]y.Iterator, 0, len(l0Tbls.tables))
	for i := 0; i < len(l0Tbls.tables); i++ {
		iter := l0Tbls.tables[i].newIterator(cf, false)
		if iter == nil {
			continue
		}
		iters = append(iters, iter)
	}
	if len(iters) == 0 {
		return nil, nil
	}
	it := table.NewMergeIterator(iters, false)
	it.Rewind()
	helper := newBuildHelper(sdb, cf)
	var buildResults []*sstable.BuildResult
	for it.Valid() {
		result, err := helper.buildOne(it)
		if err != nil {
			return nil, err
		}
		buildResults = append(buildResults, result)
	}
	return buildResults, nil
}

type shardL0BuildHelper struct {
	db        *ShardingDB
	builder   *sstable.Builder
	shardTree *shardTree
	endKey    []byte
	lastKey   y.Key
	skipKey   y.Key
	safeTS    uint64
	filter    CompactionFilter
}

func newBuildHelper(db *ShardingDB, cf byte) *shardL0BuildHelper {
	helper := &shardL0BuildHelper{db: db}
	helper.shardTree = db.loadShardTree()
	if db.opt.CompactionFilterFactory != nil {
		biggest := helper.shardTree.last().End
		helper.filter = db.opt.CompactionFilterFactory(1, nil, biggest)
	}
	if db.opt.CFs[cf].Managed {
		helper.safeTS = db.getManagedSafeTS()
	} else {
		helper.safeTS = db.getUnmanagedSafeTS()
	}
	return helper
}

func (h *shardL0BuildHelper) resetEndKey(key []byte) {
	shard := h.shardTree.get(key)
	h.endKey = shard.End
}

func (h *shardL0BuildHelper) setFD(fd *os.File) {
	if h.builder == nil {
		h.builder = sstable.NewTableBuilder(fd, nil, 1, h.db.opt.TableBuilderOptions)
	} else {
		h.builder.Reset(fd)
	}
}

func (h *shardL0BuildHelper) buildOne(it y.Iterator) (*sstable.BuildResult, error) {
	filename := sstable.NewFilename(uint64(h.db.allocFid()), h.db.opt.Dir)
	fd, err := directio.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	h.setFD(fd)
	h.lastKey.Reset()
	h.skipKey.Reset()
	h.resetEndKey(it.Key().UserKey)
	for ; it.Valid(); y.NextAllVersion(it) {
		vs := it.Value()
		key := it.Key()
		// See if we need to skip this key.
		if !h.skipKey.IsEmpty() {
			if key.SameUserKey(h.skipKey) {
				continue
			} else {
				h.skipKey.Reset()
			}
		}
		if !key.SameUserKey(h.lastKey) {
			// We only break on shard key, the generated table may exceeds max table size for L1 table.
			if bytes.Compare(key.UserKey, h.endKey) >= 0 {
				break
			}
			h.lastKey.Copy(key)
		}

		// Only consider the versions which are below the safeTS, otherwise, we might end up discarding the
		// only valid version for a running transaction.
		if key.Version <= h.safeTS {
			// key is the latest readable version of this key, so we simply discard all the rest of the versions.
			h.skipKey.Copy(key)
			if !isDeleted(vs.Meta) && h.filter != nil {
				switch h.filter.Filter(key.UserKey, vs.Value, vs.UserMeta) {
				case DecisionMarkTombstone:
					// There may have ole versions for this key, so convert to delete tombstone.
					h.builder.Add(key, y.ValueStruct{Meta: bitDelete})
					continue
				case DecisionDrop:
					continue
				case DecisionKeep:
				}
			}
		}
		h.builder.Add(key, vs)
	}
	if h.builder.Empty() {
		return nil, nil
	}
	result, err := h.builder.Finish()
	if err != nil {
		return nil, err
	}
	if fd != nil {
		result.FileName = fd.Name()
	}
	fd.Close()
	return result, nil
}
