package badger

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/ncw/directio"
	"github.com/pingcap/badger/epoch"
	"github.com/pingcap/badger/protos"
	"github.com/pingcap/badger/table"
	"github.com/pingcap/badger/table/sstable"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

func (sdb *ShardingDB) runGlobalL0CompactionLoop(c *y.Closer) {
	defer c.Done()
	for {
		l0Tbls := sdb.loadGlobalL0Tables()
		if len(l0Tbls.tables) < sdb.opt.NumLevelZeroTables {
			select {
			case <-time.After(time.Millisecond * 100):
				continue
			case <-c.HasBeenClosed():
				return
			}
		}
		log.S().Info("start global l0 compaction")
		err := sdb.doL0Compaction()
		if err != nil {
			log.Error("failed to compact L0", zap.Error(err))
		} else {
			log.S().Info("finish global l0 compaction")
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
			y.Assert(sl0Result.shard.ID != 0)
		}
	}
	for _, l0Tbl := range l0Tbls.tables {
		changes = append(changes, sdb.newManifestChange(l0Tbl.fid, 0, 0, 0, protos.ManifestChange_DELETE))
	}
	err := sdb.manifest.addChanges(sdb.orc.commitTs(), changes...)
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
	return &protos.ManifestChange{
		ShardID: shardID,
		Id:      uint64(fid),
		Op:      op,
		Level:   uint32(level),
		CF:      uint32(cf),
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
		sdb.addTableToShardCF(sl0Result.shard.cfs[i], tbl)
	}
	return nil
}

func debugTableCount(tbl table.Table) int {
	it := tbl.NewIterator(false)
	rowCnt := 0
	for it.Rewind(); it.Valid(); it.Next() {
		rowCnt++
	}
	return rowCnt
}

func (sdb *ShardingDB) addTableToShardCF(scf *shardCF, tbl table.Table) {
	for {
		oldSubLevel0 := scf.getLevelHandler(0)
		newSubLevel0 := newLevelHandler(sdb.opt.NumLevelZeroTablesStall, 0, sdb.metrics)
		newSubLevel0.totalSize = oldSubLevel0.totalSize
		newSubLevel0.totalSize += tbl.Size()
		newSubLevel0.tables = append(newSubLevel0.tables, tbl)
		newSubLevel0.tables = append(newSubLevel0.tables, oldSubLevel0.tables...)
		if scf.casLevelHandler(0, oldSubLevel0, newSubLevel0) {
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

func (sdb *ShardingDB) getCFSafeTS(cf int) uint64 {
	if sdb.opt.CFs[cf].Managed {
		return atomic.LoadUint64(&sdb.mangedSafeTS)
	}
	return atomic.LoadUint64(&sdb.safeTsTracker.safeTs)
}

func (sdb *ShardingDB) buildShardTablesForCF(l0Tbls *globalL0Tables, cf byte) ([]*sstable.BuildResult, error) {
	iters := make([]y.Iterator, 0, len(l0Tbls.tables))
	for i := 0; i < len(l0Tbls.tables); i++ {
		iter := l0Tbls.tables[i].newIterator(cf, false, nil, nil)
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
	helper.safeTS = db.getCFSafeTS(int(cf))
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
	filename := sstable.NewFilename(uint64(h.db.allocFid("shardL0")), h.db.opt.Dir)
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

func (sdb *ShardingDB) runShardInternalCompactionLoop(c *y.Closer) {
	defer c.Done()
	var priorities []compactionPriority
	for {
		priorities = priorities[:0]
		tree := sdb.loadShardTree()
		for _, shard := range tree.shards {
			priorities = append(priorities, sdb.getCompactionPriority(shard))
		}
		sort.Slice(priorities, func(i, j int) bool {
			return priorities[i].score > priorities[j].score
		})
		wg := new(sync.WaitGroup)
		for i := 0; i < sdb.opt.NumCompactors && i < len(priorities); i++ {
			pri := priorities[i]
			if pri.score > 1 {
				wg.Add(1)
				go func() {
					// TODO: the shard may be outdated after split, compaction may delete in used files.
					// need to handle this case.
					guard := sdb.resourceMgr.Acquire()
					_ = sdb.compactShard(pri, guard)
					guard.Done()
					wg.Done()
				}()
			}
		}
		wg.Wait()
		select {
		case <-c.HasBeenClosed():
			return
		case <-time.After(time.Millisecond * 100):
		}
	}
}

func (sdb *ShardingDB) getCompactionPriority(shard *Shard) compactionPriority {
	maxPri := compactionPriority{shard: shard}
	for i, scf := range shard.cfs {
		for j := 0; j < shardMaxLevel-1; j++ {
			h := scf.getLevelHandler(j)
			var score float64
			if j == 0 {
				score = float64(len(h.tables)) - 5
			} else {
				score = float64(h.getTotalSize()) / (float64(sdb.opt.LevelOneSize) * math.Pow(10, float64(j-1)))
			}
			if score > maxPri.score {
				maxPri.score = score
				maxPri.cf = i
				maxPri.level = j
			}
		}
	}
	return maxPri
}

func (sdb *ShardingDB) compactShard(pri compactionPriority, guard *epoch.Guard) error {
	shard := pri.shard
	scf := shard.cfs[pri.cf]
	log.Info("start compaction", zap.Uint32("shard", shard.ID), zap.Int("cf", pri.cf), zap.Int("level", pri.level), zap.Float64("score", pri.score))
	shard.lock.Lock()
	defer shard.lock.Unlock()
	thisLevel := scf.getLevelHandler(pri.level)
	if len(thisLevel.tables) == 0 {
		// The shard must have been truncated.
		y.Assert(shard.loadMinGlobalL0() > 0)
		log.Info("stop compaction due to shard truncated", zap.Uint32("shard", shard.ID))
		return nil
	}
	nextLevel := scf.getLevelHandler(pri.level + 1)
	cd := &CompactDef{
		Level: pri.level,
	}
	if thisLevel.level == 0 {
		ok := cd.fillTablesL0(nil, thisLevel, nextLevel)
		y.Assert(ok)
	} else {
		ok := cd.fillTables(nil, thisLevel, nextLevel)
		y.Assert(ok)
	}
	scf.setHasOverlapping(cd)
	log.Info("running compaction", zap.Stringer("def", cd))
	if err := sdb.runCompactionDef(shard, pri.cf, cd, guard); err != nil {
		// This compaction couldn't be done successfully.
		log.Error("compact failed", zap.Stringer("def", cd), zap.Error(err))
		return err
	}
	log.Info("compaction done", zap.Int("level", cd.Level))
	return nil
}

func (sdb *ShardingDB) runCompactionDef(shard *Shard, cf int, cd *CompactDef, guard *epoch.Guard) error {
	timeStart := time.Now()

	scf := shard.cfs[cf]
	thisLevel := scf.getLevelHandler(cd.Level)
	nextLevel := scf.getLevelHandler(cd.Level + 1)

	var newTables []table.Table
	var changes []*protos.ManifestChange
	defer func() {
		for _, tbl := range newTables {
			tbl.MarkCompacting(false)
		}
		for _, tbl := range cd.SkippedTbls {
			tbl.MarkCompacting(false)
		}
	}()

	if cd.moveDown() {
		// skip level 0, since it may has many table overlap with each other
		newTables = cd.Top
		for _, t := range newTables {
			changes = append(changes, newShardDeleteChange(shard.ID, t.ID(), cf), newShardCreateChange(shard.ID, t.ID(), cf, cd.Level+1))
		}
	} else {
		var err error
		newTables, err = sdb.compactBuildTables(cf, cd)
		if err != nil {
			return errors.WithStack(err)
		}
		changes = buildShardChangeSet(shard.ID, cf, cd, newTables)
	}
	// We write to the manifest _before_ we delete files (and after we created files)
	if err := sdb.manifest.addChanges(sdb.orc.commitTs(), changes...); err != nil {
		return err
	}

	// See comment earlier in this function about the ordering of these ops, and the order in which
	// we access levels when reading.
	newNextLevel := sdb.replaceTables(nextLevel, newTables, cd, guard)
	y.Assert(scf.casLevelHandler(newNextLevel.level, nextLevel, newNextLevel))
	// level 0 may have newly added tables, we need to CAS it.
	for {
		thisLevel = scf.getLevelHandler(thisLevel.level)
		newThisLevel := sdb.deleteTables(thisLevel, cd.Top)
		if scf.casLevelHandler(thisLevel.level, thisLevel, newThisLevel) {
			if !cd.moveDown() {
				del := make([]epoch.Resource, len(cd.Top))
				for i := range cd.Top {
					del[i] = cd.Top[i]
				}
				guard.Delete(del)
			}
			break
		}
	}

	// Note: For level 0, while doCompact is running, it is possible that new tables are added.
	// However, the tables are added only to the end, so it is ok to just delete the first table.

	log.Info("compaction done", zap.Int("cf", cf),
		zap.Stringer("def", cd), zap.Int("deleted", len(cd.Top)+len(cd.Bot)), zap.Int("added", len(newTables)),
		zap.Duration("duration", time.Since(timeStart)))
	return nil
}

func getTblIDs(tables []table.Table) []uint64 {
	var ids []uint64
	for _, tbl := range tables {
		ids = append(ids, tbl.ID())
	}
	return ids
}

func getShardIDs(shards []*Shard) []uint32 {
	var ids []uint32
	for _, s := range shards {
		ids = append(ids, s.ID)
	}
	return ids
}

func (sdb *ShardingDB) replaceTables(old *levelHandler, newTables []table.Table, cd *CompactDef, guard *epoch.Guard) *levelHandler {
	newHandler := newLevelHandler(sdb.opt.NumLevelZeroTablesStall, old.level, sdb.metrics)
	newHandler.totalSize = old.totalSize
	// Increase totalSize first.
	for _, tbl := range newTables {
		newHandler.totalSize += tbl.Size()
	}
	left, right := getTablesInRange(old.tables, cd.nextRange.left, cd.nextRange.right)
	toDelete := make([]epoch.Resource, 0, right-left)
	// Update totalSize and reference counts.
	for i := left; i < right; i++ {
		tbl := old.tables[i]
		if containsTable(cd.Bot, tbl) {
			newHandler.totalSize -= tbl.Size()
			toDelete = append(toDelete, tbl)
		}
	}
	tables := make([]table.Table, 0, left+len(newTables)+len(cd.SkippedTbls)+(len(old.tables)-right))
	tables = append(tables, old.tables[:left]...)
	tables = append(tables, newTables...)
	tables = append(tables, cd.SkippedTbls...)
	tables = append(tables, old.tables[right:]...)
	sortTables(tables)
	assertTablesOrder(old.level, tables, cd)
	newHandler.tables = tables
	guard.Delete(toDelete)
	return newHandler
}

func (sdb *ShardingDB) deleteTables(old *levelHandler, toDel []table.Table) *levelHandler {
	newHandler := newLevelHandler(sdb.opt.NumLevelZeroTablesStall, old.level, sdb.metrics)
	newHandler.totalSize = old.totalSize
	toDelMap := make(map[uint64]struct{})
	for _, t := range toDel {
		toDelMap[t.ID()] = struct{}{}
	}

	// Make a copy as iterators might be keeping a slice of tables.
	var newTables []table.Table
	for _, t := range old.tables {
		_, found := toDelMap[t.ID()]
		if !found {
			newTables = append(newTables, t)
			continue
		}
		newHandler.totalSize -= t.Size()
	}
	newHandler.tables = newTables

	assertTablesOrder(newHandler.level, newTables, nil)
	return newHandler
}

func newShardCreateChange(shardID uint32, fid uint64, cf, level int) *protos.ManifestChange {
	return &protos.ManifestChange{
		ShardID: shardID,
		CF:      uint32(cf),
		Id:      fid,
		Op:      protos.ManifestChange_CREATE,
		Level:   uint32(level),
	}
}

func newShardDeleteChange(shardID uint32, id uint64, cf int) *protos.ManifestChange {
	return &protos.ManifestChange{
		ShardID: shardID,
		Id:      id,
		Op:      protos.ManifestChange_DELETE,
		CF:      uint32(cf),
	}
}

func buildShardChangeSet(shardID uint32, cf int, cd *CompactDef, newTables []table.Table) []*protos.ManifestChange {
	changes := make([]*protos.ManifestChange, 0, len(cd.Top)+len(cd.Bot)+len(newTables))
	for _, table := range newTables {
		changes = append(changes,
			newShardCreateChange(shardID, table.ID(), cf, cd.Level+1))
	}
	for _, table := range cd.Top {
		changes = append(changes, newShardDeleteChange(shardID, table.ID(), cf))
	}
	for _, table := range cd.Bot {
		changes = append(changes, newShardDeleteChange(shardID, table.ID(), cf))
	}
	return changes
}

func (sdb *ShardingDB) compactBuildTables(cf int, cd *CompactDef) (newTables []table.Table, err error) {
	err = sdb.prepareCompactionDef(cf, cd)
	if err != nil {
		return nil, err
	}
	stats := &y.CompactionStats{}
	discardStats := &DiscardStats{}
	comp := &localCompactor{}
	buildResults, err := comp.compact(cd, stats, discardStats)
	if err != nil {
		return nil, err
	}
	newTables, err = sdb.openTables(buildResults)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return newTables, nil
}

func (sdb *ShardingDB) prepareCompactionDef(cf int, cd *CompactDef) error {
	// Pick up the currently pending transactions' min readTs, so we can discard versions below this
	// readTs. We should never discard any versions starting from above this timestamp, because that
	// would affect the snapshot view guarantee provided by transactions.
	cd.SafeTS = sdb.getCFSafeTS(cf)
	if sdb.opt.CompactionFilterFactory != nil {
		cd.Filter = sdb.opt.CompactionFilterFactory(cd.Level+1, cd.smallest().UserKey, cd.biggest().UserKey)
		cd.Guards = cd.Filter.Guards()
	}
	cd.Opt = sdb.opt.TableBuilderOptions
	cd.Dir = sdb.opt.Dir
	cd.AllocIDFunc = sdb.allocFidForCompaction
	cd.S3 = sdb.opt.S3Options
	for _, t := range cd.Top {
		if sst, ok := t.(*sstable.Table); ok {
			err := sst.PrepareForCompaction()
			if err != nil {
				return err
			}
		}
	}
	for _, t := range cd.Bot {
		if sst, ok := t.(*sstable.Table); ok {
			err := sst.PrepareForCompaction()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (sdb *ShardingDB) openTables(buildResults []*sstable.BuildResult) (newTables []table.Table, err error) {
	for _, result := range buildResults {
		reader, err1 := sstable.NewMMapFile(result.FileName)
		if err1 != nil {
			return nil, err1
		}
		var tbl table.Table
		tbl, err1 = sstable.OpenTable(result.FileName, reader)
		if err1 != nil {
			return nil, err1
		}
		newTables = append(newTables, tbl)
	}
	// Ensure created files' directory entries are visible.  We don't mind the extra latency
	// from not doing this ASAP after all file creation has finished because this is a
	// background operation.
	err = syncDir(sdb.opt.Dir)
	if err != nil {
		log.Error("compact sync dir error", zap.Error(err))
		return
	}
	sortTables(newTables)
	return
}
