package badger

import (
	"github.com/ncw/directio"
	"github.com/pingcap/badger/epoch"
	"github.com/pingcap/badger/protos"
	"github.com/pingcap/badger/table"
	"github.com/pingcap/badger/table/sstable"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"math"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

func newManifestChange(fid, shardID uint64, cf int, level int, op protos.ManifestChange_Operation) *protos.ManifestChange {
	return &protos.ManifestChange{
		ShardID: shardID,
		Id:      fid,
		Op:      op,
		Level:   uint32(level),
		CF:      int32(cf),
	}
}

func debugTableCount(tbl table.Table) int {
	it := tbl.NewIterator(false)
	rowCnt := 0
	for it.Rewind(); it.Valid(); it.Next() {
		rowCnt++
	}
	return rowCnt
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

type shardL0BuildHelper struct {
	db         *ShardingDB
	builder    *sstable.Builder
	shard      *Shard
	l0Tbls     *shardL0Tables
	lastKey    y.Key
	skipKey    y.Key
	safeTS     uint64
	filter     CompactionFilter
	iter       y.Iterator
	oldHandler *levelHandler
}

func newBuildHelper(db *ShardingDB, shard *Shard, l0Tbls *shardL0Tables, cf int) *shardL0BuildHelper {
	helper := &shardL0BuildHelper{db: db, shard: shard}
	if db.opt.CompactionFilterFactory != nil {
		helper.filter = db.opt.CompactionFilterFactory(1, nil, globalShardEndKey)
	}
	helper.safeTS = db.getCFSafeTS(cf)
	var iters []y.Iterator
	if l0Tbls != nil {
		for _, tbl := range l0Tbls.tables {
			it := tbl.newIterator(cf, false)
			if it != nil {
				iters = append(iters, tbl.newIterator(cf, false))
			}
		}
	}
	helper.oldHandler = shard.cfs[cf].getLevelHandler(1)
	if len(helper.oldHandler.tables) > 0 {
		iters = append(iters, table.NewConcatIterator(helper.oldHandler.tables, false))
	}
	helper.iter = table.NewMergeIterator(iters, false)
	helper.iter.Rewind()
	return helper
}

func (h *shardL0BuildHelper) setFD(fd *os.File) {
	if h.builder == nil {
		h.builder = sstable.NewTableBuilder(fd, nil, 1, h.db.opt.TableBuilderOptions)
	} else {
		h.builder.Reset(fd)
	}
}

func (h *shardL0BuildHelper) buildOne() (*sstable.BuildResult, error) {
	filename := sstable.NewFilename(h.db.idAlloc.AllocID(), h.db.opt.Dir)
	fd, err := directio.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	h.setFD(fd)
	h.lastKey.Reset()
	h.skipKey.Reset()
	it := h.iter
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
			// We only break on table size.
			if h.builder.EstimateSize() > int(h.db.opt.TableBuilderOptions.MaxTableSize) {
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

func (sdb *ShardingDB) compactShardMultiCFL0(shard *Shard, guard *epoch.Guard) error {
	l0Tbls := shard.loadL0Tables()
	var changes []*protos.ManifestChange
	var toBeDelete []epoch.Resource
	for cf := 0; cf < sdb.numCFs; cf++ {
		helper := newBuildHelper(sdb, shard, l0Tbls, cf)
		var results []*sstable.BuildResult
		for {
			result, err := helper.buildOne()
			if err != nil {
				return err
			}
			if result == nil {
				break
			}
			results = append(results, result)
		}
		newTables, err := sdb.openTables(results)
		if err != nil {
			return err
		}
		log.S().Infof("cf %d new tables %d", cf, len(newTables))
		newHandler := newLevelHandler(sdb.opt.NumLevelZeroTablesStall, 1, sdb.metrics)
		newHandler.tables = newTables
		y.Assert(shard.cfs[cf].casLevelHandler(1, helper.oldHandler, newHandler))
		for _, newTbl := range newTables {
			changes = append(changes, newManifestChange(newTbl.ID(), shard.ID, cf, 1, protos.ManifestChange_CREATE))
		}
		for _, oldTbl := range helper.oldHandler.tables {
			changes = append(changes, newManifestChange(oldTbl.ID(), shard.ID, cf, 1, protos.ManifestChange_DELETE))
			toBeDelete = append(toBeDelete, oldTbl)
		}
	}
	if l0Tbls != nil {
		for _, tbl := range l0Tbls.tables {
			changes = append(changes, newManifestChange(tbl.fid, shard.ID, -1, 0, protos.ManifestChange_DELETE))
			toBeDelete = append(toBeDelete, tbl)
		}
	}
	err := sdb.manifest.addChanges(changes...)
	if err != nil {
		return err
	}
	guard.Delete(toBeDelete)
	originL0Len := len(l0Tbls.tables)
	for {
		l0Tbls = shard.loadL0Tables()
		newAddedTbls := l0Tbls.tables[:len(l0Tbls.tables)-originL0Len]
		newL0s := &shardL0Tables{}
		newL0s.tables = append(newL0s.tables, newAddedTbls...)
		if atomic.CompareAndSwapPointer(shard.l0s, unsafe.Pointer(l0Tbls), unsafe.Pointer(newL0s)) {
			break
		}
	}
	return nil
}

func (sdb *ShardingDB) runShardInternalCompactionLoop(c *y.Closer) {
	defer c.Done()
	var priorities []compactionPriority
	for {
		priorities = priorities[:0]
		tree := sdb.loadShardTree()
		var shardIDs []uint64
		for _, shard := range tree.shards {
			shardIDs = append(shardIDs, shard.ID)
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
					guard := sdb.resourceMgr.Acquire()
					err := sdb.compactShard(pri, guard)
					if err != nil {
						log.Error("compact shard failed", zap.Uint64("shard", pri.shard.ID), zap.Error(err))
					}
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
	l0 := shard.loadL0Tables()
	if l0 != nil && len(l0.tables) > sdb.opt.NumLevelZeroTables {
		sizeScore := float64(l0.totalSize()) * 10 / float64(sdb.opt.LevelOneSize)
		numTblsScore := float64(len(l0.tables)) / float64(sdb.opt.NumLevelZeroTables)
		maxPri.score = sizeScore*0.6 + numTblsScore*0.4
		maxPri.cf = -1
		return maxPri
	}
	for i, scf := range shard.cfs {
		for level := 1; level <= shardMaxLevel; level++ {
			h := scf.getLevelHandler(level)
			score := float64(h.getTotalSize()) / (float64(sdb.opt.LevelOneSize) * math.Pow(10, float64(level-1)))
			if score > maxPri.score {
				maxPri.score = score
				maxPri.cf = i
				maxPri.level = level
			}
		}
	}
	return maxPri
}

func (sdb *ShardingDB) compactShard(pri compactionPriority, guard *epoch.Guard) error {
	shard := pri.shard
	shard.lock.Lock()
	defer shard.lock.Unlock()
	if shard.isSplitting() {
		log.S().Infof("avoid compaction for splitting shard.")
		return nil
	}
	if pri.cf == -1 {
		log.Info("compact shard multi cf", zap.Uint64("shard", shard.ID), zap.Float64("score", pri.score))
		err := sdb.compactShardMultiCFL0(shard, guard)
		log.Info("compact shard multi cf done", zap.Uint64("shard", shard.ID), zap.Error(err))
		return err
	}
	log.Info("start compaction", zap.Uint64("shard", shard.ID), zap.Int("cf", pri.cf), zap.Int("level", pri.level), zap.Float64("score", pri.score))
	scf := shard.cfs[pri.cf]
	thisLevel := scf.getLevelHandler(pri.level)
	if len(thisLevel.tables) == 0 {
		// The shard must have been truncated.
		log.Info("stop compaction due to shard truncated", zap.Uint64("shard", shard.ID))
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
	if err := sdb.manifest.addChanges(changes...); err != nil {
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

func getShardIDs(shards []*Shard) []uint64 {
	var ids []uint64
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

func newShardCreateChange(shardID, fid uint64, cf, level int) *protos.ManifestChange {
	return &protos.ManifestChange{
		ShardID: shardID,
		CF:      int32(cf),
		Id:      fid,
		Op:      protos.ManifestChange_CREATE,
		Level:   uint32(level),
	}
}

func newShardDeleteChange(shardID, id uint64, cf int) *protos.ManifestChange {
	return &protos.ManifestChange{
		ShardID: shardID,
		Id:      id,
		Op:      protos.ManifestChange_DELETE,
		CF:      int32(cf),
	}
}

func buildShardChangeSet(shardID uint64, cf int, cd *CompactDef, newTables []table.Table) []*protos.ManifestChange {
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
	cd.AllocIDFunc = sdb.idAlloc.AllocID
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
