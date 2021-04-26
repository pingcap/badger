package badger

import (
	"bytes"
	"github.com/pingcap/badger/epoch"
	"github.com/pingcap/badger/protos"
	"github.com/pingcap/badger/s3util"
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

func newTableCreate(tbl table.Table, cf int, level int) *protos.TableCreate {
	return &protos.TableCreate{
		ID:       tbl.ID(),
		Level:    uint32(level),
		CF:       int32(cf),
		Smallest: tbl.Smallest().UserKey,
		Biggest:  tbl.Biggest().UserKey,
	}
}

func newTableCreateByResult(result *sstable.BuildResult, cf int, level int) *protos.TableCreate {
	id, ok := sstable.ParseFileID(result.FileName)
	y.Assert(ok)
	return &protos.TableCreate{
		ID:       id,
		Level:    uint32(level),
		CF:       int32(cf),
		Smallest: result.Smallest.UserKey,
		Biggest:  result.Biggest.UserKey,
	}
}

func newL0CreateByResult(result *sstable.BuildResult, props *protos.ShardProperties) *protos.L0Create {
	id, ok := sstable.ParseFileID(result.FileName)
	y.Assert(ok)
	change := &protos.L0Create{
		ID:         id,
		Start:      result.Smallest.UserKey,
		End:        result.Biggest.UserKey,
		Properties: props,
	}
	return change
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
	fd, err := y.OpenSyncedFile(filename, false)
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
	comp := &protos.ShardCompaction{}
	var toBeDelete []epoch.Resource
	var shardSizeChange int64
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
			if sdb.s3c != nil {
				err = putSSTBuildResultToS3(sdb.s3c, result)
				if err != nil {
					return err
				}
			}
			results = append(results, result)
		}
		for _, result := range results {
			comp.TableCreates = append(comp.TableCreates, newTableCreateByResult(result, cf, 1))
		}
		for _, oldTbl := range helper.oldHandler.tables {
			comp.BottomDeletes = append(comp.BottomDeletes, oldTbl.ID())
			toBeDelete = append(toBeDelete, oldTbl)
		}
	}
	if l0Tbls != nil {
		// A splitting shard does not run compaction.
		for _, tbl := range l0Tbls.tables {
			comp.TopDeletes = append(comp.TopDeletes, tbl.fid)
			toBeDelete = append(toBeDelete, tbl)
			shardSizeChange -= tbl.size
		}
	}
	change := newShardChangeSet(shard)
	change.Compaction = comp
	log.S().Infof("shard %d:%d compact L0 top deletes %v", shard.ID, shard.Ver, comp.TopDeletes)
	if sdb.metaChangeListener != nil {
		sdb.metaChangeListener.OnChange(change)
		return nil
	}
	return sdb.applyCompaction(shard, change, guard)
}

func (sdb *ShardingDB) runShardInternalCompactionLoop(c *y.Closer) {
	defer c.Done()
	var priorities []CompactionPriority
	for {
		priorities = sdb.GetCompactionPriorities(priorities)
		wg := new(sync.WaitGroup)
		for i := 0; i < sdb.opt.NumCompactors && i < len(priorities); i++ {
			pri := priorities[i]
			pri.Shard.markCompacting(true)
			wg.Add(1)
			go func() {
				err := sdb.CompactShard(pri)
				if err != nil {
					log.Error("compact shard failed", zap.Uint64("shard", pri.Shard.ID), zap.Error(err))
				}
				if err != nil || sdb.metaChangeListener == nil {
					// When meta change listener is not nil, the compaction will be finished later by applyCompaction.
					pri.Shard.markCompacting(false)
				}
				wg.Done()
			}()
		}
		wg.Wait()
		select {
		case <-c.HasBeenClosed():
			return
		case <-time.After(time.Millisecond * 100):
		}
	}
}

type CompactionPriority struct {
	CF    int
	Level int
	Score float64
	Shard *Shard
}

func (sdb *ShardingDB) getCompactionPriority(shard *Shard) CompactionPriority {
	maxPri := CompactionPriority{Shard: shard}
	l0 := shard.loadL0Tables()
	if l0 != nil && len(l0.tables) > sdb.opt.NumLevelZeroTables {
		sizeScore := float64(l0.totalSize()) * 10 / float64(sdb.opt.LevelOneSize)
		numTblsScore := float64(len(l0.tables)) / float64(sdb.opt.NumLevelZeroTables)
		maxPri.Score = sizeScore*0.6 + numTblsScore*0.4
		maxPri.CF = -1
		return maxPri
	}
	for i, scf := range shard.cfs {
		for level := 1; level <= ShardMaxLevel; level++ {
			h := scf.getLevelHandler(level)
			score := float64(h.getTotalSize()) / (float64(sdb.opt.LevelOneSize) * math.Pow(10, float64(level-1)))
			if score > maxPri.Score {
				maxPri.Score = score
				maxPri.CF = i
				maxPri.Level = level
			}
		}
	}
	return maxPri
}

func (sdb *ShardingDB) GetCompactionPriorities(buf []CompactionPriority) []CompactionPriority {
	results := buf[:0]
	sdb.shardMap.Range(func(key, value interface{}) bool {
		shard := value.(*Shard)
		if !shard.IsPassive() && atomic.LoadInt32(&shard.compacting) == 0 {
			pri := sdb.getCompactionPriority(shard)
			if pri.Score > 1 {
				results = append(results, pri)
			}
		}
		return true
	})
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})
	return results
}

func (sdb *ShardingDB) CompactShard(pri CompactionPriority) error {
	guard := sdb.resourceMgr.Acquire()
	defer guard.Done()
	shard := pri.Shard
	shard.lock.Lock()
	defer shard.lock.Unlock()
	if shard.isSplitting() {
		log.S().Debugf("avoid compaction for splitting shard.")
		return nil
	}
	latest := sdb.GetShard(shard.ID)
	if latest.Ver != shard.Ver {
		log.S().Infof("avoid compaction for shard version change.")
		return nil
	}
	if shard.IsPassive() {
		log.S().Warn("avoid active shard compaction.")
		return nil
	}
	if pri.CF == -1 {
		log.Info("compact shard multi cf", zap.Uint64("shard", shard.ID), zap.Float64("score", pri.Score))
		err := sdb.compactShardMultiCFL0(shard, guard)
		log.Info("compact shard multi cf done", zap.Uint64("shard", shard.ID), zap.Error(err))
		return err
	}
	log.Info("start compaction", zap.Uint64("shard", shard.ID), zap.Int("cf", pri.CF), zap.Int("level", pri.Level), zap.Float64("score", pri.Score))
	scf := shard.cfs[pri.CF]
	thisLevel := scf.getLevelHandler(pri.Level)
	if len(thisLevel.tables) == 0 {
		// The shard must have been truncated.
		log.Info("stop compaction due to shard truncated", zap.Uint64("shard", shard.ID))
		return nil
	}
	nextLevel := scf.getLevelHandler(pri.Level + 1)
	cd := &CompactDef{
		Level: pri.Level,
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
	if err := sdb.runCompactionDef(shard, pri.CF, cd, guard); err != nil {
		// This compaction couldn't be done successfully.
		log.Error("compact failed", zap.Stringer("def", cd), zap.Error(err))
		return err
	}
	log.Info("compaction done", zap.Int("level", cd.Level))
	return nil
}

func (sdb *ShardingDB) runCompactionDef(shard *Shard, cf int, cd *CompactDef, guard *epoch.Guard) error {
	var newTables []table.Table
	comp := &protos.ShardCompaction{Cf: int32(cf), Level: uint32(cd.Level)}
	if cd.moveDown() {
		// skip level 0, since it may has many table overlap with each other
		newTables = cd.Top
		for _, t := range newTables {
			comp.TopDeletes = append(comp.TopDeletes, t.ID())
			comp.TableCreates = append(comp.TableCreates, newTableCreate(t, cf, cd.Level+1))
		}
	} else {
		var err error
		comp.TableCreates, err = sdb.compactBuildTables(cf, cd)
		if err != nil {
			return errors.WithStack(err)
		}
		for _, t := range cd.Top {
			comp.TopDeletes = append(comp.TopDeletes, t.ID())
		}
		for _, t := range cd.Bot {
			comp.BottomDeletes = append(comp.BottomDeletes, t.ID())
		}
	}
	change := newShardChangeSet(shard)
	change.Compaction = comp
	if sdb.metaChangeListener != nil {
		sdb.metaChangeListener.OnChange(change)
		return nil
	}
	return sdb.applyCompaction(shard, change, guard)
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

func (sdb *ShardingDB) compactBuildTables(cf int, cd *CompactDef) (newTables []*protos.TableCreate, err error) {
	err = sdb.prepareCompactionDef(cf, cd)
	if err != nil {
		return nil, err
	}
	stats := &y.CompactionStats{}
	discardStats := &DiscardStats{}
	comp := &localCompactor{s3c: sdb.s3c}
	buildResults, err := comp.compact(cd, stats, discardStats)
	if err != nil {
		return nil, err
	}
	newTables = make([]*protos.TableCreate, len(buildResults))
	for i, buildResult := range buildResults {
		newTables[i] = newTableCreateByResult(buildResult, cf, cd.Level+1)
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
		var tbl table.Table
		tbl, err = sstable.OpenMMapTable(result.FileName)
		if err != nil {
			return nil, err
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

// ApplyChangeSet applies long running shard file change.
// Includes:
//   - mem-table flush.
//   - compaction.
//   - split-files.
func (sdb *ShardingDB) ApplyChangeSet(changeSet *protos.ShardChangeSet) error {
	guard := sdb.resourceMgr.Acquire()
	defer guard.Done()
	shard := sdb.GetShard(changeSet.ShardID)
	if shard.Ver != changeSet.ShardVer {
		return errShardNotMatch
	}
	if changeSet.Flush != nil {
		return sdb.applyFlush(shard, changeSet)
	}
	if changeSet.Compaction != nil {
		return sdb.applyCompaction(shard, changeSet, guard)
	}
	if changeSet.SplitFiles != nil {
		return sdb.applySplitFiles(shard, changeSet, guard)
	}
	return nil
}

func (sdb *ShardingDB) applyFlush(shard *Shard, changeSet *protos.ShardChangeSet) error {
	flush := changeSet.Flush
	bt := s3util.NewBatchTasks()
	for i,  := range flush.L0Creates {
		l0 := flush.L0Creates[i]
		bt.AppendTask(func() error {
			return sdb.loadFileFromS3(l0.ID, true)
		})
	}
	if err := sdb.s3c.BatchSchedule(bt); err != nil {
		return err
	}
	if err := sdb.manifest.writeChangeSet(changeSet); err != nil {
		return err
	}
	var newL0Tbls []*shardL0Table
	for _, newL0 := range flush.L0Creates {
		filename := sstable.NewFilename(newL0.ID, sdb.opt.Dir)
		tbl, err := openShardL0Table(filename, newL0.ID)
		if err != nil {
			return err
		}
		newL0Tbls = append(newL0Tbls, tbl)
		shard.addEstimatedSize(tbl.size)
	}
	atomicAddL0(shard, newL0Tbls...)
	atomicRemoveMemTable(shard.memTbls, len(newL0Tbls))
	shard.setSplitState(changeSet.State)
	shard.setInitialFlushed()
	return nil
}

func (sdb *ShardingDB) applyCompaction(shard *Shard, changeSet *protos.ShardChangeSet, guard *epoch.Guard) error {
	defer shard.markCompacting(false)
	comp := changeSet.Compaction
	if sdb.s3c != nil {
		bt := s3util.NewBatchTasks()
		for i := range comp.TableCreates {
			tbl := comp.TableCreates[i]
			bt.AppendTask(func() error {
				return sdb.loadFileFromS3(tbl.ID, false)
			})
		}
		if err := sdb.s3c.BatchSchedule(bt); err != nil {
			return err
		}
	}
	if err := sdb.manifest.writeChangeSet(changeSet); err != nil {
		return err
	}
	del := &deletions{resources: map[uint64]epoch.Resource{}}
	if comp.Level == 0 {
		for cf := 0; cf < sdb.numCFs; cf++ {
			err := sdb.compactionUpdateLevelHandler(shard, cf, 1, comp.TableCreates, comp.BottomDeletes, del)
			if err != nil {
				return err
			}
		}
		shard.addEstimatedSize(-atomicRemoveL0(shard, len(comp.TopDeletes)))
	} else {
		err := sdb.compactionUpdateLevelHandler(shard, int(comp.Cf), int(comp.Level), nil, comp.TopDeletes, del)
		if err != nil {
			return err
		}
		// For move down operation, the TableCreates may contains TopDeletes, we don't want to delete them.
		for _, create := range comp.TableCreates {
			del.remove(create.ID)
		}
		err = sdb.compactionUpdateLevelHandler(shard, int(comp.Cf), int(comp.Level+1), comp.TableCreates, comp.BottomDeletes, del)
		if err != nil {
			return err
		}
	}
	guard.Delete(del.collect())
	return nil
}

func (sdb *ShardingDB) compactionUpdateLevelHandler(shard *Shard, cf, level int,
	creates []*protos.TableCreate, delIDs []uint64, del *deletions) error {
	oldLevel := shard.cfs[cf].getLevelHandler(level)
	newLevel := newLevelHandler(sdb.opt.NumLevelZeroTablesStall, level, sdb.metrics)
	for _, tbl := range creates {
		if int(tbl.CF) != cf {
			continue
		}
		filename := sstable.NewFilename(tbl.ID, sdb.opt.Dir)
		tbl, err := sstable.OpenMMapTable(filename)
		if err != nil {
			return err
		}
		newLevel.tables = append(newLevel.tables, tbl)
		newLevel.totalSize += tbl.Size()
	}
	for _, oldTbl := range oldLevel.tables {
		if containsUint64(delIDs, oldTbl.ID()) {
			del.add(oldTbl.ID(), oldTbl)
		} else {
			newLevel.tables = append(newLevel.tables, oldTbl)
			newLevel.totalSize += oldTbl.Size()
		}
	}
	sortTables(newLevel.tables)
	assertTablesOrder(level, newLevel.tables, nil)
	shard.cfs[cf].casLevelHandler(level, oldLevel, newLevel)
	shard.addEstimatedSize(newLevel.totalSize - oldLevel.totalSize)
	return nil
}

func (sdb *ShardingDB) applySplitFiles(shard *Shard, changeSet *protos.ShardChangeSet, guard *epoch.Guard) error {
	if shard.GetSplitState() != protos.SplitState_PRE_SPLIT_FLUSH_DONE {
		log.S().Errorf("wrong split state %s", shard.GetSplitState())
		return errShardWrongSplittingState
	}
	splitFiles := changeSet.SplitFiles
	bt := s3util.NewBatchTasks()
	for i := range splitFiles.L0Creates {
		l0 := splitFiles.L0Creates[i]
		bt.AppendTask(func() error {
			return sdb.loadFileFromS3(l0.ID, true)
		})
	}
	if err := sdb.s3c.BatchSchedule(bt); err != nil {
		return err
	}
	bt = s3util.NewBatchTasks()
	for i := range splitFiles.TableCreates {
		tbl := splitFiles.TableCreates[i]
		bt.AppendTask(func() error {
			return sdb.loadFileFromS3(tbl.ID, false)
		})
	}
	if err := sdb.s3c.BatchSchedule(bt); err != nil {
		return err
	}
	if err := sdb.manifest.writeChangeSet(changeSet); err != nil {
		return err
	}
	oldL0s := shard.loadL0Tables()
	newL0Tbls := &shardL0Tables{make([]*shardL0Table, 0, len(oldL0s.tables)*2)}
	del := &deletions{resources: map[uint64]epoch.Resource{}}
	for _, l0 := range splitFiles.L0Creates {
		filename := sstable.NewFilename(l0.ID, sdb.opt.Dir)
		tbl, err := openShardL0Table(filename, l0.ID)
		if err != nil {
			return err
		}
		newL0Tbls.tables = append(newL0Tbls.tables, tbl)
	}
	for _, oldL0 := range oldL0s.tables {
		if containsUint64(splitFiles.TableDeletes, oldL0.fid) {
			del.add(oldL0.fid, oldL0)
		} else {
			newL0Tbls.tables = append(newL0Tbls.tables, oldL0)
		}
	}
	sort.Slice(newL0Tbls.tables, func(i, j int) bool {
		return newL0Tbls.tables[i].commitTS > newL0Tbls.tables[j].commitTS
	})
	y.Assert(atomic.CompareAndSwapPointer(shard.l0s, unsafe.Pointer(oldL0s), unsafe.Pointer(newL0Tbls)))
	newHandlers := make([][]*levelHandler, sdb.numCFs)
	for cf := 0; cf < sdb.numCFs; cf++ {
		newHandlers[cf] = make([]*levelHandler, ShardMaxLevel)
	}
	for _, tbl := range splitFiles.TableCreates {
		newHandler := newHandlers[tbl.CF][tbl.Level-1]
		if newHandler == nil {
			newHandler = newLevelHandler(sdb.opt.NumLevelZeroTablesStall, int(tbl.Level), sdb.metrics)
			newHandlers[tbl.CF][tbl.Level-1] = newHandler
		}
		filename := sstable.NewFilename(tbl.ID, sdb.opt.Dir)
		tbl, err := sstable.OpenMMapTable(filename)
		if err != nil {
			return err
		}
		newHandler.tables = append(newHandler.tables, tbl)
	}
	for cf := 0; cf < sdb.numCFs; cf++ {
		for level := 1; level < ShardMaxLevel; level++ {
			newHandler := newHandlers[cf][level-1]
			if newHandler == nil {
				continue
			}
			oldHandler := shard.cfs[cf].getLevelHandler(level)
			for _, oldTbl := range oldHandler.tables {
				if containsUint64(splitFiles.TableDeletes, oldTbl.ID()) {
					del.add(oldTbl.ID(), oldTbl)
				} else {
					newHandler.tables = append(newHandler.tables, oldTbl)
				}
			}
			sort.Slice(newHandler.tables, func(i, j int) bool {
				return bytes.Compare(newHandler.tables[i].Smallest().UserKey, newHandler.tables[j].Smallest().UserKey) < 0
			})
			y.Assert(shard.cfs[cf].casLevelHandler(level, oldHandler, newHandler))
		}
	}
	shard.setSplitState(changeSet.State)
	guard.Delete(del.collect())
	return nil
}

func containsUint64(slice []uint64, v uint64) bool {
	for _, sv := range slice {
		if sv == v {
			return true
		}
	}
	return false
}
