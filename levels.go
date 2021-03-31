/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package badger

import (
	"bytes"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/ncw/directio"
	"github.com/pingcap/badger/epoch"
	"github.com/pingcap/badger/options"
	"github.com/pingcap/badger/protos"
	"github.com/pingcap/badger/table"
	"github.com/pingcap/badger/table/sstable"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type levelsController struct {
	nextFileID uint64 // Atomic

	// The following are initialized once and const.
	resourceMgr *epoch.ResourceManager
	levels      []*levelHandler
	kv          *DB

	cstatus compactStatus

	opt options.TableBuilderOptions
}

var (
	// This is for getting timings between stalls.
	lastUnstalled time.Time
)

// revertToManifest checks that all necessary table files exist and removes all table files not
// referenced by the manifest.  idMap is a set of table file id's that were read from the directory
// listing.
func revertToManifest(kv *DB, mf *Manifest, idMap map[uint64]struct{}) error {
	// 1. Check all files in manifest exist.
	for id := range mf.Tables {
		if _, ok := idMap[id]; !ok {
			return fmt.Errorf("file does not exist for table %d", id)
		}
	}

	// 2. Delete files that shouldn't exist.
	for id := range idMap {
		if _, ok := mf.Tables[id]; !ok {
			log.Info("table file not referenced in MANIFEST", zap.Uint64("id", id))
			filename := sstable.NewFilename(id, kv.opt.Dir)
			if err := os.Remove(filename); err != nil {
				return y.Wrapf(err, "While removing table %d", id)
			}
		}
	}

	return nil
}

func newLevelsController(kv *DB, mf *Manifest, mgr *epoch.ResourceManager, opt options.TableBuilderOptions) (*levelsController, error) {
	y.Assert(kv.opt.NumLevelZeroTablesStall > kv.opt.NumLevelZeroTables)
	s := &levelsController{
		kv:          kv,
		levels:      make([]*levelHandler, kv.opt.TableBuilderOptions.MaxLevels),
		opt:         opt,
		resourceMgr: mgr,
	}
	s.cstatus.levels = make([]*levelCompactStatus, kv.opt.TableBuilderOptions.MaxLevels)

	for i := 0; i < kv.opt.TableBuilderOptions.MaxLevels; i++ {
		s.levels[i] = newLevelHandler(kv, i)
		if i == 0 {
			// Do nothing.
		} else if i == 1 {
			// Level 1 probably shouldn't be too much bigger than level 0.
			s.levels[i].maxTotalSize = kv.opt.LevelOneSize
		} else {
			s.levels[i].maxTotalSize = s.levels[i-1].maxTotalSize * int64(kv.opt.TableBuilderOptions.LevelSizeMultiplier)
		}
		s.cstatus.levels[i] = new(levelCompactStatus)
	}

	// Compare manifest against directory, check for existent/non-existent files, and remove.
	if err := revertToManifest(kv, mf, getIDMap(kv.opt.Dir)); err != nil {
		return nil, err
	}

	// Some files may be deleted. Let's reload.
	tables := make([][]table.Table, kv.opt.TableBuilderOptions.MaxLevels)
	var maxFileID uint64
	for fileID, tableManifest := range mf.Tables {
		fname := sstable.NewFilename(fileID, kv.opt.Dir)
		var flags uint32 = y.Sync
		if kv.opt.ReadOnly {
			flags |= y.ReadOnly
		}

		t, err := sstable.OpenTable(fname, kv.blockCache, kv.indexCache)
		if err != nil {
			closeAllTables(tables)
			return nil, errors.Wrapf(err, "Opening table: %q", fname)
		}

		level := tableManifest.Level
		tables[level] = append(tables[level], t)

		if fileID > maxFileID {
			maxFileID = fileID
		}
	}
	s.nextFileID = maxFileID + 1
	for i, tbls := range tables {
		s.levels[i].initTables(tbls)
	}

	// Make sure key ranges do not overlap etc.
	if err := s.validate(); err != nil {
		_ = s.cleanupLevels()
		return nil, errors.Wrap(err, "Level validation")
	}

	// Sync directory (because we have at least removed some files, or previously created the
	// manifest file).
	if err := syncDir(kv.opt.Dir); err != nil {
		_ = s.close()
		return nil, err
	}

	return s, nil
}

// Closes the tables, for cleanup in newLevelsController.  (We Close() instead of using DecrRef()
// because that would delete the underlying files.)  We ignore errors, which is OK because tables
// are read-only.
func closeAllTables(tables [][]table.Table) {
	for _, tableSlice := range tables {
		for _, table := range tableSlice {
			_ = table.Close()
		}
	}
}

func (lc *levelsController) cleanupLevels() error {
	var firstErr error
	for _, l := range lc.levels {
		if err := l.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (lc *levelsController) startCompact(c *y.Closer) {
	n := lc.kv.opt.NumCompactors
	c.AddRunning(n - 1)
	for i := 0; i < n; i++ {
		// The first half compaction workers take level as priority, others take score
		// as priority.
		go lc.runWorker(c, i*2 >= n)
	}
}

func (lc *levelsController) runWorker(c *y.Closer, scorePriority bool) {
	defer c.Done()
	if lc.kv.opt.DoNotCompact {
		return
	}

	for {
		guard := lc.resourceMgr.Acquire()
		prios := lc.pickCompactLevels()
		if scorePriority {
			sort.Slice(prios, func(i, j int) bool {
				return prios[i].score > prios[j].score
			})
		}
		var didCompact bool
		for _, p := range prios {
			// TODO: Handle error.
			didCompact, _ = lc.doCompact(p, guard)
			if didCompact {
				break
			}
		}
		guard.Done()
		waitDur := time.Second * 3
		if didCompact {
			waitDur /= 10
		}
		timer := time.NewTimer(waitDur)
		select {
		case <-c.HasBeenClosed():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// Returns true if level zero may be compacted, without accounting for compactions that already
// might be happening.
func (lc *levelsController) isL0Compactable() bool {
	return lc.levels[0].numTables() >= lc.kv.opt.NumLevelZeroTables
}

// Returns true if the non-zero level may be compacted.  deltaSize provides the size of the tables
// which are currently being compacted so that we treat them as already having started being
// compacted (because they have been, yet their size is already counted in getTotalSize).
func (l *levelHandler) isCompactable(deltaSize int64) bool {
	return l.getTotalSize() >= l.maxTotalSize+deltaSize
}

type compactionPriority struct {
	level int
	score float64
}

// pickCompactLevel determines which level to compact.
// Based on: https://github.com/facebook/rocksdb/wiki/Leveled-Compaction
func (lc *levelsController) pickCompactLevels() (prios []compactionPriority) {
	// This function must use identical criteria for guaranteeing compaction's progress that
	// addLevel0Table uses.

	// cstatus is checked to see if level 0's tables are already being compacted
	if !lc.cstatus.overlapsWith(0, infRange) && lc.isL0Compactable() {
		pri := compactionPriority{
			level: 0,
			score: float64(lc.levels[0].numTables()) / float64(lc.kv.opt.NumLevelZeroTables),
		}
		prios = append(prios, pri)
	}

	// now calcalute scores from level 1
	for levelNum := 1; levelNum < len(lc.levels); levelNum++ {
		// Don't consider those tables that are already being compacted right now.
		deltaSize := lc.cstatus.deltaSize(levelNum)

		l := lc.levels[levelNum]
		if l.isCompactable(deltaSize) {
			pri := compactionPriority{
				level: levelNum,
				score: float64(l.getTotalSize()-deltaSize) / float64(l.maxTotalSize),
			}
			prios = append(prios, pri)
		}
	}
	// We used to sort compaction priorities based on the score. But, we
	// decided to compact based on the level, not the priority. So, upper
	// levels (level 0, level 1, etc) always get compacted first, before the
	// lower levels -- this allows us to avoid stalls.
	return prios
}

func (lc *levelsController) setHasOverlapTable(cd *CompactDef) {
	if cd.moveDown() {
		return
	}
	kr := getKeyRange(cd.Top)
	for i := cd.Level + 2; i < len(lc.levels); i++ {
		lh := lc.levels[i]
		lh.RLock()
		left, right := lh.overlappingTables(levelHandlerRLocked{}, kr)
		lh.RUnlock()
		if right-left > 0 {
			cd.HasOverlap = true
			return
		}
	}
	return
}

type DiscardStats struct {
	numSkips     int64
	skippedBytes int64
	ptrs         []blobPointer
}

func (ds *DiscardStats) collect(vs y.ValueStruct) {
	if vs.Meta&bitValuePointer > 0 {
		var bp blobPointer
		bp.decode(vs.Value)
		ds.ptrs = append(ds.ptrs, bp)
		ds.skippedBytes += int64(bp.length)
	}
	ds.numSkips++
}

func (ds *DiscardStats) String() string {
	return fmt.Sprintf("numSkips:%d, skippedBytes:%d", ds.numSkips, ds.skippedBytes)
}

func shouldFinishFile(key, lastKey y.Key, guard *Guard, currentSize, maxSize int64) bool {
	if lastKey.IsEmpty() {
		return false
	}
	if guard != nil {
		if !bytes.HasPrefix(key.UserKey, guard.Prefix) {
			return true
		}
		if !matchGuard(key.UserKey, lastKey.UserKey, guard) {
			if maxSize > guard.MinSize {
				maxSize = guard.MinSize
			}
		}
	}
	return currentSize > maxSize
}

func matchGuard(key, lastKey []byte, guard *Guard) bool {
	if len(lastKey) < guard.MatchLen {
		return false
	}
	return bytes.HasPrefix(key, lastKey[:guard.MatchLen])
}

func searchGuard(key []byte, guards []Guard) *Guard {
	var maxMatchGuard *Guard
	for i := range guards {
		guard := &guards[i]
		if bytes.HasPrefix(key, guard.Prefix) {
			if maxMatchGuard == nil || len(guard.Prefix) > len(maxMatchGuard.Prefix) {
				maxMatchGuard = guard
			}
		}
	}
	return maxMatchGuard
}

func overSkipTables(key y.Key, skippedTables []table.Table) (newSkippedTables []table.Table, over bool) {
	var i int
	for i < len(skippedTables) {
		t := skippedTables[i]
		if key.Compare(t.Biggest()) > 0 {
			i++
		} else {
			break
		}
	}
	return skippedTables[i:], i > 0
}

func (lc *levelsController) prepareCompactionDef(cd *CompactDef) {
	// Pick up the currently pending transactions' min readTs, so we can discard versions below this
	// readTs. We should never discard any versions starting from above this timestamp, because that
	// would affect the snapshot view guarantee provided by transactions.
	cd.SafeTS = lc.kv.getCompactSafeTs()
	if lc.kv.opt.CompactionFilterFactory != nil {
		cd.Filter = lc.kv.opt.CompactionFilterFactory(cd.Level+1, cd.smallest().UserKey, cd.biggest().UserKey)
		cd.Guards = cd.Filter.Guards()
	}
	cd.Opt = lc.opt
	cd.Dir = lc.kv.opt.Dir
	cd.AllocIDFunc = lc.reserveFileID
	cd.Limiter = lc.kv.limiter
}

func (lc *levelsController) getCompactor(cd *CompactDef) compactor {
	if len(cd.SkippedTbls) > 0 || lc.kv.opt.RemoteCompactionAddr == "" || lc.kv.opt.ValueThreshold > 0 {
		return &localCompactor{}
	}
	return &remoteCompactor{
		remoteAddr: lc.kv.opt.RemoteCompactionAddr,
	}
}

// compactBuildTables merge topTables and botTables to form a list of new tables.
func (lc *levelsController) compactBuildTables(cd *CompactDef) (newTables []table.Table, err error) {

	// Try to collect stats so that we can inform value log about GC. That would help us find which
	// value log file should be GCed.
	lc.prepareCompactionDef(cd)
	stats := &y.CompactionStats{}
	discardStats := &DiscardStats{}
	buildResults, err := lc.getCompactor(cd).compact(cd, stats, discardStats)
	if err != nil {
		return nil, err
	}
	newTables, err = lc.openTables(buildResults)
	if err != nil {
		return nil, err
	}
	lc.handleStats(cd.Level+1, stats, discardStats)
	return
}

// CompactTables compacts tables in CompactDef and returns the file names.
func CompactTables(cd *CompactDef, stats *y.CompactionStats, discardStats *DiscardStats) ([]*sstable.BuildResult, error) {
	var buildResults []*sstable.BuildResult
	it := cd.buildIterator()
	defer it.Close()

	skippedTbls := cd.SkippedTbls
	splitHints := cd.splitHints

	var lastKey, skipKey y.Key
	var builder *sstable.Builder
	for it.Valid() {
		var fd *os.File
		if !cd.InMemory {
			fileID := cd.AllocIDFunc()
			filename := sstable.NewFilename(fileID, cd.Dir)
			var err error
			fd, err = directio.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0666)
			if err != nil {
				return nil, err
			}
		}
		if builder == nil {
			builder = sstable.NewTableBuilder(fd, cd.Limiter, cd.Level+1, cd.Opt)
		} else {
			builder.Reset(fd)
		}
		lastKey.Reset()
		guard := searchGuard(it.Key().UserKey, cd.Guards)
		for ; it.Valid(); y.NextAllVersion(it) {
			stats.KeysRead++
			vs := it.Value()
			key := it.Key()
			kvSize := int(vs.EncodedSize()) + key.Len()
			stats.BytesRead += kvSize
			// See if we need to skip this key.
			if !skipKey.IsEmpty() {
				if key.SameUserKey(skipKey) {
					discardStats.collect(vs)
					continue
				} else {
					skipKey.Reset()
				}
			}
			if !key.SameUserKey(lastKey) {
				// Only break if we are on a different key, and have reached capacity. We want
				// to ensure that all versions of the key are stored in the same sstable, and
				// not divided across multiple tables at the same level.
				if len(skippedTbls) > 0 {
					var over bool
					skippedTbls, over = overSkipTables(key, skippedTbls)
					if over && !builder.Empty() {
						break
					}
				}
				if shouldFinishFile(key, lastKey, guard, int64(builder.EstimateSize()+kvSize), cd.Opt.MaxTableSize) {
					break
				}
				if len(splitHints) != 0 && key.Compare(splitHints[0]) >= 0 {
					splitHints = splitHints[1:]
					for len(splitHints) > 0 && key.Compare(splitHints[0]) >= 0 {
						splitHints = splitHints[1:]
					}
					break
				}
				lastKey.Copy(key)
			}

			// Only consider the versions which are below the minReadTs, otherwise, we might end up discarding the
			// only valid version for a running transaction.
			if key.Version <= cd.SafeTS {
				// key is the latest readable version of this key, so we simply discard all the rest of the versions.
				skipKey.Copy(key)

				if isDeleted(vs.Meta) {
					// If this key range has overlap with lower levels, then keep the deletion
					// marker with the latest version, discarding the rest. We have set skipKey,
					// so the following key versions would be skipped. Otherwise discard the deletion marker.
					if !cd.HasOverlap {
						continue
					}
				} else if cd.Filter != nil {
					switch cd.Filter.Filter(key.UserKey, vs.Value, vs.UserMeta) {
					case DecisionMarkTombstone:
						discardStats.collect(vs)
						if cd.HasOverlap {
							// There may have ole versions for this key, so convert to delete tombstone.
							builder.Add(key, y.ValueStruct{Meta: bitDelete})
						}
						continue
					case DecisionDrop:
						discardStats.collect(vs)
						continue
					case DecisionKeep:
					}
				}
			}
			builder.Add(key, vs)
			stats.KeysWrite++
			stats.BytesWrite += kvSize
		}
		if builder.Empty() {
			continue
		}
		result, err := builder.Finish()
		if err != nil {
			return nil, err
		}
		fd.Close()
		buildResults = append(buildResults, result)
	}
	return buildResults, nil
}

func (lc *levelsController) openTables(buildResults []*sstable.BuildResult) (newTables []table.Table, err error) {
	for _, result := range buildResults {
		var tbl table.Table
		tbl, err = sstable.OpenTable(result.FileName, lc.kv.blockCache, lc.kv.indexCache)
		if err != nil {
			return
		}
		newTables = append(newTables, tbl)
	}
	// Ensure created files' directory entries are visible.  We don't mind the extra latency
	// from not doing this ASAP after all file creation has finished because this is a
	// background operation.
	err = syncDir(lc.kv.opt.Dir)
	if err != nil {
		log.Error("compact sync dir error", zap.Error(err))
		return
	}
	sortTables(newTables)
	return
}

func (lc *levelsController) handleStats(nexLevel int, stats *y.CompactionStats, discardStats *DiscardStats) {
	stats.KeysDiscard = int(discardStats.numSkips)
	stats.BytesDiscard = int(discardStats.skippedBytes)
	lc.levels[nexLevel].metrics.UpdateCompactionStats(stats)
	log.Info("compact send discard stats", zap.Stringer("stats", discardStats))
	if len(discardStats.ptrs) > 0 {
		lc.kv.blobManger.discardCh <- discardStats
	}
}

func buildChangeSet(cd *CompactDef, newTables []table.Table) protos.ManifestChangeSet {
	changes := []*protos.ManifestChange{}
	for _, table := range newTables {
		changes = append(changes,
			newCreateChange(table.ID(), cd.Level+1))
	}
	for _, table := range cd.Top {
		changes = append(changes, newDeleteChange(table.ID()))
	}
	for _, table := range cd.Bot {
		changes = append(changes, newDeleteChange(table.ID()))
	}
	return protos.ManifestChangeSet{Changes: changes}
}

func sumTableSize(tables []table.Table) int64 {
	var size int64
	for _, t := range tables {
		size += t.Size()
	}
	return size
}

func calcRatio(topSize, botSize int64) float64 {
	if botSize == 0 {
		return float64(topSize)
	}
	return float64(topSize) / float64(botSize)
}

func (lc *levelsController) runCompactDef(cd *CompactDef, guard *epoch.Guard) error {
	timeStart := time.Now()

	thisLevel := lc.levels[cd.Level]
	nextLevel := lc.levels[cd.Level+1]

	var newTables []table.Table
	var changeSet protos.ManifestChangeSet
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
		changeSet = protos.ManifestChangeSet{}
		for _, t := range newTables {
			changeSet.Changes = append(changeSet.Changes, newMoveDownChange(t.ID(), cd.Level+1))
		}
	} else {
		var err error
		newTables, err = lc.compactBuildTables(cd)
		if err != nil {
			return err
		}
		changeSet = buildChangeSet(cd, newTables)
	}

	// We write to the manifest _before_ we delete files (and after we created files)
	if err := lc.kv.manifest.addChanges(changeSet.Changes, nil); err != nil {
		return err
	}

	// See comment earlier in this function about the ordering of these ops, and the order in which
	// we access levels when reading.
	nextLevel.replaceTables(newTables, cd, guard)
	thisLevel.deleteTables(cd.Top, guard, cd.moveDown())

	// Note: For level 0, while doCompact is running, it is possible that new tables are added.
	// However, the tables are added only to the end, so it is ok to just delete the first table.

	log.Info("compaction done",
		zap.Stringer("def", cd), zap.Int("deleted", len(cd.Top)+len(cd.Bot)), zap.Int("added", len(newTables)),
		zap.Duration("duration", time.Since(timeStart)))
	return nil
}

// doCompact picks some table on level l and compacts it away to the next level.
func (lc *levelsController) doCompact(p compactionPriority, guard *epoch.Guard) (bool, error) {
	l := p.level
	y.Assert(l+1 < lc.kv.opt.TableBuilderOptions.MaxLevels) // Sanity check.

	cd := &CompactDef{
		Level: l,
	}
	thisLevel := lc.levels[cd.Level]
	nextLevel := lc.levels[cd.Level+1]

	log.Info("start compaction", zap.Int("level", p.level), zap.Float64("score", p.score))

	// While picking tables to be compacted, both levels' tables are expected to
	// remain unchanged.
	if l == 0 {
		if !cd.fillTablesL0(&lc.cstatus, thisLevel, nextLevel) {
			log.Info("build compaction fill tables failed", zap.Int("level", l))
			return false, nil
		}
	} else {
		if !cd.fillTables(&lc.cstatus, thisLevel, nextLevel) {
			log.Info("build compaction fill tables failed", zap.Int("level", l))
			return false, nil
		}
	}
	lc.setHasOverlapTable(cd)
	defer lc.cstatus.delete(cd) // Remove the ranges from compaction status.

	log.Info("running compaction", zap.Stringer("def", cd))
	if err := lc.runCompactDef(cd, guard); err != nil {
		// This compaction couldn't be done successfully.
		log.Info("compact failed", zap.Stringer("def", cd), zap.Error(err))
		return false, err
	}

	log.Info("compaction done", zap.Int("level", cd.Level))
	return true, nil
}

func (lc *levelsController) addLevel0Table(t table.Table, head *protos.HeadInfo) error {
	// We update the manifest _before_ the table becomes part of a levelHandler, because at that
	// point it could get used in some compaction.  This ensures the manifest file gets updated in
	// the proper order. (That means this update happens before that of some compaction which
	// deletes the table.)
	err := lc.kv.manifest.addChanges([]*protos.ManifestChange{
		newCreateChange(t.ID(), 0),
	}, head)
	if err != nil {
		return err
	}

	for !lc.levels[0].tryAddLevel0Table(t) {
		// Stall. Make sure all levels are healthy before we unstall.
		var timeStart time.Time
		{
			log.Warn("STALLED STALLED STALLED", zap.Duration("duration", time.Since(lastUnstalled)))
			for i := 0; i < lc.kv.opt.TableBuilderOptions.MaxLevels; i++ {
				lc.cstatus.RLock()
				status := lc.cstatus.levels[i].debug()
				lc.cstatus.RUnlock()
				log.Warn("dump level status", zap.Int("level", i), zap.String("status", status),
					zap.Int64("size", lc.levels[i].getTotalSize()))
			}
			timeStart = time.Now()
		}
		// Before we unstall, we need to make sure that level 0 is healthy. Otherwise, we
		// will very quickly fill up level 0 again.
		for i := 0; ; i++ {
			// It's crucial that this behavior replicates pickCompactLevels' behavior in
			// computing compactability in order to guarantee progress.
			// Break the loop once L0 has enough space to accommodate new tables.
			if !lc.isL0Compactable() {
				break
			}
			time.Sleep(10 * time.Millisecond)
			if i%100 == 0 {
				prios := lc.pickCompactLevels()
				log.S().Warnf("waiting to add level 0 table, %+v", prios)
				i = 0
			}
		}
		log.Info("UNSTALLED UNSTALLED UNSTALLED UNSTALLED UNSTALLED UNSTALLED", zap.Duration("duration", time.Since(timeStart)))
		lastUnstalled = time.Now()
	}

	return nil
}

func (s *levelsController) close() error {
	err := s.cleanupLevels()
	return errors.Wrap(err, "levelsController.Close")
}

// get returns the found value if any. If not found, we return nil.
func (s *levelsController) get(key y.Key, keyHash uint64) y.ValueStruct {
	// It's important that we iterate the levels from 0 on upward.  The reason is, if we iterated
	// in opposite order, or in parallel (naively calling all the h.RLock() in some order) we could
	// read level L's tables post-compaction and level L+1's tables pre-compaction.  (If we do
	// parallelize this, we will need to call the h.RLock() function by increasing order of level
	// number.)
	start := time.Now()
	defer s.kv.metrics.LSMGetDuration.Observe(time.Since(start).Seconds())
	for _, h := range s.levels {
		vs := h.get(key, keyHash) // Calls h.RLock() and h.RUnlock().
		if vs.Valid() {
			return vs
		}
	}
	return y.ValueStruct{}
}

func (s *levelsController) multiGet(pairs []keyValuePair) {
	start := time.Now()
	for _, h := range s.levels {
		h.multiGet(pairs)
	}
	s.kv.metrics.LSMMultiGetDuration.Observe(time.Since(start).Seconds())
}

func appendIteratorsReversed(out []y.Iterator, th []table.Table, reversed bool) []y.Iterator {
	for i := len(th) - 1; i >= 0; i-- {
		// This will increment the reference of the table handler.
		out = append(out, table.NewConcatIterator(th[i:i+1], reversed))
	}
	return out
}

// appendIterators appends iterators to an array of iterators, for merging.
// Note: This obtains references for the table handlers. Remember to close these iterators.
func (s *levelsController) appendIterators(
	iters []y.Iterator, opts *IteratorOptions) []y.Iterator {
	// Just like with get, it's important we iterate the levels from 0 on upward, to avoid missing
	// data when there's a compaction.
	for _, level := range s.levels {
		iters = level.appendIterators(iters, opts)
	}
	return iters
}

type TableInfo struct {
	ID    uint64
	Level int
	Left  []byte
	Right []byte
}

func (lc *levelsController) getTableInfo() (result []TableInfo) {
	for _, l := range lc.levels {
		for _, t := range l.tables {
			info := TableInfo{
				ID:    t.ID(),
				Level: l.level,
				Left:  t.Smallest().UserKey,
				Right: t.Biggest().UserKey,
			}
			result = append(result, info)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Level != result[j].Level {
			return result[i].Level < result[j].Level
		}
		return result[i].ID < result[j].ID
	})
	return
}
