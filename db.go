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
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/ncw/directio"
	"github.com/pingcap/badger/cache"
	"github.com/pingcap/badger/epoch"
	"github.com/pingcap/badger/options"
	"github.com/pingcap/badger/protos"
	"github.com/pingcap/badger/table"
	"github.com/pingcap/badger/table/memtable"
	"github.com/pingcap/badger/table/sstable"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

var (
	txnKey = []byte("!badger!txn") // For indicating end of entries in txn.
)

type closers struct {
	updateSize      *y.Closer
	compactors      *y.Closer
	resourceManager *y.Closer
	blobManager     *y.Closer
	memtable        *y.Closer
	writes          *y.Closer
}

// DB provides the various functions required to interact with Badger.
// DB is thread-safe.
type DB struct {
	dirLockGuard *directoryLockGuard
	// nil if Dir and ValueDir are the same
	valueDirGuard *directoryLockGuard

	closers   closers
	mtbls     atomic.Value
	opt       Options
	manifest  *manifestFile
	lc        *levelsController
	vlog      valueLog
	logOff    logOffset // less than or equal to a pointer to the last vlog value put into mt
	syncedFid uint32    // The log fid that has been flushed to SST, older log files are safe to be deleted.
	writeCh   chan *request
	flushChan chan *flushTask // For flushing memtables.
	ingestCh  chan *ingestTask

	// mem table buffer to avoid expensive allocating big chunk of memory
	memTableCh chan *memtable.Table

	orc           *oracle
	safeTsTracker safeTsTracker

	limiter *rate.Limiter

	blockCache *cache.Cache
	indexCache *cache.Cache

	metrics      *y.MetricsSet
	lsmSize      int64
	vlogSize     int64
	volatileMode bool

	blobManger blobManager

	resourceMgr *epoch.ResourceManager
}

type memTables struct {
	tables []*memtable.Table // tables from new to old, the first one is mutable.
	length uint32            // The length is updated by the flusher.
}

func (tbls *memTables) getMutable() *memtable.Table {
	return tbls.tables[0]
}

func newMemTables(mt *memtable.Table, old *memTables) *memTables {
	newTbls := &memTables{}
	newTbls.tables = make([]*memtable.Table, 1+atomic.LoadUint32(&old.length))
	newTbls.tables[0] = mt
	copy(newTbls.tables[1:], old.tables)
	newTbls.length = uint32(len(newTbls.tables))
	return newTbls
}

const (
	kvWriteChCapacity = 1000
)

func replayFunction(out *DB) func(Entry) error {
	type txnEntry struct {
		nk y.Key
		v  y.ValueStruct
	}

	var txn []txnEntry
	var lastCommit uint64

	toLSM := func(nk y.Key, vs y.ValueStruct) {
		e := memtable.Entry{Key: nk.UserKey, Value: vs}
		mTbls := out.mtbls.Load().(*memTables)
		if out.ensureRoomForWrite(mTbls.getMutable(), e.EstimateSize()) == out.opt.MaxMemTableSize {
			mTbls = out.mtbls.Load().(*memTables)
		}
		mTbls.getMutable().PutToSkl(nk.UserKey, vs)
	}

	first := true
	return func(e Entry) error { // Function for replaying.
		if first {
			log.Info("replay wal", zap.Stringer("first key", e.Key))
		}
		first = false

		if out.orc.curRead < e.Key.Version {
			out.orc.curRead = e.Key.Version
		}

		var nk y.Key
		nk.Copy(e.Key)
		nv := make([]byte, len(e.Value))
		copy(nv, e.Value)

		v := y.ValueStruct{
			Value:    nv,
			Meta:     e.meta,
			UserMeta: e.UserMeta,
			Version:  nk.Version,
		}

		if e.meta&bitFinTxn > 0 {
			txnTs, err := strconv.ParseUint(string(e.Value), 10, 64)
			if err != nil {
				return errors.Wrapf(err, "Unable to parse txn fin: %q", e.Value)
			}
			if !out.IsManaged() {
				y.Assert(lastCommit == txnTs)
			}
			y.Assert(len(txn) > 0)
			// Got the end of txn. Now we can store them.
			for _, t := range txn {
				toLSM(t.nk, t.v)
			}
			txn = txn[:0]
			lastCommit = 0

		} else if e.meta&bitTxn == 0 {
			// This entry is from a rewrite.
			toLSM(nk, v)

			// We shouldn't get this entry in the middle of a transaction.
			y.Assert(lastCommit == 0)
			y.Assert(len(txn) == 0)

		} else {
			if lastCommit == 0 {
				lastCommit = e.Key.Version
			}
			if !out.IsManaged() {
				y.Assert(lastCommit == e.Key.Version)
			}
			te := txnEntry{nk: nk, v: v}
			txn = append(txn, te)
		}
		return nil
	}
}

// Open returns a new DB object.
func Open(opt Options) (db *DB, err error) {
	opt.maxBatchSize = (15 * opt.MaxMemTableSize) / 100
	opt.maxBatchCount = opt.maxBatchSize / int64(memtable.MaxNodeSize)

	if opt.ValueThreshold > math.MaxUint16-16 {
		return nil, ErrValueThreshold
	}

	if opt.ReadOnly {
		// Can't truncate if the DB is read only.
		opt.Truncate = false
	}

	for _, path := range []string{opt.Dir, opt.ValueDir} {
		dirExists, err := exists(path)
		if err != nil {
			return nil, y.Wrapf(err, "Invalid Dir: %q", path)
		}
		if !dirExists {
			if opt.ReadOnly {
				return nil, y.Wrapf(err, "Cannot find Dir for read-only open: %q", path)
			}
			// Try to create the directory
			err = os.Mkdir(path, 0700)
			if err != nil {
				return nil, y.Wrapf(err, "Error Creating Dir: %q", path)
			}
		}
	}
	absDir, err := filepath.Abs(opt.Dir)
	if err != nil {
		return nil, err
	}
	absValueDir, err := filepath.Abs(opt.ValueDir)
	if err != nil {
		return nil, err
	}
	var dirLockGuard, valueDirLockGuard *directoryLockGuard
	dirLockGuard, err = acquireDirectoryLock(opt.Dir, lockFile, opt.ReadOnly)
	if err != nil {
		return nil, err
	}
	defer func() {
		if dirLockGuard != nil {
			_ = dirLockGuard.release()
		}
	}()
	if absValueDir != absDir {
		valueDirLockGuard, err = acquireDirectoryLock(opt.ValueDir, lockFile, opt.ReadOnly)
		if err != nil {
			return nil, err
		}
	}
	defer func() {
		if valueDirLockGuard != nil {
			_ = valueDirLockGuard.release()
		}
	}()
	if !(opt.ValueLogFileSize <= 2<<30 && opt.ValueLogFileSize >= 1<<20) {
		return nil, ErrValueLogSize
	}
	manifestFile, manifest, err := openOrCreateManifestFile(opt.Dir, opt.ReadOnly)
	if err != nil {
		return nil, err
	}
	defer func() {
		if manifestFile != nil {
			_ = manifestFile.close()
		}
	}()

	orc := &oracle{
		isManaged:  opt.ManagedTxns,
		nextCommit: 1,
		commits:    make(map[uint64]uint64),
	}

	var blkCache, idxCache *cache.Cache
	if opt.MaxBlockCacheSize != 0 {
		var err error
		blkCache, err = cache.NewCache(&cache.Config{
			// The expected keys is MaxCacheSize / BlockSize, then x10 as documentation suggests.
			NumCounters: opt.MaxBlockCacheSize / int64(opt.TableBuilderOptions.BlockSize) * 10,
			MaxCost:     opt.MaxBlockCacheSize,
			BufferItems: 64,
			OnEvict:     sstable.OnEvict,
		})
		if err != nil {
			return nil, errors.Wrap(err, "failed to create block cache")
		}

		indexSizeHint := float64(opt.TableBuilderOptions.MaxTableSize) / 6.0
		idxCache, err = cache.NewCache(&cache.Config{
			NumCounters: int64(float64(opt.MaxIndexCacheSize) / indexSizeHint * 10),
			MaxCost:     opt.MaxIndexCacheSize,
			BufferItems: 64,
		})
		if err != nil {
			return nil, errors.Wrap(err, "failed to create index cache")
		}
	}
	db = &DB{
		flushChan:     make(chan *flushTask, opt.NumMemtables),
		writeCh:       make(chan *request, kvWriteChCapacity),
		memTableCh:    make(chan *memtable.Table, 1),
		ingestCh:      make(chan *ingestTask),
		opt:           opt,
		manifest:      manifestFile,
		dirLockGuard:  dirLockGuard,
		valueDirGuard: valueDirLockGuard,
		orc:           orc,
		metrics:       y.NewMetricSet(opt.Dir),
		blockCache:    blkCache,
		indexCache:    idxCache,
		volatileMode:  opt.VolatileMode,
	}
	db.vlog.metrics = db.metrics

	rateLimit := opt.TableBuilderOptions.BytesPerSecond
	if rateLimit > 0 {
		db.limiter = rate.NewLimiter(rate.Limit(rateLimit), rateLimit)
	}

	// Calculate initial size.
	db.calculateSize()
	db.closers.updateSize = y.NewCloser(1)
	go db.updateSize(db.closers.updateSize)

	db.closers.resourceManager = y.NewCloser(0)
	db.resourceMgr = epoch.NewResourceManager(db.closers.resourceManager, &db.safeTsTracker)

	// newLevelsController potentially loads files in directory.
	if db.lc, err = newLevelsController(db, &manifest, db.resourceMgr, opt.TableBuilderOptions); err != nil {
		return nil, err
	}

	db.closers.memtable = y.NewCloser(1)
	go func() {
		lc := db.closers.memtable
		for {
			select {
			case db.memTableCh <- memtable.New(arenaSize(db.opt), db.lc.reserveFileID()):
			case <-lc.HasBeenClosed():
				lc.Done()
				return
			}
		}
	}()
	db.mtbls.Store(newMemTables(<-db.memTableCh, &memTables{}))

	if err = db.blobManger.Open(db, opt); err != nil {
		return nil, err
	}

	if !opt.ReadOnly {
		db.closers.compactors = y.NewCloser(1)
		db.lc.startCompact(db.closers.compactors)

		db.closers.memtable.AddRunning(1)
		go db.runFlushMemTable(db.closers.memtable) // Need levels controller to be up.
	}

	if err = db.vlog.Open(db, opt); err != nil {
		return nil, err
	}

	var logOff logOffset
	head := manifest.Head
	if head != nil {
		db.orc.curRead = head.Version
		logOff.fid = head.LogID
		logOff.offset = head.LogOffset
	}

	// lastUsedCasCounter will either be the value stored in !badger!head, or some subsequently
	// written value log entry that we replay.  (Subsequent value log entries might be _less_
	// than lastUsedCasCounter, if there was value log gc so we have to max() values while
	// replaying.)
	// out.lastUsedCasCounter = item.casCounter
	// TODO: Figure this out. This would update the read timestamp, and set nextCommitTs.

	replayCloser := startWriteWorker(db)

	if err = db.vlog.Replay(logOff, replayFunction(db)); err != nil {
		return db, err
	}

	replayCloser.SignalAndWait() // Wait for replay to be applied first.
	// Now that we have the curRead, we can update the nextCommit.
	db.orc.Lock()
	db.orc.nextCommit = db.orc.curRead + 1
	db.orc.Unlock()

	db.writeCh = make(chan *request, kvWriteChCapacity)
	db.closers.writes = startWriteWorker(db)

	valueDirLockGuard = nil
	dirLockGuard = nil
	manifestFile = nil
	return db, nil
}

// DeleteFilesInRange delete files in [start, end).
// If some file contains keys outside the range, they will not be deleted.
// This function is designed to reclaim space quickly.
// If you want to ensure no future transaction can read keys in range,
// considering iterate and delete the remained keys, or using compaction filter to cleanup them asynchronously.
func (db *DB) DeleteFilesInRange(start, end []byte) {
	var (
		changes   []*protos.ManifestChange
		pruneTbls []table.Table
		startKey  = y.KeyWithTs(start, math.MaxUint64)
		endKey    = y.KeyWithTs(end, 0)
		guard     = db.resourceMgr.Acquire()
	)

	for level, lc := range db.lc.levels {
		lc.Lock()
		left, right := 0, len(lc.tables)
		if lc.level > 0 {
			left, right = getTablesInRange(lc.tables, startKey, endKey)
		}
		if left >= right {
			lc.Unlock()
			continue
		}

		newTables := lc.tables[:left]
		for _, tbl := range lc.tables[left:right] {
			if !isRangeCoversTable(startKey, endKey, tbl) || tbl.IsCompacting() {
				newTables = append(newTables, tbl)
				continue
			}
			pruneTbls = append(pruneTbls, tbl)
			changes = append(changes, newDeleteChange(tbl.ID()))
		}
		newTables = append(newTables, lc.tables[right:]...)
		for i := len(newTables); i < len(lc.tables); i++ {
			lc.tables[i] = nil
		}
		assertTablesOrder(level, newTables, nil)
		lc.tables = newTables
		lc.Unlock()
	}

	db.manifest.addChanges(changes, nil)
	var discardStats DiscardStats
	deletes := make([]epoch.Resource, len(pruneTbls))
	for i, tbl := range pruneTbls {
		it := tbl.NewIterator(false)
		// TODO: use rate limiter to avoid burst IO.
		for it.Rewind(); it.Valid(); y.NextAllVersion(it) {
			discardStats.collect(it.Value())
		}
		deletes[i] = tbl
		it.Close()
	}
	if len(discardStats.ptrs) > 0 {
		db.blobManger.discardCh <- &discardStats
	}
	guard.Delete(deletes)
	guard.Done()
}

func isRangeCoversTable(start, end y.Key, t table.Table) bool {
	left := start.Compare(t.Smallest()) <= 0
	right := t.Biggest().Compare(end) < 0
	return left && right
}

// NewExternalTableBuilder returns a new sst builder.
func (db *DB) NewExternalTableBuilder(f *os.File, compression options.CompressionType, limiter *rate.Limiter) *sstable.Builder {
	return sstable.NewExternalTableBuilder(f, limiter, db.opt.TableBuilderOptions, compression)
}

// ErrExternalTableOverlap returned by IngestExternalFiles when files overlaps.
var ErrExternalTableOverlap = errors.New("keys of external tables has overlap")

type ExternalTableSpec struct {
	Filename string
}

// IngestExternalFiles ingest external constructed tables into DB.
// Note: insure there is no concurrent write overlap with tables to be ingested.
func (db *DB) IngestExternalFiles(files []ExternalTableSpec) (int, error) {
	tbls, err := db.prepareExternalFiles(files)
	if err != nil {
		return 0, err
	}

	if err := db.checkExternalTables(tbls); err != nil {
		return 0, err
	}

	task := &ingestTask{tbls: tbls}
	task.Add(1)
	db.ingestCh <- task
	task.Wait()
	return task.cnt, task.err
}

func (db *DB) prepareExternalFiles(specs []ExternalTableSpec) ([]table.Table, error) {
	tbls := make([]table.Table, len(specs))
	for i, spec := range specs {
		id := db.lc.reserveFileID()
		filename := sstable.NewFilename(id, db.opt.Dir)

		err := os.Link(spec.Filename, filename)
		if err != nil {
			return nil, err
		}

		err = os.Link(sstable.IndexFilename(spec.Filename), sstable.IndexFilename(filename))
		if err != nil {
			return nil, err
		}

		tbl, err := sstable.OpenTable(filename, db.blockCache, db.indexCache)
		if err != nil {
			return nil, err
		}

		tbls[i] = tbl
	}

	sort.Slice(tbls, func(i, j int) bool {
		return tbls[i].Smallest().Compare(tbls[j].Smallest()) < 0
	})

	return tbls, syncDir(db.lc.kv.opt.Dir)
}

func (db *DB) checkExternalTables(tbls []table.Table) error {
	keys := make([][]byte, 0, len(tbls)*2)
	for _, t := range tbls {
		keys = append(keys, t.Smallest().UserKey, t.Biggest().UserKey)
	}
	ok := sort.SliceIsSorted(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})
	if !ok {
		return ErrExternalTableOverlap
	}

	for i := 1; i < len(keys)-1; i += 2 {
		if bytes.Compare(keys[i], keys[i+1]) == 0 {
			return ErrExternalTableOverlap
		}
	}

	return nil
}

// CacheMetrics returns the metrics for the underlying cache.
func (db *DB) CacheMetrics() *cache.Metrics {
	// Do not enable ristretto metrics in badger until issue
	// https://github.com/dgraph-io/ristretto/issues/92 is resolved.
	// return db.blockCache.Metrics()
	return nil
}

// Close closes a DB. It's crucial to call it to ensure all the pending updates
// make their way to disk. Calling DB.Close() multiple times is not safe and would
// cause panic.
func (db *DB) Close() (err error) {
	log.Info("Closing database")

	// Stop writes next.
	db.closers.writes.SignalAndWait()

	// Now close the value log.
	if vlogErr := db.vlog.Close(); err == nil {
		err = errors.Wrap(vlogErr, "DB.Close")
	}

	// Make sure that block writer is done pushing stuff into memtable!
	// Otherwise, you will have a race condition: we are trying to flush memtables
	// and remove them completely, while the block / memtable writer is still
	// trying to push stuff into the memtable. This will also resolve the value
	// offset problem: as we push into memtable, we update value offsets there.
	mTbls := db.mtbls.Load().(*memTables)
	if !mTbls.getMutable().Empty() && !db.volatileMode {
		log.Info("Flushing memtable")
		db.mtbls.Store(newMemTables(nil, mTbls))
		db.flushChan <- newFlushTask(mTbls.getMutable(), db.logOff)
	}
	db.flushChan <- newFlushTask(nil, logOffset{}) // Tell flusher to quit.

	if db.closers.memtable != nil {
		db.closers.memtable.SignalAndWait()
		log.Info("Memtable flushed")
	}
	if db.closers.compactors != nil {
		db.closers.compactors.SignalAndWait()
		log.Info("Compaction finished")
	}
	if db.opt.CompactL0WhenClose && !db.volatileMode {
		// Force Compact L0
		// We don't need to care about cstatus since no parallel compaction is running.
		cd := &CompactDef{}
		guard := db.resourceMgr.Acquire()
		defer guard.Done()
		if cd.fillTablesL0(&db.lc.cstatus, db.lc.levels[0], db.lc.levels[1]) {
			if err := db.lc.runCompactDef(cd, guard); err != nil {
				log.Info("LOG Compact FAILED", zap.Stringer("compact def", cd), zap.Error(err))
			}
		} else {
			log.Info("fillTables failed for level zero. No compaction required")
		}
	}

	if db.closers.blobManager != nil {
		db.closers.blobManager.SignalAndWait()
		log.Info("BlobManager finished")
	}
	if db.closers.resourceManager != nil {
		db.closers.resourceManager.SignalAndWait()
		log.Info("ResourceManager finished")
	}

	if lcErr := db.lc.close(); err == nil {
		err = errors.Wrap(lcErr, "DB.Close")
	}
	log.Info("Waiting for closer")
	db.closers.updateSize.SignalAndWait()
	if db.blockCache != nil {
		db.blockCache.Close()
	}

	if db.indexCache != nil {
		db.indexCache.Close()
	}

	if db.dirLockGuard != nil {
		if guardErr := db.dirLockGuard.release(); err == nil {
			err = errors.Wrap(guardErr, "DB.Close")
		}
	}
	if db.valueDirGuard != nil {
		if guardErr := db.valueDirGuard.release(); err == nil {
			err = errors.Wrap(guardErr, "DB.Close")
		}
	}
	if manifestErr := db.manifest.close(); err == nil {
		err = errors.Wrap(manifestErr, "DB.Close")
	}

	// Fsync directories to ensure that lock file, and any other removed files whose directory
	// we haven't specifically fsynced, are guaranteed to have their directory entry removal
	// persisted to disk.
	if syncErr := syncDir(db.opt.Dir); err == nil {
		err = errors.Wrap(syncErr, "DB.Close")
	}
	if syncErr := syncDir(db.opt.ValueDir); err == nil {
		err = errors.Wrap(syncErr, "DB.Close")
	}

	return err
}

const (
	lockFile = "LOCK"
)

// When you create or delete a file, you have to ensure the directory entry for the file is synced
// in order to guarantee the file is visible (if the system crashes).  (See the man page for fsync,
// or see https://github.com/coreos/etcd/issues/6368 for an example.)
func syncDir(dir string) error {
	f, err := openDir(dir)
	if err != nil {
		return errors.Wrapf(err, "While opening directory: %s.", dir)
	}
	err = f.Sync()
	closeErr := f.Close()
	if err != nil {
		return errors.Wrapf(err, "While syncing directory: %s.", dir)
	}
	return errors.Wrapf(closeErr, "While closing directory: %s.", dir)
}

// getMemtables returns the current memtables.
func (db *DB) getMemTables() []*memtable.Table {
	tbls := db.mtbls.Load().(*memTables)
	l := atomic.LoadUint32(&tbls.length)
	return tbls.tables[:l]
}

// get returns the value in memtable or disk for given key.
// Note that value will include meta byte.
//
// IMPORTANT: We should never write an entry with an older timestamp for the same key, We need to
// maintain this invariant to search for the latest value of a key, or else we need to search in all
// tables and find the max version among them.  To maintain this invariant, we also need to ensure
// that all versions of a key are always present in the same table from level 1, because compaction
// can push any table down.
func (db *DB) get(key y.Key) y.ValueStruct {
	tables := db.getMemTables() // Lock should be released.

	db.metrics.NumGets.Inc()
	for _, table := range tables {
		db.metrics.NumMemtableGets.Inc()
		vs, err := table.Get(key, 0)
		if err != nil {
			log.Error("search table meets error", zap.Error(err))
		}
		if vs.Valid() {
			return vs
		}
	}
	keyHash := farm.Fingerprint64(key.UserKey)
	return db.lc.get(key, keyHash)
}

func (db *DB) multiGet(pairs []keyValuePair) {
	tables := db.getMemTables() // Lock should be released.

	var foundCount, mtGets int
	for _, table := range tables {
		for j := range pairs {
			pair := &pairs[j]
			if pair.found {
				continue
			}
			for {
				val, err := table.Get(pair.key, 0)
				if err != nil {
					log.Error("search table meets error", zap.Error(err))
				}
				if val.Valid() {
					pair.val = val
					pair.found = true
					foundCount++
				}
				mtGets++
				break
			}
		}
	}
	db.metrics.NumMemtableGets.Add(float64(mtGets))
	db.metrics.NumGets.Add(float64(len(pairs)))

	if foundCount == len(pairs) {
		return
	}
	db.lc.multiGet(pairs)
}

func (db *DB) updateOffset(off logOffset) {
	y.Assert(!off.Less(db.logOff))
	// We don't need to protect it by a lock because the value is never accessed
	// by more than one goroutine at the same time.
	db.logOff = off
}

var requestPool = sync.Pool{
	New: func() interface{} {
		return new(request)
	},
}

func (db *DB) sendToWriteCh(entries []*Entry) (*request, error) {
	var count, size int64
	for _, e := range entries {
		size += int64(e.estimateSize())
		count++
	}

	// We can only service one request because we need each txn to be stored in a contigous section.
	// Txns should not interleave among other txns or rewrites.
	req := requestPool.Get().(*request)
	req.Entries = entries
	req.Wg = sync.WaitGroup{}
	req.Wg.Add(1)
	db.writeCh <- req // Handled in writeWorker.
	db.metrics.NumPuts.Add(float64(len(entries)))

	return req, nil
}

// batchSet applies a list of badger.Entry. If a request level error occurs it
// will be returned.
//   Check(kv.BatchSet(entries))
func (db *DB) batchSet(entries []*Entry) error {
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Key.Compare(entries[j].Key) < 0
	})
	req, err := db.sendToWriteCh(entries)
	if err != nil {
		return err
	}

	return req.Wait()
}

// batchSetAsync is the asynchronous version of batchSet. It accepts a callback
// function which is called when all the sets are complete. If a request level
// error occurs, it will be passed back via the callback.
//   err := kv.BatchSetAsync(entries, func(err error)) {
//      Check(err)
//   }
func (db *DB) batchSetAsync(entries []*Entry, f func(error)) error {
	req, err := db.sendToWriteCh(entries)
	if err != nil {
		return err
	}
	go func() {
		err := req.Wait()
		// Write is complete. Let's call the callback function now.
		f(err)
	}()
	return nil
}

// ensureRoomForWrite is always called serially.
func (db *DB) ensureRoomForWrite(mt *memtable.Table, minSize int64) int64 {
	free := db.opt.MaxMemTableSize - mt.Size()
	if free >= minSize {
		return free
	}
	_ = db.flushMemTable()
	return db.opt.MaxMemTableSize
}

func (db *DB) flushMemTable() *sync.WaitGroup {
	mTbls := db.mtbls.Load().(*memTables)
	newTbls := newMemTables(<-db.memTableCh, mTbls)
	db.mtbls.Store(newTbls)
	ft := newFlushTask(mTbls.getMutable(), db.logOff)
	db.flushChan <- ft
	log.Info("flushing memtable", zap.Int64("memtable size", mTbls.getMutable().Size()), zap.Int("size of flushChan", len(db.flushChan)))

	// New memtable is empty. We certainly have room.
	return &ft.wg
}

func arenaSize(opt Options) int64 {
	return opt.MaxMemTableSize + opt.maxBatchCount*int64(memtable.MaxNodeSize)
}

// WriteLevel0Table flushes memtable. It drops deleteValues.
func (db *DB) writeLevel0Table(s *memtable.Table, f *os.File) error {
	iter := s.NewIterator(false)
	defer iter.Close()
	var (
		bb                   *blobFileBuilder
		numWrite, bytesWrite int
		err                  error
	)
	b := sstable.NewTableBuilder(f, db.limiter, 0, db.opt.TableBuilderOptions)
	defer b.Close()

	for iter.Rewind(); iter.Valid(); y.NextAllVersion(iter) {
		key := iter.Key()
		value := iter.Value()
		if db.opt.ValueThreshold > 0 && len(value.Value) > db.opt.ValueThreshold {
			if bb == nil {
				if bb, err = db.newBlobFileBuilder(); err != nil {
					return y.Wrap(err)
				}
			}

			bp, err := bb.append(value.Value)
			if err != nil {
				return err
			}
			value.Meta |= bitValuePointer
			value.Value = bp
		}
		if err = b.Add(key, value); err != nil {
			return err
		}
		numWrite++
		bytesWrite += key.Len() + int(value.EncodedSize())
	}
	stats := &y.CompactionStats{
		KeysWrite:  numWrite,
		BytesWrite: bytesWrite,
	}
	db.lc.levels[0].metrics.UpdateCompactionStats(stats)

	if _, err = b.Finish(); err != nil {
		return y.Wrap(err)
	}
	if bb != nil {
		bf, err1 := bb.finish()
		if err1 != nil {
			return err1
		}
		log.Info("build L0 blob", zap.Uint32("id", bf.fid), zap.Uint32("size", bf.fileSize))
		err1 = db.blobManger.addFile(bf)
		if err1 != nil {
			return err1
		}
	}
	return nil
}

func (db *DB) newBlobFileBuilder() (*blobFileBuilder, error) {
	return newBlobFileBuilder(db.blobManger.allocFileID(), db.opt.Dir, db.opt.TableBuilderOptions.WriteBufferSize)
}

type flushTask struct {
	mt  *memtable.Table
	off logOffset
	wg  sync.WaitGroup
}

func newFlushTask(mt *memtable.Table, off logOffset) *flushTask {
	ft := &flushTask{mt: mt, off: off}
	ft.wg.Add(1)
	return ft
}

// TODO: Ensure that this function doesn't return, or is handled by another wrapper function.
// Otherwise, we would have no goroutine which can flush memtables.
func (db *DB) runFlushMemTable(c *y.Closer) error {
	defer c.Done()

	for ft := range db.flushChan {
		if ft.mt == nil {
			return nil
		}
		guard := db.resourceMgr.Acquire()
		var headInfo *protos.HeadInfo
		if !ft.mt.Empty() {
			headInfo = &protos.HeadInfo{
				// Pick the max commit ts, so in case of crash, our read ts would be higher than all the
				// commits.
				Version:   db.orc.commitTs(),
				LogID:     ft.off.fid,
				LogOffset: ft.off.offset,
			}
			// Store badger head even if vptr is zero, need it for readTs
			log.Info("flush memtable storing offset", zap.Uint32("fid", ft.off.fid), zap.Uint32("offset", ft.off.offset))
		}

		fileID := ft.mt.ID()
		filename := sstable.NewFilename(fileID, db.opt.Dir)
		fd, err := directio.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			log.Error("error while writing to level 0", zap.Error(err))
			return y.Wrap(err)
		}

		// Don't block just to sync the directory entry.
		dirSyncCh := make(chan error)
		go func() { dirSyncCh <- syncDir(db.opt.Dir) }()

		err = db.writeLevel0Table(ft.mt, fd)
		dirSyncErr := <-dirSyncCh
		if err != nil {
			log.Error("error while writing to level 0", zap.Error(err))
			return err
		}
		if dirSyncErr != nil {
			log.Error("error while syncing level directory", zap.Error(dirSyncErr))
			return err
		}
		atomic.StoreUint32(&db.syncedFid, ft.off.fid)
		fd.Close()
		tbl, err := sstable.OpenTable(filename, db.blockCache, db.indexCache)
		if err != nil {
			log.Info("error while opening table", zap.Error(err))
			return err
		}
		err = db.lc.addLevel0Table(tbl, headInfo)
		if err != nil {
			log.Error("error while syncing level directory", zap.Error(err))
			return err
		}
		mTbls := db.mtbls.Load().(*memTables)
		// Update the length of mTbls.
		for i, tbl := range mTbls.tables {
			if tbl == ft.mt {
				atomic.StoreUint32(&mTbls.length, uint32(i))
				break
			}
		}
		guard.Delete([]epoch.Resource{ft.mt})
		guard.Done()
		ft.wg.Done()
	}
	return nil
}

func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

// This function does a filewalk, calculates the size of vlog and sst files and stores it in
// y.LSMSize and y.VlogSize.
func (db *DB) calculateSize() {
	totalSize := func(dir string) (int64, int64) {
		var lsmSize, vlogSize int64
		err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			ext := filepath.Ext(path)
			if ext == ".sst" {
				lsmSize += info.Size()
			} else if ext == ".vlog" {
				vlogSize += info.Size()
			}
			return nil
		})
		if err != nil {
			log.Info("error while calculating total size of directory", zap.String("path", dir))
		}
		return lsmSize, vlogSize
	}

	lsmSize, vlogSize := totalSize(db.opt.Dir)
	// If valueDir is different from dir, we'd have to do another walk.
	if db.opt.ValueDir != db.opt.Dir {
		_, vlogSize = totalSize(db.opt.ValueDir)
	}
	atomic.StoreInt64(&db.lsmSize, lsmSize)
	atomic.StoreInt64(&db.vlogSize, vlogSize)
	db.metrics.LSMSize.Set(float64(lsmSize))
	db.metrics.VlogSize.Set(float64(vlogSize))
}

func (db *DB) updateSize(c *y.Closer) {
	defer c.Done()

	metricsTicker := time.NewTicker(time.Minute)
	defer metricsTicker.Stop()

	for {
		select {
		case <-metricsTicker.C:
			db.calculateSize()
		case <-c.HasBeenClosed():
			return
		}
	}
}

// Size returns the size of lsm and value log files in bytes. It can be used to decide how often to
// call RunValueLogGC.
func (db *DB) Size() (lsm int64, vlog int64) {
	return atomic.LoadInt64(&db.lsmSize), atomic.LoadInt64(&db.vlogSize)
}

func (db *DB) Tables() []TableInfo {
	return db.lc.getTableInfo()
}

func (db *DB) GetVLogOffset() uint64 {
	return db.vlog.getMaxPtr()
}

// IterateVLog iterates VLog for external replay, this function should be called only when there is no
// concurrent write operation on the DB.
func (db *DB) IterateVLog(offset uint64, fn func(e Entry)) error {
	startFid := uint32(offset >> 32)
	vOffset := uint32(offset)
	for fid := startFid; fid <= db.vlog.maxFid(); fid++ {
		lf, err := db.vlog.getFile(fid)
		if err != nil {
			return err
		}
		if fid != startFid {
			vOffset = 0
		}
		endOffset, err := db.vlog.iterate(lf, vOffset, func(e Entry) error {
			if e.meta&bitTxn > 0 {
				fn(e)
			}
			return nil
		})
		if err != nil {
			return err
		}
		if fid == db.vlog.maxFid() {
			_, err = lf.fd.Seek(int64(endOffset), io.SeekStart)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (db *DB) getCompactSafeTs() uint64 {
	return atomic.LoadUint64(&db.safeTsTracker.safeTs)
}

// UpdateSafeTs is used for Managed DB, during compaction old version smaller than the safe ts will be discarded.
// If this is not called, all old versions are kept.
func (db *DB) UpdateSafeTs(ts uint64) {
	y.Assert(db.IsManaged())
	for {
		old := db.getCompactSafeTs()
		if old < ts {
			if !atomic.CompareAndSwapUint64(&db.safeTsTracker.safeTs, old, ts) {
				continue
			}
		}
		break
	}
}

func (db *DB) IsManaged() bool {
	return db.opt.ManagedTxns
}

type safeTsTracker struct {
	safeTs uint64

	maxInactive uint64
	minActive   uint64
}

func (t *safeTsTracker) Begin() {
	// t.maxInactive = 0
	t.minActive = math.MaxUint64
}

func (t *safeTsTracker) Inspect(payload interface{}, isActive bool) {
	ts, ok := payload.(uint64)
	if !ok {
		return
	}

	if isActive {
		if ts < t.minActive {
			t.minActive = ts
		}
	} else {
		if ts > t.maxInactive {
			t.maxInactive = ts
		}
	}
}

func (t *safeTsTracker) End() {
	var safe uint64
	if t.minActive == math.MaxUint64 {
		safe = t.maxInactive
	} else {
		safe = t.minActive - 1
	}

	if safe > atomic.LoadUint64(&t.safeTs) {
		atomic.StoreUint64(&t.safeTs, safe)
	}
}
