package badger

import (
	"bytes"
	"fmt"
	"github.com/pingcap/badger/cache"
	"github.com/pingcap/badger/epoch"
	"github.com/pingcap/badger/options"
	"github.com/pingcap/badger/protos"
	"github.com/pingcap/badger/s3util"
	"github.com/pingcap/badger/table/memtable"
	"github.com/pingcap/badger/table/sstable"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"math"
	"sort"
	"sync/atomic"
	"unsafe"
)

var ShardingDBDefaultOpt = Options{
	DoNotCompact:            false,
	LevelOneSize:            16 << 20,
	MaxMemTableSize:         16 << 20,
	NumCompactors:           3,
	NumLevelZeroTables:      5,
	NumLevelZeroTablesStall: 10,
	NumMemtables:            16,
	SyncWrites:              false,
	ValueThreshold:          0,
	ValueLogFileSize:        100 << 20,
	ValueLogMaxNumFiles:     2,
	TableBuilderOptions: options.TableBuilderOptions{
		LevelSizeMultiplier: 10,
		MaxTableSize:        8 << 20,
		SuRFStartLevel:      8,
		HashUtilRatio:       0.75,
		WriteBufferSize:     2 * 1024 * 1024,
		BytesPerSecond:      -1,
		BlockSize:           64 * 1024,
		LogicalBloomFPR:     0.01,
		MaxLevels:           5,
		SuRFOptions: options.SuRFOptions{
			HashSuffixLen:  8,
			RealSuffixLen:  8,
			BitsPerKeyHint: 40,
		},
	},
	CFs: []CFConfig{{Managed: false}},
}

type ShardingDB struct {
	opt           Options
	numCFs        int
	orc           *oracle
	dirLock       *directoryLockGuard
	shardTree     unsafe.Pointer
	blkCache      *cache.Cache
	idxCache      *cache.Cache
	resourceMgr   *epoch.ResourceManager
	safeTsTracker safeTsTracker
	closers       closers
	writeCh       chan engineTask
	flushCh       chan *shardFlushTask
	metrics       *y.MetricsSet
	manifest      *ShardingManifest
	mangedSafeTS  uint64
	idAlloc       IDAllocator
	s3c           *s3util.S3Client
}

func OpenShardingDB(opt Options) (db *ShardingDB, err error) {
	log.Info("Open sharding DB")
	err = checkOptions(&opt)
	if err != nil {
		return nil, err
	}
	var dirLockGuard *directoryLockGuard
	dirLockGuard, err = acquireDirectoryLock(opt.Dir, lockFile, opt.ReadOnly)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = dirLockGuard.release()
		}
	}()
	manifest, err := OpenShardingManifest(opt.Dir)
	if err != nil {
		return nil, err
	}

	orc := &oracle{
		isManaged:  opt.ManagedTxns,
		curRead:    manifest.version,
		nextCommit: manifest.version + 1,
		commits:    make(map[uint64]uint64),
	}
	manifest.orc = orc
	manifest.metaListener = opt.MetaChangeListener

	blkCache, idxCache, err := createCache(opt)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create block cache")
	}
	metrics := y.NewMetricSet(opt.Dir)
	shardTree, err := newShardTree(manifest, opt, metrics)
	if err != nil {
		return nil, err
	}
	memTbls := &shardingMemTables{}
	db = &ShardingDB{
		opt:       opt,
		numCFs:    len(opt.CFs),
		orc:       orc,
		dirLock:   dirLockGuard,
		metrics:   metrics,
		shardTree: unsafe.Pointer(shardTree),
		blkCache:  blkCache,
		idxCache:  idxCache,
		flushCh:   make(chan *shardFlushTask, opt.NumMemtables),
		writeCh:   make(chan engineTask, kvWriteChCapacity),
		manifest:  manifest,
	}
	if opt.IDAllocator != nil {
		db.idAlloc = opt.IDAllocator
	} else {
		db.idAlloc = &localIDAllocator{latest: manifest.lastID}
	}
	if opt.S3Options.EndPoint != "" {
		db.s3c = s3util.NewS3Client(opt.S3Options)
	}
	memTbls.tables = append(memTbls.tables, memtable.NewCFTable(opt.MaxMemTableSize, len(opt.CFs)))
	db.closers.resourceManager = y.NewCloser(0)
	db.resourceMgr = epoch.NewResourceManager(db.closers.resourceManager, &db.safeTsTracker)
	db.closers.memtable = y.NewCloser(1)
	go db.runFlushMemTable(db.closers.memtable)
	db.closers.writes = y.NewCloser(1)
	go db.runWriteLoop(db.closers.writes)
	if !db.opt.DoNotCompact {
		db.closers.compactors = y.NewCloser(1)
		go db.runShardInternalCompactionLoop(db.closers.compactors)
	}
	return db, nil
}

type localIDAllocator struct {
	latest uint64
}

func (l *localIDAllocator) AllocID() uint64 {
	return atomic.AddUint64(&l.latest, 1)
}

func (sdb *ShardingDB) Close() error {
	log.S().Info("closing ShardingDB")
	sdb.closers.writes.SignalAndWait()
	log.S().Info("flush memTables before close")
	tree := sdb.loadShardTree()
	for _, shard := range tree.shards {
		writableMemTbl := shard.loadWritableMemTable()
		if writableMemTbl == nil || writableMemTbl.Empty() {
			continue
		}
		task := &shardFlushTask{
			shard:    shard,
			tbl:      writableMemTbl,
			commitTS: sdb.orc.readTs(),
		}
		sdb.flushCh <- task
	}
	close(sdb.flushCh)
	sdb.closers.memtable.SignalAndWait()
	if !sdb.opt.DoNotCompact {
		sdb.closers.compactors.SignalAndWait()
	}
	sdb.closers.resourceManager.SignalAndWait()
	return sdb.dirLock.release()
}

func (sdb *ShardingDB) GetSafeTS() (uint64, uint64, uint64) {
	return sdb.orc.readTs(), sdb.orc.commitTs(), atomic.LoadUint64(&sdb.safeTsTracker.safeTs)
}

func (sdb *ShardingDB) PrintStructure() {
	tree := sdb.loadShardTree()
	for _, shard := range tree.shards {
		if shard.isSplitting() {
			var l0IDs []uint64
			for i := 0; i < len(shard.splittingL0s); i++ {
				splittingL0s := shard.loadSplittingL0Tables(i)
				for _, tbl := range splittingL0s.tables {
					l0IDs = append(l0IDs, tbl.fid)
				}
			}
			log.S().Infof("shard %d splitting l0 tables %v", l0IDs)
		}
		l0s := shard.loadL0Tables()
		var l0IDs []uint64
		for _, tbl := range l0s.tables {
			l0IDs = append(l0IDs, tbl.fid)
		}
		log.S().Infof("shard %d l0 tables %v", shard.ID, l0IDs)
		for cf, scf := range shard.cfs {
			var tableIDs [][]uint64
			for l := 1; l <= ShardMaxLevel; l++ {
				tableIDs = append(tableIDs, getTblIDs(scf.getLevelHandler(l).tables))
			}
			log.S().Infof("shard %d cf %d tables %v", shard.ID, cf, tableIDs)
		}
	}
	for id, fi := range sdb.manifest.shards {
		cfs := make([][]uint64, sdb.numCFs)
		l0s := make([]uint64, 0, 10)
		for fid := range fi.files {
			cfLevel, ok := sdb.manifest.globalFiles[fid]
			if !ok {
				log.S().Errorf("shard %d fid %d not found in global", fi.ID, fid)
			}
			if cfLevel.cf == -1 {
				l0s = append(l0s, fid)
			} else {
				cfs[cfLevel.cf] = append(cfs[cfLevel.cf], fid)
			}
		}
		log.S().Infof("manifest shard %d l0 tables %v", id, l0s)
		for cf, cfIDs := range cfs {
			log.S().Infof("manifest shard %d cf %d tables %v", id, cf, cfIDs)
		}
	}
}

type WriteBatch struct {
	tree         *shardTree
	cfConfs      []CFConfig
	notify       chan error
	shardBatches map[uint64]*shardBatch
	currentBatch *shardBatch
}

type shardBatch struct {
	*Shard
	entries       [][]*memtable.Entry
	estimatedSize int
}

func (sdb *ShardingDB) NewWriteBatch() *WriteBatch {
	return &WriteBatch{
		tree:         sdb.loadShardTree(),
		shardBatches: map[uint64]*shardBatch{},
		cfConfs:      sdb.opt.CFs,
		notify:       make(chan error, 1),
	}
}

func (wb *WriteBatch) Put(cf byte, key []byte, val y.ValueStruct) error {
	if wb.cfConfs[cf].Managed {
		if val.Version == 0 {
			return fmt.Errorf("version is zero for managed CF")
		}
	} else {
		if val.Version != 0 {
			return fmt.Errorf("version is not zero for non-managed CF")
		}
	}
	wb.resetCurrentBatch(key)
	wb.currentBatch.entries[cf] = append(wb.currentBatch.entries[cf], &memtable.Entry{
		Key:   key,
		Value: val,
	})
	wb.currentBatch.estimatedSize += len(key) + int(val.EncodedSize()) + memtable.EstimateNodeSize
	return nil
}

func (wb *WriteBatch) resetCurrentBatch(key []byte) {
	if wb.currentBatch != nil && bytes.Compare(wb.currentBatch.Start, key) <= 0 && bytes.Compare(key, wb.currentBatch.End) < 0 {
		return
	}
	shard := wb.tree.get(key)
	var ok bool
	wb.currentBatch, ok = wb.shardBatches[shard.ID]
	if ok {
		return
	}
	wb.currentBatch = &shardBatch{Shard: shard, entries: make([][]*memtable.Entry, len(wb.cfConfs))}
	wb.shardBatches[shard.ID] = wb.currentBatch
}

func (wb *WriteBatch) Delete(cf byte, key []byte, version uint64) error {
	if wb.cfConfs[cf].Managed {
		if version == 0 {
			return fmt.Errorf("version is zero for managed CF")
		}
	} else {
		if version != 0 {
			return fmt.Errorf("version is not zero for non-managed CF")
		}
	}
	wb.resetCurrentBatch(key)
	wb.currentBatch.entries[cf] = append(wb.currentBatch.entries[cf], &memtable.Entry{
		Key:   key,
		Value: y.ValueStruct{Meta: bitDelete, Version: version},
	})
	wb.currentBatch.estimatedSize += len(key) + memtable.EstimateNodeSize
	return nil
}

func (sdb *ShardingDB) Write(wb *WriteBatch) error {
	sdb.writeCh <- engineTask{writeTask: wb}
	return <-wb.notify
}

func (sdb *ShardingDB) loadShardTree() *shardTree {
	return (*shardTree)(atomic.LoadPointer(&sdb.shardTree))
}

type Snapshot struct {
	guard    *epoch.Guard
	readTS   uint64
	shards   []*Shard
	cfs      []CFConfig
	startKey []byte
	endKey   []byte

	managedReadTS uint64

	buffer *memtable.CFTable
}

func (s *Snapshot) Get(cf int, key y.Key) (*Item, error) {
	if key.Version == 0 {
		key.Version = s.getDefaultVersion(cf)
	}
	var vs y.ValueStruct
	if s.buffer != nil {
		vs = s.buffer.Get(cf, key.UserKey, key.Version)
	}
	if !vs.Valid() {
		shard := getShard(s.shards, key.UserKey)
		vs = shard.Get(cf, key)
	}
	if !vs.Valid() {
		return nil, ErrKeyNotFound
	}
	if isDeleted(vs.Meta) {
		return nil, ErrKeyNotFound
	}
	item := new(Item)
	item.key.UserKey = key.UserKey
	item.key.Version = vs.Version
	item.meta = vs.Meta
	item.userMeta = vs.UserMeta
	item.vptr = vs.Value
	return item, nil
}

func (s *Snapshot) getDefaultVersion(cf int) uint64 {
	if s.cfs[cf].Managed {
		return math.MaxUint64
	}
	return s.readTS
}

func (s *Snapshot) MultiGet(cf int, keys [][]byte, version uint64) ([]*Item, error) {
	if version == 0 {
		version = s.getDefaultVersion(cf)
	}
	items := make([]*Item, len(keys))
	for i, key := range keys {
		item, err := s.Get(cf, y.KeyWithTs(key, version))
		if err != nil && err != ErrKeyNotFound {
			return nil, err
		}
		items[i] = item
	}
	return items, nil
}

func (s *Snapshot) Discard() {
	s.guard.Done()
}

func (s *Snapshot) SetManagedReadTS(ts uint64) {
	s.managedReadTS = ts
}

func (s *Snapshot) GetReadTS() uint64 {
	return s.readTS
}

func (s *Snapshot) SetBuffer(buf *memtable.CFTable) {
	s.buffer = buf
}

func (sdb *ShardingDB) NewSnapshot(startKey, endKey []byte) *Snapshot {
	if len(endKey) == 0 {
		endKey = globalShardEndKey
	}
	readTS := sdb.orc.readTs()
	guard := sdb.resourceMgr.AcquireWithPayload(readTS)
	return &Snapshot{
		guard:    guard,
		readTS:   readTS,
		shards:   sdb.loadShardTree().getShards(startKey, endKey),
		cfs:      sdb.opt.CFs,
		startKey: startKey,
		endKey:   endKey,
	}
}

// DeleteRange operation deletes the range between start and end, the existing Snapshot is still consistent.
func (sdb *ShardingDB) DeleteRange(start, end []byte) error {
	if len(end) == 0 {
		end = globalShardEndKey
	}
	err := sdb.Split([][]byte{start, end})
	if err != nil {
		return err
	}
	guard := sdb.resourceMgr.Acquire()
	defer guard.Done()
	tree := sdb.loadShardTree()
	shards := tree.getShards(start, end)
	y.Assert(bytes.Equal(shards[0].Start, start))
	y.Assert(bytes.Equal(shards[len(shards)-1].End, end))
	for _, shard := range shards {
		shard.lock.Lock()
	}
	defer func() {
		for _, shard := range shards {
			shard.lock.Unlock()
		}
	}()
	d := new(deletions)
	change := &protos.ManifestChangeSet{}
	for _, shard := range shards {
		l0s := shard.loadL0Tables()
		for _, tbl := range l0s.tables {
			d.Append(tbl)
			change.Changes = append(change.Changes, newManifestChange(tbl.fid, shard.ID, -1, 0, protos.ManifestChange_DELETE))
		}
		if shard.isSplitting() {
			for i := range shard.splittingL0s {
				sl0s := shard.loadSplittingL0Tables(i)
				for _, tbl := range sl0s.tables {
					d.Append(tbl)
					change.Changes = append(change.Changes, newManifestChange(tbl.fid, shard.ID, -1, 0, protos.ManifestChange_DELETE))
				}
			}
		}
		shard.foreachLevel(func(cf int, level *levelHandler) (stop bool) {
			for _, tbl := range level.tables {
				d.Append(tbl)
				change.Changes = append(change.Changes, newManifestChange(tbl.ID(), shard.ID, cf, level.level, protos.ManifestChange_DELETE))
			}
			return false
		})
	}
	// Don't need to notify meta change listener for manually triggered operation.
	err = sdb.manifest.writeChangeSet(nil, change, false)
	if err != nil {
		return err
	}
	newShards := make([]*Shard, 0, len(shards))
	for _, shard := range shards {
		newShards = append(newShards, newShard(shard.ID, shard.Start, shard.End, sdb.opt, sdb.metrics))
	}
	for {
		oldTree := sdb.loadShardTree()
		newTree := oldTree.replace(shards, newShards)
		if atomic.CompareAndSwapPointer(&sdb.shardTree, unsafe.Pointer(oldTree), unsafe.Pointer(newTree)) {
			break
		}
	}
	guard.Delete(d.resources)
	return nil
}

func (sdb *ShardingDB) createIngestTreeManifestChange(ingestTree *IngestTree, shard *Shard) (change *protos.ManifestChangeSet, d *deletions) {
	d = new(deletions)
	change = &protos.ManifestChangeSet{}
	l0s := shard.loadL0Tables()
	for _, tbl := range l0s.tables {
		d.Append(tbl)
		change.Changes = append(change.Changes, newManifestChange(tbl.fid, shard.ID, -1, 0, protos.ManifestChange_DELETE))
	}
	if shard.isSplitting() {
		for i := range shard.splittingL0s {
			sl0s := shard.loadSplittingL0Tables(i)
			for _, tbl := range sl0s.tables {
				d.Append(tbl)
				change.Changes = append(change.Changes, newManifestChange(tbl.fid, shard.ID, -1, 0, protos.ManifestChange_DELETE))
			}
		}
	}
	shard.foreachLevel(func(cf int, level *levelHandler) (stop bool) {
		for _, tbl := range level.tables {
			d.Append(tbl)
			change.Changes = append(change.Changes, newManifestChange(tbl.ID(), shard.ID, cf, level.level, protos.ManifestChange_DELETE))
		}
		return false
	})
	for _, fm := range ingestTree.Meta.AddedFiles {
		change.Changes = append(change.Changes, newManifestChange(fm.ID, shard.ID, int(fm.CF), int(fm.Level), protos.ManifestChange_CREATE))
	}
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

func (sdb *ShardingDB) GetShardTree(key []byte) *IngestTree {
	guard := sdb.resourceMgr.Acquire()
	defer guard.Done()
	tree := sdb.loadShardTree()
	shard := tree.get(key)
	shard.lock.Lock()
	defer shard.lock.Unlock()
	ingestTree := &IngestTree{
		MaxTS: sdb.orc.commitTs(),
		Meta: &protos.MetaChangeEvent{
			StartKey: shard.Start,
			EndKey:   shard.End,
		},
	}
	for _, tbl := range shard.loadL0Tables().tables {
		l0Meta := &protos.L0FileMeta{
			ID:       tbl.fid,
			CommitTS: tbl.commitTS,
		}
		for _, tbl := range tbl.cfs {
			l0Meta.MultiCFSmallest = append(l0Meta.MultiCFSmallest, tbl.Smallest().UserKey)
			l0Meta.MultiCFBiggest = append(l0Meta.MultiCFBiggest, tbl.Biggest().UserKey)
		}
		ingestTree.Meta.AddedL0Files = append(ingestTree.Meta.AddedL0Files, l0Meta)
	}
	shard.foreachLevel(func(cf int, level *levelHandler) (stop bool) {
		for _, tbl := range level.tables {
			ingestTree.Meta.AddedFiles = append(ingestTree.Meta.AddedFiles, &protos.FileMeta{
				ID:       tbl.ID(),
				CF:       int32(cf),
				Level:    uint32(level.level),
				Smallest: tbl.Smallest().UserKey,
				Biggest:  tbl.Biggest().UserKey,
			})
		}
		return false
	})
	return ingestTree
}

func (sdb *ShardingDB) GetSplitSuggestion(splitSize int64) [][]byte {
	tree := sdb.loadShardTree()
	var keys [][]byte
	for _, shard := range tree.shards {
		if atomic.LoadInt64(&shard.estimatedSize) > splitSize {
			log.S().Infof("shard(%x, %x) size %d", shard.Start, shard.End, shard.estimatedSize)
			keys = append(keys, shard.getSplitKeys(splitSize)...)
		}
	}
	return keys
}

func (sdb *ShardingDB) Size() int64 {
	tree := sdb.loadShardTree()
	var size int64
	for _, shard := range tree.shards {
		size += atomic.LoadInt64(&shard.estimatedSize)
	}
	return size
}

func (sdb *ShardingDB) NumCFs() int {
	return sdb.numCFs
}

func (sdb *ShardingDB) GetOpt() Options {
	return sdb.opt
}
