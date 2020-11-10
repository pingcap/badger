package badger

import (
	"bytes"
	"fmt"
	"github.com/pingcap/badger/cache"
	"github.com/pingcap/badger/epoch"
	"github.com/pingcap/badger/protos"
	"github.com/pingcap/badger/table/memtable"
	"github.com/pingcap/badger/table/sstable"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"sort"
	"sync/atomic"
	"unsafe"
)

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
	memTbls.tables = append(memTbls.tables, memtable.NewCFTable(opt.MaxMemTableSize, len(opt.CFs), db.idAlloc.AllocID()))
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
			shard: shard,
			tbl:   writableMemTbl,
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

func (sdb *ShardingDB) printStructure() {
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
			for l := 1; l <= shardMaxLevel; l++ {
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
}

func (s *Snapshot) Get(cf int, key y.Key) y.ValueStruct {
	if !s.cfs[cf].Managed && key.Version == 0 {
		key.Version = s.readTS
	}
	shard := getShard(s.shards, key.UserKey)
	return shard.Get(cf, key)
}

func (s *Snapshot) Discard() {
	s.guard.Done()
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
	err = sdb.manifest.writeChangeSet(change)
	if err != nil {
		return err
	}
	for _, shard := range shards {
		y.Assert(!shard.isSplitting())
		atomic.StorePointer(shard.memTbls, unsafe.Pointer(&shardingMemTables{}))
		atomic.StorePointer(shard.l0s, unsafe.Pointer(&shardL0Tables{}))
		shard.foreachLevel(func(cf int, level *levelHandler) (stop bool) {
			if len(level.tables) > 0 {
				shard.cfs[cf].casLevelHandler(level.level, level, newLevelHandler(sdb.opt.NumLevelZeroTablesStall, level.level, sdb.metrics))
			}
			return false
		})
	}
	guard.Delete(d.resources)
	return nil
}

type IngestTree struct {
	Start []byte
	End   []byte
	MaxTS uint64
	L0    []uint64
	CFs   []*IngestTreeCF
}

type IngestTreeCF struct {
	Levels []*IngestTreeCFLevel
}

type IngestTreeCFLevel struct {
	TableIDs []uint64
}

func (sdb *ShardingDB) Ingest(ingestTree *IngestTree) error {
	start, end := ingestTree.Start, ingestTree.End
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
	y.Assert(len(shards) == 1)
	shard := shards[0]
	shard.lock.Lock()
	defer shard.lock.Unlock()
	y.Assert(bytes.Equal(shard.Start, start))
	y.Assert(bytes.Equal(shard.End, end))
	y.Assert(!shard.isSplitting())
	change, d := sdb.createIngestTreeManifestChange(ingestTree, shard)
	l0s, err := sdb.openIngestTreeL0Tables(ingestTree)
	if err != nil {
		return err
	}
	levelHandlers, err := sdb.createIngestTreeLevelHandlers(ingestTree)
	if err != nil {
		return err
	}
	if err = sdb.manifest.writeChangeSet(change); err != nil {
		return err
	}
	atomic.StorePointer(shard.memTbls, unsafe.Pointer(&shardingMemTables{}))
	atomic.StorePointer(shard.l0s, unsafe.Pointer(l0s))
	shard.foreachLevel(func(cf int, level *levelHandler) (stop bool) {
		scf := shard.cfs[cf]
		y.Assert(scf.casLevelHandler(level.level, level, levelHandlers[cf][level.level-1]))
		return false
	})
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
	for _, id := range ingestTree.L0 {
		change.Changes = append(change.Changes, newManifestChange(id, shard.ID, -1, 0, protos.ManifestChange_CREATE))
	}
	for cf, ingestCF := range ingestTree.CFs {
		for l, level := range ingestCF.Levels {
			for _, id := range level.TableIDs {
				change.Changes = append(change.Changes, newManifestChange(id, shard.ID, cf, l+1, protos.ManifestChange_CREATE))
			}
		}
	}
	return
}

func (sdb *ShardingDB) openIngestTreeL0Tables(ingestTree *IngestTree) (*shardL0Tables, error) {
	l0s := &shardL0Tables{}
	for _, l0ID := range ingestTree.L0 {
		l0Tbl, err := openShardL0Table(sstable.NewFilename(l0ID, sdb.opt.Dir), l0ID)
		if err != nil {
			return nil, err
		}
		l0s.tables = append(l0s.tables, l0Tbl)
	}
	sort.Slice(l0s.tables, func(i, j int) bool {
		return l0s.tables[i].fid < l0s.tables[j].fid
	})
	return l0s, nil
}

func (sdb *ShardingDB) createIngestTreeLevelHandlers(ingestTree *IngestTree) ([][]*levelHandler, error) {
	newHandlers := make([][]*levelHandler, len(ingestTree.CFs))
	for cf, ingestCF := range ingestTree.CFs {
		for level := 1; level <= shardMaxLevel; level++ {
			ingestLevel := ingestCF.Levels[level-1]
			newHandler := newLevelHandler(sdb.opt.NumLevelZeroTablesStall, level, sdb.metrics)
			for _, id := range ingestLevel.TableIDs {
				filename := sstable.NewFilename(id, sdb.opt.Dir)
				file, err := sstable.NewMMapFile(filename)
				if err != nil {
					return nil, err
				}
				tbl, err := sstable.OpenTable(filename, file)
				if err != nil {
					return nil, err
				}
				newHandler.tables = append(newHandler.tables, tbl)
				newHandler.totalSize += tbl.Size()
			}
			sort.Slice(newHandler.tables, func(i, j int) bool {
				return newHandler.tables[i].Smallest().Compare(newHandler.tables[j].Smallest()) < 0
			})
			newHandlers[cf] = append(newHandlers[cf], newHandler)
		}
	}
	return newHandlers, nil
}

func (sdb *ShardingDB) GetShardTree(key []byte) *IngestTree {
	guard := sdb.resourceMgr.Acquire()
	defer guard.Done()
	tree := sdb.loadShardTree()
	shard := tree.get(key)
	shard.lock.Lock()
	defer shard.lock.Unlock()
	ingestTree := &IngestTree{
		Start: shard.Start,
		End:   shard.End,
		MaxTS: sdb.orc.commitTs(),
		CFs:   make([]*IngestTreeCF, sdb.numCFs),
	}
	for _, tbl := range shard.loadL0Tables().tables {
		ingestTree.L0 = append(ingestTree.L0, tbl.fid)
	}
	shard.foreachLevel(func(cf int, level *levelHandler) (stop bool) {
		ingestLevel := &IngestTreeCFLevel{
			TableIDs: make([]uint64, 0, len(level.tables)),
		}
		for _, tbl := range level.tables {
			ingestLevel.TableIDs = append(ingestLevel.TableIDs, tbl.ID())
		}
		ingestCF := ingestTree.CFs[cf]
		if ingestCF == nil {
			ingestCF = new(IngestTreeCF)
			ingestTree.CFs[cf] = ingestCF
		}
		ingestCF.Levels = append(ingestCF.Levels, ingestLevel)
		return false
	})
	return ingestTree
}

func (sdb *ShardingDB) GetSplitSuggestion(splitSize int64) [][]byte {
	tree := sdb.loadShardTree()
	var keys [][]byte
	for _, shard := range tree.shards {
		log.S().Infof("shard size %d", shard.estimatedSize)
		if atomic.LoadInt64(&shard.estimatedSize) > splitSize {
			keys = append(keys, shard.getSplitKeys(splitSize)...)
		}
	}
	return keys
}
