package badger

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/dgryski/go-farm"
	"github.com/pingcap/badger/cache"
	"github.com/pingcap/badger/epoch"
	"github.com/pingcap/badger/table/memtable"
	"github.com/pingcap/badger/table/sstable"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
)

type ShardingDB struct {
	opt           Options
	numCFs        int
	orc           *oracle
	dirLock       *directoryLockGuard
	memTbls       unsafe.Pointer
	l0Tbls        unsafe.Pointer
	shardTree     unsafe.Pointer
	blkCache      *cache.Cache
	idxCache      *cache.Cache
	resourceMgr   *epoch.ResourceManager
	safeTsTracker safeTsTracker
	closers       closers
	lastFID       uint32
	lastShardID   uint32
	writeCh       chan engineTask
	flushCh       chan *memtable.CFTable
	splitLock     sync.Mutex
	metrics       *y.MetricsSet
	manifest      *ShardingManifest
	mangedSafeTS  uint64
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
	sort.Slice(manifest.l0Files, func(i, j int) bool {
		return manifest.l0Files[i] > manifest.l0Files[j]
	})
	l0Tbls := &globalL0Tables{tables: make([]*globalL0Table, 0, len(manifest.l0Files))}
	for _, fid := range manifest.l0Files {
		filename := sstable.NewFilename(uint64(fid), opt.Dir)
		shardL0Tbl, err1 := openGlobalL0Table(filename, fid)
		if err1 != nil {
			return nil, err1
		}
		l0Tbls.tables = append(l0Tbls.tables, shardL0Tbl)
	}

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
		opt:         opt,
		numCFs:      len(opt.CFs),
		orc:         orc,
		dirLock:     dirLockGuard,
		metrics:     metrics,
		memTbls:     unsafe.Pointer(memTbls),
		l0Tbls:      unsafe.Pointer(l0Tbls),
		shardTree:   unsafe.Pointer(shardTree),
		blkCache:    blkCache,
		idxCache:    idxCache,
		flushCh:     make(chan *memtable.CFTable, opt.NumMemtables),
		writeCh:     make(chan engineTask, kvWriteChCapacity),
		manifest:    manifest,
		lastFID:     manifest.lastFileID,
		lastShardID: manifest.lastShardID,
	}
	memTbls.tables = append(memTbls.tables, memtable.NewCFTable(opt.MaxMemTableSize, len(opt.CFs), db.allocFid("l0")))
	db.closers.resourceManager = y.NewCloser(0)
	db.resourceMgr = epoch.NewResourceManager(db.closers.resourceManager, &db.safeTsTracker)
	db.closers.memtable = y.NewCloser(1)
	go db.runFlushMemTable(db.closers.memtable)
	db.closers.writes = y.NewCloser(1)
	go db.runWriteLoop(db.closers.writes)
	if !db.opt.DoNotCompact {
		db.closers.compactors = y.NewCloser(2)
		go db.runGlobalL0CompactionLoop(db.closers.compactors)
		go db.runShardInternalCompactionLoop(db.closers.compactors)
	}
	return db, nil
}

func (sdb *ShardingDB) Close() error {
	log.S().Info("closing ShardingDB")
	sdb.closers.writes.SignalAndWait()
	mtbl := sdb.loadWritableMemTable()
	if !mtbl.Empty() {
		log.S().Info("flush memTable before close")
		sdb.flushCh <- mtbl
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
		for cf, scf := range shard.cfs {
			var tableIDs [][]uint64
			for i := 0; i < len(scf.levels); i++ {
				level := scf.getLevelHandler(i)
				tableIDs = append(tableIDs, getTblIDs(level.tables))
			}
			log.S().Infof("shard %d cf %d tables %v", shard.ID, cf, tableIDs)
		}
	}
	for id, fi := range sdb.manifest.shards {
		cfs := make([][]uint32, sdb.numCFs)
		for fid := range fi.files {
			cfLevel, ok := sdb.manifest.globalFiles[fid]
			if !ok {
				log.S().Errorf("shard %d fid %d not found in global", fi.ID, fid)
			}
			cfs[cfLevel.cf] = append(cfs[cfLevel.cf], fid)
		}
		for cf, cfIDs := range cfs {
			log.S().Infof("manifest shard %d cf %d tables %v", id, cf, cfIDs)
		}
	}
}

type wbEntry struct {
	cf  byte
	key []byte
	val y.ValueStruct
}

type WriteBatch struct {
	cfConfs []CFConfig
	entries []*wbEntry
	notify  chan error
}

func NewWriteBatch(cfConfs []CFConfig) *WriteBatch {
	return &WriteBatch{
		cfConfs: cfConfs,
		notify:  make(chan error, 1),
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
	wb.entries = append(wb.entries, &wbEntry{
		cf:  cf,
		key: key,
		val: val,
	})
	return nil
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
	wb.entries = append(wb.entries, &wbEntry{
		cf:  cf,
		key: key,
		val: y.ValueStruct{Meta: bitDelete, Version: version},
	})
	return nil
}

func (sdb *ShardingDB) Write(wb *WriteBatch) error {
	sdb.writeCh <- engineTask{writeTask: wb}
	return <-wb.notify
}

func (sdb *ShardingDB) loadWritableMemTable() *memtable.CFTable {
	tbls := sdb.loadMemTables()
	return tbls.tables[0]
}

func (sdb *ShardingDB) loadMemTables() *shardingMemTables {
	return (*shardingMemTables)(atomic.LoadPointer(&sdb.memTbls))
}

func (sdb *ShardingDB) loadGlobalL0Tables() *globalL0Tables {
	return (*globalL0Tables)(atomic.LoadPointer(&sdb.l0Tbls))
}

func (sdb *ShardingDB) loadShardTree() *shardTree {
	return (*shardTree)(atomic.LoadPointer(&sdb.shardTree))
}

type Snapshot struct {
	guard    *epoch.Guard
	readTS   uint64
	memTbls  *shardingMemTables
	l0Tbls   *globalL0Tables
	shards   []*Shard
	cfs      []CFConfig
	startKey []byte
	endKey   []byte
}

func (s *Snapshot) Get(cf byte, key y.Key) y.ValueStruct {
	if !s.cfs[cf].Managed && key.Version == 0 {
		key.Version = s.readTS
	}
	for _, mtbl := range s.memTbls.tables {
		v := mtbl.Get(cf, key.UserKey, key.Version)
		if v.Valid() {
			return v
		}
	}
	keyHash := farm.Fingerprint64(key.UserKey)
	for _, l0Tbl := range s.l0Tbls.tables {
		v := l0Tbl.Get(cf, key, keyHash)
		if v.Valid() {
			return v
		}
	}
	shard := getShard(s.shards, key.UserKey)
	scf := shard.cfs[cf]
	for i := range scf.levels {
		level := scf.getLevelHandler(i)
		if len(level.tables) == 0 {
			continue
		}
		v := level.get(key, keyHash)
		if v.Valid() {
			return v
		}
	}
	return y.ValueStruct{}
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
		memTbls:  sdb.loadMemTables(),
		l0Tbls:   sdb.loadGlobalL0Tables(),
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
	guard := sdb.resourceMgr.Acquire()
	defer guard.Done()
	sdb.splitLock.Lock()
	defer sdb.splitLock.Unlock()
	tree := sdb.loadShardTree()
	shards := tree.getShards(start, end)
	var splitKeys [][]byte
	if !bytes.Equal(shards[0].Start, start) {
		splitKeys = append(splitKeys, start)
	}
	if !bytes.Equal(shards[len(shards)-1].End, end) {
		splitKeys = append(splitKeys, end)
	}
	log.S().Infof("split keys %s", splitKeys)
	if len(splitKeys) > 0 {
		err := sdb.splitInLock(splitKeys, guard)
		if err != nil {
			return err
		}
		tree = sdb.loadShardTree()
		shards = tree.getShards(start, end)
		log.S().Infof("shard Start %s start %s", shards[0].Start, start)
		y.Assert(bytes.Equal(shards[0].Start, start))
		log.S().Infof("shard end %s end %s", shards[0].End, end)
		y.Assert(bytes.Equal(shards[len(shards)-1].End, end))
	}
	task := &truncateTask{
		guard:  guard,
		shards: shards,
		notify: make(chan error, 1),
	}
	sdb.writeCh <- engineTask{truncates: task}
	return <-task.notify
}
