package badger

import (
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
	log.S().Infof("manifest l0 %d", len(manifest.l0Files))
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
	memTbls := &shardingMemTables{tables: []*memtable.CFTable{memtable.NewCFTable(opt.MaxMemTableSize, len(opt.CFs))}}
	db = &ShardingDB{
		opt:       opt,
		numCFs:    len(opt.CFs),
		orc:       orc,
		dirLock:   dirLockGuard,
		metrics:   metrics,
		memTbls:   unsafe.Pointer(memTbls),
		l0Tbls:    unsafe.Pointer(l0Tbls),
		shardTree: unsafe.Pointer(shardTree),
		blkCache:  blkCache,
		idxCache:  idxCache,
		flushCh:   make(chan *memtable.CFTable, opt.NumMemtables),
		writeCh:   make(chan engineTask, kvWriteChCapacity),
		manifest:  manifest,
		lastFID:   manifest.lastFileID,
	}
	db.closers.resourceManager = y.NewCloser(0)
	db.resourceMgr = epoch.NewResourceManager(db.closers.resourceManager, &db.safeTsTracker)
	db.closers.memtable = y.NewCloser(1)
	go db.runFlushMemTable(db.closers.memtable)
	db.closers.writes = y.NewCloser(1)
	go db.runWriteLoop(db.closers.writes)
	db.closers.compactors = y.NewCloser(1)
	if db.opt.DoNotCompact {
		db.closers.compactors.Done()
	} else {
		go db.runL0CompactionLoop(db.closers.compactors)
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
	sdb.closers.compactors.SignalAndWait()
	sdb.closers.resourceManager.SignalAndWait()
	return sdb.dirLock.release()
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

func (sdb *ShardingDB) Get(cf byte, key y.Key) y.ValueStruct {
	guard := sdb.resourceMgr.Acquire()
	defer guard.Done()
	if !sdb.opt.CFs[cf].Managed && key.Version == 0 {
		key.Version = sdb.orc.readTs()
	}
	memTbls := sdb.loadMemTables()
	for _, mtbl := range memTbls.tables {
		v := mtbl.Get(cf, key.UserKey, key.Version)
		if v.Valid() {
			return v
		}
	}
	keyHash := farm.Fingerprint64(key.UserKey)
	l0Tables := sdb.loadGlobalL0Tables()
	for _, l0Tbl := range l0Tables.tables {
		v := l0Tbl.Get(cf, key, keyHash)
		if v.Valid() {
			return v
		}
	}
	tree := sdb.loadShardTree()
	shard := tree.get(key.UserKey)
	return shard.Get(cf, key, keyHash)
}
