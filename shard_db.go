package badger

import (
	"unsafe"

	"github.com/pingcap/badger/cache"
	"github.com/pingcap/badger/epoch"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
)

type ShardingDB struct {
	opt           Options
	orc           *oracle
	dirLock       *directoryLockGuard
	shards        *ShardManager
	memTable      unsafe.Pointer
	blkCache      *cache.Cache
	idxCache      *cache.Cache
	resourceMgr   *epoch.ResourceManager
	safeTsTracker safeTsTracker
	closers       closers
	lastFID       uint32
	writeCh       chan engineTask
	flushCh       chan *ShardingMemTable
	metrics       *y.MetricsSet
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

	blkCache, idxCache, err := createCache(opt)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create block cache")
	}
	metrics := y.NewMetricSet(opt.Dir)
	sm, err := NewShardManager(manifest, opt, metrics)
	if err != nil {
		return nil, err
	}
	db = &ShardingDB{
		opt:      opt,
		orc:      orc,
		dirLock:  dirLockGuard,
		shards:   sm,
		metrics:  metrics,
		memTable: unsafe.Pointer(NewShardingMemTable(opt.MaxMemTableSize, opt.NumCFs)),
		blkCache: blkCache,
		idxCache: idxCache,
		flushCh:  make(chan *ShardingMemTable, opt.NumMemtables),
		writeCh:  make(chan engineTask, kvWriteChCapacity),
	}
	db.closers.resourceManager = y.NewCloser(0)
	db.resourceMgr = epoch.NewResourceManager(db.closers.resourceManager, &db.safeTsTracker)
	db.closers.memtable = y.NewCloser(1)
	go db.runFlushMemTable(db.closers.memtable)
	db.closers.writes = y.NewCloser(1)
	go db.runWriteLoop(db.closers.writes)
	db.closers.compactors = y.NewCloser(db.opt.NumCompactors)
	for i := 0; i < db.opt.NumCompactors; i++ {
		go db.runCompactionLoop(db.closers.compactors)
	}
	return db, nil
}

func (sdb *ShardingDB) Close() error {
	sdb.closers.writes.SignalAndWait()
	mtbl := sdb.loadMemTable()
	if !mtbl.Empty() {
		sdb.flushCh <- mtbl
	}
	close(sdb.flushCh)
	sdb.closers.memtable.SignalAndWait()
	sdb.closers.compactors.SignalAndWait()
	sdb.closers.resourceManager.SignalAndWait()
	return nil
}
