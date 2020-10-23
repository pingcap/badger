package badger

import (
	"bytes"
	"sort"
	"sync"
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
	orc           *oracle
	dirLock       *directoryLockGuard
	shards        *ShardManager
	memTbls       unsafe.Pointer
	l0Tbls        unsafe.Pointer
	blkCache      *cache.Cache
	idxCache      *cache.Cache
	resourceMgr   *epoch.ResourceManager
	safeTsTracker safeTsTracker
	closers       closers
	lastFID       uint32
	writeCh       chan engineTask
	flushCh       chan *memtable.CFTable
	splitLock     sync.Mutex
	metrics       *y.MetricsSet
	manifest      *ShardingManifest
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
	l0Tbls := &shardL0Tables{tables: make([]*shardL0Table, 0, len(manifest.l0Files))}
	for _, fid := range manifest.l0Files {
		filename := sstable.NewFilename(uint64(fid), opt.Dir)
		shardL0Tbl, err1 := openShardL0Table(filename, fid)
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
	sm, err := NewShardManager(manifest, opt, metrics)
	if err != nil {
		return nil, err
	}
	memTbls := &shardingMemTables{tables: []*memtable.CFTable{memtable.NewCFTable(opt.MaxMemTableSize, opt.NumCFs)}}
	db = &ShardingDB{
		opt:      opt,
		orc:      orc,
		dirLock:  dirLockGuard,
		shards:   sm,
		metrics:  metrics,
		memTbls:  unsafe.Pointer(memTbls),
		l0Tbls:   unsafe.Pointer(l0Tbls),
		blkCache: blkCache,
		idxCache: idxCache,
		flushCh:  make(chan *memtable.CFTable, opt.NumMemtables),
		writeCh:  make(chan engineTask, kvWriteChCapacity),
		manifest: manifest,
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
	entries []*wbEntry
	notify  chan error
}

func NewWriteBatch() *WriteBatch {
	return &WriteBatch{
		notify: make(chan error, 1),
	}
}

func (wb *WriteBatch) Put(cf byte, key []byte, val y.ValueStruct) {
	wb.entries = append(wb.entries, &wbEntry{
		cf:  cf,
		key: key,
		val: val,
	})
}

func (wb *WriteBatch) Delete(cf byte, key []byte, version uint64) {
	wb.entries = append(wb.entries, &wbEntry{
		cf:  cf,
		key: key,
		val: y.ValueStruct{Meta: bitDelete, Version: version},
	})
}

func (sdb *ShardingDB) Write(wb *WriteBatch) error {
	sdb.writeCh <- engineTask{writeTask: wb}
	return <-wb.notify
}

func (sdb *ShardingDB) Get(cf byte, key y.Key) y.ValueStruct {
	memTbls := sdb.loadMemTableSlice()
	for _, mtbl := range memTbls.tables {
		v := mtbl.Get(cf, key.UserKey, key.Version)
		if v.Valid() {
			return v
		}
	}
	keyHash := farm.Fingerprint64(key.UserKey)
	l0Tables := sdb.loadShardL0Tables()
	for _, l0Tbl := range l0Tables.tables {
		v := l0Tbl.Get(cf, key, keyHash)
		if v.Valid() {
			return v
		}
	}
	return y.ValueStruct{}
}

func (sdb *ShardingDB) Split(keys [][]byte) error {
	sdb.splitLock.Lock()
	defer sdb.splitLock.Unlock()
	shardByKey := sdb.shards.loadShardByKeyTree()

	var shardTasks []shardSplitTask
	for _, key := range keys {
		if len(shardTasks) > 0 {
			shardTask := shardTasks[len(shardTasks)-1]
			if bytes.Compare(shardTask.shard.End, key) > 0 {
				shardTask.keys = append(shardTask.keys, key)
				continue
			}
		}
		shardTask := shardSplitTask{
			shard: shardByKey.get(key),
			keys:  [][]byte{key},
		}
		shardTasks = append(shardTasks, shardTask)
	}
	for _, shardTask := range shardTasks {
		if err := sdb.shards.preSplit(shardTask.shard, shardTask.keys); err != nil {
			return err
		}
	}
	task := &splitTask{
		shards: shardTasks,
		notify: make(chan error, 1),
	}
	sdb.writeCh <- engineTask{splitTask: task}
	return <-task.notify
}
