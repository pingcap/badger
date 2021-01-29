package badger

import (
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
	"sync"
	"sync/atomic"
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

var (
	errShardNotFound = errors.New("shard not found")
	errShardNotMatch = errors.New("shard not match")
)

type ShardingDB struct {
	opt           Options
	numCFs        int
	orc           *oracle
	dirLock       *directoryLockGuard
	shardMap      sync.Map
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
	db = &ShardingDB{
		opt:      opt,
		numCFs:   len(opt.CFs),
		orc:      orc,
		dirLock:  dirLockGuard,
		metrics:  metrics,
		blkCache: blkCache,
		idxCache: idxCache,
		flushCh:  make(chan *shardFlushTask, opt.NumMemtables),
		writeCh:  make(chan engineTask, kvWriteChCapacity),
		manifest: manifest,
	}
	if err = db.loadShards(); err != nil {
		return nil, err
	}

	if opt.IDAllocator != nil {
		db.idAlloc = opt.IDAllocator
	} else {
		db.idAlloc = &localIDAllocator{latest: manifest.lastID}
	}
	if opt.S3Options.EndPoint != "" {
		db.s3c = s3util.NewS3Client(opt.S3Options)
	}
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

func (sdb *ShardingDB) loadShards() error {
	for _, mShard := range sdb.manifest.shards {
		shard := newShard(mShard.properties.toPB(mShard.ID), mShard.Ver, mShard.Start, mShard.End, sdb.opt, sdb.metrics)
		for fid := range mShard.files {
			cfLevel, ok := sdb.manifest.globalFiles[fid]
			y.Assert(ok)
			cf := cfLevel.cf
			if cf == -1 {
				filename := sstable.NewFilename(fid, sdb.opt.Dir)
				sl0Tbl, err := openShardL0Table(filename, fid)
				if err != nil {
					return err
				}
				shard.addEstimatedSize(sl0Tbl.size)
				l0Tbls := shard.loadL0Tables()
				l0Tbls.tables = append(l0Tbls.tables, sl0Tbl)
				continue
			}
			level := cfLevel.level
			scf := shard.cfs[cf]
			handler := scf.getLevelHandler(int(level))
			filename := sstable.NewFilename(fid, sdb.opt.Dir)
			reader, err := sstable.NewMMapFile(filename)
			if err != nil {
				return err
			}
			tbl, err := sstable.OpenTable(filename, reader)
			if err != nil {
				return err
			}
			shard.addEstimatedSize(tbl.Size())
			handler.tables = append(handler.tables, tbl)
		}
		l0Tbls := shard.loadL0Tables()
		// Sort the l0 tables by age.
		sort.Slice(l0Tbls.tables, func(i, j int) bool {
			return l0Tbls.tables[i].fid > l0Tbls.tables[j].fid
		})
		for cf := 0; cf < len(sdb.opt.CFs); cf++ {
			scf := shard.cfs[cf]
			for level := 1; level <= ShardMaxLevel; level++ {
				handler := scf.getLevelHandler(level)
				sortTables(handler.tables)
			}
		}
		sdb.shardMap.Store(shard.ID, shard)
	}
	return nil
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
	var allShards []*Shard
	sdb.shardMap.Range(func(key, value interface{}) bool {
		allShards = append(allShards, value.(*Shard))
		return true
	})
	for _, shard := range allShards {
		l0s := shard.loadL0Tables()
		var l0IDs []uint64
		for _, tbl := range l0s.tables {
			l0IDs = append(l0IDs, tbl.fid)
		}
		shard.foreachLevel(func(cf int, level *levelHandler) (stop bool) {
			assertTablesOrder(level.level, level.tables, nil)
			for _, tbl := range level.tables {
				y.Assert(shard.OverlapKey(tbl.Smallest().UserKey))
				y.Assert(shard.OverlapKey(tbl.Biggest().UserKey))
			}
			return false
		})
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
	shard         *Shard
	cfConfs       []CFConfig
	entries       [][]*memtable.Entry
	estimatedSize int64
	notify        chan error
	properties    map[string][]byte
}

func (sdb *ShardingDB) NewWriteBatch(shard *Shard) *WriteBatch {
	return &WriteBatch{
		shard:   shard,
		cfConfs: sdb.opt.CFs,
		notify:  make(chan error, 1),
		entries: make([][]*memtable.Entry, sdb.numCFs),
	}
}

func (wb *WriteBatch) Put(cf int, key []byte, val y.ValueStruct) error {
	if wb.cfConfs[cf].Managed {
		if val.Version == 0 {
			return fmt.Errorf("version is zero for managed CF")
		}
	} else {
		if val.Version != 0 {
			return fmt.Errorf("version is not zero for non-managed CF")
		}
	}
	wb.entries[cf] = append(wb.entries[cf], &memtable.Entry{
		Key:   key,
		Value: val,
	})
	wb.estimatedSize += int64(len(key) + int(val.EncodedSize()) + memtable.EstimateNodeSize)
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
	wb.entries[cf] = append(wb.entries[cf], &memtable.Entry{
		Key:   key,
		Value: y.ValueStruct{Meta: bitDelete, Version: version},
	})
	wb.estimatedSize += int64(len(key) + memtable.EstimateNodeSize)
	return nil
}

func (wb *WriteBatch) SetProperty(key string, val []byte) {
	wb.properties[key] = val
}

func (sdb *ShardingDB) Write(wb *WriteBatch) error {
	sdb.writeCh <- engineTask{writeTask: wb}
	return <-wb.notify
}

type Snapshot struct {
	guard  *epoch.Guard
	readTS uint64
	shard  *Shard
	cfs    []CFConfig

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
		vs = s.shard.Get(cf, key)
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

func (sdb *ShardingDB) NewSnapshot(shard *Shard) *Snapshot {
	readTS := sdb.orc.readTs()
	guard := sdb.resourceMgr.AcquireWithPayload(readTS)
	return &Snapshot{
		guard:  guard,
		shard:  shard,
		readTS: readTS,
		cfs:    sdb.opt.CFs,
	}
}

func (sdb *ShardingDB) RemoveShard(shardID uint64, removeFile bool) error {
	shardVal, ok := sdb.shardMap.Load(shardID)
	if !ok {
		return errors.New("shard not found")
	}
	change := &protos.ShardChangeSet{ShardID: shardID, ShardDelete: true}
	shard := shardVal.(*Shard)
	err := sdb.manifest.writeChangeSet(shard, change, false)
	if err != nil {
		return err
	}
	shard.removeFilesOnDel = removeFile
	sdb.shardMap.Delete(shardID)
	guard := sdb.resourceMgr.Acquire()
	defer guard.Done()
	guard.Delete([]epoch.Resource{shard})
	return nil
}

func (sdb *ShardingDB) GetShard(shardID uint64) *Shard {
	shardVal, ok := sdb.shardMap.Load(shardID)
	if !ok {
		return nil
	}
	return shardVal.(*Shard)
}

func (sdb *ShardingDB) GetSplitSuggestion(shardID uint64, splitSize int64) [][]byte {
	shard := sdb.GetShard(shardID)
	var keys [][]byte
	if atomic.LoadInt64(&shard.estimatedSize) > splitSize {
		log.S().Infof("shard(%x, %x) size %d", shard.Start, shard.End, shard.estimatedSize)
		keys = append(keys, shard.getSplitKeys(splitSize)...)
	}
	return keys
}

func (sdb *ShardingDB) Size() int64 {
	var size int64
	sdb.shardMap.Range(func(key, value interface{}) bool {
		shard := value.(*Shard)
		size += atomic.LoadInt64(&shard.estimatedSize)
		return true
	})
	return size
}

func (sdb *ShardingDB) NumCFs() int {
	return sdb.numCFs
}

func (sdb *ShardingDB) GetOpt() Options {
	return sdb.opt
}
