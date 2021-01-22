package badger

import (
	"bytes"
	"fmt"
	"github.com/pingcap/badger/fileutil"
	"github.com/pingcap/badger/options"
	"github.com/pingcap/badger/protos"
	"github.com/pingcap/badger/table"
	"github.com/pingcap/badger/table/sstable"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"sync/atomic"
	"time"
	"unsafe"
)

// PreSplit  the ShardingDB to shards by keys.
func (sdb *ShardingDB) PreSplit(shardID, ver uint64, keys [][]byte) error {
	guard := sdb.resourceMgr.Acquire()
	defer guard.Done()
	shard := sdb.GetShard(shardID)
	if shard == nil {
		return errShardNotFound
	}
	if shard.Ver != ver {
		log.Info("shard not match", zap.Uint64("current", shard.Ver), zap.Uint64("request", ver))
		return errShardNotMatch
	}
	task := &preSplitTask{
		shard:  shard,
		keys:   keys,
		notify: make(chan error, 1),
	}
	sdb.writeCh <- engineTask{preSplitTask: task}
	err := <-task.notify
	if err != nil {
		return err
	}
	d := new(deletions)
	err = sdb.splitShardFiles(task, d)
	if err != nil {
		return err
	}
	guard.Delete(d.resources)
	return nil
}

func (sdb *ShardingDB) splitShardFiles(task *preSplitTask, d *deletions) error {
	change := &protos.ManifestChangeSet{}
	shard := task.shard
	shard.lock.Lock()
	defer shard.lock.Unlock()
	keys := task.keys
	sdb.waitForAllMemTablesFlushed(shard)
	err := sdb.splitShardL0Tables(task, d, change)
	if err != nil {
		return err
	}
	for cf := 0; cf < sdb.numCFs; cf++ {
		for lvl := 1; lvl <= ShardMaxLevel; lvl++ {
			if err := sdb.splitTables(shard, cf, lvl, keys, d, change); err != nil {
				return err
			}
		}
	}
	return sdb.manifest.writeChangeSet(nil, change, false)
}

func (sdb *ShardingDB) waitForAllMemTablesFlushed(shard *Shard) {
	for {
		memTbls := shard.loadMemTables()
		if len(memTbls.tables) == 0 || (len(memTbls.tables) == 1 && memTbls.tables[0].Empty()) {
			break
		}
		time.Sleep(time.Millisecond * 100)
		var sizes []int64
		for _, tbl := range memTbls.tables {
			sizes = append(sizes, tbl.Size())
		}
		log.S().Infof("memtable size %v", sizes)
	}
}

func (sdb *ShardingDB) splitShardL0Tables(task *preSplitTask, d *deletions, change *protos.ManifestChangeSet) error {
	shard := task.shard
	keys := task.keys
	l0s := shard.loadL0Tables()
	var allNewL0s [][]*shardL0Table
	for i := len(l0s.tables) - 1; i >= 0; i-- {
		// iterator the l0s in reverse order to ensure older tables get smaller id.
		l0Tbl := l0s.tables[i]
		newL0s, err := sdb.splitShardL0Table(task, l0Tbl)
		if err != nil {
			return err
		}
		for _, newL0 := range newL0s {
			change.Changes = append(change.Changes, newManifestChange(newL0.fid, shard.ID, -1, 0, protos.ManifestChange_CREATE))
		}
		allNewL0s = append(allNewL0s, newL0s)
		d.Append(l0Tbl)
		change.Changes = append(change.Changes, newManifestChange(l0Tbl.fid, shard.ID, -1, 0, protos.ManifestChange_DELETE))
	}
	for i := 0; i <= len(keys); i++ {
		for {
			splitingL0s := shard.loadSplittingL0Tables(i)
			newSplitingL0s := &shardL0Tables{}
			newSplitingL0s.tables = append(newSplitingL0s.tables, splitingL0s.tables...)
			for j := 0; j < len(allNewL0s); j++ {
				newSplitingL0s.tables = append(newSplitingL0s.tables, allNewL0s[j][i])
			}
			if shard.casSplittingL0Tables(i, splitingL0s, newSplitingL0s) {
				break
			}
		}
	}
	atomic.StorePointer(shard.l0s, unsafe.Pointer(&shardL0Tables{}))
	return nil
}

func (sdb *ShardingDB) splitShardL0Table(task *preSplitTask, l0 *shardL0Table) ([]*shardL0Table, error) {
	iters := make([]y.Iterator, sdb.numCFs)
	for cf := 0; cf < sdb.numCFs; cf++ {
		iters[cf] = l0.newIterator(cf, false)
		if iters[cf] != nil {
			it := iters[cf]
			for it.Rewind(); it.Valid(); it.Next() {
			}
			it.Rewind()
		}
	}
	var newL0s []*shardL0Table
	for _, key := range task.keys {
		newL0, err := sdb.buildShardL0BeforeKey(iters, key, task, l0.commitTS)
		if err != nil {
			return nil, err
		}
		newL0s = append(newL0s, newL0)
	}
	lastL0, err := sdb.buildShardL0BeforeKey(iters, globalShardEndKey, task, l0.commitTS)
	if err != nil {
		return nil, err
	}
	newL0s = append(newL0s, lastL0)
	return newL0s, nil
}

func (sdb *ShardingDB) buildShardL0BeforeKey(iters []y.Iterator, key []byte, task *preSplitTask, commitTS uint64) (*shardL0Table, error) {
	builder := newShardL0Builder(sdb.numCFs, commitTS, sdb.opt.TableBuilderOptions)
	for cf := 0; cf < sdb.numCFs; cf++ {
		iter := iters[cf]
		if iter == nil {
			continue
		}
		for ; iter.Valid(); y.NextAllVersion(iter) {
			if bytes.Compare(iter.Key().UserKey, key) >= 0 {
				break
			}
			builder.Add(cf, iter.Key(), iter.Value())
		}
	}
	shardL0Data := builder.Finish()
	fid := task.reservedIDs[0]
	task.reservedIDs = task.reservedIDs[1:]
	fd, err := sdb.createL0File(fid)
	if err != nil {
		panic(err)
	}
	writer := fileutil.NewBufferedWriter(fd, sdb.opt.TableBuilderOptions.WriteBufferSize, nil)
	_, err = writer.Write(shardL0Data)
	if err != nil {
		return nil, err
	}
	err = writer.Finish()
	if err != nil {
		return nil, err
	}
	tbl, err := openShardL0Table(fd.Name(), fid)
	if err != nil {
		return nil, err
	}
	return tbl, nil
}

func (sdb *ShardingDB) splitTables(shard *Shard, cf int, level int, keys [][]byte, d *deletions, changeSet *protos.ManifestChangeSet) error {
	scf := shard.cfs[cf]
	oldHandler := scf.getLevelHandler(level)
	oldTables := oldHandler.tables
	newTbls := make([]table.Table, 0, len(oldTables)+len(keys))
	toDeleteIDs := make(map[uint64]struct{})
	var relatedKeys [][]byte
	for _, tbl := range oldTables {
		relatedKeys = relatedKeys[:0]
		for _, key := range keys {
			if bytes.Compare(tbl.Smallest().UserKey, key) < 0 &&
				bytes.Compare(key, tbl.Biggest().UserKey) <= 0 {
				relatedKeys = append(relatedKeys, key)
			}
		}
		if len(relatedKeys) == 0 {
			newTbls = append(newTbls, tbl)
			continue
		}
		toDeleteIDs[tbl.ID()] = struct{}{}
		// append an end key to build the last table.
		relatedKeys = append(relatedKeys, globalShardEndKey)
		itr := tbl.NewIterator(false)
		itr.Rewind()
		for _, relatedKey := range relatedKeys {
			ntbl, err := sdb.buildTableBeforeKey(itr, relatedKey, level, sdb.opt.TableBuilderOptions)
			if err != nil {
				return err
			}
			if ntbl != nil {
				changeSet.Changes = append(changeSet.Changes, newShardCreateChange(shard.ID, ntbl.ID(), cf, level))
				newTbls = append(newTbls, ntbl)
			}
		}
		changeSet.Changes = append(changeSet.Changes, newShardDeleteChange(shard.ID, tbl.ID(), cf))
	}
	newHandler := newLevelHandler(sdb.opt.NumLevelZeroTablesStall, level, sdb.metrics)
	newHandler.totalSize = oldHandler.totalSize
	newHandler.tables = newTbls
	y.Assert(scf.casLevelHandler(level, oldHandler, newHandler))
	for _, oldTbl := range oldTables {
		if _, ok := toDeleteIDs[oldTbl.ID()]; ok {
			d.Append(oldTbl)
		}
	}
	return nil
}

func (sdb *ShardingDB) buildTableBeforeKey(itr y.Iterator, key []byte, level int, opt options.TableBuilderOptions) (table.Table, error) {
	filename := sstable.NewFilename(sdb.idAlloc.AllocID(), sdb.opt.Dir)
	fd, err := y.OpenSyncedFile(filename, false)
	if err != nil {
		return nil, err
	}
	b := sstable.NewTableBuilder(fd, nil, level, opt)
	for itr.Valid() {
		if len(key) > 0 && bytes.Compare(itr.Key().UserKey, key) >= 0 {
			return builderToTable(b)
		}
		err = b.Add(itr.Key(), itr.Value())
		if err != nil {
			return nil, err
		}
		y.NextAllVersion(itr)
	}
	if sdb.s3c != nil && !b.Empty() {
		err = putSSTBuildResultToS3(sdb.s3c, &sstable.BuildResult{FileName: filename})
		if err != nil {
			return nil, err
		}
	}
	return builderToTable(b)
}

func builderToTable(b *sstable.Builder) (table.Table, error) {
	if b.Empty() {
		return nil, nil
	}
	result, err1 := b.Finish()
	if err1 != nil {
		return nil, err1
	}
	mmapFile, err1 := sstable.NewMMapFile(result.FileName)
	if err1 != nil {
		return nil, err1
	}
	return sstable.OpenTable(result.FileName, mmapFile)
}

// FinishSplit finishes the Split process on a Shard in PreSplitState.
// This is done after preSplit is done, so we don't need to acquire any lock, just atomic CAS will do.
func (sdb *ShardingDB) FinishSplit(oldShardID, ver uint64, newShardIDs []uint64) (newShards []*Shard, err error) {
	oldShard := sdb.GetShard(oldShardID)
	if oldShard.Ver != ver {
		return nil, errShardNotMatch
	}
	if !oldShard.isSplitting() {
		return nil, errors.New("shard is not in splitting state")
	}
	if len(newShardIDs) != len(oldShard.splittingMemTbls) {
		return nil, fmt.Errorf("new ShardIDs length %d is not equals to splittingMemTbls length %d", len(newShardIDs), len(oldShard.splittingL0s))
	}
	oldShard.lock.Lock()
	defer func() {
		if err == nil {
			atomic.StoreUint32(&oldShard.splitState, splitStateSplitDone)
		}
		oldShard.lock.Unlock()
	}()
	changeSet := &protos.ManifestChangeSet{
		Split: &protos.ShardSplit{
			OldID: oldShardID,
			IDs:   newShardIDs,
			Keys:  oldShard.splitKeys,
		},
	}
	err = sdb.manifest.writeChangeSet(nil, changeSet, true)
	if err != nil {
		return nil, err
	}
	newShards = make([]*Shard, len(oldShard.splittingMemTbls))
	for i := range oldShard.splittingMemTbls {
		startKey, endKey := getSplittingStartEnd(oldShard.Start, oldShard.End, oldShard.splitKeys, i)
		ver := uint64(1)
		if newShardIDs[i] == oldShardID {
			ver = oldShard.Ver + 1
		}
		shard := newShard(newShardIDs[i], ver, startKey, endKey, sdb.opt, sdb.metrics)
		shard.memTbls = oldShard.splittingMemTbls[i]
		writable := shard.loadWritableMemTable()
		if writable != nil {
			iter := writable.NewIterator(0, false)
			iter.Rewind()
		}
		shard.l0s = oldShard.splittingL0s[i]
		newShards[i] = shard
	}
	for cf, scf := range oldShard.cfs {
		for l := 1; l <= ShardMaxLevel; l++ {
			level := scf.getLevelHandler(l)
			for _, t := range level.tables {
				sdb.insertTableToNewShard(t, cf, level.level, newShards, oldShard.splitKeys)
			}
		}
	}
	for _, nShard := range newShards {
		sdb.shardMap.Store(nShard.ID, nShard)
	}
	log.S().Infof("shard %d split to %v", oldShardID, newShardIDs)
	return newShards, nil
}

func (sdb *ShardingDB) insertTableToNewShard(t table.Table, cf, level int, shards []*Shard, splitKeys [][]byte) {
	idx := getSplitShardIndex(splitKeys, t.Smallest().UserKey)
	shard := shards[idx]
	y.Assert(shard.OverlapKey(t.Smallest().UserKey))
	y.Assert(shard.OverlapKey(t.Biggest().UserKey))
	sCF := shard.cfs[cf]
	handler := sCF.getLevelHandler(level)
	handler.tables = append(handler.tables, t)
}

func getSplitShardIndex(splitKeys [][]byte, key []byte) int {
	for i := 0; i < len(splitKeys); i++ {
		if bytes.Compare(key, splitKeys[i]) < 0 {
			return i
		}
	}
	return len(splitKeys)
}
