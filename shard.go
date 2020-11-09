package badger

import (
	"bytes"
	"encoding/binary"
	"github.com/dgryski/go-farm"
	"github.com/ncw/directio"
	"github.com/pingcap/badger/epoch"
	"github.com/pingcap/badger/fileutil"
	"github.com/pingcap/badger/options"
	"github.com/pingcap/badger/protos"
	"github.com/pingcap/badger/table"
	"github.com/pingcap/badger/table/memtable"
	"github.com/pingcap/badger/table/sstable"
	"github.com/pingcap/badger/y"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const shardMaxLevel = 4

func newShardTree(manifest *ShardingManifest, opt Options, metrics *y.MetricsSet) (*shardTree, error) {
	tree := &shardTree{
		shards: make([]*Shard, 0, len(manifest.shards)),
	}
	for _, mShard := range manifest.shards {
		shard := newShard(mShard.ID, mShard.Start, mShard.End, opt, metrics)
		for fid := range mShard.files {
			cfLevel, ok := manifest.globalFiles[fid]
			y.Assert(ok)
			cf := cfLevel.cf
			if cf == -1 {
				filename := sstable.NewFilename(uint64(fid), opt.Dir)
				sl0Tbl, err := openShardL0Table(filename, fid)
				if err != nil {
					return nil, err
				}
				l0Tbls := shard.loadL0Tables()
				l0Tbls.tables = append(l0Tbls.tables, sl0Tbl)
				continue
			}
			level := cfLevel.level
			scf := shard.cfs[cf]
			handler := scf.getLevelHandler(int(level))
			filename := sstable.NewFilename(uint64(fid), opt.Dir)
			reader, err := sstable.NewMMapFile(filename)
			if err != nil {
				return nil, err
			}
			tbl, err := sstable.OpenTable(filename, reader)
			if err != nil {
				return nil, err
			}
			handler.tables = append(handler.tables, tbl)
		}
		l0Tbls := shard.loadL0Tables()
		// Sort the l0 tables by age.
		sort.Slice(l0Tbls.tables, func(i, j int) bool {
			return l0Tbls.tables[i].fid > l0Tbls.tables[j].fid
		})
		for cf := 0; cf < len(opt.CFs); cf++ {
			scf := shard.cfs[cf]
			for level := 1; level <= shardMaxLevel; level++ {
				handler := scf.getLevelHandler(level)
				sortTables(handler.tables)
			}
		}
		tree.shards = append(tree.shards, shard)
	}
	sort.Slice(tree.shards, func(i, j int) bool {
		return bytes.Compare(tree.shards[i].Start, tree.shards[j].Start) < 0
	})
	return tree, nil
}

// This data structure is rarely written, so we can make it immutable.
type shardTree struct {
	shards []*Shard
}

func (tree *shardTree) last() *Shard {
	return tree.shards[len(tree.shards)-1]
}

// the removes and adds has the same total range, so we can append before and after.
func (st *shardTree) replace(removes []*Shard, adds []*Shard) *shardTree {
	newShards := make([]*Shard, 0, len(st.shards)+len(adds)-len(removes))
	left, right := getShardRange(st.shards, removes[0].Start, removes[len(removes)-1].End)
	newShards = append(newShards, st.shards[:left]...)
	newShards = append(newShards, adds...)
	newShards = append(newShards, st.shards[right:]...)
	return &shardTree{shards: newShards}
}

func (st *shardTree) get(key []byte) *Shard {
	return getShard(st.shards, key)
}

func getShard(shards []*Shard, key []byte) *Shard {
	idx := sort.Search(len(shards), func(i int) bool {
		return bytes.Compare(key, shards[i].End) < 0
	})
	return shards[idx]
}

func getShardRange(shards []*Shard, start, end []byte) (left, right int) {
	left = sort.Search(len(shards), func(i int) bool {
		return bytes.Compare(start, shards[i].End) < 0
	})
	right = sort.Search(len(shards), func(i int) bool {
		return bytes.Compare(end, shards[i].Start) <= 0
	})
	return
}

func (st *shardTree) getShards(start, end []byte) []*Shard {
	left, right := getShardRange(st.shards, start, end)
	return st.shards[left:right]
}

func (sdb *ShardingDB) allocShardID() uint32 {
	return atomic.AddUint32(&sdb.lastShardID, 1)
}

// Shard split can be performed by the following steps:
// 1. set the splitKeys and mark the state to splitting.
// 2. a splitting Shard will separate all the files by the SplitKeys.
// 3. incoming SST is also split by SplitKeys.
// 4. After all existing files are split by the split keys, the state is changed to SplitDone.
// 5. After SplitDone, the shard map replace the old shard two new shardsByID.
type Shard struct {
	ID    uint32
	Start []byte
	End   []byte
	cfs   []*shardCF
	lock  sync.Mutex

	memTbls *unsafe.Pointer
	l0s     *unsafe.Pointer
	flushCh chan *shardFlushTask

	// split state transition: initial(0) -> set keys (1) -> splitting (2) -> splitDone (3)
	splitState       uint32
	splitKeys        [][]byte
	splittingMemTbls []*unsafe.Pointer
	splittingL0s     []*unsafe.Pointer
}

const (
	splitStateInitial   uint32 = 0
	splitStateSetKeys   uint32 = 1
	splitStateSplitting uint32 = 2
	splitStateSplitDone uint32 = 3
)

func newShard(id uint32, start, end []byte, opt Options, metrics *y.MetricsSet) *Shard {
	shard := &Shard{
		ID:    id,
		Start: start,
		End:   end,
		cfs:   make([]*shardCF, len(opt.CFs)),
	}
	shard.memTbls = new(unsafe.Pointer)
	atomic.StorePointer(shard.memTbls, unsafe.Pointer(&shardingMemTables{}))
	shard.l0s = new(unsafe.Pointer)
	atomic.StorePointer(shard.l0s, unsafe.Pointer(&shardingMemTables{}))
	for i := 0; i < len(opt.CFs); i++ {
		sCF := &shardCF{
			levels: make([]unsafe.Pointer, shardMaxLevel),
		}
		for j := 1; j <= shardMaxLevel; j++ {
			sCF.casLevelHandler(j, nil, newLevelHandler(opt.NumLevelZeroTablesStall, j, metrics))
		}
		shard.cfs[i] = sCF
	}
	return shard
}

func (s *Shard) tableIDs() []uint32 {
	var ids []uint32
	l0s := s.loadL0Tables()
	for _, tbl := range l0s.tables {
		ids = append(ids, tbl.fid)
	}
	if s.isSplitting() {
		for i := range s.splittingL0s {
			splittingL0s := s.loadSplittingL0Tables(i)
			for _, tbl := range splittingL0s.tables {
				ids = append(ids, tbl.fid)
			}
		}
	}
	s.foreachLevel(func(cf int, level *levelHandler) (stop bool) {
		for _, tbl := range level.tables {
			ids = append(ids, uint32(tbl.ID()))
		}
		return false
	})
	return ids
}

func (s *Shard) isSplitting() bool {
	return atomic.LoadUint32(&s.splitState) >= splitStateSplitting
}

func (s *Shard) setSplitKeys(keys [][]byte) bool {
	if atomic.CompareAndSwapUint32(&s.splitState, splitStateInitial, splitStateSetKeys) {
		s.splitKeys = keys
		s.splittingMemTbls = make([]*unsafe.Pointer, len(keys)+1)
		s.splittingL0s = make([]*unsafe.Pointer, len(keys)+1)
		for i := range s.splittingMemTbls {
			memPtr := new(unsafe.Pointer)
			*memPtr = unsafe.Pointer(&shardingMemTables{})
			s.splittingMemTbls[i] = memPtr
			l0Ptr := new(unsafe.Pointer)
			*l0Ptr = unsafe.Pointer(&shardL0Tables{})
			s.splittingL0s[i] = l0Ptr
		}
		y.Assert(atomic.CompareAndSwapUint32(&s.splitState, splitStateSetKeys, splitStateSplitting))
		return true
	}
	return false
}

func (s *Shard) setSplitDone() {
	y.Assert(atomic.CompareAndSwapUint32(&s.splitState, splitStateSplitting, splitStateSplitDone))
}

func (s *Shard) foreachLevel(f func(cf int, level *levelHandler) (stop bool)) {
	for cf, scf := range s.cfs {
		for i := 1; i <= shardMaxLevel; i++ {
			l := scf.getLevelHandler(i)
			if stop := f(cf, l); stop {
				return
			}
		}
	}
}

func (s *Shard) Get(cf int, key y.Key) y.ValueStruct {
	keyHash := farm.Fingerprint64(key.UserKey)
	if s.isSplitting() {
		idx := s.getSplittingIndex(key.UserKey)
		memTbls := s.loadSplittingMemTables(idx)
		for _, tbl := range memTbls.tables {
			v := tbl.Get(cf, key.UserKey, key.Version)
			if v.Valid() {
				return v
			}
		}
		l0Tbls := s.loadSplittingL0Tables(idx)
		for _, tbl := range l0Tbls.tables {
			v := tbl.Get(cf, key, keyHash)
			if v.Valid() {
				return v
			}
		}
	}
	memTbls := s.loadMemTables()
	for _, tbl := range memTbls.tables {
		v := tbl.Get(cf, key.UserKey, key.Version)
		if v.Valid() {
			return v
		}
	}
	l0Tbls := s.loadL0Tables()
	for _, tbl := range l0Tbls.tables {
		v := tbl.Get(cf, key, keyHash)
		if v.Valid() {
			return v
		}
	}
	scf := s.cfs[cf]
	for i := 1; i <= shardMaxLevel; i++ {
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

func (s *Shard) getSplittingIndex(key []byte) int {
	var i int
	for ; i < len(s.splitKeys); i++ {
		if bytes.Compare(key, s.splitKeys[i]) < 0 {
			break
		}
	}
	return i
}

func (s *Shard) loadSplittingL0Tables(i int) *shardL0Tables {
	return (*shardL0Tables)(atomic.LoadPointer(s.splittingL0s[i]))
}

func (s *Shard) casSplittingL0Tables(i int, old, new *shardL0Tables) bool {
	return atomic.CompareAndSwapPointer(s.splittingL0s[i], unsafe.Pointer(old), unsafe.Pointer(new))
}

func (s *Shard) loadSplittingMemTables(i int) *shardingMemTables {
	return (*shardingMemTables)(atomic.LoadPointer(s.splittingMemTbls[i]))
}

func (s *Shard) loadSplittingWritableMemTable(i int) *memtable.CFTable {
	tbls := s.loadSplittingMemTables(i)
	if tbls != nil && len(tbls.tables) > 0 {
		return tbls.tables[0]
	}
	return nil
}

func (s *Shard) loadMemTables() *shardingMemTables {
	return (*shardingMemTables)(atomic.LoadPointer(s.memTbls))
}

func (s *Shard) loadWritableMemTable() *memtable.CFTable {
	tbls := s.loadMemTables()
	if len(tbls.tables) > 0 {
		return tbls.tables[0]
	}
	return nil
}

func (s *Shard) loadL0Tables() *shardL0Tables {
	return (*shardL0Tables)(atomic.LoadPointer(s.l0s))
}

type shardCF struct {
	levels []unsafe.Pointer
}

func (scf *shardCF) getLevelHandler(level int) *levelHandler {
	return (*levelHandler)(atomic.LoadPointer(&scf.levels[level-1]))
}

func (scf *shardCF) casLevelHandler(level int, oldH, newH *levelHandler) bool {
	return atomic.CompareAndSwapPointer(&scf.levels[level-1], unsafe.Pointer(oldH), unsafe.Pointer(newH))
}

func (scf *shardCF) setHasOverlapping(cd *CompactDef) {
	if cd.moveDown() {
		return
	}
	kr := getKeyRange(cd.Top)
	for lvl := cd.Level + 2; lvl < len(scf.levels); lvl++ {
		lh := scf.getLevelHandler(lvl)
		left, right := lh.overlappingTables(levelHandlerRLocked{}, kr)
		if right-left > 0 {
			cd.HasOverlap = true
			return
		}
	}
	return
}

type Level struct {
	tables []table.Table
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
	filename := sstable.NewFilename(uint64(sdb.allocFid("splitTable")), sdb.opt.Dir)
	fd, err := directio.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0666)
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

// Split splits the ShardingDB to shards by keys.
func (sdb *ShardingDB) Split(keys [][]byte) error {
	guard := sdb.resourceMgr.Acquire()
	defer guard.Done()
	task := sdb.buildSplitTask(keys)
	if task == nil {
		return nil
	}
	sdb.writeCh <- engineTask{splitTask: task}
	err := <-task.notify
	if err != nil {
		return err
	}
	d := new(deletions)
	for _, shardTask := range task.shards {
		err = sdb.splitShard(shardTask, d)
		if err != nil {
			return err
		}
	}
	guard.Delete(d.resourses)
	return nil
}

func (sdb *ShardingDB) buildSplitTask(keys [][]byte) *splitTask {
	tree := sdb.loadShardTree()
	shardTasks := map[uint32]*shardSplitTask{}
	for _, key := range keys {
		shard := tree.get(key)
		if bytes.Equal(shard.Start, key) {
			continue
		}
		task, ok := shardTasks[shard.ID]
		if !ok {
			task = &shardSplitTask{
				shard: shard,
			}
			shardTasks[shard.ID] = task
		}
		task.keys = append(task.keys, key)
	}
	if len(shardTasks) == 0 {
		return nil
	}
	return &splitTask{
		shards: shardTasks,
		notify: make(chan error, 1),
	}
}

type deletions struct {
	resourses []epoch.Resource
}

func (d *deletions) Append(res epoch.Resource) {
	d.resourses = append(d.resourses, res)
}

func (sdb *ShardingDB) splitShard(task *shardSplitTask, d *deletions) error {
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
		for lvl := 1; lvl <= shardMaxLevel; lvl++ {
			if err := sdb.splitTables(shard, cf, lvl, keys, d, change); err != nil {
				return err
			}
		}
	}
	newShards := sdb.finishSplit(shard, keys)
	for _, newShard := range newShards {
		change.ShardChange = append(change.ShardChange, &protos.ShardChange{
			ShardID:  newShard.ID,
			Op:       protos.ShardChange_CREATE,
			StartKey: newShard.Start,
			EndKey:   newShard.End,
			TableIDs: newShard.tableIDs(),
		})
	}
	change.ShardChange = append(change.ShardChange, &protos.ShardChange{
		ShardID: shard.ID,
		Op:      protos.ShardChange_DELETE,
	})
	err = sdb.manifest.writeChangeSet(change)
	if err != nil {
		return err
	}
	for {
		oldTree := sdb.loadShardTree()
		newTree := oldTree.replace([]*Shard{shard}, newShards)
		if atomic.CompareAndSwapPointer(&sdb.shardTree, unsafe.Pointer(oldTree), unsafe.Pointer(newTree)) {
			break
		}
	}
	return nil
}

func (sdb *ShardingDB) waitForAllMemTablesFlushed(shard *Shard) {
	for {
		memTbls := shard.loadMemTables()
		if len(memTbls.tables) == 0 || (len(memTbls.tables) == 1 && memTbls.tables[0].Empty()) {
			break
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func (sdb *ShardingDB) splitShardL0Tables(task *shardSplitTask, d *deletions, change *protos.ManifestChangeSet) error {
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

func (sdb *ShardingDB) splitShardL0Table(task *shardSplitTask, l0 *shardL0Table) ([]*shardL0Table, error) {
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
		newL0, err := sdb.buildShardL0BeforeKey(iters, key, task)
		if err != nil {
			return nil, err
		}
		newL0s = append(newL0s, newL0)
	}
	lastL0, err := sdb.buildShardL0BeforeKey(iters, globalShardEndKey, task)
	if err != nil {
		return nil, err
	}
	newL0s = append(newL0s, lastL0)
	return newL0s, nil
}

func (sdb *ShardingDB) buildShardL0BeforeKey(iters []y.Iterator, key []byte, task *shardSplitTask) (*shardL0Table, error) {
	builder := newShardL0Builder(sdb.numCFs, sdb.opt.TableBuilderOptions)
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
	writer := fileutil.NewDirectWriter(fd, sdb.opt.TableBuilderOptions.WriteBufferSize, nil)
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

// At this stage, all files are split by splitKeys, it is a fast operation.
// This is done after preSplit is done, so we don't need to acquire any lock, just atomic CAS will do.
func (sdb *ShardingDB) finishSplit(s *Shard, splitKeys [][]byte) []*Shard {
	newShards := make([]*Shard, 0, len(splitKeys)+1)
	firstShard := newShard(sdb.allocShardID(), s.Start, splitKeys[0], sdb.opt, sdb.metrics)
	newShards = append(newShards, firstShard)
	for i, splitKey := range splitKeys {
		var endKey []byte
		if i == len(splitKeys)-1 {
			endKey = s.End
		} else {
			endKey = splitKeys[i+1]
		}
		newShards = append(newShards, newShard(sdb.allocShardID(), splitKey, endKey, sdb.opt, sdb.metrics))
	}
	for i := range s.splittingMemTbls {
		newShards[i].memTbls = s.splittingMemTbls[i]
		newShards[i].l0s = s.splittingL0s[i]
	}

	for cf, scf := range s.cfs {
		for l := 1; l <= shardMaxLevel; l++ {
			level := scf.getLevelHandler(l)
			for _, t := range level.tables {
				sdb.insertTableToNewShard(t, cf, level.level, newShards)
			}
		}
	}
	return newShards
}

func (sdb *ShardingDB) insertTableToNewShard(t table.Table, cf, level int, shards []*Shard) {
	shard := getShard(shards, t.Smallest().UserKey)
	sCF := shard.cfs[cf]
	handler := sCF.getLevelHandler(level)
	handler.tables = append(handler.tables, t)
}

type shardL0Builder struct {
	builders []*sstable.Builder
}

func newShardL0Builder(numCFs int, opt options.TableBuilderOptions) *shardL0Builder {
	sdb := &shardL0Builder{
		builders: make([]*sstable.Builder, numCFs),
	}
	for i := 0; i < numCFs; i++ {
		sdb.builders[i] = sstable.NewTableBuilder(nil, nil, 0, opt)
	}
	return sdb
}

func (e *shardL0Builder) Add(cf int, key y.Key, value y.ValueStruct) {
	e.builders[cf].Add(key, value)
}

/*
shard Data format:
 | CF0 Data | CF0 index | CF1 Data | CF1 index | cfs index
*/
func (e *shardL0Builder) Finish() []byte {
	cfDatas := make([][]byte, 0, len(e.builders)*2)
	cfsIndex := make([]byte, len(e.builders)*8)
	var fileSize int
	for i, builder := range e.builders {
		result, _ := builder.Finish()
		cfDatas = append(cfDatas, result.FileData)
		fileSize += len(result.FileData)
		binary.LittleEndian.PutUint32(cfsIndex[i*8:], uint32(fileSize))
		cfDatas = append(cfDatas, result.IndexData)
		fileSize += len(result.IndexData)
		binary.LittleEndian.PutUint32(cfsIndex[i*8+4:], uint32(fileSize))
	}
	result := make([]byte, 0, fileSize+len(cfsIndex)+1)
	result = append(result)
	for _, cfData := range cfDatas {
		result = append(result, cfData...)
	}
	result = append(result, cfsIndex...)
	result = append(result, byte(len(e.builders))) // number of CF
	return result
}
