package badger

import (
	"bytes"
	"fmt"
	"github.com/pingcap/badger/protos"
	"github.com/pingcap/badger/table/sstable"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestShardingDB(t *testing.T) {
	runPprof()
	dir, err := ioutil.TempDir("", "sharding")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	opts := getTestOptions(dir)
	opts.NumCompactors = 2
	opts.NumLevelZeroTables = 1
	opts.CFs = []CFConfig{{Managed: true}, {Managed: false}, {Managed: false}}
	db, err := OpenShardingDB(opts)
	require.NoError(t, err)
	sc := &shardingCase{
		t: t,
		n: 20000,
	}
	ch := make(chan time.Duration)
	go func() {
		time.Sleep(time.Millisecond * 500)
		begin := time.Now()
		for i := 1000; i < 20000; i += 4000 {
			sc.split(db, iToKey(i), iToKey(i+2000))
		}
		ch <- time.Since(begin)
	}()
	begin := time.Now()
	sc.loadData(db)
	log.S().Infof("time split %v; load %v", <-ch, time.Since(begin))
	time.Sleep(time.Second * 3)
	db.PrintStructure()
	sc.checkData(db)
	err = db.Close()
	require.NoError(t, err)
	db, err = OpenShardingDB(opts)
	require.NoError(t, err)
	db.PrintStructure()
	sc.checkData(db)
}

func TestShardingDeleteRange(t *testing.T) {
	runPprof()
	dir, err := ioutil.TempDir("", "sharding")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	opts := getTestOptions(dir)
	opts.NumCompactors = 2
	opts.NumLevelZeroTables = 1
	opts.CFs = []CFConfig{{Managed: true}, {Managed: false}, {Managed: false}}
	db, err := OpenShardingDB(opts)
	require.NoError(t, err)
	sc := &shardingCase{
		t: t,
		n: 20000,
	}
	sc.loadData(db)
	sc.split(db, iToKey(10000))
	err = db.DeleteRange(iToKey(1000), iToKey(3000))
	require.NoError(t, err)
	err = db.DeleteRange(iToKey(18000), iToKey(20000))
	require.NoError(t, err)
	err = db.DeleteRange(iToKey(9000), iToKey(11000))
	require.NoError(t, err)
	snap := db.NewSnapshot(nil, nil)
	defer snap.Discard()
	for cf := 0; cf < 3; cf++ {
		it := snap.NewIterator(0, false, true)
		cnt := 0
		for it.Rewind(); it.Valid(); it.Next() {
			cnt++
		}
		require.Equal(t, cnt, 14000)
	}
	require.NoError(t, db.Close())
	snap = db.NewSnapshot(nil, nil)
	defer snap.Discard()
	for cf := 0; cf < 3; cf++ {
		it := snap.NewIterator(0, false, true)
		cnt := 0
		for it.Rewind(); it.Valid(); it.Next() {
			cnt++
		}
		require.Equal(t, cnt, 14000)
	}
}

type shardingCase struct {
	t *testing.T
	n int
}

func iToKey(i int) []byte {
	return []byte(fmt.Sprintf("key%06d", i))
}

func (sc *shardingCase) loadData(db *ShardingDB) {
	wb := db.NewWriteBatch()
	for i := 0; i < sc.n; i++ {
		key := iToKey(i)
		require.NoError(sc.t, wb.Put(0, key, y.ValueStruct{Value: key, Version: 1}))
		require.NoError(sc.t, wb.Put(1, key, y.ValueStruct{Value: bytes.Repeat(key, 2)}))
		if i%100 == 99 {
			err := db.Write(wb)
			require.NoError(sc.t, err)
			wb = db.NewWriteBatch()
		}
	}
}

func (sc *shardingCase) checkGet(snap *Snapshot) {
	for i := 0; i < sc.n; i++ {
		key := iToKey(i)
		item, err := snap.Get(0, y.KeyWithTs(key, 2))
		require.Nil(sc.t, err)
		require.Equal(sc.t, string(item.vptr), string(key))
		item2, err := snap.Get(1, y.KeyWithTs(key, 0))
		require.Nil(sc.t, err)
		require.Equal(sc.t, string(item2.vptr), strings.Repeat(string(key), 2))
	}
}

func (sc *shardingCase) checkIterator(snap *Snapshot) {
	for cf := 0; cf < 2; cf++ {
		iter := snap.NewIterator(cf, false, false)
		i := 0
		for iter.Rewind(); iter.Valid(); iter.Next() {
			key := iToKey(i)
			item := iter.Item()
			require.EqualValues(sc.t, key, item.key.UserKey)
			require.EqualValues(sc.t, bytes.Repeat(key, int(cf)+1), item.vptr)
			i++
		}
		require.Equal(sc.t, sc.n, i)
	}
}

func (sc *shardingCase) split(db *ShardingDB, keys ...[]byte) {
	err := db.Split(keys)
	require.NoError(sc.t, err)
}

func (sc *shardingCase) checkData(db *ShardingDB) {
	snap := db.NewSnapshot(nil, nil)
	log.S().Infof("shard ids %v", getShardIDs(snap.shards))
	sc.checkGet(snap)
	sc.checkIterator(snap)
	snap.Discard()
}

func TestShardingTree(t *testing.T) {
	shardID := new(uint64)
	shardKeys := [][]byte{nil, []byte("a"), []byte("b"), []byte("c"), globalShardEndKey}
	var shards []*Shard
	for i := 0; i < 4; i++ {
		shards = append(shards, &Shard{
			ID:    atomic.AddUint64(shardID, 1),
			Start: shardKeys[i],
			End:   shardKeys[i+1],
		})
	}
	tree := &shardTree{shards: shards}
	shard := tree.get([]byte(""))
	require.Equal(t, shard.ID, uint64(1))
	shard = tree.get([]byte("a"))
	require.Equal(t, shard.ID, uint64(2))
	shard = tree.get([]byte("abc"))
	require.Equal(t, shard.ID, uint64(2))
	shard = tree.get([]byte("b"))
	require.Equal(t, shard.ID, uint64(3))
	shard = tree.get([]byte("bcd"))
	require.Equal(t, shard.ID, uint64(3))
	shard = tree.get([]byte("cde"))
	require.Equal(t, shard.ID, uint64(4))
	shard = tree.get([]byte("fsd"))
	require.Equal(t, shard.ID, uint64(4))

	gotShards := tree.getShards([]byte("abc"), []byte("abc"))
	require.Equal(t, len(gotShards), 1)
	require.Equal(t, gotShards[0].ID, uint64(2))
	gotShards = tree.getShards([]byte("b"), []byte("b"))
	require.Equal(t, len(gotShards), 1)
	require.Equal(t, gotShards[0].ID, uint64(3))

	shards = []*Shard{
		{
			ID:    atomic.AddUint64(shardID, 1),
			Start: nil,
			End:   globalShardEndKey,
		},
	}
	tree = &shardTree{shards: shards}
	old := shards[0]
	newShards := []*Shard{
		{
			ID:  atomic.AddUint64(shardID, 1),
			End: []byte("a"),
		},
		{
			ID:    atomic.AddUint64(shardID, 1),
			Start: []byte("a"),
			End:   globalShardEndKey,
		},
	}
	tree = tree.replace([]*Shard{old}, newShards)
	require.Equal(t, len(tree.shards), 2)
}

func TestIngestTree(t *testing.T) {
	dir, err := ioutil.TempDir("", "sharding")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	alloc := new(localIDAllocator)
	opts := getTestOptions(dir)
	opts.NumCompactors = 2
	opts.NumLevelZeroTables = 1
	opts.CFs = []CFConfig{{Managed: true}, {Managed: false}, {Managed: false}}
	opts.IDAllocator = alloc
	db, err := OpenShardingDB(opts)
	require.NoError(t, err)
	sc := &shardingCase{
		t: t,
		n: 20000,
	}
	sc.loadData(db)
	sc.split(db, iToKey(5000), iToKey(10000))
	require.NoError(t, db.Close())
	opts.DoNotCompact = true
	db, err = OpenShardingDB(opts)
	require.NoError(t, err)
	ingestTree := db.GetShardTree(iToKey(5000))
	require.NoError(t, db.Close())
	dir2, err := ioutil.TempDir("", "sharding")
	require.NoError(t, err)
	defer os.RemoveAll(dir2)
	for _, fm := range ingestTree.Meta.AddedFiles {
		oldName := sstable.NewFilename(fm.ID, dir)
		newName := sstable.NewFilename(fm.ID, dir2)
		err = os.Link(oldName, newName)
		require.NoError(t, err)
		if fm.Level > 0 {
			err = os.Link(sstable.IndexFilename(oldName), sstable.IndexFilename(newName))
			require.NoError(t, err)
		}
	}
	opts2 := getTestOptions(dir2)
	opts2.NumCompactors = 2
	opts2.NumLevelZeroTables = 1
	opts2.CFs = []CFConfig{{Managed: true}, {Managed: false}, {Managed: false}}
	opts2.IDAllocator = alloc
	db2, err := OpenShardingDB(opts2)
	require.NoError(t, err)
	sc2 := &shardingCase{
		t: t,
		n: 5000,
	}
	sc2.loadData(db2)
	require.NoError(t, db2.Ingest(ingestTree))
	sc2.n = 10000
	sc2.checkData(db2)
}

func TestSplitSuggestion(t *testing.T) {
	dir, err := ioutil.TempDir("", "sharding")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	alloc := new(localIDAllocator)
	opts := getTestOptions(dir)
	opts.NumCompactors = 2
	opts.NumLevelZeroTables = 1
	opts.CFs = []CFConfig{{Managed: true}, {Managed: false}, {Managed: false}}
	opts.IDAllocator = alloc
	db, err := OpenShardingDB(opts)
	require.NoError(t, err)
	sc := &shardingCase{
		t: t,
		n: 20000,
	}
	sc.loadData(db)
	time.Sleep(time.Second * 2)
	keys := db.GetSplitSuggestion(opts.MaxMemTableSize)
	log.S().Infof("split keys %s", keys)
	require.Greater(t, len(keys), 2)
	require.NoError(t, db.Close())
}

func TestShardingMetaChangeListener(t *testing.T) {
	dir, err := ioutil.TempDir("", "sharding")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	opts := getTestOptions(dir)
	opts.NumCompactors = 2
	opts.NumLevelZeroTables = 1
	opts.CFs = []CFConfig{{Managed: false}}
	l := new(metaListener)
	opts.MetaChangeListener = l
	db, err := OpenShardingDB(opts)
	require.NoError(t, err)
	numKeys := 1000
	numVers := 10
	for ver := 0; ver < numVers; ver++ {
		wb := db.NewWriteBatch()
		for j := 0; j < numKeys; j++ {
			key := iToKey(j)
			val := append(iToKey(j), iToKey(ver)...)
			require.NoError(t, wb.Put(0, key, y.ValueStruct{Value: val}))
		}
		err := db.Write(wb)
		require.NoError(t, err)
	}
	allCommitTS := l.getAllCommitTS()
	require.True(t, len(allCommitTS) > 0)
	snap := db.NewSnapshot(nil, nil)
	allVals := map[string]struct{}{}
	for i := 0; i < numKeys; i++ {
		key := iToKey(i)
		for _, commitTS := range allCommitTS {
			item, _ := snap.Get(0, y.KeyWithTs(key, commitTS))
			if item == nil {
				continue
			}
			allVals[string(item.vptr)] = struct{}{}
		}
	}
	log.S().Info("all values ", len(allVals))
	// With old version, we can get the old values, so the number of all values is greater than number of keys.
	require.Greater(t, len(allVals), numKeys)
	db.Close()
}

type metaListener struct {
	lock     sync.Mutex
	commitTS []uint64
}

func (l *metaListener) OnChange(e *protos.MetaChangeEvent) {
	l.lock.Lock()
	for _, addedL0 := range e.AddedL0Files {
		l.commitTS = append(l.commitTS, addedL0.CommitTS)
	}
	l.lock.Unlock()
}

func (l *metaListener) getAllCommitTS() []uint64 {
	var result []uint64
	l.lock.Lock()
	result = append(result, l.commitTS...)
	l.lock.Unlock()
	return result
}

func runPprof() {
	go func() {
		http.ListenAndServe(":9291", nil)
	}()
}
