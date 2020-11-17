package badger

import (
	"bytes"
	"fmt"
	"github.com/pingcap/badger/table/sstable"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"
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
			sc.split(db, sc.iToKey(i), sc.iToKey(i+2000))
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
	sc.split(db, sc.iToKey(10000))
	err = db.DeleteRange(sc.iToKey(1000), sc.iToKey(3000))
	require.NoError(t, err)
	err = db.DeleteRange(sc.iToKey(18000), sc.iToKey(20000))
	require.NoError(t, err)
	err = db.DeleteRange(sc.iToKey(9000), sc.iToKey(11000))
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

func (sc *shardingCase) iToKey(i int) []byte {
	return []byte(fmt.Sprintf("key%06d", i))
}

func (sc *shardingCase) loadData(db *ShardingDB) {
	wb := db.NewWriteBatch()
	for i := 0; i < sc.n; i++ {
		key := sc.iToKey(i)
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
		key := sc.iToKey(i)
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
			key := sc.iToKey(i)
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
	sc.split(db, sc.iToKey(5000), sc.iToKey(10000))
	require.NoError(t, db.Close())
	opts.DoNotCompact = true
	db, err = OpenShardingDB(opts)
	require.NoError(t, err)
	ingestTree := db.GetShardTree(sc.iToKey(5000))
	require.NoError(t, db.Close())
	dir2, err := ioutil.TempDir("", "sharding")
	require.NoError(t, err)
	defer os.RemoveAll(dir2)
	for _, id := range ingestTree.L0 {
		oldName := sstable.NewFilename(id, dir)
		newName := sstable.NewFilename(id, dir2)
		err = os.Link(oldName, newName)
		require.NoError(t, err)
	}
	for _, cf := range ingestTree.CFs {
		for _, level := range cf.Levels {
			for _, id := range level.TableIDs {
				oldName := sstable.NewFilename(id, dir)
				newName := sstable.NewFilename(id, dir2)
				err = os.Link(oldName, newName)
				require.NoError(t, err)
				err = os.Link(sstable.IndexFilename(oldName), sstable.IndexFilename(newName))
				require.NoError(t, err)
			}
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

func runPprof() {
	go func() {
		http.ListenAndServe(":9291", nil)
	}()
}
