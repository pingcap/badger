package badger

import (
	"bytes"
	"fmt"
	"io/ioutil"
	_ "net/http/pprof"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/badger/y"
	"github.com/stretchr/testify/require"
)

func TestShardingDB(t *testing.T) {
	dir, err := ioutil.TempDir("", "sharding")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	opts := getTestOptions(dir)
	opts.NumCompactors = 2
	opts.NumLevelZeroTables = 1
	opts.CFs = []CFConfig{{Managed: true}, {Managed: false}, {Managed: false}, {Managed: false}}
	db, err := OpenShardingDB(opts)
	require.NoError(t, err)
	sc := &shardingCase{
		t: t,
		n: 20000,
	}
	sc.loadData(db)
	sc.checkData(db)
	time.Sleep(time.Second * 2)
	err = db.Close()
	require.NoError(t, err)
	db, err = OpenShardingDB(opts)
	require.NoError(t, err)
	sc.checkData(db)
}

type shardingCase struct {
	t *testing.T
	n int
}

func (sc *shardingCase) iToKey(i int) []byte {
	return []byte(fmt.Sprintf("key%06d", i))
}

func (sc *shardingCase) loadData(db *ShardingDB) {
	wb := NewWriteBatch(db.opt.CFs)
	for i := 0; i < sc.n; i++ {
		key := sc.iToKey(i)
		require.NoError(sc.t, wb.Put(0, key, y.ValueStruct{Value: key, Version: 1}))
		require.NoError(sc.t, wb.Put(1, key, y.ValueStruct{Value: bytes.Repeat(key, 2)}))
		require.NoError(sc.t, wb.Put(2, key, y.ValueStruct{Value: bytes.Repeat(key, 3)}))
		if i%100 == 99 {
			err := db.Write(wb)
			require.NoError(sc.t, err)
			wb = NewWriteBatch(db.opt.CFs)
		}
	}
}

func (sc *shardingCase) checkGet(snap *Snapshot) {
	for i := 0; i < sc.n; i++ {
		key := sc.iToKey(i)
		val := snap.Get(0, y.KeyWithTs(key, 2))
		require.Equal(sc.t, string(val.Value), string(key))
		val2 := snap.Get(1, y.KeyWithTs(key, 0))
		require.Equal(sc.t, string(val2.Value), strings.Repeat(string(key), 2))
		val3 := snap.Get(2, y.KeyWithTs(key, 0))
		require.Equal(sc.t, string(val3.Value), strings.Repeat(string(key), 3))
	}
}

func (sc *shardingCase) checkIterator(snap *Snapshot) {
	for cf := byte(0); cf < 3; cf++ {
		iter := snap.NewIterator(cf, false, false)
		i := 0
		for iter.Rewind(); iter.Valid(); iter.Next() {
			key := sc.iToKey(i)
			item := iter.Item()
			require.EqualValues(sc.t, key, item.key.UserKey)
			val, err1 := item.Value()
			require.NoError(sc.t, err1)
			require.EqualValues(sc.t, bytes.Repeat(key, int(cf)+1), val)
			i++
		}
		require.Equal(sc.t, sc.n, i)
	}
}

func (sc *shardingCase) checkData(db *ShardingDB) {
	snap := db.NewSnapshot(nil, nil)
	sc.checkGet(snap)
	sc.checkIterator(snap)
	snap.Discard()
}

func TestShardingTree(t *testing.T) {
	shardKeys := [][]byte{nil, []byte("a"), []byte("b"), []byte("c"), nil}
	var shards []*Shard
	for i := 0; i < 4; i++ {
		shards = append(shards, &Shard{
			ID:    uint32(i + 1),
			Start: shardKeys[i],
			End:   shardKeys[i+1],
		})
	}
	tree := &shardTree{shards: shards}
	shard := tree.get([]byte(""))
	require.Equal(t, shard.ID, uint32(1))
	shard = tree.get([]byte("a"))
	require.Equal(t, shard.ID, uint32(2))
	shard = tree.get([]byte("abc"))
	require.Equal(t, shard.ID, uint32(2))
	shard = tree.get([]byte("b"))
	require.Equal(t, shard.ID, uint32(3))
	shard = tree.get([]byte("bcd"))
	require.Equal(t, shard.ID, uint32(3))
	shard = tree.get([]byte("cde"))
	require.Equal(t, shard.ID, uint32(4))
	shard = tree.get([]byte("fsd"))
	require.Equal(t, shard.ID, uint32(4))
}
