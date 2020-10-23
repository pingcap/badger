package badger

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"testing"

	"github.com/pingcap/badger/y"
	"github.com/stretchr/testify/require"
	_ "net/http/pprof"
)

func TestShardingDB(t *testing.T) {
	go func() {
		http.ListenAndServe(":9291", nil)
	}()
	dir, err := ioutil.TempDir("", "sharding")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	opts := getTestOptions(dir)
	opts.NumCFs = 2
	db, err := OpenShardingDB(opts)
	require.NoError(t, err)

	wb := NewWriteBatch()
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("key%04d", i))
		wb.Put(0, key, y.ValueStruct{Value: key, Version: 1})
		wb.Put(1, key, y.ValueStruct{Value: bytes.Repeat(key, 2), Version: 1})
		if i%100 == 99 {
			err = db.Write(wb)
			require.NoError(t, err)
			wb = NewWriteBatch()
		}
	}
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("key%04d", i))
		val := db.Get(0, y.KeyWithTs(key, 2))
		require.Equal(t, string(key), string(val.Value))
		val2 := db.Get(1, y.KeyWithTs(key, 2))
		require.Equal(t, string(val2.Value), string(key)+string(key))
	}
	err = db.Close()
	require.NoError(t, err)
	db, err = OpenShardingDB(opts)
	require.NoError(t, err)
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("key%04d", i))
		val := db.Get(0, y.KeyWithTs(key, 2))
		require.Equal(t, string(val.Value), string(key))
		val2 := db.Get(1, y.KeyWithTs(key, 2))
		require.Equal(t, string(val2.Value), string(key)+string(key))
	}
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
	tree := &shardByKeyTree{shards: shards}
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
