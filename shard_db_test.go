package badger

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/pingcap/badger/protos"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestShardingDB(t *testing.T) {
	runPprof()
	dir, err := ioutil.TempDir("", "sharding")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	opts := getTestOptions(dir)
	opts.NumCompactors = 1
	opts.NumLevelZeroTables = 1
	opts.CFs = []CFConfig{{Managed: true}, {Managed: false}, {Managed: false}}
	db, err := OpenShardingDB(opts)
	require.NoError(t, err)
	initialIngest(t, db)
	sc := &shardingCase{
		t:      t,
		n:      10000,
		tester: newShardTester(db),
	}
	ch := make(chan time.Duration, 1)
	go func() {
		time.Sleep(time.Millisecond * 100)
		begin := time.Now()
		ver := uint64(1)
		for i := 1000; i < 10000; i += 3000 {
			sc.preSplit(1, ver, iToKey(i))
			sc.finishSplit(1, ver, []uint64{uint64(i), 1})
			ver += 1
		}
		ch <- time.Since(begin)
	}()
	begin := time.Now()
	sc.loadData()
	log.S().Infof("time split %v; load %v", <-ch, time.Since(begin))
	db.PrintStructure()
	sc.checkData()
	err = db.Close()
	require.NoError(t, err)
	//db, err = OpenShardingDB(opts)
	//require.NoError(t, err)
	//db.PrintStructure()
	//sc.tester.db = db
	//sc.checkData()
}

func TestWALRecovery(t *testing.T) {
	runPprof()
	dir, err := ioutil.TempDir("", "sharding")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	opts := getTestOptions(dir)
	opts.NumCompactors = 1
	opts.NumLevelZeroTables = 1
	opts.CFs = []CFConfig{{Managed: true}, {Managed: false}, {Managed: false}}
	db, err := OpenShardingDB(opts)
	require.NoError(t, err)
	initialIngest(t, db)
}

type shardingCase struct {
	t      *testing.T
	n      int
	tester *shardTester
}

func iToKey(i int) []byte {
	return []byte(fmt.Sprintf("key%06d", i))
}

func (sc *shardingCase) loadData() {
	var entries []*testerEntry
	for i := 0; i < sc.n; i++ {
		key := iToKey(i)
		entries = append(entries, &testerEntry{
			cf:  0,
			key: key,
			val: key,
			ver: 1,
		}, &testerEntry{
			cf:  1,
			key: key,
			val: bytes.Repeat(key, 2),
		})
		if i%100 == 99 {
			err := sc.tester.write(entries...)
			require.NoError(sc.t, err)
			entries = nil
		}
	}
}

func initialIngest(t *testing.T, db *ShardingDB) {
	err := db.Ingest(&IngestTree{
		ChangeSet: &protos.ShardChangeSet{
			ShardID:  1,
			ShardVer: 1,
			ShardCreate: &protos.ShardCreate{
				StartKey:   nil,
				EndKey:     globalShardEndKey,
				Properties: &protos.ShardProperties{ShardID: 1},
			},
		},
	})
	require.NoError(t, err)
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

func (sc *shardingCase) preSplit(shardID, ver uint64, keys ...[]byte) {
	err := sc.tester.preSplit(shardID, ver, keys)
	require.NoError(sc.t, err)
}

func (sc *shardingCase) finishSplit(shardID, ver uint64, newIDs []uint64) {
	newProps := make([]*protos.ShardProperties, len(newIDs))
	for i, newID := range newIDs {
		newProps[i] = &protos.ShardProperties{ShardID: newID}
	}
	err := sc.tester.finishSplit(shardID, ver, newProps)
	require.NoError(sc.t, err)
}

func (sc *shardingCase) checkData() {
	var i int
	err := sc.tester.iterate(nil, globalShardEndKey, 0, func(key, val []byte) {
		require.Equal(sc.t, string(iToKey(i)), string(key))
		require.Equal(sc.t, string(key), string(val))
		i++
	})
	require.Nil(sc.t, err)
	require.Equal(sc.t, i, sc.n)
	i = 0
	err = sc.tester.iterate(nil, globalShardEndKey, 1, func(key, val []byte) {
		require.Equal(sc.t, key, iToKey(i))
		require.Equal(sc.t, string(val), strings.Repeat(string(key), 2))
		i++
	})
	require.Nil(sc.t, err)
	require.Equal(sc.t, i, sc.n)
}

func TestIngestTree(t *testing.T) {
	dir, err := ioutil.TempDir("", "sharding")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
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
	initialIngest(t, db)
	sc := &shardingCase{
		t:      t,
		n:      20000,
		tester: newShardTester(db),
	}
	sc.loadData()
	time.Sleep(time.Second * 2)
	keys := db.GetSplitSuggestion(1, opts.MaxMemTableSize)
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
	initialIngest(t, db)

	numKeys := 1000
	numVers := 10
	for ver := 0; ver < numVers; ver++ {
		wb := db.NewWriteBatch(db.GetShard(1))
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
	snap := db.NewSnapshot(db.GetShard(1))
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
	for _, l0 := range e.L0Creates {
		if l0.Properties != nil {
			for i, key := range l0.Properties.Keys {
				if key == commitTSKey {
					l.commitTS = append(l.commitTS, binary.LittleEndian.Uint64(l0.Properties.Values[i]))
				}
			}
		}
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

var once = sync.Once{}

func runPprof() {
	once.Do(func() {
		go func() {
			http.ListenAndServe(":9291", nil)
		}()
	})
}
