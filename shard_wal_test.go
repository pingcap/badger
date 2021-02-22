package badger

import (
	"encoding/binary"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"testing"
)

func TestShardingWAL(t *testing.T) {
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
		tester: newShardTester(db),
	}
	sc.loadData(0, 2000)
	sc.loadData(3000, 5000)
	sc.preSplit(1, 1, iToKey(2500))
	sc.loadData(2000, 3000)
	db.PrintStructure()
	err = db.Close()
	require.NoError(t, err)
	sc.tester.close()
	db, err = OpenShardingDB(opts)
	require.NoError(t, err)
	sc.tester = newShardTester(db)
	sc.checkData(0, 5000)
	sc.loadData(5000, 6000)
	sc.finishSplit(1, 1, []uint64{1, 2})
	err = db.Close()
	require.NoError(t, err)
	sc.tester.close()
	db, err = OpenShardingDB(opts)
	require.NoError(t, err)
	sc.tester = newShardTester(db)
	require.True(t, len(sc.tester.loadShardTree().shards) == 2)
	sc.checkData(0, 6000)
}

func TestLogRecovery(t *testing.T) {
	dir, err := ioutil.TempDir("", "sharding")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	opts := getTestOptions(dir)
	opts.NumCompactors = 1
	opts.NumLevelZeroTables = 1
	opts.CFs = []CFConfig{{Managed: true}, {Managed: false}}
	db, err := OpenShardingDB(opts)
	require.NoError(t, err)
	initialIngest(t, db)
	sc := &shardingCase{
		t:      t,
		tester: newShardTester(db),
	}
	sc.loadData(0, 2000)
	err = db.Close()
	require.NoError(t, err)
	sc.tester.close()
	db, err = OpenShardingDB(opts)
	require.NoError(t, err)
	oldTesterTree := sc.tester.loadShardTree()
	sc.tester = newShardTester(db)
	newTesterTree := sc.tester.loadShardTree()
	for i, oldShard := range oldTesterTree.shards {
		nShard := newTesterTree.shards[i]
		nShard.writeLog = oldShard.writeLog
		idxBin, ok := nShard.GetProperty(appliedIndex)
		require.True(t, ok)
		idx := int(binary.LittleEndian.Uint32(idxBin))
		log.S().Infof("applied index %d, logLen %d", idx, nShard.writeLog.length())
		var requests []*testerWriteRequest
		for i := idx; i < nShard.writeLog.length(); i++ {
			entries := nShard.writeLog.entries[i]
			req := &testerWriteRequest{entries: entries, shard: nShard, resp: make(chan error, 1), replay: true}
			sc.tester.writeCh <- req
			requests = append(requests, req)
		}
		for _, req := range requests {
			err = <-req.resp
			require.Nil(t, err)
		}
	}
	sc.checkData(0, 2000)
}
