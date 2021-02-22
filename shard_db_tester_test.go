package badger

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/pingcap/badger/protos"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

type shardTester struct {
	shardIDAlloc uint64
	writeCh      chan interface{}
	db           *ShardingDB
	shardTree    unsafe.Pointer
	wg           sync.WaitGroup
	requestLog   []*testerWriteRequest
}

func newShardTester(db *ShardingDB) *shardTester {
	tester := &shardTester{
		shardIDAlloc: 0,
		writeCh:      make(chan interface{}, 256),
		db:           db,
	}
	var shards []*shardWithWriteLog
	db.shardMap.Range(func(key, value interface{}) bool {
		shard := value.(*Shard)
		shards = append(shards, &shardWithWriteLog{Shard: shard, writeLog: &testerWriteLog{}})
		return true
	})
	sort.Slice(shards, func(i, j int) bool {
		return bytes.Compare(shards[i].Start, shards[j].Start) < 0
	})
	tester.shardTree = unsafe.Pointer(&testerShardTree{
		shards: shards,
	})
	tester.wg.Add(1)
	go tester.runWriter()
	return tester
}

type testerWriteRequest struct {
	shard   *shardWithWriteLog
	entries []*testerEntry
	replay  bool
	resp    chan error
}

type testerEntry struct {
	cf  int
	key []byte
	val []byte
	ver uint64
}

type testerShardTree struct {
	shards []*shardWithWriteLog
}

type shardWithWriteLog struct {
	*Shard
	writeLog *testerWriteLog
}

type testerWriteLog struct {
	entries [][]*testerEntry
}

func (wl *testerWriteLog) length() int {
	return len(wl.entries)
}

func (tree *testerShardTree) getShard(key []byte) *shardWithWriteLog {
	for _, shd := range tree.shards {
		if bytes.Compare(key, shd.End) >= 0 {
			continue
		}
		if bytes.Compare(shd.Start, key) <= 0 {
			return shd
		}
		break
	}
	return nil
}

func (tree *testerShardTree) buildGetSnapRequests(start, end []byte) []*testerGetSnapRequest {
	var results []*testerGetSnapRequest
	for _, shd := range tree.shards {
		if shd.OverlapRange(start, end) {
			reqStart := shd.Start
			if bytes.Compare(shd.Start, start) < 0 {
				reqStart = start
			}
			reqEnd := shd.End
			if bytes.Compare(shd.End, end) > 0 {
				reqEnd = end
			}
			req := &testerGetSnapRequest{
				start: reqStart,
				end:   reqEnd,
			}
			results = append(results, req)
		}
	}
	return results
}

func (tree *testerShardTree) split(oldID uint64, newShards []*Shard) *testerShardTree {
	newTree := &testerShardTree{}
	for _, shd := range tree.shards {
		if shd.ID == oldID {
			for _, newShd := range newShards {
				if newShd.ID == oldID {
					newTree.shards = append(newTree.shards, &shardWithWriteLog{Shard: newShd, writeLog: shd.writeLog})
				} else {
					newTree.shards = append(newTree.shards, &shardWithWriteLog{Shard: newShd, writeLog: &testerWriteLog{}})
				}
			}
		} else {
			newTree.shards = append(newTree.shards, shd)
		}
	}
	return newTree
}

type testerPreSplitRequest struct {
	shardID uint64
	ver     uint64
	keys    [][]byte
	resp    chan error
}

type testerFinishSplitRequest struct {
	shardID  uint64
	ver      uint64
	newProps []*protos.ShardProperties
	keys     [][]byte
	resp     chan error
}

func (st *shardTester) runWriter() {
	defer st.wg.Done()
	for {
		val := <-st.writeCh
		switch x := val.(type) {
		case nil:
			return
		case *testerWriteRequest:
			st.handleWriteRequest(x)
		case *testerPreSplitRequest:
			x.resp <- st.db.PreSplit(x.shardID, x.ver, x.keys)
		case *testerFinishSplitRequest:
			newShards, err := st.db.FinishSplit(x.shardID, x.ver, x.newProps)
			if err == nil {
				log.S().Info("tester finish split")
				atomic.StorePointer(&st.shardTree, unsafe.Pointer(st.loadShardTree().split(x.shardID, newShards)))
			}
			x.resp <- err
		case *testerGetSnapRequest:
			st.handleIterateRequest(x)
		}
	}
}

func (st *shardTester) handleWriteRequest(req *testerWriteRequest) {
	shard := st.db.GetShard(req.shard.ID)
	if shard.Ver != req.shard.Ver {
		req.resp <- errShardNotMatch
		return
	}
	if !req.replay {
		req.shard.writeLog.entries = append(req.shard.writeLog.entries, req.entries)
	}
	wb := st.db.NewWriteBatch(shard)
	for _, e := range req.entries {
		err := wb.Put(e.cf, e.key, y.ValueStruct{Value: e.val, Version: e.ver})
		if err != nil {
			req.resp <- err
			return
		}
	}
	idxBin := make([]byte, 4)
	binary.LittleEndian.PutUint32(idxBin, uint32(len(req.shard.writeLog.entries)))
	wb.SetProperty(appliedIndex, idxBin)
	err := st.db.Write(wb)
	req.resp <- err
}

const appliedIndex = "applied_index"

func (st *shardTester) handleIterateRequest(req *testerGetSnapRequest) {
	shard := st.db.GetShard(req.shard.ID)
	if shard == nil {
		log.Error("shard not found", zap.Uint64("shard", req.shard.ID))
		req.resp <- errShardNotFound
		return
	}
	if shard.Ver != req.shard.Ver {
		log.S().Infof("shard version not match, %d %d", shard.Ver, req.shard.Ver)
		req.resp <- errShardNotMatch
		return
	}
	req.snap = st.db.NewSnapshot(shard)
	req.resp <- nil
}

func (st *shardTester) loadShardTree() *testerShardTree {
	return (*testerShardTree)(atomic.LoadPointer(&st.shardTree))
}

func (st *shardTester) write(entries ...*testerEntry) error {
	tree := st.loadShardTree()
	requests := make(map[uint64]*testerWriteRequest)
	for _, entry := range entries {
		shard := tree.getShard(entry.key)
		if shard == nil {
			for _, treeShard := range tree.shards {
				log.S().Infof("tree shard %s %s", treeShard.Start, treeShard.End)
			}
			return fmt.Errorf("shard not found for key %s", entry.key)
		}
		shardID := shard.ID
		req, ok := requests[shardID]
		if !ok {
			req = &testerWriteRequest{
				shard: shard,
				resp:  make(chan error, 1),
			}
			requests[shardID] = req
		}
		req.entries = append(req.entries, entry)
	}
	for _, req := range requests {
		st.writeCh <- req
	}
	var retries []*testerEntry
	for _, req := range requests {
		err := <-req.resp
		if err == errShardNotMatch {
			log.S().Infof("write shard not match %s %s %d %d", entries[0].key, entries[len(entries)-1].key, req.shard.ID, req.shard.Ver)
			retries = append(retries, req.entries...)
		} else if err != nil {
			return err
		}
	}
	if len(retries) != 0 {
		time.Sleep(time.Millisecond * 10)
		err := st.write(retries...)
		log.S().Infof("retry %d entries err %v", len(retries), err)
	}
	return nil
}

func (st *shardTester) preSplit(shardID, ver uint64, keys [][]byte) error {
	req := &testerPreSplitRequest{
		shardID: shardID,
		ver:     ver,
		keys:    keys,
		resp:    make(chan error, 1),
	}
	st.writeCh <- req
	return <-req.resp
}

func (st *shardTester) finishSplit(shardID, ver uint64, props []*protos.ShardProperties) error {
	req := &testerFinishSplitRequest{
		shardID:  shardID,
		ver:      ver,
		newProps: props,
		resp:     make(chan error, 1),
	}
	st.writeCh <- req
	return <-req.resp
}

type testerGetSnapRequest struct {
	shard *Shard
	start []byte
	end   []byte
	resp  chan error
	snap  *Snapshot
}

func (st *shardTester) iterate(start, end []byte, cf int, iterFunc func(key, val []byte)) error {
	tree := st.loadShardTree()
	var requests []*testerGetSnapRequest
	for _, shd := range tree.shards {
		if shd.OverlapRange(start, end) {
			reqStart := shd.Start
			if bytes.Compare(shd.Start, start) < 0 {
				reqStart = start
			}
			reqEnd := shd.End
			if bytes.Compare(shd.End, end) > 0 {
				reqEnd = end
			}
			req := &testerGetSnapRequest{
				shard: shd.Shard,
				start: reqStart,
				end:   reqEnd,
				resp:  make(chan error, 1),
			}
			requests = append(requests, req)
		}
	}
	for _, req := range requests {
		st.writeCh <- req
	}
	for _, req := range requests {
		err := <-req.resp
		if err == errShardNotMatch {
			time.Sleep(time.Millisecond * 10)
			err = st.iterate(req.start, req.end, cf, iterFunc)
			if err != nil {
				return err
			}
		} else if err != nil {
			log.Error("iterate req error", zap.Error(err))
			return err
		} else {
			snap := req.snap
			iter := snap.NewIterator(cf, false, false)
			for iter.Seek(req.start); iter.Valid(); iter.Next() {
				item := iter.Item()
				if bytes.Compare(item.Key(), req.end) >= 0 {
					break
				}
				iterFunc(item.Key(), item.vptr)
			}
			iter.Close()
			snap.Discard()
		}
	}
	return nil
}

func (st *shardTester) close() {
	st.writeCh <- nil
	st.wg.Wait()
}
