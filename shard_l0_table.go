package badger

import (
	"bytes"
	"encoding/binary"
	"io/ioutil"
	"os"
	"sort"
	"sync/atomic"

	"github.com/pingcap/badger/table/sstable"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type shardL0Table struct {
	fd       *os.File
	data     []byte
	l0Shards []*l0Shard
	fid      uint32
}

func openShardL0Table(filename string, fid uint32) (*shardL0Table, error) {
	log.S().Infof("open shard l0 table %d", fid)
	shardIdxData, err := ioutil.ReadFile(sstable.IndexFilename(filename))
	if err != nil {
		return nil, err
	}
	shardIdx := &l0ShardIndex{}
	shardIdx.decode(shardIdxData)
	fd, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	data, err := y.Mmap(fd, false, int64(shardIdx.endOffsets[len(shardIdx.endOffsets)-1]))
	if err != nil {
		return nil, err
	}
	shardL0Table := &shardL0Table{
		fid:      fid,
		fd:       fd,
		data:     data,
		l0Shards: make([]*l0Shard, len(shardIdx.endOffsets)),
	}
	for i, startKey := range shardIdx.startKeys {
		l0Shard := &l0Shard{
			start: startKey,
		}
		if i == len(shardIdx.startKeys)-1 {
			l0Shard.end = shardIdx.endKey
		} else {
			l0Shard.end = shardIdx.startKeys[i+1]
		}
		startOff := uint32(0)
		if i != 0 {
			startOff = shardIdx.endOffsets[i-1]
		}
		endOff := shardIdx.endOffsets[i]
		shardData := data[startOff:endOff]

		if err = l0Shard.loadTables(shardData); err != nil {
			return nil, err
		}
		shardL0Table.l0Shards[i] = l0Shard
	}
	return shardL0Table, nil
}

type l0Shard struct {
	start []byte
	end   []byte
	cfs   []*sstable.Table
}

func (s *l0Shard) loadTables(shardData []byte) error {
	numCF := int(shardData[len(shardData)-1])
	shardData = shardData[:len(shardData)-1]
	s.cfs = make([]*sstable.Table, 0, numCF)
	cfIdx := shardData[len(shardData)-numCF*8:]
	for i := 0; i < len(cfIdx); i += 8 {
		dataStartOff := uint32(0)
		if i != 0 {
			dataStartOff = binary.LittleEndian.Uint32(cfIdx[i-4:])
		}
		dataEndOff := binary.LittleEndian.Uint32(cfIdx[i:])
		data := shardData[dataStartOff:dataEndOff]
		if len(data) == 0 {
			s.cfs = append(s.cfs, nil)
			continue
		}
		idxEndOff := binary.LittleEndian.Uint32(cfIdx[i+4:])
		idxData := shardData[dataEndOff:idxEndOff]
		inMemFile := sstable.NewInMemFile(data, idxData)
		tbl, err := sstable.OpenInMemoryTable(inMemFile)
		if err != nil {
			return err
		}
		s.cfs = append(s.cfs, tbl)
	}
	return nil
}

func (t *shardL0Table) Get(cf byte, key y.Key, keyHash uint64) y.ValueStruct {
	idx := sort.Search(len(t.l0Shards), func(i int) bool {
		shard := t.l0Shards[i]
		return bytes.Compare(shard.end, key.UserKey) > 0
	})
	if idx == len(t.l0Shards) {
		return y.ValueStruct{}
	}
	if idx == 0 && bytes.Compare(t.l0Shards[0].start, key.UserKey) > 0 {
		return y.ValueStruct{}
	}
	tbl := t.l0Shards[idx].cfs[cf]
	if tbl == nil {
		return y.ValueStruct{}
	}
	v, err := tbl.Get(key, keyHash)
	if err != nil {
		// TODO: handle error
		log.Error("get data in table failed", zap.Error(err))
	}
	return v
}

type shardL0Tables struct {
	tables []*shardL0Table
}

func (sdb *ShardingDB) loadShardL0Tables() *shardL0Tables {
	return (*shardL0Tables)(atomic.LoadPointer(&sdb.l0Tbls))
}
