package badger

import (
	"bytes"
	"encoding/binary"
	"io/ioutil"
	"math"
	"os"
	"sort"

	"github.com/pingcap/badger/table"
	"github.com/pingcap/badger/table/sstable"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// globalL0Table contains data in all shards.
type globalL0Table struct {
	fd       *os.File
	data     []byte
	l0Shards []*l0Shard
	cfs      [][]table.Table
	fid      uint32
}

// l0 index file format
//  | numShards(4) | shardDataOffsets(4) ... | shardKeys(2 + len(key)) ...
type l0ShardIndex struct {
	startKeys  [][]byte
	endKey     []byte
	endOffsets []uint32
}

func (idx *l0ShardIndex) encode() []byte {
	l := 4 + 4 + 4 + len(idx.endOffsets)*4
	for _, key := range idx.startKeys {
		l += 2 + len(key)
	}
	l += 2 + len(idx.endKey)
	data := make([]byte, l)
	off := 0
	y.Assert(len(idx.endOffsets) > 0)
	binary.LittleEndian.PutUint32(data[off:], uint32(len(idx.endOffsets)))
	off += 4
	for _, endOff := range idx.endOffsets {
		binary.LittleEndian.PutUint32(data[off:], endOff)
		off += 4
	}
	for _, startKey := range idx.startKeys {
		binary.LittleEndian.PutUint16(data[off:], uint16(len(startKey)))
		off += 2
		copy(data[off:], startKey)
		off += len(startKey)
	}
	binary.LittleEndian.PutUint16(data[off:], uint16(len(idx.endKey)))
	off += 2
	copy(data[off:], idx.endKey)
	return data
}

func (idx *l0ShardIndex) decode(data []byte) {
	off := 0
	numShard := int(binary.LittleEndian.Uint32(data[off:]))
	y.Assert(numShard > 0)
	off += 4
	idx.endOffsets = sstable.BytesToU32Slice(data[off : off+numShard*4])
	off += numShard * 4
	idx.startKeys = make([][]byte, numShard)
	for i := 0; i < numShard; i++ {
		keyLen := int(binary.LittleEndian.Uint16(data[off:]))
		off += 2
		idx.startKeys[i] = data[off : off+keyLen]
		off += keyLen
	}
	endKeyLen := int(binary.LittleEndian.Uint16(data[off:]))
	off += 2
	idx.endKey = data[off : off+endKeyLen]
}

func openGlobalL0Table(filename string, fid uint32) (*globalL0Table, error) {
	globalL0IdxData, err := ioutil.ReadFile(sstable.IndexFilename(filename))
	if err != nil {
		return nil, err
	}
	shardIdx := &l0ShardIndex{}
	shardIdx.decode(globalL0IdxData)
	fd, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	data, err := y.Mmap(fd, false, int64(shardIdx.endOffsets[len(shardIdx.endOffsets)-1]))
	if err != nil {
		return nil, err
	}
	shardL0Table := &globalL0Table{
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
	numCFs := len(shardL0Table.l0Shards[0].cfs)
	numShards := len(shardL0Table.l0Shards)
	shardL0Table.cfs = make([][]table.Table, numCFs)
	for i := 0; i < numCFs; i++ {
		cfTables := make([]table.Table, 0, numShards)
		for j := 0; j < numShards; j++ {
			tbl := shardL0Table.l0Shards[j].cfs[i]
			if tbl != nil {
				cfTables = append(cfTables, tbl)
			}
		}
		shardL0Table.cfs[i] = cfTables
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

func (t *globalL0Table) Get(cf byte, key y.Key, keyHash uint64) y.ValueStruct {
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

func (t *globalL0Table) newIterator(cf byte, reverse bool, stareKey, endKey []byte) y.Iterator {
	if len(t.cfs[cf]) == 0 {
		return nil
	}
	if len(endKey) == 0 {
		endKey = globalShardEndKey
	}
	tbls := t.cfs[cf]
	left, right := getTablesInRange(tbls, y.KeyWithTs(stareKey, math.MaxUint64), y.KeyWithTs(endKey, 0))
	if left == right {
		return nil
	}
	return table.NewConcatIterator(tbls[left:right], reverse)
}

type globalL0Tables struct {
	tables []*globalL0Table
}
