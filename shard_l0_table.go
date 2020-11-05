package badger

import (
	"encoding/binary"
	"io/ioutil"
	"os"

	"github.com/pingcap/badger/table/sstable"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type shardL0Table struct {
	cfs      []*sstable.Table
	fid      uint32
	filename string
	size     int64
}

func (st *shardL0Table) Delete() error {
	return os.Remove(st.filename)
}

func openShardL0Table(filename string, fid uint32) (*shardL0Table, error) {
	shardData, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	l0 := &shardL0Table{
		fid:      fid,
		filename: filename,
		size:     int64(len(shardData)),
	}
	numCF := int(shardData[len(shardData)-1])
	shardData = shardData[:len(shardData)-1]
	l0.cfs = make([]*sstable.Table, 0, numCF)
	cfIdx := shardData[len(shardData)-numCF*8:]
	for i := 0; i < len(cfIdx); i += 8 {
		dataStartOff := uint32(0)
		if i != 0 {
			dataStartOff = binary.LittleEndian.Uint32(cfIdx[i-4:])
		}
		dataEndOff := binary.LittleEndian.Uint32(cfIdx[i:])
		data := shardData[dataStartOff:dataEndOff]
		if len(data) == 0 {
			l0.cfs = append(l0.cfs, nil)
			continue
		}
		idxEndOff := binary.LittleEndian.Uint32(cfIdx[i+4:])
		idxData := shardData[dataEndOff:idxEndOff]
		inMemFile := sstable.NewInMemFile(data, idxData)
		tbl, err := sstable.OpenInMemoryTable(inMemFile)
		if err != nil {
			return nil, err
		}
		l0.cfs = append(l0.cfs, tbl)
	}
	return l0, nil
}

func (sl0 *shardL0Table) Get(cf int, key y.Key, keyHash uint64) y.ValueStruct {
	v, err := sl0.cfs[cf].Get(key, keyHash)
	if err != nil {
		// TODO: handle error
		log.Error("get data in table failed", zap.Error(err))
	}
	return v
}

func (sl0 *shardL0Table) newIterator(cf int, reverse bool) y.Iterator {
	tbl := sl0.cfs[cf]
	if tbl == nil {
		return nil
	}
	return tbl.NewIterator(reverse)
}

type shardL0Tables struct {
	tables []*shardL0Table
}

func (sl0s *shardL0Tables) totalSize() int64 {
	var size int64
	for _, tbl := range sl0s.tables {
		size += tbl.size
	}
	return size
}
