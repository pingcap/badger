package badger

import (
	"encoding/binary"
	"github.com/pingcap/badger/protos"
	"github.com/pingcap/badger/table/sstable"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"hash/crc32"
	"io"
	"os"
)

type shardSplitWAL struct {
	fd  *os.File
	buf []byte
}

func newShardSplitWAL(filename string) (*shardSplitWAL, error) {
	fd, err := y.OpenSyncedFile(filename, false)
	if err != nil {
		return nil, errors.New("failed to create shard split WAL")
	}
	return &shardSplitWAL{fd: fd, buf: make([]byte, 8)}, nil
}

const (
	walTypeEntry    = 0
	walTypeProperty = 1
	walTypeSwitch   = 2
	walTypeFinish   = 3
)

func (wal *shardSplitWAL) appendEntry(splitIdx int, cf int, key []byte, val y.ValueStruct) {
	wal.buf = append(wal.buf, walTypeEntry)
	wal.buf = append(wal.buf, byte(splitIdx))
	wal.buf = append(wal.buf, byte(cf))
	keyLen := uint16(len(key))
	wal.buf = append(wal.buf, byte(keyLen), byte(keyLen>>8))
	wal.buf = append(wal.buf, key...)
	valLen := val.EncodedSize()
	lenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuf, valLen)
	wal.buf = append(wal.buf, lenBuf...)
	wal.buf = val.EncodeTo(wal.buf)
}

func (wal *shardSplitWAL) appendProperty(key string, val []byte) {
	wal.buf = append(wal.buf, walTypeProperty)
	keyLen := uint16(len(key))
	wal.buf = append(wal.buf, byte(keyLen), byte(keyLen>>8))
	wal.buf = append(wal.buf, key...)
	valLen := uint16(len(val))
	wal.buf = append(wal.buf, byte(valLen), byte(valLen>>8))
	wal.buf = append(wal.buf, val...)
}

func (wal *shardSplitWAL) finish(properties []*protos.ShardProperties) error {
	for _, props := range properties {
		wal.buf = append(wal.buf, walTypeFinish)
		val, _ := props.Marshal()
		valLen := uint16(len(val))
		wal.buf = append(wal.buf, byte(valLen), byte(valLen>>8))
		wal.buf = append(wal.buf, val...)
	}
	err := wal.flush()
	if err != nil {
		return err
	}
	err = wal.sync()
	if err != nil {
		return err
	}
	return wal.fd.Close()
}

func (wal *shardSplitWAL) appendSwitchMemTable(splitIdx int, minSize uint64) {
	wal.buf = append(wal.buf, walTypeSwitch)
	wal.buf = append(wal.buf, byte(splitIdx))
	wal.buf = append(wal.buf, sstable.U64ToBytes(minSize)...)
}

func (wal *shardSplitWAL) flush() error {
	length := uint32(len(wal.buf) - 8)
	binary.BigEndian.PutUint32(wal.buf, length)
	checksum := crc32.Checksum(wal.buf[8:], crc32.MakeTable(crc32.Castagnoli))
	binary.BigEndian.PutUint32(wal.buf[4:], checksum)
	_, err := wal.fd.Write(wal.buf)
	wal.buf = wal.buf[:8]
	return err
}

func (wal *shardSplitWAL) sync() error {
	return wal.fd.Sync()
}

func (wal *shardSplitWAL) replay(fn replayFn) error {
	_, err := wal.fd.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	var offset int64
	for {
		var buf []byte
		buf, err = readChecksumPacket(wal.fd)
		if err != nil {
			return err
		}
		if len(buf) == 0 {
			err = wal.fd.Truncate(offset)
			if err != nil {
				return err
			}
			_, err = wal.fd.Seek(offset, 0)
			return err
		}
		offset += 8 + int64(len(buf))
		err = fn(buf)
		if err != nil {
			return err
		}
	}
}

func (wal *shardSplitWAL) decodeByte(packet []byte) (remain []byte, b byte) {
	return packet[1:], packet[0]
}

func (wal *shardSplitWAL) decodeShortVal(packet []byte) (remain, val []byte) {
	l := binary.LittleEndian.Uint16(packet)
	packet = packet[2:]
	return packet[l:], packet[:l]
}

func (wal *shardSplitWAL) decodeLongVal(packet []byte) (remain, val []byte) {
	l := binary.LittleEndian.Uint32(packet)
	packet = packet[4:]
	return packet[l:], packet[:l]
}

func (s *Shard) enableWAL() error {
	fd, err := y.OpenTruncFile(s.walFilename, false)
	if err != nil {
		return err
	}
	s.wal = &shardSplitWAL{fd: fd}
	return nil
}

func (s *Shard) disableWAL() error {
	if s.wal != nil {
		_ = s.wal.fd.Close()
		s.wal = nil
		return os.Remove(s.walFilename)
	}
	return nil
}

type replayFn = func(packet []byte) error

func (sdb *ShardingDB) replayWAL(shard *Shard) error {
	// The wal must exists if we are in pre-split state.
	wal, err := newShardSplitWAL(shard.walFilename)
	if err != nil {
		return err
	}
	shard.wal = wal
	return wal.replay(func(packet []byte) error {
		var props []*protos.ShardProperties
		for len(packet) > 0 {
			tp := packet[0]
			packet = packet[1:]
			switch tp {
			case walTypeSwitch:
				splitIdx := int(packet[0])
				packet = packet[1:]
				minSize := binary.LittleEndian.Uint64(packet)
				packet = packet[8:]
				sdb.switchSplittingMemTable(shard, splitIdx, int64(minSize))
			case walTypeEntry:
				var splitIdx, cf int
				splitIdx = int(packet[0])
				packet = packet[1:]
				cf = int(packet[0])
				packet = packet[1:]
				var key, valBin []byte
				packet, key = wal.decodeShortVal(packet)
				packet, valBin = wal.decodeLongVal(packet)
				var val y.ValueStruct
				val.Decode(valBin)
				mem := shard.loadSplittingWritableMemTable(splitIdx)
				if !sdb.opt.CFs[cf].Managed && val.Version > sdb.orc.curRead {
					sdb.orc.curRead = val.Version
					sdb.orc.nextCommit = val.Version + 1
				}
				mem.Put(cf, key, val)
			case walTypeProperty:
				var key, val []byte
				packet, key = wal.decodeShortVal(packet)
				packet, val = wal.decodeShortVal(packet)
				shard.properties.set(string(key), y.Copy(val))
			case walTypeFinish:
				var val []byte
				packet, val = wal.decodeShortVal(packet)
				prop := new(protos.ShardProperties)
				err = prop.Unmarshal(val)
				y.Assert(err == nil)
				props = append(props, prop)
			}
		}
		if len(props) > 0 {
			_, flushTask := sdb.buildSplitShards(shard, props)
			err1 := sdb.flushFinishSplit(flushTask)
			if err1 != nil {
				return err1
			}
		}
		return nil
	})
}

func takeByte(buf []byte) ([]byte, byte) {
	return buf[1:], buf[0]
}
