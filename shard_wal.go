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
	fd, err := y.OpenTruncFile(filename, false)
	if err != nil {
		return nil, errors.New("failed to create shard split WAL")
	}
	return &shardSplitWAL{fd: fd}, nil
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
	length := uint32(len(wal.buf)) + 4
	binary.LittleEndian.PutUint32(wal.buf, length)
	checksum := crc32.Checksum(wal.buf[:4], crc32.MakeTable(crc32.Castagnoli))
	checksumBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(checksumBuf, checksum)
	wal.buf = append(wal.buf, checksumBuf...)
	_, err := wal.fd.Write(wal.buf)
	wal.buf = wal.buf[:4]
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
	for {
		err = wal.readPacket()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if !wal.verifyCheckSum() {
			return io.EOF
		}
		err = fn(wal.buf)
		if err != nil {
			return err
		}
	}
}

func (wal *shardSplitWAL) readPacket() error {
	lenBuf := make([]byte, 4)
	_, err := io.ReadFull(wal.fd, lenBuf)
	if err != nil {
		return err
	}
	length := binary.LittleEndian.Uint32(lenBuf)
	if cap(wal.buf) < int(length) {
		wal.buf = make([]byte, length)
	}
	wal.buf = wal.buf[:length]
	_, err = io.ReadFull(wal.fd, wal.buf)
	if err != nil {
		return err
	}
	return nil
}

func (wal *shardSplitWAL) verifyCheckSum() bool {
	crcVal := crc32.Checksum(wal.buf[:len(wal.buf)-4], crc32.MakeTable(crc32.Castagnoli))
	return crcVal == binary.LittleEndian.Uint32(wal.buf[len(wal.buf)-4:])
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
	fd, err := os.Open(shard.walFilename)
	if err != nil {
		return err
	}
	wal := &shardSplitWAL{fd: fd}
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
				splitIdx := int(packet[0])
				packet = packet[1:]
				cf := int(packet[0])
				packet = packet[1:]
				keyLen := binary.LittleEndian.Uint16(packet)
				packet = packet[2:]
				key := packet[:keyLen]
				packet = packet[keyLen:]
				var val y.ValueStruct
				val.Decode(packet)
				packet = packet[val.EncodedSize():]
				mem := shard.loadSplittingWritableMemTable(splitIdx)
				mem.Put(cf, key, val)
			case walTypeProperty:
				keyLen := binary.LittleEndian.Uint16(packet)
				packet = packet[2:]
				key := packet[:keyLen]
				packet = packet[keyLen:]
				valLen := binary.LittleEndian.Uint16(packet)
				packet = packet[2:]
				val := packet[:valLen]
				packet = packet[valLen:]
				shard.properties.set(string(key), y.Copy(val))
			case walTypeFinish:
				valLen := binary.LittleEndian.Uint16(packet)
				val := packet[:valLen]
				packet = packet[valLen:]
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
