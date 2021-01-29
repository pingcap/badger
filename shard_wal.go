package badger

import (
	"encoding/binary"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"hash/crc32"
	"io"
	"os"
)

type shardSplitWAL struct {
	fd     *os.File
	buf    []byte
	offset int64
}

func newShardSplitWAL(filename string, splitKeys [][]byte) (*shardSplitWAL, error) {
	fd, err := y.OpenTruncFile(filename, false)
	if err != nil {
		return nil, errors.New("failed to create shard split WAL")
	}
	wal := &shardSplitWAL{fd: fd}
	for _, splitKey := range splitKeys {
		wal.appendSplitKey(splitKey)
	}
	err = wal.flush()
	if err != nil {
		return nil, err
	}
	return wal, nil
}

const (
	walTypeSplitKey = 0
	walTypeEntry    = 1
	walTypeProperty = 2
)

func (wal *shardSplitWAL) appendEntry(cf int, key []byte, val y.ValueStruct) {
	wal.buf = append(wal.buf, walTypeEntry)
	wal.buf = append(wal.buf, byte(cf))
	keyLen := uint16(len(key))
	wal.buf = append(wal.buf, byte(keyLen), byte(keyLen>>8))
	wal.buf = append(wal.buf, key...)
	wal.buf = val.EncodeTo(wal.buf)
}

func (wal *shardSplitWAL) appendSplitKey(key []byte) {
	wal.buf = append(wal.buf, walTypeSplitKey)
	keyLen := uint16(len(key))
	wal.buf = append(wal.buf, byte(keyLen), byte(keyLen>>8))
	wal.buf = append(wal.buf, key...)
}

func (wal *shardSplitWAL) appendProperty(key string, val []byte) {
	wal.buf = append(wal.buf, walTypeProperty)
	keyLen := uint16(len(key))
	wal.buf = append(wal.buf, byte(keyLen), byte(keyLen>>8))
	wal.buf = append(wal.buf, key...)
	valLen := uint16(len(val))
	wal.buf = append(wal.buf, byte(valLen), byte(valLen>>8))
	wal.buf = append(wal.buf, key...)
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
	wal.offset += int64(len(wal.buf))
	return err
}

func (wal *shardSplitWAL) sync() error {
	return wal.fd.Sync()
}

func (wal *shardSplitWAL) replay(fn replayFn) error {
	_, err := wal.fd.Seek(wal.offset, io.SeekStart)
	if err != nil {
		return err
	}
	defer func() {
		wal.fd.Seek(wal.offset, 0)
	}()
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
		wal.offset += int64(4 + len(wal.buf))
		wal.replayPacket(fn)
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

func (wal *shardSplitWAL) replayPacket(fn replayFn) {
	data := wal.buf
	for len(data) > 0 {
		cf := int(data[0])
		data = data[1:]
		keyLen := binary.LittleEndian.Uint16(data)
		data = data[2:]
		key := data[:keyLen]
		data = data[keyLen:]
		var val y.ValueStruct
		val.Decode(data)
		data = data[val.EncodedSize():]
		fn(cf, key, val)
	}
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

type replayFn = func(cf int, key []byte, val y.ValueStruct)

func (s *Shard) replayWAL(offset int64, fn replayFn) error {
	fd, err := y.OpenSyncedFile(s.walFilename, false)
	if err != nil {
		return err
	}
	s.wal = &shardSplitWAL{fd: fd, offset: offset}
	return s.wal.replay(fn)
}
