package badger

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/pingcap/badger/protos"
	"github.com/pingcap/badger/y"
)

// The manifest file is used to restore the tree
type ShardingManifest struct {
	dir         string
	l0Files     []uint32
	shards      map[uint32]*ShardInfo
	lastFileID  uint32
	lastShardID uint32
	version     uint64
	fd          *os.File
	deletions   int
	creations   int

	// Guards appends, which includes access to the manifest field.
	appendLock sync.Mutex
	// We make this configurable so that unit tests can hit rewrite() code quickly
	deletionsRewriteThreshold int
}

type ShardInfo struct {
	ID    uint32
	Start []byte
	End   []byte
	// fid -> level
	files map[uint32]cfLevel
}

// ShardLevel is the struct that contains shard id and level id,
type LevelCF struct {
	Level uint16
	CF    uint16
}

func OpenShardingManifest(dir string) (*ShardingManifest, error) {
	path := filepath.Join(dir, ManifestFilename)
	fd, err := y.OpenExistingFile(path, 0) // We explicitly sync in addChanges, outside the lock.
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		m := &ShardingManifest{
			dir:    dir,
			shards: map[uint32]*ShardInfo{},
		}
		endKey := make([]byte, 16)
		for i := range endKey {
			endKey[i] = 255
		}
		initShard := &ShardInfo{
			ID:    1,
			End:   endKey,
			files: map[uint32]cfLevel{},
		}
		m.shards[initShard.ID] = initShard
		err = m.rewrite()
		if err != nil {
			return nil, err
		}
		return m, err
	}
	m, truncOffset, err := ReplayShardingManifestFile(fd)
	if err != nil {
		return nil, err
	}
	// Truncate file so we don't have a half-written entry at the end.
	if err = fd.Truncate(truncOffset); err != nil {
		_ = fd.Close()
		return nil, err
	}
	if _, err = fd.Seek(0, io.SeekEnd); err != nil {
		_ = fd.Close()
		return nil, err
	}
	return m, nil
}

func (m *ShardingManifest) toChangeSet() *protos.ManifestChangeSet {
	cs := &protos.ManifestChangeSet{
		Changes:     make([]*protos.ManifestChange, 0, len(m.shards)),
		Head:        &protos.HeadInfo{Version: m.version},
		ShardChange: make([]*protos.ShardChange, 0, len(m.shards)),
	}
	for shardID, shard := range m.shards {
		for fid, levelCF := range shard.files {
			cs.Changes = append(cs.Changes, &protos.ManifestChange{
				Id:    uint64(newSFID(shardID, fid)),
				Op:    protos.ManifestChange_CREATE,
				Level: uint32(levelCF),
			})
		}
		cs.ShardChange = append(cs.ShardChange, &protos.ShardChange{
			ShardID:  shardID,
			Op:       protos.ShardChange_CREATE,
			StartKey: shard.Start,
			EndKey:   shard.End,
		})
	}
	return cs
}

func (m *ShardingManifest) rewrite() error {
	changeSet := m.toChangeSet()
	changeBuf, err := changeSet.Marshal()
	if err != nil {
		return err
	}
	if m.fd != nil {
		m.fd.Close()
	}
	m.fd, err = rewriteManifest(changeBuf, m.dir)
	return nil
}

func (m *ShardingManifest) Close() error {
	return m.fd.Close()
}

func (m *ShardingManifest) ApplyChangeSet(cs *protos.ManifestChangeSet) error {
	for _, change := range cs.ShardChange {
		switch change.Op {
		case protos.ShardChange_CREATE:
			m.shards[change.ShardID] = &ShardInfo{
				ID:    change.ShardID,
				Start: change.StartKey,
				End:   change.EndKey,
				files: map[uint32]cfLevel{},
			}
			if m.lastShardID < change.ShardID {
				m.lastShardID = change.ShardID
			}
		case protos.ShardChange_DELETE:
			delete(m.shards, change.ShardID)
		}
	}
	for _, change := range cs.Changes {
		id := sfID(change.Id)
		shardID := id.shardID()
		fid := id.fid()
		if fid > m.lastFileID {
			m.lastFileID = fid
		}
		if shardID == 0 {
			m.applyL0Change(fid, change.Op)
			continue
		}
		shardInfo := m.shards[shardID]
		if shardInfo == nil {
			return fmt.Errorf("shard %d not found", shardID)
		}
		switch change.Op {
		case protos.ManifestChange_CREATE:
			shardInfo.files[fid] = cfLevel(change.Level)
		case protos.ManifestChange_DELETE:
			delete(shardInfo.files, fid)
		case protos.ManifestChange_MOVE_DOWN:
			if _, ok := shardInfo.files[fid]; !ok {
				return fmt.Errorf("move down file %d not found", fid)
			}
			shardInfo.files[fid] = cfLevel(change.Level)
		}
	}
	m.version = cs.Head.Version
	return nil
}

func (m *ShardingManifest) applyL0Change(fid uint32, op protos.ManifestChange_Operation) {
	if op == protos.ManifestChange_CREATE {
		m.l0Files = append(m.l0Files, fid)
		return
	}
	for i, l0fid := range m.l0Files {
		if fid == l0fid {
			m.l0Files = append(m.l0Files[:i], m.l0Files[i+1:]...)
			return
		}
	}
}

func (m *ShardingManifest) addChanges(head *protos.HeadInfo, changesParam ...*protos.ManifestChange) error {
	changes := protos.ManifestChangeSet{Changes: changesParam, Head: head}
	buf, err := changes.Marshal()
	if err != nil {
		return err
	}

	// Maybe we could use O_APPEND instead (on certain file systems)
	m.appendLock.Lock()
	defer m.appendLock.Unlock()
	if err := m.ApplyChangeSet(&changes); err != nil {
		return err
	}
	// Rewrite manifest if it'd shrink by 1/10 and it's big enough to care
	if m.deletions > m.deletionsRewriteThreshold &&
		m.deletions > manifestDeletionsRatio*(m.creations-m.deletions) {
		if err := m.rewrite(); err != nil {
			return err
		}
	} else {
		var lenCrcBuf [8]byte
		binary.BigEndian.PutUint32(lenCrcBuf[0:4], uint32(len(buf)))
		binary.BigEndian.PutUint32(lenCrcBuf[4:8], crc32.Checksum(buf, y.CastagnoliCrcTable))
		buf = append(lenCrcBuf[:], buf...)
		if _, err := m.fd.Write(buf); err != nil {
			return err
		}
	}
	return m.fd.Sync()
}

type sfID uint64

func newSFID(shardID, fid uint32) sfID {
	return sfID(shardID)<<32 + sfID(fid)
}

func (id sfID) shardID() uint32 {
	return uint32(id >> 32)
}

func (id sfID) fid() uint32 {
	return uint32(id)
}

type cfLevel uint32

func newCFLevel(cf, level uint16) cfLevel {
	return cfLevel(cf)<<16 + cfLevel(level)
}

func (cl cfLevel) cf() uint16 {
	return uint16(cl >> 16)
}

func (cl cfLevel) level() uint16 {
	return uint16(cl)
}

func ReplayShardingManifestFile(fp *os.File) (ret *ShardingManifest, truncOffset int64, err error) {
	r := &countingReader{wrapped: bufio.NewReader(fp)}
	if err = readManifestMagic(r); err != nil {
		return nil, 0, err
	}
	ret = &ShardingManifest{
		shards: map[uint32]*ShardInfo{},
		fd:     fp,
	}
	var offset int64
	for {
		offset = r.count
		var changeSet *protos.ManifestChangeSet
		changeSet, err = readChangeSet(r)
		if err != nil {
			return nil, 0, err
		}
		if changeSet == nil {
			break
		}
		err = ret.ApplyChangeSet(changeSet)
		if err != nil {
			return nil, 0, err
		}
	}
	return ret, offset, nil
}
