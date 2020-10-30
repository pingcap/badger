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
	globalFiles map[uint32]cfLevel
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
	files map[uint32]struct{}
}

// ShardLevel is the struct that contains shard id and level id,
type LevelCF struct {
	Level uint16
	CF    uint16
}

var globalShardEndKey = []byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255}

func OpenShardingManifest(dir string) (*ShardingManifest, error) {
	path := filepath.Join(dir, ManifestFilename)
	fd, err := y.OpenExistingFile(path, 0) // We explicitly sync in addChanges, outside the lock.
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		m := &ShardingManifest{
			dir:         dir,
			shards:      map[uint32]*ShardInfo{},
			lastShardID: 1,
			globalFiles: map[uint32]cfLevel{},
		}
		initShard := &ShardInfo{
			ID:    1,
			End:   globalShardEndKey,
			files: map[uint32]struct{}{},
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
		for fid := range shard.files {
			cfLevel := m.globalFiles[fid]
			cs.Changes = append(cs.Changes, &protos.ManifestChange{
				ShardID: shardID,
				Id:      uint64(fid),
				Op:      protos.ManifestChange_CREATE,
				Level:   cfLevel.level,
				CF:      cfLevel.cf,
			})
		}
		tblIDs := make([]uint32, 0, len(shard.files))
		for fid := range shard.files {
			tblIDs = append(tblIDs, fid)
		}
		cs.ShardChange = append(cs.ShardChange, &protos.ShardChange{
			ShardID:  shardID,
			Op:       protos.ShardChange_CREATE,
			StartKey: shard.Start,
			EndKey:   shard.End,
			TableIDs: tblIDs,
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
	for _, change := range cs.Changes {
		shardID := change.ShardID
		fid := uint32(change.Id)
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
			m.creations++
			shardInfo.files[fid] = struct{}{}
			m.globalFiles[fid] = cfLevel{cf: change.CF, level: change.Level}
		case protos.ManifestChange_DELETE:
			m.deletions++
			delete(shardInfo.files, fid)
			delete(m.globalFiles, fid)
		case protos.ManifestChange_MOVE_DOWN:
			m.creations++
			m.deletions++
			if _, ok := shardInfo.files[fid]; !ok {
				return fmt.Errorf("move down file %d not found", fid)
			}
			m.globalFiles[fid] = cfLevel{cf: change.CF, level: change.Level}
		}
	}
	for _, change := range cs.ShardChange {
		switch change.Op {
		case protos.ShardChange_CREATE:
			shard := &ShardInfo{
				ID:    change.ShardID,
				Start: change.StartKey,
				End:   change.EndKey,
				files: map[uint32]struct{}{},
			}
			for _, tID := range change.TableIDs {
				shard.files[tID] = struct{}{}
			}
			m.shards[change.ShardID] = shard
			if m.lastShardID < change.ShardID {
				m.lastShardID = change.ShardID
			}
		case protos.ShardChange_DELETE:
			delete(m.shards, change.ShardID)
		}
	}
	m.version = cs.Head.Version
	return nil
}

func (m *ShardingManifest) applyL0Change(fid uint32, op protos.ManifestChange_Operation) {
	if op == protos.ManifestChange_CREATE {
		m.l0Files = append(m.l0Files, fid)
		m.creations++
		return
	}
	m.deletions++
	for i, l0fid := range m.l0Files {
		if fid == l0fid {
			m.l0Files = append(m.l0Files[:i], m.l0Files[i+1:]...)
			return
		}
	}
}

func (m *ShardingManifest) addChanges(commitTS uint64, changes ...*protos.ManifestChange) error {
	changeSet := &protos.ManifestChangeSet{Changes: changes, Head: &protos.HeadInfo{Version: commitTS}}
	return m.writeChangeSet(changeSet)
}

func (m *ShardingManifest) writeChangeSet(changeSet *protos.ManifestChangeSet) error {
	buf, err := changeSet.Marshal()
	if err != nil {
		return err
	}

	// Maybe we could use O_APPEND instead (on certain file systems)
	m.appendLock.Lock()
	defer m.appendLock.Unlock()
	if err := m.ApplyChangeSet(changeSet); err != nil {
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

type cfLevel struct {
	cf    uint32
	level uint32
}

func ReplayShardingManifestFile(fp *os.File) (ret *ShardingManifest, truncOffset int64, err error) {
	r := &countingReader{wrapped: bufio.NewReader(fp)}
	if err = readManifestMagic(r); err != nil {
		return nil, 0, err
	}
	ret = &ShardingManifest{
		shards:      map[uint32]*ShardInfo{},
		globalFiles: map[uint32]cfLevel{},
		fd:          fp,
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
