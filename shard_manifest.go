package badger

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/pingcap/badger/protos"
	"github.com/pingcap/badger/y"
)

// The manifest file is used to restore the tree
type ShardingManifest struct {
	l0Files     []uint32
	shards      map[uint32]*ShardInfo
	oldShards   map[uint32]*ShardInfo
	lastFileID  uint32
	lastShardID uint32
	version     uint64
	fd          *os.File
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
			shards:    map[uint32]*ShardInfo{},
			oldShards: map[uint32]*ShardInfo{},
		}
		m.fd, err = m.rewrite(dir)
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

func (m *ShardingManifest) rewrite(dir string) (*os.File, error) {
	changeSet := m.toChangeSet()
	changeBuf, err := changeSet.Marshal()
	if err != nil {
		return nil, err
	}
	return rewriteManifest(changeBuf, dir)
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
			}
			if m.lastShardID < change.ShardID {
				m.lastShardID = change.ShardID
			}
		case protos.ShardChange_DELETE:
			oldShard, ok := m.shards[change.ShardID]
			if !ok {
				return fmt.Errorf("delete shard %d not found", change.ShardID)
			}
			delete(m.shards, change.ShardID)
			m.oldShards[change.ShardID] = oldShard
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
		shardInfo := m.getShard(shardID)
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

func (m *ShardingManifest) getShard(shardID uint32) *ShardInfo {
	if shard, ok := m.shards[shardID]; ok {
		return shard
	}
	if shard, ok := m.oldShards[shardID]; ok {
		return shard
	}
	return nil
}

func ReplayShardingManifestFile(fp *os.File) (ret *ShardingManifest, truncOffset int64, err error) {
	r := &countingReader{wrapped: bufio.NewReader(fp)}
	if err = readManifestMagic(r); err != nil {
		return nil, 0, err
	}
	ret = &ShardingManifest{
		shards:    map[uint32]*ShardInfo{},
		oldShards: map[uint32]*ShardInfo{},
		fd:        fp,
	}

	if err = readManifestMagic(r); err != nil {
		return nil, 0, err
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
