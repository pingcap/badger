package badger

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/pingcap/log"
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
	shards      map[uint64]*ShardInfo
	globalFiles map[uint64]cfLevel
	lastID      uint64
	version     uint64
	fd          *os.File
	deletions   int
	creations   int

	// Guards appends, which includes access to the manifest field.
	appendLock sync.Mutex
	// We make this configurable so that unit tests can hit rewrite() code quickly
	deletionsRewriteThreshold int
	orc                       *oracle
}

type ShardInfo struct {
	ID    uint64
	Start []byte
	End   []byte
	// fid -> level
	files map[uint64]struct{}
}

// ShardLevel is the struct that contains shard id and level id,
type LevelCF struct {
	Level uint16
	CF    uint16
}

var globalShardEndKey = []byte{255, 255, 255, 255, 255, 255, 255, 255}

func OpenShardingManifest(dir string) (*ShardingManifest, error) {
	path := filepath.Join(dir, ManifestFilename)
	fd, err := y.OpenExistingFile(path, 0) // We explicitly sync in addChanges, outside the lock.
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		m := &ShardingManifest{
			dir:         dir,
			shards:      map[uint64]*ShardInfo{},
			lastID:      1,
			globalFiles: map[uint64]cfLevel{},
		}
		initShard := &ShardInfo{
			ID:    1,
			End:   globalShardEndKey,
			files: map[uint64]struct{}{},
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
				Id:      fid,
				Op:      protos.ManifestChange_CREATE,
				Level:   cfLevel.level,
				CF:      cfLevel.cf,
			})
		}
		tblIDs := make([]uint64, 0, len(shard.files))
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
	log.Info("rewrite manifest")
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

var errShardNotFound = errors.New("shard not found")

func (m *ShardingManifest) ApplyChangeSet(cs *protos.ManifestChangeSet) error {
	for _, change := range cs.ShardChange {
		if change.Op != protos.ShardChange_CREATE {
			continue
		}
		shard := &ShardInfo{
			ID:    change.ShardID,
			Start: change.StartKey,
			End:   change.EndKey,
			files: map[uint64]struct{}{},
		}
		for _, tID := range change.TableIDs {
			shard.files[tID] = struct{}{}
		}
		m.shards[change.ShardID] = shard
		if m.lastID < change.ShardID {
			m.lastID = change.ShardID
		}
	}
	for _, change := range cs.Changes {
		shardID := change.ShardID
		fid := change.Id
		if fid > m.lastID {
			m.lastID = fid
		}
		shardInfo := m.shards[shardID]
		if shardInfo == nil {
			return errShardNotFound
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
		if change.Op != protos.ShardChange_DELETE {
			continue
		}
		delete(m.shards, change.ShardID)
	}
	m.version = cs.Head.Version
	return nil
}

func (m *ShardingManifest) addChanges(changes ...*protos.ManifestChange) error {
	changeSet := &protos.ManifestChangeSet{Changes: changes}
	return m.writeChangeSet(changeSet)
}

func (m *ShardingManifest) writeChangeSet(changeSet *protos.ManifestChangeSet) error {
	// Maybe we could use O_APPEND instead (on certain file systems)
	m.appendLock.Lock()
	defer m.appendLock.Unlock()
	changeSet.Head = &protos.HeadInfo{Version: m.orc.commitTs()}
	buf, err := changeSet.Marshal()
	if err != nil {
		return err
	}
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
	cf    int32
	level uint32
}

func ReplayShardingManifestFile(fp *os.File) (ret *ShardingManifest, truncOffset int64, err error) {
	r := &countingReader{wrapped: bufio.NewReader(fp)}
	if err = readManifestMagic(r); err != nil {
		return nil, 0, err
	}
	ret = &ShardingManifest{
		shards:      map[uint64]*ShardInfo{},
		globalFiles: map[uint64]cfLevel{},
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
