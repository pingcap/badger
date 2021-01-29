package badger

import (
	"bufio"
	"github.com/pingcap/badger/protos"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// The manifest file is used to restore the tree
type ShardingManifest struct {
	dir         string
	shards      map[uint64]*ShardInfo
	globalFiles map[uint64]fileMeta
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
	metaListener              MetaChangeListener
}

type ShardInfo struct {
	ID    uint64
	Ver   uint64
	Start []byte
	End   []byte
	// fid -> level
	files map[uint64]struct{}
	// properties in ShardInfo is only updated on every mem-table flush, it's different than properties in the shard
	// which is updated on every write operation.
	properties *shardProperties
	preSplit   *protos.ShardPreSplit
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
			globalFiles: map[uint64]fileMeta{},
		}
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

func (m *ShardingManifest) toChangeSets() []*protos.ShardChangeSet {
	var shards []*protos.ShardChangeSet
	for _, shard := range m.shards {
		cs := &protos.ShardChangeSet{
			Version:  m.version,
			ShardID:  shard.ID,
			ShardVer: shard.Ver,
		}
		for fid := range shard.files {
			fileMeta := m.globalFiles[fid]
			if fileMeta.level == 0 {
				cs.L0Creates = append(cs.L0Creates, &protos.L0Create{
					ID:         fid,
					Properties: nil, // Store properties in ShardCreate.
				})
			} else {
				cs.TableCreates = append(cs.TableCreates, &protos.TableCreate{
					ID:       fid,
					Level:    fileMeta.level,
					CF:       fileMeta.cf,
					Smallest: fileMeta.smallest,
					Biggest:  fileMeta.biggest,
				})
			}
		}
		tblIDs := make([]uint64, 0, len(shard.files))
		for fid := range shard.files {
			tblIDs = append(tblIDs, fid)
		}
		cs.ShardCreate = &protos.ShardCreate{
			StartKey:   shard.Start,
			EndKey:     shard.End,
			TableIDs:   tblIDs,
			Properties: shard.properties.toPB(shard.ID),
		}
		shards = append(shards, cs)
	}
	return shards
}

func (m *ShardingManifest) rewrite() error {
	log.Info("rewrite manifest")
	changeSets := m.toChangeSets()
	var changeSetsBuf []byte
	for _, cs := range changeSets {
		data, err := cs.Marshal()
		if err != nil {
			return err
		}
		changeSetsBuf = appendChecksumPacket(changeSetsBuf, data)
	}
	if m.fd != nil {
		m.fd.Close()
	}
	var err error
	m.fd, err = rewriteManifest(changeSetsBuf, m.dir)
	return err
}

func (m *ShardingManifest) Close() error {
	return m.fd.Close()
}

func (m *ShardingManifest) ApplyChangeSet(cs *protos.ShardChangeSet) error {
	if cs.ShardCreate != nil {
		shard := &ShardInfo{
			ID:         cs.ShardID,
			Ver:        cs.ShardVer,
			Start:      cs.ShardCreate.StartKey,
			End:        cs.ShardCreate.EndKey,
			files:      map[uint64]struct{}{},
			properties: newShardProperties().applyPB(cs.ShardCreate.Properties),
		}
		for _, tID := range cs.ShardCreate.TableIDs {
			shard.files[tID] = struct{}{}
		}
		m.shards[cs.ShardID] = shard
	}
	shardInfo := m.shards[cs.ShardID]
	if shardInfo == nil {
		return errors.WithStack(errShardNotFound)
	}
	if cs.PreSplit != nil {
		m.shards[cs.ShardID].preSplit = cs.PreSplit
	}
	if cs.Split != nil {
		m.applySplit(cs)
	}
	for _, fid := range cs.TableDeletes {
		m.deletions++
		delete(shardInfo.files, fid)
		delete(m.globalFiles, fid)
	}
	for _, create := range cs.L0Creates {
		fid := create.ID
		if fid > m.lastID {
			m.lastID = fid
		}
		m.creations++
		shardInfo.files[fid] = struct{}{}
		m.globalFiles[fid] = fileMeta{cf: -1, level: 0, smallest: create.Start, biggest: create.End}
	}
	for _, create := range cs.TableCreates {
		fid := create.ID
		if fid > m.lastID {
			m.lastID = fid
		}
		m.creations++
		shardInfo.files[fid] = struct{}{}
		m.globalFiles[fid] = fileMeta{
			cf:       create.CF,
			level:    create.Level,
			smallest: create.Smallest,
			biggest:  create.Biggest,
		}
	}
	if cs.ShardDelete {
		delete(m.shards, cs.ShardID)
	}
	m.version = cs.Version
	return nil
}

func (m *ShardingManifest) applySplit(cs *protos.ShardChangeSet) {
	split := cs.Split
	old := m.shards[cs.ShardID]
	newShards := make([]*ShardInfo, len(split.NewShards))
	for i := 0; i < len(split.NewShards); i++ {
		startKey, endKey := getSplittingStartEnd(old.Start, old.End, split.Keys, i)
		id := split.NewShards[i].ShardID
		ver := uint64(1)
		var properties *shardProperties
		if id == old.ID {
			ver = old.Ver + 1
			// inherit old shard properties.
			properties = m.shards[id].properties
		} else {
			properties = newShardProperties()
		}
		shardInfo := &ShardInfo{
			ID:         id,
			Ver:        ver,
			Start:      startKey,
			End:        endKey,
			files:      map[uint64]struct{}{},
			properties: properties.applyPB(split.NewShards[i]),
		}
		m.shards[id] = shardInfo
		newShards[i] = shardInfo
	}
	for fid := range old.files {
		fileMeta := m.globalFiles[fid]
		shardIdx := getSplitShardIndex(split.Keys, fileMeta.smallest)
		newShards[shardIdx].files[fid] = struct{}{}
	}
}

func (m *ShardingManifest) writeChangeSet(shard *Shard, changeSet *protos.ShardChangeSet, notifyMetaListener bool) error {
	// Maybe we could use O_APPEND instead (on certain file systems)
	m.appendLock.Lock()
	defer m.appendLock.Unlock()
	changeSet.ShardID = shard.ID
	changeSet.ShardVer = shard.Ver
	changeSet.Version = m.orc.commitTs()
	buf, err := changeSet.Marshal()
	if err != nil {
		return err
	}
	if err = m.ApplyChangeSet(changeSet); err != nil {
		return err
	}
	if m.metaListener != nil && notifyMetaListener {
		m.metaListener.OnChange(&protos.MetaChangeEvent{
			ShardID:      changeSet.ShardID,
			ShardVer:     changeSet.ShardVer,
			StartKey:     shard.Start,
			EndKey:       shard.End,
			L0Creates:    changeSet.L0Creates,
			TableCreates: changeSet.TableCreates,
			TableDeletes: changeSet.TableDeletes,
		})
	}
	// Rewrite manifest if it'd shrink by 1/10 and it's big enough to care
	if m.deletions > m.deletionsRewriteThreshold &&
		m.deletions > manifestDeletionsRatio*(m.creations-m.deletions) {
		if err = m.rewrite(); err != nil {
			return err
		}
	} else {
		buf = appendChecksumPacket([]byte{}, buf)
		if _, err = m.fd.Write(buf); err != nil {
			return err
		}
	}
	return m.fd.Sync()
}

func (m *ShardingManifest) createMetaChangeEvents(changeSet *protos.ShardChangeSet, startKey, endKey []byte) *protos.MetaChangeEvent {
	return &protos.MetaChangeEvent{
		ShardID:      changeSet.ShardID,
		ShardVer:     changeSet.ShardVer,
		StartKey:     startKey,
		EndKey:       endKey,
		L0Creates:    changeSet.L0Creates,
		TableCreates: changeSet.TableCreates,
		TableDeletes: changeSet.TableDeletes,
	}
}

type fileMeta struct {
	cf       int32
	level    uint32
	smallest []byte
	biggest  []byte
}

func ReplayShardingManifestFile(fp *os.File) (ret *ShardingManifest, truncOffset int64, err error) {
	r := &countingReader{wrapped: bufio.NewReader(fp)}
	if err = readManifestMagic(r); err != nil {
		return nil, 0, err
	}
	ret = &ShardingManifest{
		shards:      map[uint64]*ShardInfo{},
		globalFiles: map[uint64]fileMeta{},
		fd:          fp,
	}
	var offset int64
	for {
		offset = r.count
		var buf []byte
		buf, err = readChecksumPacket(r)
		if err != nil {
			return nil, 0, err
		}
		if len(buf) == 0 {
			break
		}
		changeSet := new(protos.ShardChangeSet)
		err = changeSet.Unmarshal(buf)
		if err != nil {
			return nil, 0, err
		}
		err = ret.ApplyChangeSet(changeSet)
		if err != nil {
			return nil, 0, err
		}
	}
	return ret, offset, nil
}
