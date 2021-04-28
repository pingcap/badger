package badger

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/pingcap/badger/protos"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
)

// The manifest file is used to restore the tree
type ShardingManifest struct {
	dir         string
	shards      map[uint64]*ShardMeta
	globalFiles map[uint64]fileMeta
	lastID      uint64
	dataVersion uint64
	fd          *os.File
	deletions   int
	creations   int

	// Guards appends, which includes access to the manifest field.
	appendLock sync.Mutex
	// We make this configurable so that unit tests can hit rewrite() code quickly
	deletionsRewriteThreshold int
	orc                       *oracle
}

type ShardMeta struct {
	ID    uint64
	Ver   uint64
	Start []byte
	End   []byte
	// fid -> level
	files map[uint64]int
	// properties in ShardMeta is only updated on every mem-table flush, it's different than properties in the shard
	// which is updated on every write operation.
	properties *shardProperties
	preSplit   *protos.ShardPreSplit
	split      *protos.ShardSplit
	splitState protos.SplitState
	commitTS   uint64
	parent     *ShardMeta
	recovered  bool
}

func (si *ShardMeta) FileLevel(fid uint64) (int, bool) {
	level, ok := si.files[fid]
	return level, ok
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
			shards:      map[uint64]*ShardMeta{},
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

func (m *ShardingManifest) toChangeSets() ([]*protos.ShardChangeSet, error) {
	var shards []*protos.ShardChangeSet
	for id := range m.shards {
		cs, err := m.toChangeSet(id)
		if err != nil {
			return nil, err
		}
		shards = append(shards, cs)
	}
	return shards, nil
}

var ErrHasParent = errors.New("has parent")

func (m *ShardingManifest) toChangeSet(shardID uint64) (*protos.ShardChangeSet, error) {
	shard := m.shards[shardID]
	if shard.parent != nil {
		return nil, ErrHasParent
	}
	cs := &protos.ShardChangeSet{
		DataVer:  m.dataVersion,
		ShardID:  shard.ID,
		ShardVer: shard.Ver,
		State:    shard.splitState,
	}
	if shard.preSplit != nil {
		cs.PreSplit = shard.preSplit
	}
	shardSnap := &protos.ShardSnapshot{
		Start:      shard.Start,
		End:        shard.End,
		Properties: shard.properties.toPB(shard.ID),
		CommitTS:   shard.commitTS,
	}
	cs.Snapshot = shardSnap
	for fid := range shard.files {
		fileMeta := m.globalFiles[fid]
		if fileMeta.level == 0 {
			shardSnap.L0Creates = append(shardSnap.L0Creates, &protos.L0Create{
				ID:         fid,
				Properties: nil, // Store properties in ShardCreate.
			})
		} else {
			shardSnap.TableCreates = append(shardSnap.TableCreates, &protos.TableCreate{
				ID:       fid,
				Level:    fileMeta.level,
				CF:       fileMeta.cf,
				Smallest: fileMeta.smallest,
				Biggest:  fileMeta.biggest,
			})
		}
	}
	return cs, nil
}

func (m *ShardingManifest) rewrite() error {
	log.Info("rewrite manifest")
	changeSets, err := m.toChangeSets()
	if err != nil {
		if err == ErrHasParent {
			return nil
		}
		return err
	}
	changeSetsBuf := make([]byte, 8)
	copy(changeSetsBuf, magicText[:])
	binary.BigEndian.PutUint32(changeSetsBuf[4:], magicVersion)
	var creations int
	for _, cs := range changeSets {
		data, _ := cs.Marshal()
		creations += len(cs.Snapshot.L0Creates) + len(cs.Snapshot.TableCreates)
		changeSetsBuf = appendChecksumPacket(changeSetsBuf, data)
	}
	if m.fd != nil {
		m.fd.Close()
	}
	m.fd, err = rewriteManifest(changeSetsBuf, m.dir)
	if err != nil {
		return err
	}
	m.creations = creations
	m.deletions = 0
	return nil
}

func (m *ShardingManifest) Close() error {
	return m.fd.Close()
}

func (m *ShardingManifest) ApplyChangeSet(cs *protos.ShardChangeSet) error {
	if m.dataVersion < cs.DataVer {
		m.dataVersion = cs.DataVer
	}
	if cs.Snapshot != nil {
		m.applySnapshot(cs)
		return nil
	}
	shardInfo := m.shards[cs.ShardID]
	if shardInfo == nil {
		return errors.WithStack(errShardNotFound)
	}
	y.Assert(shardInfo.Ver == cs.ShardVer)
	if cs.Flush != nil {
		m.applyFlush(cs, shardInfo)
		if cs.State == protos.SplitState_PRE_SPLIT_FLUSH_DONE {
			shardInfo.splitState = protos.SplitState_PRE_SPLIT_FLUSH_DONE
			if shardInfo.preSplit != nil && shardInfo.preSplit.MemProps != nil {
				shardInfo.preSplit.MemProps = nil
			}
		}
		return nil
	}
	if cs.Compaction != nil {
		m.applyCompaction(cs, shardInfo)
		return nil
	}
	if cs.PreSplit != nil {
		y.Assert(cs.PreSplit.MemProps != nil)
		shardInfo.preSplit = cs.PreSplit
		shardInfo.splitState = protos.SplitState_PRE_SPLIT
		return nil
	}
	if cs.SplitFiles != nil {
		m.applySplitFiles(cs, shardInfo)
		shardInfo.splitState = protos.SplitState_SPLIT_FILE_DONE
		return nil
	}
	if cs.Split != nil {
		m.applySplit(cs.ShardID, cs.Split)
		return nil
	}
	if cs.ShardDelete {
		delete(m.shards, cs.ShardID)
	}
	return nil
}

func (m *ShardingManifest) applySnapshot(cs *protos.ShardChangeSet) {
	log.S().Infof("%d:%d apply snapshot", cs.ShardID, cs.ShardVer)
	snap := cs.Snapshot
	shard := &ShardMeta{
		ID:         cs.ShardID,
		Ver:        cs.ShardVer,
		Start:      snap.Start,
		End:        snap.End,
		files:      map[uint64]int{},
		properties: newShardProperties().applyPB(snap.Properties),
		splitState: cs.State,
		commitTS:   snap.CommitTS,
	}
	if len(cs.Snapshot.SplitKeys) > 0 {
		shard.preSplit = &protos.ShardPreSplit{Keys: cs.Snapshot.SplitKeys}
	}
	for _, l0 := range snap.L0Creates {
		m.addFile(l0.ID, -1, 0, l0.Start, l0.End, shard)
	}
	for _, tbl := range snap.TableCreates {
		m.addFile(tbl.ID, tbl.CF, tbl.Level, tbl.Smallest, tbl.Biggest, shard)
	}
	m.shards[cs.ShardID] = shard
}

func (m *ShardingManifest) applyFlush(cs *protos.ShardChangeSet, shardInfo *ShardMeta) {
	log.S().Infof("%d:%d apply flush", cs.ShardID, cs.ShardVer)
	shardInfo.commitTS = cs.Flush.CommitTS
	shardInfo.parent = nil
	for _, create := range cs.Flush.L0Creates {
		if create.Properties != nil {
			for i, key := range create.Properties.Keys {
				shardInfo.properties.set(key, create.Properties.Values[i])
			}
		}
		m.addFile(create.ID, -1, 0, create.Start, create.End, shardInfo)
	}
}

func (m *ShardingManifest) addFile(fid uint64, cf int32, level uint32, smallest, biggest []byte, shardInfo *ShardMeta) {
	log.S().Infof("manifest add file %d l%d smalleset %v biggest %v", fid, level, smallest, biggest)
	if fid > m.lastID {
		m.lastID = fid
	}
	m.creations++
	shardInfo.files[fid] = int(level)
	m.globalFiles[fid] = fileMeta{cf: cf, level: level, smallest: smallest, biggest: biggest}
}

func (m *ShardingManifest) deleteFile(fid uint64, shardInfo *ShardMeta) {
	log.S().Infof("%d:%d manifest del file %d", shardInfo.ID, shardInfo.Ver, fid)
	m.deletions++
	delete(shardInfo.files, fid)
	delete(m.globalFiles, fid)
}

func (m *ShardingManifest) applyCompaction(cs *protos.ShardChangeSet, shardInfo *ShardMeta) {
	log.S().Infof("%d:%d apply compaction", cs.ShardID, cs.ShardVer)
	for _, id := range cs.Compaction.TopDeletes {
		m.deleteFile(id, shardInfo)
	}
	for _, id := range cs.Compaction.BottomDeletes {
		m.deleteFile(id, shardInfo)
	}
	for _, create := range cs.Compaction.TableCreates {
		m.addFile(create.ID, create.CF, create.Level, create.Smallest, create.Biggest, shardInfo)
	}
}

func (m *ShardingManifest) applySplitFiles(cs *protos.ShardChangeSet, shardInfo *ShardMeta) {
	log.S().Infof(" %d:%d apply split files", shardInfo.ID, shardInfo.Ver)
	for _, id := range cs.SplitFiles.TableDeletes {
		m.deleteFile(id, shardInfo)
	}
	for _, l0 := range cs.SplitFiles.L0Creates {
		m.addFile(l0.ID, -1, 0, l0.Start, l0.End, shardInfo)
	}
	for _, tbl := range cs.SplitFiles.TableCreates {
		m.addFile(tbl.ID, tbl.CF, tbl.Level, tbl.Smallest, tbl.Biggest, shardInfo)
	}
}

func (m *ShardingManifest) applySplit(shardID uint64, split *protos.ShardSplit) {
	old := m.shards[shardID]
	log.S().Infof("%d:%d apply split, files %v", old.ID, old.Ver, old.files)
	newShards := make([]*ShardMeta, len(split.NewShards))
	newVer := old.Ver + uint64(len(newShards)) - 1
	for i := 0; i < len(split.NewShards); i++ {
		startKey, endKey := getSplittingStartEnd(old.Start, old.End, split.Keys, i)
		id := split.NewShards[i].ShardID
		if id == old.ID {
			old.split = split
		}
		shardInfo := &ShardMeta{
			ID:         id,
			Ver:        newVer,
			Start:      startKey,
			End:        endKey,
			files:      map[uint64]int{},
			properties: newShardProperties().applyPB(split.NewShards[i]),
			parent:     old,
		}
		m.shards[id] = shardInfo
		newShards[i] = shardInfo
	}
	for fid := range old.files {
		fileMeta := m.globalFiles[fid]
		shardIdx := getSplitShardIndex(split.Keys, fileMeta.smallest)
		newShards[shardIdx].files[fid] = int(fileMeta.level)
	}
	for _, nShard := range newShards {
		log.S().Infof("new shard %d:%d smallest %v biggest %v files %v",
			nShard.ID, nShard.Ver, nShard.Start, nShard.End, nShard.files)
	}
}

var errDupChange = errors.New("duplicated change")

func (m *ShardingManifest) writeChangeSet(changeSet *protos.ShardChangeSet) error {
	// Maybe we could use O_APPEND instead (on certain file systems)
	m.appendLock.Lock()
	defer m.appendLock.Unlock()
	if m.isDuplicatedChange(changeSet) {
		return errDupChange
	}
	changeSet.DataVer = m.orc.commitTs()
	buf, err := changeSet.Marshal()
	if err != nil {
		return err
	}
	// Rewrite manifest if it'd shrink by 1/10 and it's big enough to care
	if m.deletions > m.deletionsRewriteThreshold &&
		m.deletions > manifestDeletionsRatio*(m.creations-m.deletions) {
		log.S().Infof("deletions %d createions %d", m.deletions, m.creations)
		if err = m.rewrite(); err != nil {
			return err
		}
	} else {
		buf = appendChecksumPacket([]byte{}, buf)
		if _, err = m.fd.Write(buf); err != nil {
			return err
		}
	}
	err = m.fd.Sync()
	if err != nil {
		return err
	}
	if err = m.ApplyChangeSet(changeSet); err != nil {
		return err
	}
	return nil
}

func (m *ShardingManifest) isDuplicatedChange(change *protos.ShardChangeSet) bool {
	meta, ok := m.shards[change.ShardID]
	if !ok {
		return false
	}
	if flush := change.Flush; flush != nil {
		if meta.parent != nil {
			return false
		}
		if len(flush.L0Creates) == 0 {
			return meta.splitState >= change.State
		}
		return meta.commitTS >= flush.CommitTS
	}
	if comp := change.Compaction; comp != nil {
		// TODO: It is a temporary solution that can fail in very rare case.
		// It is possible that a duplicated compaction's all new tables are removed by future compaction.
		for _, tbl := range comp.TableCreates {
			level, ok := meta.FileLevel(tbl.ID)
			if ok && level > int(change.Compaction.Level) {
				return true
			}
		}
	}
	if splitFiles := change.SplitFiles; splitFiles != nil {
		return meta.splitState == change.State
	}
	return false
}

type fileMeta struct {
	cf       int32
	level    uint32
	smallest []byte
	biggest  []byte
}

func ReplayShardingManifestFile(fp *os.File) (ret *ShardingManifest, truncOffset int64, err error) {
	log.Info("replay manifest")
	r := &countingReader{wrapped: bufio.NewReader(fp)}
	if err = readManifestMagic(r); err != nil {
		return nil, 0, err
	}
	ret = &ShardingManifest{
		shards:      map[uint64]*ShardMeta{},
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

func newShardChangeSet(shard *Shard) *protos.ShardChangeSet {
	return &protos.ShardChangeSet{
		ShardID:  shard.ID,
		ShardVer: shard.Ver,
		State:    shard.GetSplitState(),
	}
}
