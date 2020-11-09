package memtable

import (
	"github.com/pingcap/badger/y"
)

type CFTable struct {
	id    uint32
	skls  []*skiplist
	arena *arena
}

func NewCFTable(arenaSize int64, numCFs int, id uint32) *CFTable {
	t := &CFTable{
		skls:  make([]*skiplist, numCFs),
		arena: newArena(arenaSize),
		id:    id,
	}
	for i := 0; i < numCFs; i++ {
		head := newNode(t.arena, nil, y.ValueStruct{}, maxHeight)
		t.skls[i] = &skiplist{
			height: 1,
			head:   head,
			arena:  t.arena,
		}
	}
	return t
}

func (cft *CFTable) Put(cf int, key []byte, val y.ValueStruct) {
	cft.skls[cf].Put(key, val)
}

func (cft *CFTable) PutEntries(cf int, entries []*Entry) {
	var h hint
	skl := cft.skls[cf]
	for _, entry := range entries {
		skl.PutWithHint(entry.Key, entry.Value, &h)
	}
}

func (cft *CFTable) Size() int64 {
	return cft.arena.size()
}

func (cft *CFTable) Get(cf int, key []byte, version uint64) y.ValueStruct {
	return cft.skls[cf].Get(key, version)
}

func (cft *CFTable) DeleteKey(cf byte, key []byte) bool {
	return cft.skls[cf].DeleteKey(key)
}

func (cft *CFTable) NewIterator(cf int, reversed bool) *UniIterator {
	if cft.skls[cf].Empty() {
		return nil
	}
	return cft.skls[cf].NewUniIterator(reversed)
}

func (cft *CFTable) Empty() bool {
	for _, skl := range cft.skls {
		if !skl.Empty() {
			return false
		}
	}
	return true
}

func (cft *CFTable) ID() uint32 {
	return cft.id
}
