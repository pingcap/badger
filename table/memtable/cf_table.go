package memtable

import "github.com/pingcap/badger/y"

type CFTable struct {
	skls  []*skiplist
	arena *arena
}

func NewCFTable(arenaSize int64, numCFs int) *CFTable {
	t := &CFTable{
		skls:  make([]*skiplist, numCFs),
		arena: newArena(arenaSize),
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

func (cft *CFTable) Put(cf byte, key []byte, val y.ValueStruct) {
	cft.skls[cf].Put(key, val)
}

func (cft *CFTable) PutEntries(cf byte, entries []*Entry) {
	var h hint
	skl := cft.skls[cf]
	for _, entry := range entries {
		skl.PutWithHint(entry.Key, entry.Value, &h)
	}
}

func (cft *CFTable) Size() int64 {
	return cft.arena.size()
}

func (cft *CFTable) Get(cf byte, key []byte, version uint64) y.ValueStruct {
	return cft.skls[cf].Get(key, version)
}

func (cft *CFTable) NewIterator(cf byte) *Iterator {
	return cft.skls[cf].NewIterator()
}

func (cft *CFTable) Empty() bool {
	for _, skl := range cft.skls {
		if !skl.Empty() {
			return false
		}
	}
	return true
}
