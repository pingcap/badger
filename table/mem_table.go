package table

import (
	"bytes"
	"sort"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/coocood/badger/skl"
	"github.com/coocood/badger/y"
)

type Entry struct {
	Key   []byte
	Value y.ValueStruct
}

type MemTable struct {
	skl         *skl.Skiplist
	pendingList unsafe.Pointer // *listNode
	mergeMu     sync.Mutex
}

func NewMemTable(arenaSize int64) *MemTable {
	return &MemTable{skl: skl.NewSkiplist(arenaSize)}
}

func (mt *MemTable) IncrRef() {
	mt.skl.IncrRef()
}

func (mt *MemTable) DecrRef() {
	mt.skl.DecrRef()
}

func (mt *MemTable) Empty() bool {
	return atomic.LoadPointer(&mt.pendingList) == nil && mt.skl.Empty()
}

func (mt *MemTable) Get(key []byte) y.ValueStruct {
	curr := (*listNode)(atomic.LoadPointer(&mt.pendingList))
	for curr != nil {
		if curr.isMerged() {
			break
		}
		if v, ok := curr.get(key); ok {
			return v
		}
		curr = (*listNode)(curr.next)
	}
	return mt.skl.Get(key)
}

func (mt *MemTable) NewIterator(reverse bool) y.Iterator {
	var (
		sklItr = mt.skl.NewUniIterator(reverse)
		its    []y.Iterator
	)
	curr := (*listNode)(atomic.LoadPointer(&mt.pendingList))
	for curr != nil {
		if curr.isMerged() {
			break
		}
		its = append(its, curr.newIterator(reverse))
		curr = (*listNode)(curr.next)
	}

	if len(its) == 0 {
		return sklItr
	}
	its = append(its, sklItr)
	return NewMergeIterator(its, reverse)
}

func (mt *MemTable) MemSize() int64 {
	var sz int64
	curr := (*listNode)(atomic.LoadPointer(&mt.pendingList))
	for curr != nil {
		sz += curr.memSize
		curr = (*listNode)(curr.next)
	}
	return mt.skl.MemSize() + sz
}

func (mt *MemTable) Put(key []byte, v y.ValueStruct) {
	mt.MergeListToSkl()
	mt.skl.Put(key, v)
}

func (mt *MemTable) PutWithHint(key []byte, v y.ValueStruct, hint *skl.Hint) {
	mt.MergeListToSkl()
	mt.skl.PutWithHint(key, v, hint)
}

// PutToPendingList put entries to pending list, and you can call MergeListToSkl to merge them to SkipList later.
func (mt *MemTable) PutToPendingList(entries []Entry) {
	mt.putToList(entries)
}

// MergeListToSkl merge all entries in pending list to SkipList.
func (mt *MemTable) MergeListToSkl() {
	head := (*listNode)(atomic.LoadPointer(&mt.pendingList))
	if head == nil {
		return
	}

	mt.mergeMu.Lock()
	head = (*listNode)(atomic.LoadPointer(&mt.pendingList))
	if head == nil {
		mt.mergeMu.Unlock()
		return
	}
	mt.mergeListToSkl(head)

	// No new node inserted, just update head of list.
	if atomic.CompareAndSwapPointer(&mt.pendingList, unsafe.Pointer(head), nil) {
		mt.mergeMu.Unlock()
		return
	}
	// New node inserted, iterate to find `prev` of old head.
	curr := (*listNode)(atomic.LoadPointer(&mt.pendingList))
	for curr != nil {
		next := atomic.LoadPointer(&curr.next)
		if unsafe.Pointer(head) == next {
			atomic.StorePointer(&curr.next, nil)
			mt.mergeMu.Unlock()
			return
		}
		curr = (*listNode)(next)
	}
}

func (mt *MemTable) putToSkl(entries []Entry) {
	var hint skl.Hint
	for _, e := range entries {
		mt.skl.PutWithHint(e.Key, e.Value, &hint)
	}
}

func (mt *MemTable) putToList(entries []Entry) {
	n := newListNode(entries)
	for {
		old := atomic.LoadPointer(&mt.pendingList)
		n.next = old
		if atomic.CompareAndSwapPointer(&mt.pendingList, old, unsafe.Pointer(n)) {
			return
		}
	}
}

func (mt *MemTable) mergeListToSkl(n *listNode) {
	next := (*listNode)(atomic.LoadPointer(&n.next))
	if next != nil {
		mt.mergeListToSkl(next)
	}
	atomic.StorePointer(&n.next, nil)
	mt.putToSkl(n.entries)
	atomic.StoreUint32(&n.merged, 1)
}

type listNode struct {
	next    unsafe.Pointer // *listNode
	merged  uint32
	entries []Entry
	memSize int64
}

func newListNode(entries []Entry) *listNode {
	n := &listNode{entries: entries}
	for _, e := range n.entries {
		sz := len(e.Key) + int(e.Value.EncodedSize()) + skl.EstimateNodeSize
		n.memSize += int64(sz)
	}
	for _, e := range entries {
		e.Value.Version = y.ParseTs(e.Key)
	}
	return n
}

func (n *listNode) isMerged() bool {
	return atomic.LoadUint32(&n.merged) == 1
}

func (n *listNode) get(key []byte) (y.ValueStruct, bool) {
	i := sort.Search(len(n.entries), func(i int) bool {
		return y.CompareKeys(n.entries[i].Key, key) >= 0
	})
	if i < len(n.entries) && y.SameKey(key, n.entries[i].Key) {
		return n.entries[i].Value, true
	}
	return y.ValueStruct{}, false
}

func (n *listNode) newIterator(reverse bool) *listNodeIterator {
	return &listNodeIterator{reversed: reverse, n: n}
}

type listNodeIterator struct {
	idx      int
	n        *listNode
	reversed bool
}

func (it *listNodeIterator) Next() {
	if !it.reversed {
		it.idx++
	} else {
		it.idx--
	}
}

func (it *listNodeIterator) Rewind() {
	if !it.reversed {
		it.idx = 0
	} else {
		it.idx = len(it.n.entries) - 1
	}
}

func (it *listNodeIterator) Seek(key []byte) {
	it.idx = sort.Search(len(it.n.entries), func(i int) bool {
		return y.CompareKeys(it.n.entries[i].Key, key) >= 0
	})
	if it.reversed {
		if !bytes.Equal(it.Key(), key) {
			it.idx--
		}
	}
}

func (it *listNodeIterator) Key() []byte { return it.n.entries[it.idx].Key }

func (it *listNodeIterator) Value() y.ValueStruct { return it.n.entries[it.idx].Value }

func (it *listNodeIterator) FillValue(vs *y.ValueStruct) { *vs = it.Value() }

func (it *listNodeIterator) Valid() bool {
	return !it.n.isMerged() && it.idx >= 0 && it.idx < len(it.n.entries)
}

func (it *listNodeIterator) Close() error { return nil }
