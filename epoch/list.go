package epoch

import (
	"sync/atomic"
	"unsafe"
)

type guardList struct {
	head unsafe.Pointer
}

func (l *guardList) add(g *Guard) {
	for {
		head := atomic.LoadPointer(&l.head)
		g.next = head
		if atomic.CompareAndSwapPointer(&l.head, head, unsafe.Pointer(g)) {
			return
		}
	}
}

func (l *guardList) newIterator() *guardIterator {
	return &guardIterator{list: l}
}

// note: guardIterator is designed for single thread (gc worker) use.
type guardIterator struct {
	loc     *unsafe.Pointer
	curr    *Guard
	delCurr bool
	list    *guardList
}

func (it *guardIterator) rewind() {
	it.loc = &it.list.head
	it.curr = (*Guard)(atomic.LoadPointer(&it.list.head))
}

func (it *guardIterator) next() {
	next := atomic.LoadPointer(&it.curr.next)

	// if current node is the head of list when start iteration
	// we cannot delete it from list, because the `it.list.head` may 
	// point to a new node, if `it.loc` is updated we will lost the newly added nodes.
	if it.delCurr && it.loc != &it.list.head {
		atomic.StorePointer(it.loc, next)
	} else {
		it.loc = &it.curr.next
	}
	it.curr = (*Guard)(next)
	it.delCurr = false
}

func (it *guardIterator) valid() bool {
	return it.curr != nil
}

func (it *guardIterator) guard() *Guard {
	return it.curr
}

func (it *guardIterator) delete() {
	it.delCurr = true
}
