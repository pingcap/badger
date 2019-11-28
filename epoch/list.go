package epoch

import (
	"sync/atomic"
	"unsafe"
)

type guardList struct {
	head unsafe.Pointer

	it struct {
		loc     *unsafe.Pointer
		curr    *Guard
		delCurr bool
	}
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

func (l *guardList) iterate(f func(*Guard) bool) {
	it := &l.it
	it.loc = &l.head
	it.curr = (*Guard)(atomic.LoadPointer(&l.head))

	for it.curr != nil {
		delete := f(it.curr)

		next := it.curr.next
		// if current node is the head of list when start iteration
		// we cannot delete it from list, because the `it.list.head` may
		// point to a new node, if `it.loc` is updated we will lost the newly added nodes.
		if delete && it.loc != &l.head {
			*it.loc = next
		} else {
			it.loc = &it.curr.next
		}
		it.curr = (*Guard)(next)
	}
}
