/*
 * Copyright 2018 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package y

import (
	"container/heap"
	"sync/atomic"
	"unsafe"
)

type uint64Heap []uint64

func (u uint64Heap) Len() int               { return len(u) }
func (u uint64Heap) Less(i int, j int) bool { return u[i] < u[j] }
func (u uint64Heap) Swap(i int, j int)      { u[i], u[j] = u[j], u[i] }
func (u *uint64Heap) Push(x interface{})    { *u = append(*u, x.(uint64)) }
func (u *uint64Heap) Pop() interface{} {
	old := *u
	n := len(old)
	x := old[n-1]
	*u = old[0 : n-1]
	return x
}

type mark struct {
	readTs uint64
	done   bool // Set to true if the pending mutation is done.
}
type WaterMark struct {
	markCh    chan mark
	minReadTs uint64
}

// Init initializes a WaterMark struct. MUST be called before using it.
func (w *WaterMark) Init() {
	w.markCh = make(chan mark, 1000)
	go w.process()
}

func (w *WaterMark) Begin(readTs uint64) {
	w.markCh <- mark{readTs: readTs, done: false}
}
func (w *WaterMark) Done(readTs uint64) {
	w.markCh <- mark{readTs: readTs, done: true}
}

// DoneUntil returns the maximum index until which all tasks are done.
func (w *WaterMark) MinReadTs() uint64 {
	return atomic.LoadUint64(&w.minReadTs)
}

// process is used to process the Mark channel. This is not thread-safe,
// so only run one goroutine for process. One is sufficient, because
// all ops in the goroutine use only memory and cpu.
func (w *WaterMark) process() {
	var reads uint64Heap
	// pending maps raft proposal index to the number of pending mutations for this proposal.
	pending := make(map[uint64]int)

	heap.Init(&reads)
	var loop uint64

	processOne := func(readTs uint64, done bool) {
		// If not already done, then set. Otherwise, don't undo a done entry.
		prev, present := pending[readTs]
		if !present {
			heap.Push(&reads, readTs)
		}

		delta := 1
		if done {
			delta = -1
		}
		pending[readTs] = prev + delta

		loop++

		// Update mark by going through all reads in order; and checking if they have
		// been done. Stop at the first readTs, which isn't done.
		minReadTs := w.MinReadTs()
		// Don't assert that minReadTs < readTs, to avoid any inconsistencies caused by managed
		// transactions, or testing where we explicitly set the readTs for transactions like in
		// TestTxnVersions.
		until := minReadTs
		loops := 0

		for len(reads) > 0 {
			min := reads[0]
			if done := pending[min]; done != 0 {
				break // len(reads) will be > 0.
			}
			heap.Pop(&reads)
			delete(pending, min)
			until = min
			loops++
		}
		if until != minReadTs {
			Assert(atomic.CompareAndSwapUint64(&w.minReadTs, minReadTs, until))
		}
	}

	for mark := range w.markCh {
		if mark.readTs > 0 {
			processOne(mark.readTs, mark.done)
		}
	}
}

type FastWaterMark struct {
	head      unsafe.Pointer
	minReadTS uint64
}

type WaterMarkNode struct {
	ReadTS  uint64
	isAlive uint64
	next    unsafe.Pointer
}

func NewFastWaterMark() *FastWaterMark {
	return &FastWaterMark{
		head: unsafe.Pointer(&WaterMarkNode{}),
	}
}

// Begin marks readTS to prevent the SST files being deleted while reading.
// The real readTS to use is the returned WaterMarkNode.ReadTS to make sure readTS is increasing monotonically.
// MinReadTS will be less than or equal to readTS until Done is called.
func (wm *FastWaterMark) Begin(readTSHint uint64) *WaterMarkNode {
	n := &WaterMarkNode{
		isAlive: 1,
	}
	for {
		headPtr := atomic.LoadPointer(&wm.head)
		head := (*WaterMarkNode)(headPtr)
		if head.ReadTS >= readTSHint {
			// Make sure the ReadTS is increasing monotonically.
			n.ReadTS = head.ReadTS
		} else {
			n.ReadTS = readTSHint
		}
		n.next = headPtr
		if atomic.CompareAndSwapPointer(&wm.head, headPtr, unsafe.Pointer(n)) {
			return n
		}
	}
}

// Done unmark the WaterMarkNode.ReadTS, it may increase the MinReadTS if the all the older WaterMarkNode is dead.
func (wm *FastWaterMark) Done(n *WaterMarkNode) {
	next := (*WaterMarkNode)(atomic.LoadPointer(&n.next))
	var nextChanged bool
	for {
		if next == nil {
			atomic.StoreUint64(&wm.minReadTS, n.ReadTS)
			break
		}
		if atomic.LoadUint64(&next.isAlive) > 0 {
			break
		}
		next = (*WaterMarkNode)(atomic.LoadPointer(&next.next))
		nextChanged = true
	}
	if nextChanged {
		atomic.StorePointer(&n.next, unsafe.Pointer(next))
	}
	atomic.StoreUint64(&n.isAlive, 0)
}

// MinReadTS returns the minimum readTS in used, so older version files can be safely deleted.
func (wm *FastWaterMark) MinReadTS() uint64 {
	return atomic.LoadUint64(&wm.minReadTS)
}
