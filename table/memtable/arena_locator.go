// Copyright 2019-present PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package memtable

import (
	"math"
	"time"
	"unsafe"

	"github.com/pingcap/log"
)

type arenaAddr uint64

const (
	blockAlign                = int(unsafe.Sizeof(uint64(0))) - 1
	alignMask                 = 1<<32 - int(unsafe.Sizeof(uint64(0))) // 29 bit 1 and 3 bit 0.
	nullBlockOffset           = math.MaxUint32
	nullArenaAddr   arenaAddr = 0

	// Time waited until we reuse the empty block.
	// Data corruption can happen under this time sequence.
	// 1. a reader reads a node.
	// 2. a writer delete the node, then free the block, put it into the writable queue
	// 	and this block become the first writable block.
	// 3. The writer insert another node, overwrite the block we just freed.
	// 4. The reader reads the key/value of that delete node.
	// But because the time between 1 and 4 is very short, this is very unlikely to happen but it can happen.
	// So we wait for a while so the reader can finish reading before we overwrite the empty block.
	reuseSafeDuration = time.Millisecond * 100

	blockIdxShift uint64 = 48
	blockIdxMask  uint64 = -1 ^ -1<<15<<blockIdxShift

	blockOffsetShift uint64 = 24
	blockOffsetMask  uint64 = -1 ^ -1<<24<<blockOffsetShift

	sizeMask = -1 ^ -1<<24

	blockSize        = 1<<24 - 1
	alignedBlockSize = (blockSize + blockAlign) & alignMask
)

func (addr arenaAddr) blockIdx() int {
	return int(uint64(addr)&blockIdxMask>>blockIdxShift - 1)
}

func (addr arenaAddr) blockOffset() uint32 {
	return uint32(uint64(addr) & blockOffsetMask >> blockOffsetShift)
}

func (addr arenaAddr) size() int {
	return int(uint64(addr) & sizeMask)
}

func newArenaAddr(blockIdx int, blockOffset uint32, size int) arenaAddr {
	return arenaAddr(uint64(blockIdx+1)<<blockIdxShift | uint64(blockOffset)<<blockOffsetShift | uint64(size))
}

type arenaLocator struct {
	blockSize     int
	blocks        []*arenaBlock
	writableQueue []int
	pendingBlocks []pendingBlock
}

type pendingBlock struct {
	blockIdx     int
	reusableTime time.Time
}

func newArenaLocator() *arenaLocator {
	return &arenaLocator{
		blockSize:     alignedBlockSize,
		blocks:        []*arenaBlock{newArenaBlock(alignedBlockSize)},
		writableQueue: []int{0},
	}
}

func (a *arenaLocator) get(addr arenaAddr) []byte {
	if addr.blockIdx() >= len(a.blocks) {
		log.S().Fatalf("arena.get out of range. len(blocks)=%v, addr.blockIdx()=%v, addr.blockOffset()=%v, addr.size()=%v", len(a.blocks), addr.blockIdx(), addr.blockOffset(), addr.size())
	}
	return a.blocks[addr.blockIdx()].get(addr.blockOffset(), addr.size())
}

func (a *arenaLocator) alloc(size int) arenaAddr {
	for {
		if len(a.writableQueue) == 0 {
			if len(a.pendingBlocks) > 0 {
				pending := a.pendingBlocks[0]
				if time.Now().After(pending.reusableTime) {
					a.writableQueue = append(a.writableQueue, pending.blockIdx)
					a.pendingBlocks = a.pendingBlocks[1:]
					continue
				}
			}
			return nullArenaAddr
		}
		availIdx := a.writableQueue[len(a.writableQueue)-1]
		block := a.blocks[availIdx]
		blockOffset := block.alloc(size)
		if blockOffset != nullBlockOffset {
			return newArenaAddr(availIdx, blockOffset, size)
		}
		a.writableQueue = a.writableQueue[:len(a.writableQueue)-1]
	}
}

// free decrease the arena block reference and makes the block reusable.
// We don't know if there is concurrent reader who may reference the deleted entry.
// So we must make sure the old data is not referenced for long time, and we only overwrite
// it after a safe amount of time.
func (a *arenaLocator) free(addr arenaAddr) {
	arena := a.blocks[addr.blockIdx()]
	arena.ref--
	// No reference, the arenaBlock can be reused.
	if arena.ref == 0 && arena.length > len(arena.buf) {
		a.pendingBlocks = append(a.pendingBlocks, pendingBlock{
			blockIdx:     addr.blockIdx(),
			reusableTime: time.Now().Add(reuseSafeDuration),
		})
		arena.length = 0
	}
}

func (a *arenaLocator) grow() *arenaLocator {
	newLoc := new(arenaLocator)
	newLoc.blockSize = a.blockSize
	newLoc.blocks = make([]*arenaBlock, 0, len(a.blocks)+1)
	newLoc.blocks = append(newLoc.blocks, a.blocks...)
	availIdx := len(newLoc.blocks)
	newLoc.blocks = append(newLoc.blocks, newArenaBlock(a.blockSize))
	newLoc.writableQueue = append(newLoc.writableQueue, availIdx)
	newLoc.pendingBlocks = a.pendingBlocks
	return newLoc
}

type arenaBlock struct {
	buf    []byte
	ref    uint64
	length int
}

func newArenaBlock(blockSize int) *arenaBlock {
	return &arenaBlock{
		buf: make([]byte, blockSize),
	}
}

func (a *arenaBlock) get(offset uint32, size int) []byte {
	if size > 0 {
		return a.buf[offset : offset+uint32(size)]
	}
	return a.buf[offset:]
}

func (a *arenaBlock) alloc(size int) uint32 {
	// The returned addr should be aligned in 8 bytes.
	offset := (a.length + blockAlign) & alignMask
	a.length = offset + size
	if a.length > len(a.buf) {
		return nullBlockOffset
	}
	a.ref++
	return uint32(offset)
}
