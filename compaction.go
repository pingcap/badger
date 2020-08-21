/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
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

package badger

import (
	"bytes"
	"fmt"
	"log"
	"sync"

	"github.com/pingcap/badger/table"
	"github.com/pingcap/badger/y"
)

type keyRange struct {
	left  y.Key
	right y.Key
	inf   bool
}

var infRange = keyRange{inf: true}

func (r keyRange) String() string {
	return fmt.Sprintf("[left=%x, right=%x, inf=%v]", r.left, r.right, r.inf)
}

func (r keyRange) equals(dst keyRange) bool {
	return r.left.Equal(dst.left) &&
		r.right.Equal(dst.right) &&
		r.inf == dst.inf
}

func (r keyRange) overlapsWith(dst keyRange) bool {
	if r.inf || dst.inf {
		return true
	}

	// If my left is greater than dst right, we have no overlap.
	if r.left.Compare(dst.right) > 0 {
		return false
	}
	// If my right is less than dst left, we have no overlap.
	if r.right.Compare(dst.left) < 0 {
		return false
	}
	// We have overlap.
	return true
}

func getKeyRange(tables []table.Table) keyRange {
	y.Assert(len(tables) > 0)
	smallest := tables[0].Smallest()
	biggest := tables[0].Biggest()
	for i := 1; i < len(tables); i++ {
		if tables[i].Smallest().Compare(smallest) < 0 {
			smallest = tables[i].Smallest()
		}
		if tables[i].Biggest().Compare(biggest) > 0 {
			biggest = tables[i].Biggest()
		}
	}
	return keyRange{
		left:  smallest,
		right: biggest,
	}
}

type levelCompactStatus struct {
	ranges    []keyRange
	deltaSize int64
}

func (lcs *levelCompactStatus) debug() string {
	var b bytes.Buffer
	for _, r := range lcs.ranges {
		b.WriteString(r.String())
	}
	return b.String()
}

func (lcs *levelCompactStatus) overlapsWith(dst keyRange) bool {
	for _, r := range lcs.ranges {
		if r.overlapsWith(dst) {
			return true
		}
	}
	return false
}

func (lcs *levelCompactStatus) remove(dst keyRange) bool {
	final := lcs.ranges[:0]
	var found bool
	for _, r := range lcs.ranges {
		if !r.equals(dst) {
			final = append(final, r)
		} else {
			found = true
		}
	}
	lcs.ranges = final
	return found
}

type compactStatus struct {
	sync.RWMutex
	levels []*levelCompactStatus
}

func (cs *compactStatus) overlapsWith(level int, this keyRange) bool {
	cs.RLock()
	defer cs.RUnlock()

	thisLevel := cs.levels[level]
	return thisLevel.overlapsWith(this)
}

func (cs *compactStatus) deltaSize(l int) int64 {
	cs.RLock()
	defer cs.RUnlock()
	return cs.levels[l].deltaSize
}

type thisAndNextLevelRLocked struct{}

// compareAndAdd will check whether we can run this compactDef. That it doesn't overlap with any
// other running compaction. If it can be run, it would store this run in the compactStatus state.
func (cs *compactStatus) compareAndAdd(_ thisAndNextLevelRLocked, cd compactDef) bool {
	cs.Lock()
	defer cs.Unlock()

	level := cd.thisLevel.level

	y.AssertTruef(level < len(cs.levels)-1, "Got level %d. Max levels: %d", level, len(cs.levels))
	thisLevel := cs.levels[level]
	nextLevel := cs.levels[level+1]

	if thisLevel.overlapsWith(cd.thisRange) {
		return false
	}
	if nextLevel.overlapsWith(cd.nextRange) {
		return false
	}
	// Check whether this level really needs compaction or not. Otherwise, we'll end up
	// running parallel compactions for the same level.
	// NOTE: We can directly call thisLevel.totalSize, because we already have acquire a read lock
	// over this and the next level.
	if cd.thisLevel.totalSize-thisLevel.deltaSize < cd.thisLevel.maxTotalSize {
		return false
	}

	thisLevel.ranges = append(thisLevel.ranges, cd.thisRange)
	nextLevel.ranges = append(nextLevel.ranges, cd.nextRange)
	thisLevel.deltaSize += cd.topSize
	cd.markTablesCompacting()
	return true
}

func (cs *compactStatus) delete(cd *compactDef) {
	cs.Lock()
	defer cs.Unlock()

	level := cd.thisLevel.level
	y.AssertTruef(level < len(cs.levels)-1, "Got level %d. Max levels: %d", level, len(cs.levels))

	thisLevel := cs.levels[level]
	nextLevel := cs.levels[level+1]

	thisLevel.deltaSize -= cd.topSize
	found := thisLevel.remove(cd.thisRange)
	found = nextLevel.remove(cd.nextRange) && found

	if !found {
		this := cd.thisRange
		next := cd.nextRange
		fmt.Printf("Looking for: [%q, %q, %v] in this level.\n", this.left, this.right, this.inf)
		fmt.Printf("This Level:\n%s\n", thisLevel.debug())
		fmt.Println()
		fmt.Printf("Looking for: [%q, %q, %v] in next level.\n", next.left, next.right, next.inf)
		fmt.Printf("Next Level:\n%s\n", nextLevel.debug())
		log.Fatal("keyRange not found")
	}
}

func (cs *compactStatus) isCompacting(level int, tables ...table.Table) bool {
	if len(tables) == 0 {
		return false
	}
	kr := keyRange{
		left:  tables[0].Smallest(),
		right: tables[len(tables)-1].Biggest(),
	}
	y.Assert(!kr.left.IsEmpty())
	y.Assert(!kr.right.IsEmpty())
	return cs.overlapsWith(level, kr)
}

type compactDef struct {
	thisLevel *levelHandler
	nextLevel *levelHandler

	top []table.Table
	bot []table.Table

	skippedTbls []table.Table

	thisRange keyRange
	nextRange keyRange

	topSize     int64
	topLeftIdx  int
	topRightIdx int
	botSize     int64
	botLeftIdx  int
	botRightIdx int
}

func (cd *compactDef) String() string {
	return fmt.Sprintf("%d top:[%d:%d](%d), bot:[%d:%d](%d), skip:%d, write_amp:%.2f",
		cd.thisLevel.level, cd.topLeftIdx, cd.topRightIdx, cd.topSize,
		cd.botLeftIdx, cd.botRightIdx, cd.botSize, len(cd.skippedTbls), float64(cd.topSize+cd.botSize)/float64(cd.topSize))
}

func (cd *compactDef) lockLevels() {
	cd.thisLevel.RLock()
	cd.nextLevel.RLock()
}

func (cd *compactDef) unlockLevels() {
	cd.nextLevel.RUnlock()
	cd.thisLevel.RUnlock()
}

func (cd *compactDef) smallest() y.Key {
	if len(cd.bot) > 0 && cd.nextRange.left.Compare(cd.thisRange.left) < 0 {
		return cd.nextRange.left
	}
	return cd.thisRange.left
}

func (cd *compactDef) biggest() y.Key {
	if len(cd.bot) > 0 && cd.nextRange.right.Compare(cd.thisRange.right) > 0 {
		return cd.nextRange.right
	}
	return cd.thisRange.right
}

func (cd *compactDef) markTablesCompacting() {
	for _, tbl := range cd.top {
		tbl.MarkCompacting(true)
	}
	for _, tbl := range cd.bot {
		tbl.MarkCompacting(true)
	}
	for _, tbl := range cd.skippedTbls {
		tbl.MarkCompacting(true)
	}
}

const minSkippedTableSize = 1024 * 1024

func (cd *compactDef) fillBottomTables(overlappingTables []table.Table) {
	for _, t := range overlappingTables {
		// If none of the top tables contains the range in an overlapping bottom table,
		// we can skip it during compaction to reduce write amplification.
		var added bool
		for _, topTbl := range cd.top {
			if topTbl.HasOverlap(t.Smallest(), t.Biggest(), true) {
				cd.bot = append(cd.bot, t)
				added = true
				break
			}
		}
		if !added {
			if t.Size() >= minSkippedTableSize {
				// We need to limit the minimum size of the table to be skipped,
				// otherwise the number of tables in a level will keep growing
				// until we meet too many open files error.
				cd.skippedTbls = append(cd.skippedTbls, t)
			} else {
				cd.bot = append(cd.bot, t)
			}
		}
	}
}

func (cd *compactDef) fillTablesL0(cs *compactStatus) bool {
	cd.lockLevels()
	defer cd.unlockLevels()

	if len(cd.thisLevel.tables) == 0 {
		return false
	}

	cd.top = make([]table.Table, len(cd.thisLevel.tables))
	copy(cd.top, cd.thisLevel.tables)
	for _, t := range cd.top {
		cd.topSize += t.Size()
	}
	cd.topRightIdx = len(cd.top)

	cd.thisRange = infRange

	kr := getKeyRange(cd.top)
	left, right := cd.nextLevel.overlappingTables(levelHandlerRLocked{}, kr)
	overlappingTables := cd.nextLevel.tables[left:right]
	cd.botLeftIdx = left
	cd.botRightIdx = right
	cd.fillBottomTables(overlappingTables)
	for _, t := range cd.bot {
		cd.botSize += t.Size()
	}

	if len(overlappingTables) == 0 { // the bottom-most level
		cd.nextRange = kr
	} else {
		cd.nextRange = getKeyRange(overlappingTables)
	}

	if !cs.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
		return false
	}

	return true
}

const maxCompactionExpandSize = 1 << 30 // 1GB

func (cd *compactDef) fillTables(cs *compactStatus) bool {
	cd.lockLevels()
	defer cd.unlockLevels()

	if len(cd.thisLevel.tables) == 0 {
		return false
	}
	this := make([]table.Table, len(cd.thisLevel.tables))
	copy(this, cd.thisLevel.tables)
	next := make([]table.Table, len(cd.nextLevel.tables))
	copy(next, cd.nextLevel.tables)

	// First pick one table has max topSize/bottomSize ratio.
	var candidateRatio float64
	for i, t := range this {
		if cs.isCompacting(cd.thisLevel.level, t) {
			continue
		}
		left, right := getTablesInRange(next, t.Smallest(), t.Biggest())
		if cs.isCompacting(cd.nextLevel.level, next[left:right]...) {
			continue
		}
		botSize := sumTableSize(next[left:right])
		ratio := calcRatio(t.Size(), botSize)
		if ratio > candidateRatio {
			candidateRatio = ratio
			cd.topLeftIdx = i
			cd.topRightIdx = i + 1
			cd.top = this[cd.topLeftIdx:cd.topRightIdx:cd.topRightIdx]
			cd.topSize = t.Size()
			cd.botLeftIdx = left
			cd.botRightIdx = right
			cd.botSize = botSize
		}
	}
	if len(cd.top) == 0 {
		return false
	}
	bots := next[cd.botLeftIdx:cd.botRightIdx:cd.botRightIdx]
	// Expand to left to include more tops as long as the ratio doesn't decrease and the total size
	// do not exceeds maxCompactionExpandSize.
	for i := cd.topLeftIdx - 1; i >= 0; i-- {
		t := this[i]
		if cs.isCompacting(cd.thisLevel.level, t) {
			break
		}
		left, right := getTablesInRange(next, t.Smallest(), t.Biggest())
		if right < cd.botLeftIdx {
			// A bottom table is skipped, we can compact in another run.
			break
		}
		if cs.isCompacting(cd.nextLevel.level, next[left:cd.botLeftIdx]...) {
			break
		}
		newTopSize := t.Size() + cd.topSize
		newBotSize := sumTableSize(next[left:cd.botLeftIdx]) + cd.botSize
		newRatio := calcRatio(newTopSize, newBotSize)
		if newRatio > candidateRatio && (newTopSize+newBotSize) < maxCompactionExpandSize {
			cd.top = append([]table.Table{t}, cd.top...)
			cd.topLeftIdx--
			bots = append(next[left:cd.botLeftIdx:cd.botLeftIdx], bots...)
			cd.botLeftIdx = left
			cd.topSize = newTopSize
			cd.botSize = newBotSize
		} else {
			break
		}
	}
	// Expand to right to include more tops as long as the ratio doesn't decrease and the total size
	// do not exceeds maxCompactionExpandSize.
	for i := cd.topRightIdx; i < len(this); i++ {
		t := this[i]
		if cs.isCompacting(cd.thisLevel.level, t) {
			break
		}
		left, right := getTablesInRange(next, t.Smallest(), t.Biggest())
		if left > cd.botRightIdx {
			// A bottom table is skipped, we can compact in another run.
			break
		}
		if cs.isCompacting(cd.nextLevel.level, next[cd.botRightIdx:right]...) {
			break
		}
		newTopSize := t.Size() + cd.topSize
		newBotSize := sumTableSize(next[cd.botRightIdx:right]) + cd.botSize
		newRatio := calcRatio(newTopSize, newBotSize)
		if newRatio > candidateRatio && (newTopSize+newBotSize) < maxCompactionExpandSize {
			cd.top = append(cd.top, t)
			cd.topRightIdx++
			bots = append(bots, next[cd.botRightIdx:right]...)
			cd.botRightIdx = right
			cd.topSize = newTopSize
			cd.botSize = newBotSize
		} else {
			break
		}
	}
	cd.thisRange = keyRange{left: cd.top[0].Smallest(), right: cd.top[len(cd.top)-1].Biggest()}
	if len(bots) > 0 {
		cd.nextRange = keyRange{left: bots[0].Smallest(), right: bots[len(bots)-1].Biggest()}
	} else {
		cd.nextRange = cd.thisRange
	}
	cd.fillBottomTables(bots)
	for _, t := range cd.skippedTbls {
		cd.botSize -= t.Size()
	}
	return cs.compareAndAdd(thisAndNextLevelRLocked{}, *cd)
}
