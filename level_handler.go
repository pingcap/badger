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
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/pingcap/badger/epoch"
	"github.com/pingcap/badger/table"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type levelHandler struct {
	// Guards tables, totalSize.
	sync.RWMutex

	// For level >= 1, tables are sorted by key ranges, which do not overlap.
	// For level 0, tables are sorted by time.
	// For level 0, newest table are at the back. Compact the oldest one first, which is at the front.
	tables    []table.Table
	totalSize int64

	// The following are initialized once and const.
	level        int
	strLevel     string
	maxTotalSize int64
	db           *DB
	metrics      *y.LevelMetricsSet
}

func (s *levelHandler) getTotalSize() int64 {
	s.RLock()
	defer s.RUnlock()
	return s.totalSize
}

// initTables replaces s.tables with given tables. This is done during loading.
func (s *levelHandler) initTables(tables []table.Table) {
	s.Lock()
	defer s.Unlock()

	s.tables = tables
	s.totalSize = 0
	for _, t := range tables {
		s.totalSize += t.Size()
	}

	if s.level == 0 {
		// Key range will overlap. Just sort by fileID in ascending order
		// because newer tables are at the end of level 0.
		sort.Slice(s.tables, func(i, j int) bool {
			return s.tables[i].ID() < s.tables[j].ID()
		})
	} else {
		// Sort tables by keys.
		sortTables(s.tables)
	}
}

// deleteTables remove tables idx0, ..., idx1-1.
func (s *levelHandler) deleteTables(toDel []table.Table, guard *epoch.Guard, isMove bool) {
	s.Lock() // s.Unlock() below

	toDelMap := make(map[uint64]struct{})
	for _, t := range toDel {
		toDelMap[t.ID()] = struct{}{}
	}

	// Make a copy as iterators might be keeping a slice of tables.
	var newTables []table.Table
	for _, t := range s.tables {
		_, found := toDelMap[t.ID()]
		if !found {
			newTables = append(newTables, t)
			continue
		}
		s.totalSize -= t.Size()
	}
	s.tables = newTables

	assertTablesOrder(s.level, newTables, nil)
	s.Unlock()

	if !isMove {
		del := make([]epoch.Resource, len(toDel))
		for i := range toDel {
			del[i] = toDel[i]
		}
		guard.Delete(del)
	}
}

func assertTablesOrder(level int, tables []table.Table, cd *CompactDef) {
	if level == 0 {
		return
	}

	for i := 0; i < len(tables)-1; i++ {
		if tables[i].Smallest().Compare(tables[i].Biggest()) > 0 ||
			tables[i].Smallest().Compare(tables[i+1].Smallest()) >= 0 ||
			tables[i].Biggest().Compare(tables[i+1].Biggest()) >= 0 {

			var sb strings.Builder
			if cd != nil {
				fmt.Fprintf(&sb, "%s\n", cd)
			}
			fmt.Fprintf(&sb, "the order of level %d tables is invalid:\n", level)
			for idx, tbl := range tables {
				tag := "  "
				if idx == i {
					tag = "->"
				}
				fmt.Fprintf(&sb, "%s %v %v\n", tag, tbl.Smallest(), tbl.Biggest())
			}
			panic(sb.String())
		}
	}
}

func sortTables(tables []table.Table) {
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].Smallest().Compare(tables[j].Smallest()) < 0
	})
}

// replaceTables will replace tables[left:right] with newTables. Note this EXCLUDES tables[right].
// You must call decr() to delete the old tables _after_ writing the update to the manifest.
func (s *levelHandler) replaceTables(newTables []table.Table, cd *CompactDef, guard *epoch.Guard) {
	// Do not return even if len(newTables) is 0 because we need to delete bottom tables.
	assertTablesOrder(s.level, newTables, cd)

	s.Lock() // We s.Unlock() below.

	// Increase totalSize first.
	for _, tbl := range newTables {
		s.totalSize += tbl.Size()
	}
	left, right := s.overlappingTables(levelHandlerRLocked{}, cd.nextRange)
	toDelete := make([]epoch.Resource, 0, right-left)
	// Update totalSize and reference counts.
	for i := left; i < right; i++ {
		tbl := s.tables[i]
		if containsTable(cd.Bot, tbl) {
			s.totalSize -= tbl.Size()
			toDelete = append(toDelete, tbl)
		}
	}
	tables := make([]table.Table, 0, left+len(newTables)+len(cd.SkippedTbls)+(len(s.tables)-right))
	tables = append(tables, s.tables[:left]...)
	tables = append(tables, newTables...)
	tables = append(tables, cd.SkippedTbls...)
	tables = append(tables, s.tables[right:]...)
	sortTables(tables)
	assertTablesOrder(s.level, tables, cd)
	s.tables = tables
	s.Unlock()
	guard.Delete(toDelete)
}

func containsTable(tables []table.Table, tbl table.Table) bool {
	for _, t := range tables {
		if tbl == t {
			return true
		}
	}
	return false
}

func newLevelHandler(db *DB, level int) *levelHandler {
	label := fmt.Sprintf("L%d", level)
	return &levelHandler{
		level:    level,
		strLevel: label,
		db:       db,
		metrics:  db.metrics.NewLevelMetricsSet(label),
	}
}

// tryAddLevel0Table returns true if ok and no stalling.
func (s *levelHandler) tryAddLevel0Table(t table.Table) bool {
	y.Assert(s.level == 0)
	// Need lock as we may be deleting the first table during a level 0 compaction.
	s.Lock()
	defer s.Unlock()
	// Return false only if number of tables is more than number of
	// ZeroTableStall. For on disk L0, we should just add the tables to the level.
	if len(s.tables) >= s.db.opt.NumLevelZeroTablesStall {
		return false
	}

	s.tables = append(s.tables, t)
	s.totalSize += t.Size()

	return true
}

func (s *levelHandler) addTable(t table.Table) {
	s.Lock()
	defer s.Unlock()

	s.totalSize += t.Size()
	if s.level == 0 {
		s.tables = append(s.tables, t)
		return
	}

	i := sort.Search(len(s.tables), func(i int) bool {
		return s.tables[i].Smallest().Compare(t.Biggest()) >= 0
	})
	if i == len(s.tables) {
		s.tables = append(s.tables, t)
	} else {
		s.tables = append(s.tables[:i+1], s.tables[i:]...)
		s.tables[i] = t
	}
}

func (s *levelHandler) numTables() int {
	s.RLock()
	defer s.RUnlock()
	return len(s.tables)
}

func (s *levelHandler) close() error {
	s.RLock()
	defer s.RUnlock()
	var err error
	for _, t := range s.tables {
		if closeErr := t.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	return errors.Wrap(err, "levelHandler.close")
}

// getTablesForKey acquires a read-lock to access s.tables. It returns a list of tables.
func (s *levelHandler) getTablesForKey(key y.Key) []table.Table {
	s.RLock()
	defer s.RUnlock()

	if s.level == 0 {
		return s.getLevel0Tables()
	}
	tbl := s.getLevelNTable(key)
	if tbl == nil {
		return nil
	}
	return []table.Table{tbl}
}

// getTablesForKeys returns tables for pairs.
// level0 returns all tables.
// level1+ returns tables for every key.
func (s *levelHandler) getTablesForKeys(pairs []keyValuePair) []table.Table {
	s.RLock()
	defer s.RUnlock()
	if s.level == 0 {
		return s.getLevel0Tables()
	}
	out := make([]table.Table, len(pairs))
	for i, pair := range pairs {
		out[i] = s.getLevelNTable(pair.key)
	}
	return out
}

func (s *levelHandler) getLevel0Tables() []table.Table {
	// For level 0, we need to check every table. Remember to make a copy as s.tables may change
	// once we exit this function, and we don't want to lock s.tables while seeking in tables.
	// CAUTION: Reverse the tables.
	out := make([]table.Table, 0, len(s.tables))
	for i := len(s.tables) - 1; i >= 0; i-- {
		out = append(out, s.tables[i])
	}
	return out
}

func (s *levelHandler) getLevelNTable(key y.Key) table.Table {
	// For level >= 1, we can do a binary search as key range does not overlap.
	idx := sort.Search(len(s.tables), func(i int) bool {
		return s.tables[i].Biggest().Compare(key) >= 0
	})
	if idx >= len(s.tables) {
		// Given key is strictly > than every element we have.
		return nil
	}
	tbl := s.tables[idx]
	return tbl
}

// get returns value for a given key or the key after that. If not found, return nil.
func (s *levelHandler) get(key y.Key, keyHash uint64) y.ValueStruct {
	tables := s.getTablesForKey(key)
	return s.getInTables(key, keyHash, tables)
}

func (s *levelHandler) getInTables(key y.Key, keyHash uint64, tables []table.Table) y.ValueStruct {
	for _, table := range tables {
		result := s.getInTable(key, keyHash, table)
		if result.Valid() {
			return result
		}
	}
	return y.ValueStruct{}
}

func (s *levelHandler) getInTable(key y.Key, keyHash uint64, table table.Table) y.ValueStruct {
	s.metrics.NumLSMGets.Inc()
	// TODO: error handling here
	result, err := table.Get(key, keyHash)
	if err != nil {
		log.Error("get data in table failed", zap.Error(err))
	}
	if !result.Valid() {
		s.metrics.NumLSMBloomFalsePositive.Inc()
	}
	return result
}

func (s *levelHandler) multiGet(pairs []keyValuePair) {
	tables := s.getTablesForKeys(pairs)
	if s.level == 0 {
		s.multiGetLevel0(pairs, tables)
	} else {
		s.multiGetLevelN(pairs, tables)
	}
}

func (s *levelHandler) multiGetLevel0(pairs []keyValuePair, tables []table.Table) {
	for _, table := range tables {
		for i := range pairs {
			pair := &pairs[i]
			if pair.found {
				continue
			}
			if pair.key.Compare(table.Smallest()) < 0 || pair.key.Compare(table.Biggest()) > 0 {
				continue
			}
			for {
				val := s.getInTable(pair.key, pair.hash, table)
				if val.Valid() {
					pair.val = val
					pair.found = true
				}
				break
			}
		}
	}
}

func (s *levelHandler) multiGetLevelN(pairs []keyValuePair, tables []table.Table) {
	for i := range pairs {
		pair := &pairs[i]
		if pair.found {
			continue
		}
		table := tables[i]
		if table == nil {
			continue
		}
		for {
			val := s.getInTable(pair.key, pair.hash, table)
			if val.Valid() {
				pair.val = val
				pair.found = true
			}
			break
		}
	}
}

// appendIterators appends iterators to an array of iterators, for merging.
// Note: This obtains references for the table handlers. Remember to close these iterators.
func (s *levelHandler) appendIterators(iters []y.Iterator, opts *IteratorOptions) []y.Iterator {
	s.RLock()
	defer s.RUnlock()

	if s.level == 0 {
		// Remember to add in reverse order!
		// The newer table at the end of s.tables should be added first as it takes precedence.
		overlapTables := make([]table.Table, 0, len(s.tables))
		for _, t := range s.tables {
			if opts.OverlapTable(t) {
				overlapTables = append(overlapTables, t)
			}
		}
		return appendIteratorsReversed(iters, overlapTables, opts.Reverse)
	}
	overlapTables := opts.OverlapTables(s.tables)
	if len(overlapTables) == 0 {
		return iters
	}
	return append(iters, table.NewConcatIterator(overlapTables, opts.Reverse))
}

type levelHandlerRLocked struct{}

// overlappingTables returns the tables that intersect with key range. Returns a half-interval.
// This function should already have acquired a read lock, and this is so important the caller must
// pass an empty parameter declaring such.
func (s *levelHandler) overlappingTables(_ levelHandlerRLocked, kr keyRange) (int, int) {
	return getTablesInRange(s.tables, kr.left, kr.right)
}

func getTablesInRange(tbls []table.Table, start, end y.Key) (int, int) {
	left := sort.Search(len(tbls), func(i int) bool {
		return start.Compare(tbls[i].Biggest()) <= 0
	})
	right := sort.Search(len(tbls), func(i int) bool {
		return end.Compare(tbls[i].Smallest()) < 0
	})
	return left, right
}
