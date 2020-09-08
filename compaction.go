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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"sync"

	"github.com/pingcap/badger/options"
	"github.com/pingcap/badger/table"
	"github.com/pingcap/badger/table/sstable"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/log"
	"golang.org/x/time/rate"
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

// compareAndAdd will check whether we can run this CompactDef. That it doesn't overlap with any
// other running compaction. If it can be run, it would store this run in the compactStatus state.
func (cs *compactStatus) compareAndAdd(_ thisAndNextLevelRLocked, cd *CompactDef, thisHandler *levelHandler) bool {
	cs.Lock()
	defer cs.Unlock()

	level := cd.Level

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
	if thisHandler.totalSize-thisLevel.deltaSize < thisHandler.maxTotalSize {
		return false
	}

	thisLevel.ranges = append(thisLevel.ranges, cd.thisRange)
	nextLevel.ranges = append(nextLevel.ranges, cd.nextRange)
	thisLevel.deltaSize += cd.topSize
	cd.markTablesCompacting()
	return true
}

func (cs *compactStatus) delete(cd *CompactDef) {
	cs.Lock()
	defer cs.Unlock()

	level := cd.Level
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

type CompactDef struct {
	Level int

	Top []table.Table
	Bot []table.Table

	SkippedTbls []table.Table
	SafeTS      uint64
	Guards      []Guard
	Filter      CompactionFilter
	HasOverlap  bool
	Opt         options.TableBuilderOptions
	Dir         string
	AllocIDFunc func() uint64
	Limiter     *rate.Limiter
	InMemory    bool

	splitHints []y.Key

	thisRange keyRange
	nextRange keyRange

	topSize     int64
	topLeftIdx  int
	topRightIdx int
	botSize     int64
	botLeftIdx  int
	botRightIdx int
}

func (cd *CompactDef) String() string {
	return fmt.Sprintf("%d top:[%d:%d](%d), bot:[%d:%d](%d), skip:%d, write_amp:%.2f",
		cd.Level, cd.topLeftIdx, cd.topRightIdx, cd.topSize,
		cd.botLeftIdx, cd.botRightIdx, cd.botSize, len(cd.SkippedTbls), float64(cd.topSize+cd.botSize)/float64(cd.topSize))
}

func (cd *CompactDef) smallest() y.Key {
	if len(cd.Bot) > 0 && cd.nextRange.left.Compare(cd.thisRange.left) < 0 {
		return cd.nextRange.left
	}
	return cd.thisRange.left
}

func (cd *CompactDef) biggest() y.Key {
	if len(cd.Bot) > 0 && cd.nextRange.right.Compare(cd.thisRange.right) > 0 {
		return cd.nextRange.right
	}
	return cd.thisRange.right
}

func (cd *CompactDef) markTablesCompacting() {
	for _, tbl := range cd.Top {
		tbl.MarkCompacting(true)
	}
	for _, tbl := range cd.Bot {
		tbl.MarkCompacting(true)
	}
	for _, tbl := range cd.SkippedTbls {
		tbl.MarkCompacting(true)
	}
}

const minSkippedTableSize = 1024 * 1024

func (cd *CompactDef) fillBottomTables(overlappingTables []table.Table) {
	for _, t := range overlappingTables {
		// If none of the Top tables contains the range in an overlapping bottom table,
		// we can skip it during compaction to reduce write amplification.
		var added bool
		for _, topTbl := range cd.Top {
			if topTbl.HasOverlap(t.Smallest(), t.Biggest(), true) {
				cd.Bot = append(cd.Bot, t)
				added = true
				break
			}
		}
		if !added {
			if t.Size() >= minSkippedTableSize {
				// We need to limit the minimum size of the table to be skipped,
				// otherwise the number of tables in a level will keep growing
				// until we meet too many open files error.
				cd.SkippedTbls = append(cd.SkippedTbls, t)
			} else {
				cd.Bot = append(cd.Bot, t)
			}
		}
	}
}

func (cd *CompactDef) fillTablesL0(cs *compactStatus, thisLevel, nextLevel *levelHandler) bool {
	cd.lockLevels(thisLevel, nextLevel)
	defer cd.unlockLevels(thisLevel, nextLevel)

	if len(thisLevel.tables) == 0 {
		return false
	}

	cd.Top = make([]table.Table, len(thisLevel.tables))
	copy(cd.Top, thisLevel.tables)
	for _, t := range cd.Top {
		cd.topSize += t.Size()
	}
	cd.topRightIdx = len(cd.Top)

	cd.thisRange = infRange

	kr := getKeyRange(cd.Top)
	left, right := nextLevel.overlappingTables(levelHandlerRLocked{}, kr)
	overlappingTables := nextLevel.tables[left:right]
	cd.botLeftIdx = left
	cd.botRightIdx = right
	cd.fillBottomTables(overlappingTables)
	for _, t := range cd.Bot {
		cd.botSize += t.Size()
	}

	if len(overlappingTables) == 0 { // the bottom-most level
		cd.nextRange = kr
	} else {
		cd.nextRange = getKeyRange(overlappingTables)
	}

	if !cs.compareAndAdd(thisAndNextLevelRLocked{}, cd, thisLevel) {
		return false
	}

	return true
}

const maxCompactionExpandSize = 1 << 30 // 1GB

func (cd *CompactDef) fillTables(cs *compactStatus, thisLevel, nextLevel *levelHandler) bool {
	cd.lockLevels(thisLevel, nextLevel)
	defer cd.unlockLevels(thisLevel, nextLevel)

	if len(thisLevel.tables) == 0 {
		return false
	}
	this := make([]table.Table, len(thisLevel.tables))
	copy(this, thisLevel.tables)
	next := make([]table.Table, len(nextLevel.tables))
	copy(next, nextLevel.tables)

	// First pick one table has max topSize/bottomSize ratio.
	var candidateRatio float64
	for i, t := range this {
		if cs.isCompacting(thisLevel.level, t) {
			continue
		}
		left, right := getTablesInRange(next, t.Smallest(), t.Biggest())
		if cs.isCompacting(nextLevel.level, next[left:right]...) {
			continue
		}
		botSize := sumTableSize(next[left:right])
		ratio := calcRatio(t.Size(), botSize)
		if ratio > candidateRatio {
			candidateRatio = ratio
			cd.topLeftIdx = i
			cd.topRightIdx = i + 1
			cd.Top = this[cd.topLeftIdx:cd.topRightIdx:cd.topRightIdx]
			cd.topSize = t.Size()
			cd.botLeftIdx = left
			cd.botRightIdx = right
			cd.botSize = botSize
		}
	}
	if len(cd.Top) == 0 {
		return false
	}
	bots := next[cd.botLeftIdx:cd.botRightIdx:cd.botRightIdx]
	// Expand to left to include more tops as long as the ratio doesn't decrease and the total size
	// do not exceeds maxCompactionExpandSize.
	for i := cd.topLeftIdx - 1; i >= 0; i-- {
		t := this[i]
		if cs.isCompacting(thisLevel.level, t) {
			break
		}
		left, right := getTablesInRange(next, t.Smallest(), t.Biggest())
		if right < cd.botLeftIdx {
			// A bottom table is skipped, we can compact in another run.
			break
		}
		if cs.isCompacting(nextLevel.level, next[left:cd.botLeftIdx]...) {
			break
		}
		newTopSize := t.Size() + cd.topSize
		newBotSize := sumTableSize(next[left:cd.botLeftIdx]) + cd.botSize
		newRatio := calcRatio(newTopSize, newBotSize)
		if newRatio > candidateRatio && (newTopSize+newBotSize) < maxCompactionExpandSize {
			cd.Top = append([]table.Table{t}, cd.Top...)
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
		if cs.isCompacting(thisLevel.level, t) {
			break
		}
		left, right := getTablesInRange(next, t.Smallest(), t.Biggest())
		if left > cd.botRightIdx {
			// A bottom table is skipped, we can compact in another run.
			break
		}
		if cs.isCompacting(nextLevel.level, next[cd.botRightIdx:right]...) {
			break
		}
		newTopSize := t.Size() + cd.topSize
		newBotSize := sumTableSize(next[cd.botRightIdx:right]) + cd.botSize
		newRatio := calcRatio(newTopSize, newBotSize)
		if newRatio > candidateRatio && (newTopSize+newBotSize) < maxCompactionExpandSize {
			cd.Top = append(cd.Top, t)
			cd.topRightIdx++
			bots = append(bots, next[cd.botRightIdx:right]...)
			cd.botRightIdx = right
			cd.topSize = newTopSize
			cd.botSize = newBotSize
		} else {
			break
		}
	}
	cd.thisRange = keyRange{left: cd.Top[0].Smallest(), right: cd.Top[len(cd.Top)-1].Biggest()}
	if len(bots) > 0 {
		cd.nextRange = keyRange{left: bots[0].Smallest(), right: bots[len(bots)-1].Biggest()}
	} else {
		cd.nextRange = cd.thisRange
	}
	cd.fillBottomTables(bots)
	for _, t := range cd.SkippedTbls {
		cd.botSize -= t.Size()
	}
	return cs.compareAndAdd(thisAndNextLevelRLocked{}, cd, thisLevel)
}

func (cd *CompactDef) lockLevels(this, next *levelHandler) {
	this.RLock()
	next.RLock()
}

func (cd *CompactDef) unlockLevels(this, next *levelHandler) {
	next.RUnlock()
	this.RUnlock()
}

func (cd *CompactDef) moveDown() bool {
	return cd.Level > 0 && len(cd.Bot) == 0 && len(cd.SkippedTbls) == 0
}

func (cd *CompactDef) buildIterator() y.Iterator {
	// Create iterators across all the tables involved first.
	var iters []y.Iterator
	if cd.Level == 0 {
		iters = appendIteratorsReversed(iters, cd.Top, false)
	} else {
		iters = []y.Iterator{table.NewConcatIterator(cd.Top, false)}
	}

	// Next level has level>=1 and we can use ConcatIterator as key ranges do not overlap.
	iters = append(iters, table.NewConcatIterator(cd.Bot, false))
	it := table.NewMergeIterator(iters, false)

	it.Rewind()
	return it
}

type compactor interface {
	compact(cd *CompactDef, stats *y.CompactionStats, discardStats *DiscardStats) ([]*sstable.BuildResult, error)
}

type localCompactor struct {
}

func (c *localCompactor) compact(cd *CompactDef, stats *y.CompactionStats, discardStats *DiscardStats) ([]*sstable.BuildResult, error) {
	return CompactTables(cd, stats, discardStats)
}

type remoteCompactor struct {
	remoteAddr string
	allFiles   []*os.File
	req        *CompactionReq
}

type CompactionReq struct {
	Level        int     `json:"level"`
	Overlap      bool    `json:"overlap"`
	NumTop       int     `json:"num_top"`
	FileSizes    []int64 `json:"file_sizes"`
	SafeTS       uint64  `json:"safe_ts"`
	MaxTableSize int64   `json:"max_table_size"`
}

type CompactionResp struct {
	Error     string             `json:"error"`
	FileSizes []int64            `json:"file_sizes"`
	Stats     *y.CompactionStats `json:"stats"`
	NumSkip   int64              `json:"num_skip"`
	SkipBytes int64              `json:"skip_bytes"`
}

func (rc *remoteCompactor) compact(cd *CompactDef, stats *y.CompactionStats, discardStats *DiscardStats) ([]*sstable.BuildResult, error) {
	defer rc.cleanup()
	rc.req = &CompactionReq{
		Level:        cd.Level,
		Overlap:      cd.HasOverlap,
		NumTop:       len(cd.Top),
		SafeTS:       cd.SafeTS,
		MaxTableSize: cd.Opt.MaxTableSize,
	}
	err := rc.appendFiles(cd.Top)
	if err != nil {
		return nil, err
	}
	err = rc.appendFiles(cd.Bot)
	if err != nil {
		return nil, err
	}
	conn, err := net.Dial("tcp", rc.remoteAddr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	err = writeJSON(conn, rc.req)
	if err != nil {
		return nil, err
	}
	for _, file := range rc.allFiles {
		_, err = io.Copy(conn, file)
		if err != nil {
			return nil, err
		}
	}
	resp := new(CompactionResp)
	err = readJSON(conn, resp)
	if err != nil {
		return nil, err
	}
	if len(resp.Error) > 0 {
		return nil, fmt.Errorf("remote compaction error:%s", resp.Error)
	}
	*stats = *resp.Stats
	discardStats.numSkips = resp.NumSkip
	discardStats.skippedBytes = resp.SkipBytes
	var newFileNames []*sstable.BuildResult
	for i := 0; i < len(resp.FileSizes); i += 2 {
		fileID := cd.AllocIDFunc()
		filename := sstable.NewFilename(fileID, cd.Dir)
		err = readFile(conn, filename, resp.FileSizes[i])
		if err != nil {
			return nil, err
		}
		err = readFile(conn, sstable.IndexFilename(filename), resp.FileSizes[i+1])
		if err != nil {
			return nil, err
		}
		newFileNames = append(newFileNames, &sstable.BuildResult{FileName: filename})
	}
	return newFileNames, nil
}

func (rc *remoteCompactor) appendFiles(tbls []table.Table) error {
	for _, tbl := range tbls {
		sst := tbl.(*sstable.Table)
		fn := sst.Filename()
		err := rc.appendFile(fn)
		if err != nil {
			return err
		}
		err = rc.appendFile(sstable.IndexFilename(fn))
		if err != nil {
			return err
		}
	}
	return nil
}

func (rc *remoteCompactor) appendFile(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	rc.allFiles = append(rc.allFiles, file)
	stat, err := file.Stat()
	if err != nil {
		return err
	}
	rc.req.FileSizes = append(rc.req.FileSizes, stat.Size())
	return nil
}

func (rc *remoteCompactor) cleanup() {
	for _, file := range rc.allFiles {
		file.Close()
	}
}

type CompactionServer struct {
	l net.Listener
}

func NewCompactionServer(addr string) (*CompactionServer, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &CompactionServer{
		l: l,
	}, nil
}

func (s *CompactionServer) Close() {
	s.l.Close()
}

func (s *CompactionServer) Run() {
	for {
		conn, err := s.l.Accept()
		if err != nil {
			log.S().Error(err)
			return
		}
		go func() {
			err := s.handleConn(conn)
			if err != nil {
				s.sendError(conn, err)
			}
			conn.Close()
		}()
	}
}

func (c *CompactionServer) handleConn(conn net.Conn) error {
	req := new(CompactionReq)
	err := readJSON(conn, req)
	if err != nil {
		return err
	}
	var recvFiles []*sstable.BuildResult
	for i := 0; i < len(req.FileSizes); i += 2 {
		result := new(sstable.BuildResult)
		result.FileData = make([]byte, req.FileSizes[i])
		result.IndexData = make([]byte, req.FileSizes[i+1])
		_, err = io.ReadFull(conn, result.FileData)
		if err != nil {
			return err
		}
		_, err = io.ReadFull(conn, result.IndexData)
		if err != nil {
			return err
		}
		recvFiles = append(recvFiles, result)
	}
	cd := new(CompactDef)
	cd.HasOverlap = req.Overlap
	for i, result := range recvFiles {
		t, err := sstable.OpenInMemoryTable(result.FileData, result.IndexData)
		if err != nil {
			return err
		}
		if i < req.NumTop {
			cd.Top = append(cd.Top, t)
		} else {
			cd.Bot = append(cd.Bot, t)
		}
	}
	cd.Level = req.Level
	cd.Opt.MaxTableSize = req.MaxTableSize
	cd.SafeTS = req.SafeTS
	cd.Opt = DefaultOptions.TableBuilderOptions
	cd.Opt.CompressionPerLevel = make([]options.CompressionType, 7)
	cd.InMemory = true
	stats := new(y.CompactionStats)
	discardStats := new(DiscardStats)
	newFilenames, err := CompactTables(cd, stats, discardStats)
	if err != nil {
		return err
	}
	for _, t := range cd.Top {
		t.Close()
	}
	for _, t := range cd.Bot {
		t.Close()
	}
	resp := new(CompactionResp)
	resp.Stats = stats
	resp.NumSkip = discardStats.numSkips
	resp.SkipBytes = discardStats.skippedBytes
	c.sendResponse(conn, newFilenames, resp)
	return nil
}

func (c *CompactionServer) sendResponse(conn net.Conn, newFiles []*sstable.BuildResult, resp *CompactionResp) {
	for _, file := range newFiles {
		resp.FileSizes = append(resp.FileSizes, int64(len(file.FileData)), int64(len(file.IndexData)))
	}
	err := writeJSON(conn, resp)
	if err != nil {
		log.S().Error(err)
	}
	for _, file := range newFiles {
		_, err = conn.Write(file.FileData)
		if err != nil {
			log.S().Error(err)
			break
		}
		_, err = conn.Write(file.IndexData)
		if err != nil {
			log.S().Error(err)
		}
	}
}

func (c *CompactionServer) sendError(conn net.Conn, err error) error {
	resp := &CompactionResp{Error: err.Error()}
	return writeJSON(conn, resp)
}

func readJSON(conn net.Conn, v interface{}) error {
	metaSizeBuf := make([]byte, 4)
	_, err := io.ReadFull(conn, metaSizeBuf)
	if err != nil {
		return err
	}
	metabuf := make([]byte, binary.BigEndian.Uint32(metaSizeBuf))
	_, err = io.ReadFull(conn, metabuf)
	if err != nil {
		return err
	}
	return json.Unmarshal(metabuf, v)
}

func writeJSON(conn net.Conn, v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	sizeBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBuf, uint32(len(data)))
	_, err = conn.Write(sizeBuf)
	if err != nil {
		return err
	}
	_, err = conn.Write(data)
	return err
}

func readFile(conn net.Conn, fileName string, fileSize int64) error {
	reader := io.LimitReader(conn, fileSize)
	file, err := os.Create(fileName)
	if err != nil {
		log.S().Error(err)
		return err
	}
	defer file.Close()
	_, err = io.Copy(file, reader)
	if err != nil {
		log.S().Error(err)
	}
	return err
}
