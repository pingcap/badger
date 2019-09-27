package badger

import (
	"math"
	"sync"

	"github.com/coocood/badger/protos"
	"github.com/coocood/badger/table"
	"github.com/coocood/badger/y"
)

type ingestTask struct {
	sync.WaitGroup
	tbls []*table.Table
	cnt  int
	err  error
}

func (w *writeWorker) ingestTables(task *ingestTask) {
	ts, err := w.prepareIngestTask(task)
	if err != nil {
		task.err = err
		task.Done()
		return
	}

	// Because there is no concurrent write into ingesting key ranges,
	// we can resume other writes and finish the ingest job in background.
	go func() {
		defer task.Done()
		defer w.orc.doneCommit(ts)

		for _, t := range task.tbls {
			if task.err = t.SetGlobalTs(ts); task.err != nil {
				return
			}
		}

		w.waitOverlappedMemTableFlush(task.tbls)

		for _, tbl := range task.tbls {
			if task.err = w.ingestTable(tbl); task.err != nil {
				return
			}
			task.cnt++
		}
	}()
}

func (w *writeWorker) prepareIngestTask(task *ingestTask) (uint64, error) {
	w.orc.writeLock.Lock()
	ts := w.orc.allocTs()
	reqs := w.pollWriteCh(make([]*request, len(w.writeCh)))
	w.orc.writeLock.Unlock()

	if err := w.writeVLog(reqs); err != nil {
		return 0, err
	}

	it := w.mt.NewIterator(false)
	defer it.Close()
	for _, t := range task.tbls {
		it.Seek(y.KeyWithTs(t.Smallest(), math.MaxUint64))
		if it.Valid() && y.CompareKeysWithVer(it.Key(), y.KeyWithTs(t.Biggest(), 0)) <= 0 {
			if err := w.flushMemTable(); err != nil {
				return 0, err
			}
			break
		}
	}

	return ts, nil
}

func (w *writeWorker) ingestTable(tbl *table.Table) error {
	cs := &w.lc.cstatus
	kr := keyRange{
		left:  tbl.Smallest(),
		right: tbl.Biggest(),
	}

	var (
		targetLevel       int
		overlappingTables []*table.Table
	)

	cs.Lock()
	for targetLevel = 0; targetLevel < w.opt.TableBuilderOptions.MaxLevels; targetLevel++ {
		if tbls, ok := w.checkRangeInLevel(kr, targetLevel); !ok {
			overlappingTables = tbls
			break
		}
	}

	if len(overlappingTables) != 0 {
		overlapLeft := overlappingTables[0].Smallest()
		if y.CompareKeysWithVer(overlapLeft, kr.left) < 0 {
			kr.left = overlapLeft
		}
		overRight := overlappingTables[len(overlappingTables)-1].Biggest()
		if y.CompareKeysWithVer(overRight, kr.right) > 0 {
			kr.right = overRight
		}
	}
	l := cs.levels[targetLevel]
	l.ranges = append(l.ranges, kr)
	cs.Unlock()
	defer l.remove(kr)

	if targetLevel != 0 && len(overlappingTables) != 0 {
		return w.runIngestCompact(targetLevel, tbl, overlappingTables)
	}

	change := makeTableCreateChange(tbl.ID(), targetLevel)
	if err := w.manifest.addChanges([]*protos.ManifestChange{change}); err != nil {
		return err
	}
	l0 := w.lc.levels[0]
	l0.Lock()
	l0.tables = append(l0.tables, tbl)
	l0.Unlock()
	return nil
}

func (w *writeWorker) runIngestCompact(level int, tbl *table.Table, overlappingTables []*table.Table) error {
	cd := compactDef{
		nextLevel: w.lc.levels[level],
		top:       []*table.Table{tbl},
	}
	w.lc.fillBottomTables(&cd, overlappingTables)
	newTables, err := w.lc.compactBuildTables(level-1, cd, w.limiter)
	if err != nil {
		return err
	}
	defer forceDecrRefs(newTables)

	var changes []*protos.ManifestChange
	for _, t := range newTables {
		changes = append(changes, makeTableCreateChange(t.ID(), level))
	}
	for _, t := range cd.bot {
		changes = append(changes, makeTableDeleteChange(t.ID()))
	}

	if err := w.manifest.addChanges(changes); err != nil {
		return err
	}
	return cd.nextLevel.replaceTables(newTables, cd.skippedTbls)
}

func (w *writeWorker) waitOverlappedMemTableFlush(tbls []*table.Table) error {
	for _, t := range tbls {
		kr := keyRange{left: t.Smallest(), right: t.Biggest()}
		w.Lock()
		for w.overlapWithFlushingMemTables(kr) {
			w.flushMemTableCond.Wait()
		}
		w.Unlock()
	}
	return nil
}

func (w *writeWorker) overlapWithFlushingMemTables(kr keyRange) bool {
	for _, mt := range w.imm {
		it := mt.NewIterator(false)
		it.Seek(kr.left)
		if !it.Valid() || y.CompareKeysWithVer(it.Key(), kr.right) <= 0 {
			it.Close()
			return true
		}
		it.Close()
	}
	return false
}

func (w *writeWorker) checkRangeInLevel(kr keyRange, level int) ([]*table.Table, bool) {
	cs := &w.lc.cstatus
	handler := w.lc.levels[level]
	handler.RLock()
	defer handler.RUnlock()

	if len(handler.tables) == 0 && level != 0 {
		return nil, false
	}

	l := cs.levels[level]
	if l.overlapsWith(kr) {
		return nil, false
	}

	var left, right int
	if level == 0 {
		left, right = 0, len(handler.tables)
	} else {
		left, right = handler.overlappingTables(levelHandlerRLocked{}, kr)
	}

	ok := true
	for i := left; i < right; i++ {
		it := handler.tables[i].NewIteratorNoRef(false)
		it.Seek(kr.left)
		if it.Valid() && y.CompareKeysWithVer(it.Key(), kr.right) <= 0 {
			ok = false
			break
		}
	}
	return handler.tables[left:right], ok
}
