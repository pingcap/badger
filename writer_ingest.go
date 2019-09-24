package badger

import (
	"sync"

	"github.com/coocood/badger/protos"
	"github.com/coocood/badger/table"
	"github.com/coocood/badger/y"
	"github.com/ngaut/log"
)

type ingestTask struct {
	sync.WaitGroup
	tbls []*table.Table
	err  error
}

func (w *writeWorker) ingestTables(task *ingestTask) {
	tbls, ts, err := w.prepareIngest(task)
	if err != nil {
		task.err = err
		return
	}
	changes := make([]*protos.ManifestChange, len(tbls))

	for i, tbl := range tbls {
		level, err := w.findLevelToPlace(tbl)
		if err != nil {
			task.err = err
			return
		}
		log.Infof("place table %d at level %d", tbl.ID(), level)
		changes[i] = makeTableCreateChange(tbl.ID(), level)
	}

	if err := w.manifest.addChanges(changes); err != nil {
		task.err = err
		return
	}

	for i, change := range changes {
		w.lc.levels[change.Level].addTable(tbls[i])
	}

	w.orc.doneCommit(ts)
}

func (w *writeWorker) prepareIngest(task *ingestTask) ([]*table.Table, uint64, error) {
	w.orc.writeLock.Lock()
	ts := w.orc.allocTs()
	reqs := w.pollWriteCh(make([]*request, len(w.writeCh)))
	w.orc.writeLock.Unlock()

	if err := w.writeVLog(reqs); err != nil {
		return nil, 0, err
	}

	for _, t := range task.tbls {
		if err := t.SetGlobalTs(ts); err != nil {
			return nil, 0, err
		}
	}

	return task.tbls, ts, nil
}

func (w *writeWorker) findLevelToPlace(tbl *table.Table) (int, error) {
	cs := w.lc.cstatus
	kr := keyRange{
		left:  tbl.Smallest(),
		right: tbl.Biggest(),
	}

	if err := w.checkRangeInMemTables(kr); err != nil {
		return 0, err
	}

	cs.Lock()
	var level int
	for level = 1; level < w.opt.TableBuilderOptions.MaxLevels; level++ {
		if !w.checkRangeInLevel(kr, level) {
			break
		}
	}

	finalLevel := level - 1
	l := cs.levels[finalLevel]
	l.ranges = append(l.ranges, kr)
	cs.Unlock()

	return finalLevel, nil
}

func (w *writeWorker) checkRangeInMemTables(kr keyRange) error {
	it := w.mt.NewIterator(false)
	it.Seek(kr.left)
	if it.Valid() && y.CompareKeysWithVer(it.Key(), kr.right) <= 0 {
		wg := new(sync.WaitGroup)
		wg.Add(1)
		if err := w.flushMemTable(wg); err != nil {
			return err
		}
		wg.Wait()
	}

	w.Lock()
	i := 0
	for _, mt := range w.imm {
		it := mt.NewIterator(false)
		it.Seek(kr.left)
		if !it.Valid() || y.CompareKeysWithVer(it.Key(), kr.right) > 0 {
			w.imm[i] = mt
			i++
		}
	}
	w.imm = w.imm[:i]
	w.Unlock()

	return nil
}

func (w *writeWorker) checkRangeInLevel(kr keyRange, level int) bool {
	cs := w.lc.cstatus
	handler := w.lc.levels[level]
	handler.RLock()
	defer handler.RUnlock()

	if len(handler.tables) == 0 {
		return false
	}

	l := cs.levels[level]
	if l.overlapsWith(kr) {
		return false
	}

	left, right := handler.overlappingTables(levelHandlerRLocked{}, kr)
	if right-left > 0 {
		overlap := handler.tables[left]
		it := overlap.NewIteratorNoRef(false)
		it.Seek(kr.left)
		if it.Valid() && y.CompareKeysWithVer(it.Key(), kr.right) <= 0 {
			return false
		}
	}

	return true
}
