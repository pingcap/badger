package badger

import (
	"math"
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
	ts, err := w.prepareIngestTask(task)
	if err != nil {
		task.err = err
		task.Done()
		return
	}

	// Because there is no concurrent write into ingesting key ranges,
	// we can resume other writes and finish the ingest job in background.
	go func() {
		// Finish ingest job by these steps:
		// 1. Update external tables' global ts;
		// 2. Wait all overlapped MemTable flushed to disk;
		// 3. Find the target level, will split overlapped SST in target levels if needed;
		// 4. Add changes into manifest;
		// 5. Add tables to level controllers.
		defer task.Done()
		tbls := task.tbls

		for _, t := range task.tbls {
			if task.err = t.SetGlobalTs(ts); task.err != nil {
				return
			}
		}

		w.waitOverlappedMemTableFlush(tbls)

		changes := make([]*protos.ManifestChange, 0, len(tbls))
		for _, tbl := range tbls {
			var err error
			if changes, err = w.findLevelToPlace(changes, tbl); err != nil {
				task.err = err
				return
			}
		}

		if err := w.manifest.addChanges(changes); err != nil {
			task.err = err
			return
		}

		for i, change := range changes {
			w.lc.levels[change.Level].addTable(tbls[i])
		}

		w.orc.doneCommit(ts)
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

func (w *writeWorker) findLevelToPlace(changes []*protos.ManifestChange, tbl *table.Table) ([]*protos.ManifestChange, error) {
	cs := &w.lc.cstatus
	kr := keyRange{
		left:  tbl.Smallest(),
		right: tbl.Biggest(),
	}

	cs.Lock()
	var level int
	for level = 1; level < w.opt.TableBuilderOptions.MaxLevels; level++ {
		if !w.canPlaceRangeInLevel(kr, level) {
			break
		}
	}

	finalLevel := level - 1
	l := cs.levels[finalLevel]
	l.ranges = append(l.ranges, kr)
	cs.Unlock()

	log.Infof("place table %d at level %d", tbl.ID(), finalLevel)
	ingestChange := makeTableCreateChange(tbl.ID(), finalLevel)
	changes = append(changes, ingestChange)

	return changes, nil
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

func (w *writeWorker) canPlaceRangeInLevel(kr keyRange, level int) bool {
	cs := &w.lc.cstatus
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
