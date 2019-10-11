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
	"os"
	"runtime"
	"time"

	"github.com/coocood/badger/fileutil"
	"github.com/coocood/badger/table"
	"github.com/coocood/badger/y"
	"github.com/ngaut/log"
)

type writeWorker struct {
	*DB
	writeLSMCh chan postLogTask
	mergeLSMCh chan *table.MemTable
	flushCh    chan postLogTask
}

type postLogTask struct {
	logFile *os.File
	reqs    []*request
}

func startWriteWorker(db *DB) *y.Closer {
	numWorkers := 3
	if db.opt.SyncWrites {
		numWorkers += 1
	}
	closer := y.NewCloser(numWorkers)
	w := &writeWorker{
		DB:         db,
		writeLSMCh: make(chan postLogTask, 1),
		mergeLSMCh: make(chan *table.MemTable, 1),
		flushCh:    make(chan postLogTask),
	}
	if db.opt.SyncWrites {
		go w.runFlusher(closer)
	}
	go w.runWriteVLog(closer)
	go w.runWriteLSM(closer)
	go w.runMergeLSM(closer)
	return closer
}

func (w *writeWorker) runFlusher(lc *y.Closer) {
	defer lc.Done()
	for {
		select {
		case t := <-w.flushCh:
			start := time.Now()
			err := fileutil.Fdatasync(t.logFile)
			w.metrics.VlogSyncDuration.Observe(time.Since(start).Seconds())
			if err != nil {
				w.done(t.reqs, err)
				continue
			}
			w.writeLSMCh <- t
		case <-lc.HasBeenClosed():
			return
		}
	}
}

func (w *writeWorker) runWriteVLog(lc *y.Closer) {
	defer lc.Done()
	for {
		var r *request
		select {
		case task := <-w.ingestCh:
			w.ingestTables(task)
		case r = <-w.writeCh:
			reqs := make([]*request, len(w.writeCh)+1)
			reqs[0] = r
			w.pollWriteCh(reqs[1:])
			if err := w.writeVLog(reqs); err != nil {
				return
			}
		case <-lc.HasBeenClosed():
			w.closeWriteVLog()
			return
		}
	}
}

func (w *writeWorker) pollWriteCh(buf []*request) []*request {
	for i := 0; i < len(buf); i++ {
		buf[i] = <-w.writeCh
	}
	return buf
}

func (w *writeWorker) writeVLog(reqs []*request) error {
	err := w.vlog.write(reqs)
	if err != nil {
		w.done(reqs, err)
		return err
	}
	t := postLogTask{
		logFile: w.vlog.currentLogFile().fd,
		reqs:    reqs,
	}
	if w.opt.SyncWrites {
		w.flushCh <- t
	} else {
		w.writeLSMCh <- t
	}
	return nil
}

func (w *writeWorker) runWriteLSM(lc *y.Closer) {
	defer lc.Done()
	runtime.LockOSThread()
	for {
		t, ok := <-w.writeLSMCh
		if !ok {
			close(w.mergeLSMCh)
			return
		}
		start := time.Now()
		w.writeLSM(t.reqs)
		w.metrics.WriteLSMDuration.Observe(time.Since(start).Seconds())
	}
}

func (w *writeWorker) runMergeLSM(lc *y.Closer) {
	defer lc.Done()
	for mt := range w.mergeLSMCh {
		mt.MergeListToSkl()
		mt.DecrRef()
	}
}

func (w *writeWorker) closeWriteVLog() {
	close(w.writeCh)
	var reqs []*request
	for r := range w.writeCh { // Flush the channel.
		reqs = append(reqs, r)
	}
	err := w.vlog.write(reqs)
	if err != nil {
		w.done(reqs, err)
	} else {
		if err := w.vlog.curWriter.Sync(); err != nil {
			w.done(reqs, err)
		} else {
			w.writeLSMCh <- postLogTask{
				logFile: w.vlog.currentLogFile().fd,
				reqs:    reqs,
			}
		}
	}
	close(w.writeLSMCh)
}

// writeLSM is called serially by only one goroutine.
func (w *writeWorker) writeLSM(reqs []*request) {
	if len(reqs) == 0 {
		return
	}
	var count int
	for _, b := range reqs {
		if len(b.Entries) == 0 {
			continue
		}
		count += len(b.Entries)
		if err := w.writeToLSM(b.Entries); err != nil {
			w.done(reqs, err)
			return
		}
		w.updateOffset(b.off)
	}

	w.done(reqs, nil)
	log.Debugf("%d entries written", count)
	return
}

func (w *writeWorker) done(reqs []*request, err error) {
	for _, r := range reqs {
		r.Err = err
		r.Wg.Done()
	}
	if err != nil {
		log.Warnf("ERROR in Badger::writeLSM: %v", err)
	}
}

func (w *writeWorker) writeToLSM(entries []*Entry) error {
	if err := w.ensureRoomForWrite(); err != nil {
		return err
	}

	es := make([]table.Entry, 0, len(entries)-1)
	for _, entry := range entries {
		if entry.meta&bitFinTxn != 0 {
			continue
		}
		es = append(es, table.Entry{
			Key: entry.Key,
			Value: y.ValueStruct{
				Value:    entry.Value,
				Meta:     entry.meta,
				UserMeta: entry.UserMeta,
				Version:  y.ParseTs(entry.Key),
			},
		})
	}
	w.mt.PutToPendingList(es)
	w.mt.IncrRef()
	w.mergeLSMCh <- w.mt
	return nil
}
