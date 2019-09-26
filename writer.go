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
	"runtime"
	"time"

	"github.com/coocood/badger/fileutil"
	"github.com/coocood/badger/table"
	"github.com/coocood/badger/y"
	"github.com/ngaut/log"
	"github.com/pingcap/errors"
)

type writeWorker struct {
	*DB
	writeLSMCh chan []*request
	mergeLSMCh chan *table.MemTable
	flushCh    chan flushLogTask
}

type flushLogTask struct {
	writer *fileutil.BufferedWriter
	reqs   []*request
}

func startWriteWorker(db *DB) *y.Closer {
	numWorkers := 3
	if db.opt.SyncWrites {
		numWorkers += 1
	}
	closer := y.NewCloser(numWorkers)
	w := &writeWorker{
		DB:         db,
		writeLSMCh: make(chan []*request, 1),
		mergeLSMCh: make(chan *table.MemTable, 1),
		flushCh:    make(chan flushLogTask),
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
			err := t.writer.Sync()
			w.metrics.VlogSyncDuration.Observe(time.Since(start).Seconds())
			if err != nil {
				w.done(t.reqs, err)
				continue
			}
			w.writeLSMCh <- t.reqs
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
	if w.opt.SyncWrites {
		w.flushCh <- flushLogTask{
			writer: w.vlog.curWriter,
			reqs:   reqs,
		}
	} else {
		w.writeLSMCh <- reqs
	}
	return nil
}

func (w *writeWorker) runWriteLSM(lc *y.Closer) {
	defer lc.Done()
	runtime.LockOSThread()
	for {
		reqs, ok := <-w.writeLSMCh
		if !ok {
			close(w.mergeLSMCh)
			return
		}
		start := time.Now()
		w.writeLSM(reqs)
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
			w.writeLSMCh <- reqs
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
		if err := w.writeToLSM(b.Entries, b.Ptrs); err != nil {
			w.done(reqs, err)
			return
		}
		w.updateOffset(b.Ptrs)
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

func (w *writeWorker) writeToLSM(entries []*Entry, ptrs []valuePointer) error {
	if len(ptrs) != len(entries) {
		return errors.Errorf("Ptrs and Entries don't match: %v, %v", entries, ptrs)
	}

	if err := w.ensureRoomForWrite(); err != nil {
		return err
	}

	es := make([]table.Entry, 0, len(entries)-1)
	for i, entry := range entries {
		if entry.meta&bitFinTxn != 0 {
			continue
		}
		if w.shouldWriteValueToLSM(entry) { // Will include deletion / tombstone case.
			es = append(es, table.Entry{
				Key: entry.Key,
				Value: y.ValueStruct{
					Value:    entry.Value,
					Meta:     entry.meta,
					UserMeta: entry.UserMeta,
					Version:  y.ParseTs(entry.Key),
				},
			})
		} else {
			var offsetBuf []byte
			if len(entry.Value) < vptrSize {
				offsetBuf = make([]byte, vptrSize)
			} else {
				offsetBuf = entry.Value[:vptrSize]
			}
			es = append(es, table.Entry{
				Key: entry.Key,
				Value: y.ValueStruct{
					Value:    ptrs[i].Encode(offsetBuf),
					Meta:     entry.meta | bitValuePointer,
					UserMeta: entry.UserMeta,
					Version:  y.ParseTs(entry.Key),
				},
			})
		}
	}
	w.mt.PutToPendingList(es)
	w.mt.IncrRef()
	w.mergeLSMCh <- w.mt
	return nil
}
