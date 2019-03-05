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

	"github.com/coocood/badger/table"
	"github.com/coocood/badger/y"
	"github.com/ngaut/log"
	"github.com/pingcap/errors"
)

type writeWorker struct {
	*DB
	writeLSMCh chan []*request
	mergeLSMCh chan *table.MemTable
}

func startWriteWorker(db *DB) *y.Closer {
	closer := y.NewCloser(3)
	w := &writeWorker{
		DB:         db,
		writeLSMCh: make(chan []*request, 1),
		mergeLSMCh: make(chan *table.MemTable, 1),
	}
	go w.runWriteVLog(closer)
	go w.runWriteLSM(closer)
	go w.runMergeLSM(closer)
	return closer
}

func (w *writeWorker) runWriteVLog(lc *y.Closer) {
	defer lc.Done()
	for {
		var r *request
		select {
		case r = <-w.writeCh:
		case <-lc.HasBeenClosed():
			w.closeWriteVLog()
			return
		}
		l := len(w.writeCh)
		reqs := make([]*request, l+1)
		reqs[0] = r
		for i := 0; i < l; i++ {
			reqs[i+1] = <-w.writeCh
		}
		err := w.vlog.write(reqs)
		if err != nil {
			w.done(reqs, err)
			return
		}
		w.writeLSMCh <- reqs
	}
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
		w.writeLSM(reqs)
	}
}

func (w *writeWorker) runMergeLSM(lc *y.Closer) {
	defer lc.Done()
	for mt := range w.mergeLSMCh {
		mt.MergeListToSkl()
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
		w.writeLSMCh <- reqs
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

	var i uint64
	var err error
	for err = w.ensureRoomForWrite(); err == errNoRoom; err = w.ensureRoomForWrite() {
		i++
		if i%100 == 0 {
			log.Warnf("Making room for writes")
		}
		// We need to poll a bit because both hasRoomForWrite and the flusher need access to s.imm.
		// When flushChan is full and you are blocked there, and the flusher is trying to update s.imm,
		// you will get a deadlock.
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil {
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
	w.mergeLSMCh <- w.mt
	return nil
}
