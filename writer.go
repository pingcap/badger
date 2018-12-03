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
	"sort"
	"time"

	"github.com/coocood/badger/options"
	"github.com/coocood/badger/skl"
	"github.com/coocood/badger/table"
	"github.com/coocood/badger/y"
	"github.com/ngaut/log"
	"github.com/pkg/errors"
)

type writeWorker struct {
	*DB
	writeLSMCh chan []*request
}

func startWriteWorker(db *DB) *y.Closer {
	closer := y.NewCloser(2)
	w := &writeWorker{
		DB:         db,
		writeLSMCh: make(chan []*request, 1),
	}
	go w.runWriteVLog(closer)
	go w.runWriteLSM(closer)
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
			return
		}
		w.writeLSM(reqs)
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

type txnBatch struct {
	entries      []*Entry
	ptrs         []valuePointer
	latestOffset valuePointer
}

func (b *txnBatch) Len() int { return len(b.entries) }

func (b *txnBatch) Less(i, j int) bool { return y.CompareKeys(b.entries[i].Key, b.entries[j].Key) < 0 }

func (b *txnBatch) Swap(i, j int) {
	b.entries[i], b.entries[j] = b.entries[j], b.entries[i]
	b.ptrs[i], b.ptrs[j] = b.ptrs[j], b.ptrs[i]
}

// writeLSM is called serially by only one goroutine.
func (w *writeWorker) writeLSM(reqs []*request) {
	if len(reqs) == 0 {
		return
	}
	var count int
	if w.opt.LSMWriterOptions.MergeSmallTxn {
		batches := w.mergeSmallTxn(reqs)
		for _, b := range batches {
			count += len(b.entries)
			if err := w.writeToLSM(b.entries, b.ptrs); err != nil {
				w.done(reqs, err)
				return
			}
			w.updateOffset(b.latestOffset)
		}
	} else {
		for _, b := range reqs {
			if len(b.Entries) == 0 {
				continue
			}
			count += len(b.Entries)
			if err := w.writeToLSM(b.Entries, b.Ptrs); err != nil {
				w.done(reqs, err)
				return
			}
			w.updateOffset(w.latestOffset(b.Ptrs))
		}
	}

	w.done(reqs, nil)
	log.Debugf("%d entries written", count)
	return
}

func (w *writeWorker) mergeSmallTxn(reqs []*request) []txnBatch {
	var (
		result []txnBatch
		i, j   int
	)
	for i < len(reqs) {
		var (
			batch   txnBatch
			cnt, sz int
		)
		for j = i; j < len(reqs); j++ {
			entries, ptrs := w.getEntriesAndPtrs(reqs[j])
			if len(entries) == 0 {
				continue
			}
			cnt, sz = cnt+len(entries), sz+w.sizeOfEntries(entries)
			if int64(cnt) >= w.opt.maxBatchCount || int64(sz) >= w.opt.maxBatchSize {
				break
			}
			batch.entries = append(batch.entries, entries...)
			batch.ptrs = append(batch.ptrs, ptrs...)
			if off := w.latestOffset(ptrs); !off.IsZero() {
				batch.latestOffset = off
			}
		}
		i = j
		sort.Sort(&batch)
		result = append(result, batch)
	}

	return result
}

func (w *writeWorker) sizeOfEntries(entries []*Entry) int {
	var sz int
	for _, e := range entries {
		sz += len(e.Key) + e.estimateSize(w.opt.ValueThreshold)
	}
	return sz
}

func (w *writeWorker) getEntriesAndPtrs(req *request) ([]*Entry, []valuePointer) {
	if req.Entries[len(req.Entries)-1].meta&bitFinTxn != 0 {
		return req.Entries[:len(req.Entries)-1], req.Ptrs[:len(req.Ptrs)-1]
	}
	return req.Entries, req.Ptrs
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

	switch w.opt.LSMWriterOptions.WriteMethod {
	case options.DelayedWrite:
		return w.writeToLSMBatchPut(entries, ptrs)
	case options.NormalWrite:
		return w.writeToLSMPut(entries, ptrs)
	default:
		panic("invalid option")
	}
}

func (w *writeWorker) writeToLSMPut(entries []*Entry, ptrs []valuePointer) error {
	var hint skl.Hint
	for i, entry := range entries {
		if entry.meta&bitFinTxn != 0 {
			continue
		}
		if w.shouldWriteValueToLSM(entry) { // Will include deletion / tombstone case.
			w.mt.PutWithHint(entry.Key,
				y.ValueStruct{
					Value:    entry.Value,
					Meta:     entry.meta,
					UserMeta: entry.UserMeta,
				}, &hint)
		} else {
			var offsetBuf [vptrSize]byte
			w.mt.PutWithHint(entry.Key,
				y.ValueStruct{
					Value:    ptrs[i].Encode(offsetBuf[:]),
					Meta:     entry.meta | bitValuePointer,
					UserMeta: entry.UserMeta,
				}, &hint)
		}
	}
	return nil
}

func (w *writeWorker) writeToLSMBatchPut(entries []*Entry, ptrs []valuePointer) error {
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
			var offsetBuf [vptrSize]byte
			es = append(es, table.Entry{
				Key: entry.Key,
				Value: y.ValueStruct{
					Value:    ptrs[i].Encode(offsetBuf[:]),
					Meta:     entry.meta | bitValuePointer,
					UserMeta: entry.UserMeta,
					Version:  y.ParseTs(entry.Key),
				},
			})
		}
	}
	w.mt.PutToPendingList(es)
	go func() { w.mt.MergeListToSkl() }()
	return nil
}
