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
	"math"
	"sort"
	"sync/atomic"

	"github.com/dgryski/go-farm"
	"github.com/pingcap/badger/table"
	"github.com/pingcap/badger/table/memtable"
	"github.com/pingcap/badger/y"
)

// Item is returned during iteration. Both the Key() and Value() output is only valid until
// iterator.Next() is called.
type Item struct {
	err      error
	db       *DB
	key      y.Key
	vptr     []byte
	meta     byte // We need to store meta to know about bitValuePointer.
	userMeta []byte
	slice    *y.Slice
	next     *Item
	txn      *Txn
}

// String returns a string representation of Item
func (item *Item) String() string {
	return fmt.Sprintf("key=%q, version=%d, meta=%x", item.Key(), item.Version(), item.meta)
}

// Key returns the key.
//
// Key is only valid as long as item is valid, or transaction is valid.  If you need to use it
// outside its validity, please use KeyCopy
func (item *Item) Key() []byte {
	return item.key.UserKey
}

// KeyCopy returns a copy of the key of the item, writing it to dst slice.
// If nil is passed, or capacity of dst isn't sufficient, a new slice would be allocated and
// returned.
func (item *Item) KeyCopy(dst []byte) []byte {
	return y.SafeCopy(dst, item.key.UserKey)
}

// Version returns the commit timestamp of the item.
func (item *Item) Version() uint64 {
	return item.key.Version
}

// IsEmpty checks if the value is empty.
func (item *Item) IsEmpty() bool {
	return len(item.vptr) == 0
}

// Value retrieves the value of the item from the value log.
//
// This method must be called within a transaction. Calling it outside a
// transaction is considered undefined behavior. If an iterator is being used,
// then Item.Value() is defined in the current iteration only, because items are
// reused.
//
// If you need to use a value outside a transaction, please use Item.ValueCopy
// instead, or copy it yourself. Value might change once discard or commit is called.
// Use ValueCopy if you want to do a Set after Get.
func (item *Item) Value() ([]byte, error) {
	if item.meta&bitValuePointer > 0 {
		if item.slice == nil {
			item.slice = new(y.Slice)
		}
		if item.txn.blobCache == nil {
			item.txn.blobCache = map[uint32]*blobCache{}
		}
		return item.db.blobManger.read(item.vptr, item.slice, item.txn.blobCache)
	}
	return item.vptr, nil
}

// ValueSize returns the size of the value without the cost of retrieving the value.
func (item *Item) ValueSize() int {
	if item.meta&bitValuePointer > 0 {
		var bp blobPointer
		bp.decode(item.vptr)
		return int(bp.length)
	}
	return len(item.vptr)
}

// ValueCopy returns a copy of the value of the item from the value log, writing it to dst slice.
// If nil is passed, or capacity of dst isn't sufficient, a new slice would be allocated and
// returned. Tip: It might make sense to reuse the returned slice as dst argument for the next call.
//
// This function is useful in long running iterate/update transactions to avoid a write deadlock.
// See Github issue: https://github.com/pingcap/badger/issues/315
func (item *Item) ValueCopy(dst []byte) ([]byte, error) {
	buf, err := item.Value()
	if err != nil {
		return nil, err
	}
	return y.SafeCopy(dst, buf), nil
}

func (item *Item) hasValue() bool {
	if item.meta == 0 && item.vptr == nil {
		// key not found
		return false
	}
	return true
}

// IsDeleted returns true if item contains deleted or expired value.
func (item *Item) IsDeleted() bool {
	return isDeleted(item.meta)
}

// EstimatedSize returns approximate size of the key-value pair.
//
// This can be called while iterating through a store to quickly estimate the
// size of a range of key-value pairs (without fetching the corresponding
// values).
func (item *Item) EstimatedSize() int64 {
	if !item.hasValue() {
		return 0
	}
	return int64(item.key.Len() + len(item.vptr))
}

// UserMeta returns the userMeta set by the user. Typically, this byte, optionally set by the user
// is used to interpret the value.
func (item *Item) UserMeta() []byte {
	return item.userMeta
}

// IteratorOptions is used to set options when iterating over Badger key-value
// stores.
//
// This package provides DefaultIteratorOptions which contains options that
// should work for most applications. Consider using that as a starting point
// before customizing it for your own needs.
type IteratorOptions struct {
	Reverse     bool // Direction of iteration. False is forward, true is backward.
	AllVersions bool // Fetch all valid versions of the same key.

	// StartKey and EndKey are used to prune non-overlapping table iterators.
	// They are not boundary limits, the EndKey is exclusive.
	StartKey y.Key
	EndKey   y.Key

	internalAccess bool // Used to allow internal access to badger keys.
}

func (opts *IteratorOptions) hasRange() bool {
	return !opts.StartKey.IsEmpty() && !opts.EndKey.IsEmpty()
}

func (opts *IteratorOptions) OverlapPending(it *pendingWritesIterator) bool {
	if it == nil {
		return false
	}
	if !opts.hasRange() {
		return true
	}
	if opts.EndKey.Compare(it.entries[0].Key) <= 0 {
		return false
	}
	if opts.StartKey.Compare(it.entries[len(it.entries)-1].Key) > 0 {
		return false
	}
	return true
}

func (opts *IteratorOptions) OverlapMemTable(t *memtable.Table) bool {
	if t.Empty() {
		return false
	}
	if !opts.hasRange() {
		return true
	}
	iter := t.NewIterator(false)
	defer iter.Close()
	iter.Seek(opts.StartKey.UserKey)
	if !iter.Valid() {
		return false
	}
	if bytes.Compare(iter.Key().UserKey, opts.EndKey.UserKey) >= 0 {
		return false
	}
	return true
}

func (opts *IteratorOptions) OverlapTable(t table.Table) bool {
	if !opts.hasRange() {
		return true
	}
	return t.HasOverlap(opts.StartKey, opts.EndKey, false)
}

func (opts *IteratorOptions) OverlapTables(tables []table.Table) []table.Table {
	if len(tables) == 0 {
		return nil
	}
	if !opts.hasRange() {
		return tables
	}
	startIdx := sort.Search(len(tables), func(i int) bool {
		t := tables[i]
		return opts.StartKey.Compare(t.Biggest()) <= 0
	})
	if startIdx == len(tables) {
		return nil
	}
	tables = tables[startIdx:]
	endIdx := sort.Search(len(tables), func(i int) bool {
		t := tables[i]
		return t.Smallest().Compare(opts.EndKey) >= 0
	})
	tables = tables[:endIdx]
	overlapTables := make([]table.Table, 0, 8)
	for _, t := range tables {
		if opts.OverlapTable(t) {
			overlapTables = append(overlapTables, t)
		}
	}
	return overlapTables
}

// DefaultIteratorOptions contains default options when iterating over Badger key-value stores.
var DefaultIteratorOptions = IteratorOptions{
	Reverse:     false,
	AllVersions: false,
}

// Iterator helps iterating over the KV pairs in a lexicographically sorted order.
type Iterator struct {
	iitr   y.Iterator
	txn    *Txn
	readTs uint64

	opt   IteratorOptions
	item  *Item
	itBuf Item
	vs    y.ValueStruct

	closed bool
}

// NewIterator returns a new iterator. Depending upon the options, either only keys, or both
// key-value pairs would be fetched. The keys are returned in lexicographically sorted order.
// Avoid long running iterations in update transactions.
func (txn *Txn) NewIterator(opt IteratorOptions) *Iterator {
	atomic.AddInt32(&txn.numIterators, 1)

	tables := txn.db.getMemTables()
	if !opt.StartKey.IsEmpty() {
		opt.StartKey.Version = math.MaxUint64
	}
	if !opt.EndKey.IsEmpty() {
		opt.EndKey.Version = math.MaxUint64
	}
	var iters []y.Iterator
	if itr := txn.newPendingWritesIterator(opt.Reverse); opt.OverlapPending(itr) {
		iters = append(iters, itr)
	}
	for i := 0; i < len(tables); i++ {
		if opt.OverlapMemTable(tables[i]) {
			iters = append(iters, tables[i].NewIterator(opt.Reverse))
		}
	}
	iters = txn.db.lc.appendIterators(iters, &opt) // This will increment references.
	res := &Iterator{
		txn:    txn,
		iitr:   table.NewMergeIterator(iters, opt.Reverse),
		opt:    opt,
		readTs: txn.readTs,
	}
	res.itBuf.db = txn.db
	res.itBuf.txn = txn
	res.itBuf.slice = new(y.Slice)
	return res
}

// Item returns pointer to the current key-value pair.
// This item is only valid until it.Next() gets called.
func (it *Iterator) Item() *Item {
	tx := it.txn
	if tx.update {
		// Track reads if this is an update txn.
		tx.reads = append(tx.reads, farm.Fingerprint64(it.item.Key()))
	}
	return it.item
}

// Valid returns false when iteration is done.
func (it *Iterator) Valid() bool { return it.item != nil }

// ValidForPrefix returns false when iteration is done
// or when the current key is not prefixed by the specified prefix.
func (it *Iterator) ValidForPrefix(prefix []byte) bool {
	return it.item != nil && bytes.HasPrefix(it.item.key.UserKey, prefix)
}

// Close would close the iterator. It is important to call this when you're done with iteration.
func (it *Iterator) Close() {
	if it.closed {
		return
	}
	it.closed = true
	it.iitr.Close()
	atomic.AddInt32(&it.txn.numIterators, -1)
}

// Next would advance the iterator by one. Always check it.Valid() after a Next()
// to ensure you have access to a valid it.Item().
func (it *Iterator) Next() {
	if it.opt.AllVersions && it.Valid() && it.iitr.NextVersion() {
		it.updateItem()
		return
	}
	it.iitr.Next()
	it.parseItem()
	return
}

func (it *Iterator) updateItem() {
	it.iitr.FillValue(&it.vs)
	item := &it.itBuf
	item.key = it.iitr.Key()
	item.meta = it.vs.Meta
	item.userMeta = it.vs.UserMeta
	item.vptr = it.vs.Value
	it.item = item
}

func (it *Iterator) parseItem() {
	iitr := it.iitr
	for iitr.Valid() {
		key := iitr.Key()
		if !it.opt.internalAccess && key.UserKey[0] == '!' {
			iitr.Next()
			continue
		}
		if key.Version > it.readTs {
			if !y.SeekToVersion(iitr, it.readTs) {
				iitr.Next()
				continue
			}
		}
		it.updateItem()
		if !it.opt.AllVersions && isDeleted(it.vs.Meta) {
			iitr.Next()
			continue
		}
		return
	}
	it.item = nil
}

func isDeleted(meta byte) bool {
	return meta&bitDelete > 0
}

// Seek would seek to the provided key if present. If absent, it would seek to the next smallest key
// greater than provided if iterating in the forward direction. Behavior would be reversed is
// iterating backwards.
func (it *Iterator) Seek(key []byte) {
	if !it.opt.Reverse {
		it.iitr.Seek(key)
	} else {
		if len(key) == 0 {
			it.iitr.Rewind()
		} else {
			it.iitr.Seek(key)
		}
	}
	it.parseItem()
}

// Rewind would rewind the iterator cursor all the way to zero-th position, which would be the
// smallest key if iterating forward, and largest if iterating backward. It does not keep track of
// whether the cursor started with a Seek().
func (it *Iterator) Rewind() {
	it.iitr.Rewind()
	it.parseItem()
}

func (it *Iterator) SetAllVersions(allVersions bool) {
	it.opt.AllVersions = allVersions
}
