package table

import (
	"github.com/coocood/badger/epoch"
	"github.com/coocood/badger/y"
)

type Table interface {
	epoch.Resource
	ID() uint64
	NewIterator(reversed bool) y.Iterator
	Get(key y.Key, keyHash uint64) (y.ValueStruct, error)
	Size() int64
	Smallest() y.Key
	Biggest() y.Key
	HasOverlap(start, end y.Key, includeEnd bool) bool
	IsCompacting() bool
	MarkCompacting(flag bool)
	Close() error
}
