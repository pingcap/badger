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

package y

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"sync"

	"github.com/pingcap/errors"
)

// ErrEOF indicates an end of file when trying to read from a memory mapped file
// and encountering the end of slice.
var ErrEOF = errors.New("End of mapped region")

const (
	// Sync indicates that O_DSYNC should be set on the underlying file,
	// ensuring that data writes do not return until the data is flushed
	// to disk.
	Sync = 1 << iota
	// ReadOnly opens the underlying file on a read-only basis.
	ReadOnly
)

var (
	// This is O_DSYNC (datasync) on platforms that support it -- see file_unix.go
	datasyncFileFlag = 0x0

	// CastagnoliCrcTable is a CRC32 polynomial table
	CastagnoliCrcTable = crc32.MakeTable(crc32.Castagnoli)
)

// OpenExistingFile opens an existing file, errors if it doesn't exist.
func OpenExistingFile(filename string, flags uint32) (*os.File, error) {
	openFlags := os.O_RDWR
	if flags&ReadOnly != 0 {
		openFlags = os.O_RDONLY
	}

	if flags&Sync != 0 {
		openFlags |= datasyncFileFlag
	}
	return os.OpenFile(filename, openFlags, 0)
}

// CreateSyncedFile creates a new file (using O_EXCL), errors if it already existed.
func CreateSyncedFile(filename string, sync bool) (*os.File, error) {
	flags := os.O_RDWR | os.O_CREATE | os.O_EXCL
	if sync {
		flags |= datasyncFileFlag
	}
	return os.OpenFile(filename, flags, 0666)
}

// OpenSyncedFile creates the file if one doesn't exist.
func OpenSyncedFile(filename string, sync bool) (*os.File, error) {
	flags := os.O_RDWR | os.O_CREATE
	if sync {
		flags |= datasyncFileFlag
	}
	return os.OpenFile(filename, flags, 0666)
}

// OpenTruncFile opens the file with O_RDWR | O_CREATE | O_TRUNC
func OpenTruncFile(filename string, sync bool) (*os.File, error) {
	flags := os.O_RDWR | os.O_CREATE | os.O_TRUNC
	if sync {
		flags |= datasyncFileFlag
	}
	return os.OpenFile(filename, flags, 0666)
}

// SafeCopy does append(a[:0], src...).
func SafeCopy(a []byte, src []byte) []byte {
	return append(a[:0], src...)
}

// Copy copies a byte slice and returns the copied slice.
func Copy(a []byte) []byte {
	b := make([]byte, len(a))
	copy(b, a)
	return b
}

// KeyWithTs generates a new key by appending ts to key.
func KeyWithTs(key []byte, ts uint64) Key {
	return Key{
		UserKey: key,
		Version: ts,
	}
}

// ParseKey parses the actual key from the key bytes.
func ParseKey(keyBytes []byte) Key {
	if keyBytes == nil {
		return Key{}
	}
	return Key{
		UserKey: keyBytes[:len(keyBytes)-8],
		Version: math.MaxUint64 - binary.BigEndian.Uint64(keyBytes[len(keyBytes)-8:]),
	}
}

// Slice holds a reusable buf, will reallocate if you request a larger size than ever before.
// One problem is with n distinct sizes in random order it'll reallocate log(n) times.
type Slice struct {
	buf []byte
}

// Resize reuses the Slice's buffer (or makes a new one) and returns a slice in that buffer of
// length sz.
func (s *Slice) Resize(sz int) []byte {
	if cap(s.buf) < sz {
		s.buf = make([]byte, sz)
	}
	return s.buf[0:sz]
}

// Closer holds the two things we need to close a goroutine and wait for it to finish: a chan
// to tell the goroutine to shut down, and a WaitGroup with which to wait for it to finish shutting
// down.
type Closer struct {
	closed  chan struct{}
	waiting sync.WaitGroup
}

// NewCloser constructs a new Closer, with an initial count on the WaitGroup.
func NewCloser(initial int) *Closer {
	ret := &Closer{closed: make(chan struct{})}
	ret.waiting.Add(initial)
	return ret
}

// AddRunning Add()'s delta to the WaitGroup.
func (lc *Closer) AddRunning(delta int) {
	lc.waiting.Add(delta)
}

// Signal signals the HasBeenClosed signal.
func (lc *Closer) Signal() {
	close(lc.closed)
}

// HasBeenClosed gets signaled when Signal() is called.
func (lc *Closer) HasBeenClosed() <-chan struct{} {
	return lc.closed
}

// Done calls Done() on the WaitGroup.
func (lc *Closer) Done() {
	lc.waiting.Done()
}

// Wait waits on the WaitGroup.  (It waits for NewCloser's initial value, AddRunning, and Done
// calls to balance out.)
func (lc *Closer) Wait() {
	lc.waiting.Wait()
}

// SignalAndWait calls Signal(), then Wait().
func (lc *Closer) SignalAndWait() {
	lc.Signal()
	lc.Wait()
}

// Key is the struct for user key with version.
type Key struct {
	UserKey []byte
	Version uint64
}

func (k Key) Compare(k2 Key) int {
	cmp := bytes.Compare(k.UserKey, k2.UserKey)
	if cmp != 0 {
		return cmp
	}
	if k.Version > k2.Version {
		// Greater version is considered smaller because we need to iterate keys from newer to older.
		return -1
	} else if k.Version < k2.Version {
		return 1
	}
	return 0
}

func (k Key) Equal(k2 Key) bool {
	if !bytes.Equal(k.UserKey, k2.UserKey) {
		return false
	}
	return k.Version == k2.Version
}

func (k Key) IsEmpty() bool {
	return len(k.UserKey) == 0
}

func (k *Key) Copy(k2 Key) {
	k.UserKey = append(k.UserKey[:0], k2.UserKey...)
	k.Version = k2.Version
}

func (k *Key) Reset() {
	k.UserKey = k.UserKey[:0]
}

func (k Key) Len() int {
	return len(k.UserKey) + 8
}

func (k Key) AppendTo(buf []byte) []byte {
	buf = append(buf, k.UserKey...)
	var uBuf [8]byte
	binary.BigEndian.PutUint64(uBuf[:], math.MaxUint64-k.Version)
	return append(buf, uBuf[:]...)
}

func (k Key) WriteTo(w io.Writer) error {
	_, err := w.Write(k.UserKey)
	if err != nil {
		return err
	}
	var uBuf [8]byte
	binary.BigEndian.PutUint64(uBuf[:], math.MaxUint64-k.Version)
	_, err = w.Write(uBuf[:])
	return err
}

func (k Key) SameUserKey(k2 Key) bool {
	return bytes.Equal(k.UserKey, k2.UserKey)
}

func (k Key) String() string {
	return fmt.Sprintf("%x(%d)", k.UserKey, k.Version)
}
