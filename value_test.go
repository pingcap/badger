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
	"bufio"
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/pingcap/badger/y"
	"github.com/stretchr/testify/require"
)

func TestValueBasic(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	y.Check(err)
	defer os.RemoveAll(dir)

	kv, _ := Open(getTestOptions(dir))
	defer kv.Close()
	log := &kv.vlog

	// Use value big enough that the value log writes them even if SyncWrites is false.
	const val1 = "sampleval012345678901234567890123"
	const val2 = "samplevalb012345678901234567890123"
	require.True(t, len(val1) >= kv.opt.ValueThreshold)
	e := &Entry{
		Key:   y.KeyWithTs([]byte("samplekey"), 100),
		Value: []byte(val1),
		meta:  bitTxn,
	}
	e2 := &Entry{
		Key:   y.KeyWithTs([]byte("samplekeyb"), 100),
		Value: []byte(val2),
		meta:  bitTxn,
	}
	eFin := &Entry{
		Key:  y.KeyWithTs(txnKey, 100),
		meta: bitFinTxn,
	}
	offset := log.getMaxPtr()
	b := new(request)
	b.Entries = []*Entry{e, e2, eFin}
	err = log.write([]*request{b})
	newOffset := log.getMaxPtr()
	require.True(t, newOffset > offset)
	require.Nil(t, err)
	expectedEntries := []Entry{*e, *e2}
	var i int
	err = kv.IterateVLog(offset, func(e Entry) {
		expectedEntry := expectedEntries[i]
		expectedEntry.offset = e.offset
		expectedEntry.logOffset = logOffset{}
		require.Equal(t, expectedEntry, e)
		i++
	})
	require.Nil(t, err)
	require.Equal(t, 2, i)
}

func TestValueBasicManaged(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	y.Check(err)
	defer os.RemoveAll(dir)

	testOpt := getTestOptions(dir)
	testOpt.ManagedTxns = true
	kv, _ := Open(testOpt)
	defer kv.Close()
	log := &kv.vlog

	// Use value big enough that the value log writes them even if SyncWrites is false.
	const val1 = "sampleval012345678901234567890123"
	const val2 = "samplevalb012345678901234567890123"
	require.True(t, len(val1) >= kv.opt.ValueThreshold)
	e := &Entry{
		Key:   y.KeyWithTs([]byte("samplekey"), 100),
		Value: []byte(val1),
		meta:  bitTxn,
	}
	e2 := &Entry{
		Key:   y.KeyWithTs([]byte("samplekeyb"), 100),
		Value: []byte(val2),
		meta:  bitTxn,
	}
	eFin := &Entry{
		Key:  y.KeyWithTs(txnKey, 0),
		meta: bitFinTxn,
	}
	e3 := &Entry{
		Key:   y.KeyWithTs([]byte("samplekey"), 101),
		Value: []byte(val1),
		meta:  bitTxn,
	}
	e4 := &Entry{
		Key:   y.KeyWithTs([]byte("samplekeyb"), 101),
		Value: []byte(val2),
		meta:  bitTxn,
	}
	eFin2 := &Entry{
		Key:  y.KeyWithTs(txnKey, 0),
		meta: bitFinTxn,
	}
	offset := log.getMaxPtr()
	err = log.write([]*request{{
		Entries: []*Entry{e, e2, eFin},
	}})
	require.Nil(t, err)
	err = log.write([]*request{{
		Entries: []*Entry{e3, e4, eFin2},
	}})
	newOffset := log.getMaxPtr()
	require.True(t, newOffset > offset)
	expectedEntries := []Entry{*e, *e2, *e3, *e4}
	var i int
	err = kv.IterateVLog(offset, func(e Entry) {
		expectedEntry := expectedEntries[i]
		expectedEntry.offset = e.offset
		expectedEntry.logOffset = logOffset{}
		require.Equal(t, expectedEntry, e)
		i++
	})
	require.Nil(t, err)
	require.Equal(t, 4, i)
}

func TestChecksums(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// Set up SST with K1=V1
	opts := getTestOptions(dir)
	opts.Truncate = true
	opts.ValueLogFileSize = 100 * 1024 * 1024 // 100Mb
	kv, err := Open(opts)
	require.NoError(t, err)
	require.NoError(t, kv.Close())

	var (
		k0 = []byte("k0")
		k1 = []byte("k1")
		k2 = []byte("k2")
		k3 = []byte("k3")
		v0 = []byte("value0-012345678901234567890123012345678901234567890123")
		v1 = []byte("value1-012345678901234567890123012345678901234567890123")
		v2 = []byte("value2-012345678901234567890123012345678901234567890123")
		v3 = []byte("value3-012345678901234567890123012345678901234567890123")
	)
	// Make sure the value log would actually store the item
	require.True(t, len(v0) >= kv.opt.ValueThreshold)

	// Use a vlog with K0=V0 and a (corrupted) second transaction(k1,k2)
	buf := createVlog(t, []*Entry{
		{Key: y.KeyWithTs(k0, 0), Value: y.Copy(v0)},
		{Key: y.KeyWithTs(k1, 0), Value: y.Copy(v1)},
		{Key: y.KeyWithTs(k2, 0), Value: y.Copy(v2)},
	})
	buf[len(buf)-1]++ // Corrupt last byte
	require.NoError(t, ioutil.WriteFile(vlogFilePath(dir, 0), buf, 0777))

	// K1 should exist, but K2 shouldn't.
	kv, err = Open(opts)
	require.NoError(t, err)

	require.NoError(t, kv.View(func(txn *Txn) error {
		item, err := txn.Get(k0)
		require.NoError(t, err)
		require.Equal(t, getItemValue(t, item), v0)

		_, err = txn.Get(k1)
		require.Equal(t, ErrKeyNotFound, err)

		_, err = txn.Get(k2)
		require.Equal(t, ErrKeyNotFound, err)
		return nil
	}))

	// Write K3 at the end of the vlog.
	txnSet(t, kv, k3, y.Copy(v3), 0)
	require.NoError(t, kv.Close())

	// The vlog should contain K0 and K3 (K1 and k2 was lost when Badger started up
	// last due to checksum failure).
	kv, err = Open(opts)
	require.NoError(t, err)

	{
		txn := kv.NewTransaction(false)

		iter := txn.NewIterator(DefaultIteratorOptions)
		iter.Seek(k0)
		require.True(t, iter.Valid())
		it := iter.Item()
		require.Equal(t, it.Key(), k0)
		require.Equal(t, getItemValue(t, it), v0)
		iter.Next()
		require.True(t, iter.Valid())
		it = iter.Item()
		require.Equal(t, it.Key(), k3)
		require.Equal(t, getItemValue(t, it), v3)

		iter.Close()
		txn.Discard()
	}

	require.NoError(t, kv.Close())
}

func TestPartialAppendToValueLog(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// Create skeleton files.
	opts := getTestOptions(dir)
	opts.Truncate = true
	opts.ValueLogFileSize = 100 * 1024 * 1024 // 100Mb
	kv, err := Open(opts)
	require.NoError(t, err)
	require.NoError(t, kv.Close())

	var (
		k0 = []byte("k0")
		k1 = []byte("k1")
		k2 = []byte("k2")
		k3 = []byte("k3")
		v0 = []byte("value0-01234567890123456789012012345678901234567890123")
		v1 = []byte("value1-01234567890123456789012012345678901234567890123")
		v2 = []byte("value2-01234567890123456789012012345678901234567890123")
		v3 = []byte("value3-01234567890123456789012012345678901234567890123")
	)
	// Values need to be long enough to actually get written to value log.
	require.True(t, len(v3) >= kv.opt.ValueThreshold)

	// Create truncated vlog to simulate a partial append.
	// k0 - single transaction, k1 and k2 in another transaction
	buf := createVlog(t, []*Entry{
		{Key: y.KeyWithTs(k0, 0), Value: y.Copy(v0)},
		{Key: y.KeyWithTs(k1, 0), Value: y.Copy(v1)},
		{Key: y.KeyWithTs(k2, 0), Value: y.Copy(v2)},
	})
	buf = buf[:len(buf)-6]
	require.NoError(t, ioutil.WriteFile(vlogFilePath(dir, 0), buf, 0777))

	// Badger should now start up
	kv, err = Open(opts)
	require.NoError(t, err)

	require.NoError(t, kv.View(func(txn *Txn) error {
		item, err := txn.Get(k0)
		require.NoError(t, err)
		require.Equal(t, v0, getItemValue(t, item))

		_, err = txn.Get(k1)
		require.Equal(t, ErrKeyNotFound, err)
		_, err = txn.Get(k2)
		require.Equal(t, ErrKeyNotFound, err)
		return nil
	}))

	// When K3 is set, it should be persisted after a restart.
	txnSet(t, kv, k3, v3, 0)
	require.NoError(t, kv.Close())
	kv, err = Open(getTestOptions(dir))
	require.NoError(t, err)
	checkKeys(t, kv, [][]byte{k3})

	// Replay value log from beginning, badger head is past k2.
	kv.vlog.Replay(logOffset{fid: 0}, replayFunction(kv))
	require.NoError(t, kv.Close())
}

func TestReadOnlyOpenWithPartialAppendToValueLog(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// Create skeleton files.
	opts := getTestOptions(dir)
	opts.ValueLogFileSize = 100 * 1024 * 1024 // 100Mb
	kv, err := Open(opts)
	require.NoError(t, err)
	require.NoError(t, kv.Close())

	var (
		k0 = []byte("k0")
		k1 = []byte("k1")
		k2 = []byte("k2")
		v0 = []byte("value0-012345678901234567890123")
		v1 = []byte("value1-012345678901234567890123")
		v2 = []byte("value2-012345678901234567890123")
	)

	// Create truncated vlog to simulate a partial append.
	// k0 - single transaction, k1 and k2 in another transaction
	buf := createVlog(t, []*Entry{
		{Key: y.KeyWithTs(k0, 0), Value: v0},
		{Key: y.KeyWithTs(k1, 0), Value: v1},
		{Key: y.KeyWithTs(k2, 0), Value: v2},
	})
	buf = buf[:len(buf)-6]
	require.NoError(t, ioutil.WriteFile(vlogFilePath(dir, 0), buf, 0777))

	opts.ReadOnly = true
	// Badger should fail a read-only open with values to replay
	kv, err = Open(opts)
	require.Error(t, err)
	require.Regexp(t, "Database was not properly closed, cannot open read-only|Read-only mode is not supported on Windows", err.Error())
}

func createVlog(t *testing.T, entries []*Entry) []byte {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	opts := getTestOptions(dir)
	opts.ValueLogFileSize = 100 * 1024 * 1024 // 100Mb
	kv, err := Open(opts)
	require.NoError(t, err)
	txnSet(t, kv, entries[0].Key.UserKey, entries[0].Value, entries[0].meta)
	entries = entries[1:]
	txn := kv.NewTransaction(true)
	for _, entry := range entries {
		require.NoError(t, txn.SetWithMeta(entry.Key.UserKey, entry.Value, entry.meta))
	}
	require.NoError(t, txn.Commit())
	require.NoError(t, kv.Close())

	filename := vlogFilePath(dir, 0)
	buf, err := ioutil.ReadFile(filename)
	require.NoError(t, err)

	read, reader := &safeRead{}, bufio.NewReader(bytes.NewReader(buf))
	var offset int
	for {
		e, err := read.Entry(reader)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		offset += headerBufSize + len(e.Key.UserKey) + len(e.Value) + 4
	}
	return buf[:offset]
}

func checkKeys(t *testing.T, kv *DB, keys [][]byte) {
	i := 0
	txn := kv.NewTransaction(false)
	iter := txn.NewIterator(IteratorOptions{})
	defer iter.Close()
	for iter.Seek(keys[0]); iter.Valid(); iter.Next() {
		require.Equal(t, iter.Item().Key(), keys[i])
		i++
	}
	require.Equal(t, i, len(keys))
}
