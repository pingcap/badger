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
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/badger/options"
	"github.com/pingcap/badger/table/sstable"
	"github.com/pingcap/badger/y"
	"github.com/stretchr/testify/require"
)

var mmap = flag.Bool("vlog_mmap", true, "Specify if value log must be memory-mapped")

func getTestCompression(tp options.CompressionType) []options.CompressionType {
	tps := make([]options.CompressionType, DefaultOptions.TableBuilderOptions.MaxLevels)
	for i := range tps {
		tps[i] = tp
	}
	return tps
}

func getTestOptions(dir string) Options {
	opt := DefaultOptions
	opt.TableBuilderOptions.MaxTableSize = 4 << 15 // Force more compaction.
	opt.MaxMemTableSize = 4 << 15                  // Force more compaction.
	opt.LevelOneSize = 4 << 15                     // Force more compaction.
	opt.Dir = dir
	opt.ValueDir = dir
	opt.SyncWrites = false
	opt.TableBuilderOptions.CompressionPerLevel = getTestCompression(options.ZSTD)
	return opt
}

func getItemValue(t *testing.T, item *Item) (val []byte) {
	v, err := item.Value()
	if err != nil {
		t.Error(err)
	}
	if v == nil {
		return nil
	}
	vSize := item.ValueSize()
	require.Equal(t, vSize, len(v))
	another, err := item.ValueCopy(nil)
	require.NoError(t, err)
	require.Equal(t, v, another)
	return v
}

func txnSet(t *testing.T, kv *DB, key []byte, val []byte, meta byte) {
	txn := kv.NewTransaction(true)
	require.NoError(t, txn.SetWithMeta(key, val, meta))
	require.NoError(t, txn.Commit())
}

func txnDelete(t *testing.T, kv *DB, key []byte) {
	txn := kv.NewTransaction(true)
	require.NoError(t, txn.Delete(key))
	require.NoError(t, txn.Commit())
}

// Opens a badger db and runs a a test on it.
func runBadgerTest(t *testing.T, opts *Options, test func(t *testing.T, db *DB)) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	if opts == nil {
		opts = new(Options)
		*opts = getTestOptions(dir)
	}
	db, err := Open(*opts)
	require.NoError(t, err)
	defer db.Close()
	test(t, db)
}

func TestWrite(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		for i := 0; i < 100; i++ {
			txnSet(t, db, []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i)), 0x00)
		}
	})
}

func TestUpdateAndView(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		err := db.Update(func(txn *Txn) error {
			for i := 0; i < 10; i++ {
				err := txn.Set([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i)))
				if err != nil {
					return err
				}
			}
			return nil
		})
		require.NoError(t, err)

		err = db.View(func(txn *Txn) error {
			for i := 0; i < 10; i++ {
				item, err := txn.Get([]byte(fmt.Sprintf("key%d", i)))
				if err != nil {
					return err
				}

				val, err := item.Value()
				if err != nil {
					return err
				}
				expected := []byte(fmt.Sprintf("val%d", i))
				require.Equal(t, expected, val,
					"Invalid value for key %q. expected: %q, actual: %q",
					item.Key(), expected, val)
			}
			return nil
		})
		require.NoError(t, err)
	})
}

func TestConcurrentWrite(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		// Not a benchmark. Just a simple test for concurrent writes.
		n := 20
		m := 500
		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				for j := 0; j < m; j++ {
					txnSet(t, db, []byte(fmt.Sprintf("k%05d_%08d", i, j)),
						[]byte(fmt.Sprintf("v%05d_%08d", i, j)), byte(j%127))
				}
			}(i)
		}
		wg.Wait()

		t.Log("Starting iteration")
		txn := db.NewTransaction(true)
		it := txn.NewIterator(DefaultIteratorOptions)
		defer it.Close()
		var i, j int
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			if k == nil {
				break // end of iteration.
			}

			require.EqualValues(t, fmt.Sprintf("k%05d_%08d", i, j), string(k))
			v := getItemValue(t, item)
			require.EqualValues(t, fmt.Sprintf("v%05d_%08d", i, j), string(v))
			require.Equal(t, item.UserMeta(), []byte{byte(j % 127)})
			j++
			if j == m {
				i++
				j = 0
			}
		}
		require.EqualValues(t, n, i)
		require.EqualValues(t, 0, j)
	})
}

func TestGet(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		txnSet(t, db, []byte("key1"), []byte("val1"), 0x08)

		txn := db.NewTransaction(false)
		item, err := txn.Get([]byte("key1"))
		require.NoError(t, err)
		require.EqualValues(t, "val1", getItemValue(t, item))
		require.Equal(t, []byte{0x08}, item.UserMeta())
		txn.Discard()

		txnSet(t, db, []byte("key1"), []byte("val2"), 0x09)

		txn = db.NewTransaction(false)
		item, err = txn.Get([]byte("key1"))
		require.NoError(t, err)
		require.EqualValues(t, "val2", getItemValue(t, item))
		require.Equal(t, []byte{0x09}, item.UserMeta())
		txn.Discard()

		txnDelete(t, db, []byte("key1"))

		txn = db.NewTransaction(false)
		_, err = txn.Get([]byte("key1"))
		require.Equal(t, ErrKeyNotFound, err)
		txn.Discard()

		txnSet(t, db, []byte("key1"), []byte("val3"), 0x01)

		txn = db.NewTransaction(false)
		item, err = txn.Get([]byte("key1"))
		require.NoError(t, err)
		require.EqualValues(t, "val3", getItemValue(t, item))
		require.Equal(t, []byte{0x01}, item.UserMeta())
		txn.Discard()

		longVal := make([]byte, 1000)
		txnSet(t, db, []byte("key1"), y.Copy(longVal), 0x00)

		txn = db.NewTransaction(false)
		item, err = txn.Get([]byte("key1"))
		require.NoError(t, err)
		require.EqualValues(t, longVal, getItemValue(t, item))
		txn.Discard()
	})
}

func TestGetAfterDelete(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		// populate with one entry
		key := []byte("key")
		txnSet(t, db, key, []byte("val1"), 0x00)
		require.NoError(t, db.Update(func(txn *Txn) error {
			err := txn.Delete(key)
			require.NoError(t, err)

			_, err = txn.Get(key)
			require.Equal(t, ErrKeyNotFound, err)
			return nil
		}))
	})
}

func TestTxnTooBig(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		data := func(i int) []byte {
			return []byte(fmt.Sprintf("%b", i))
		}
		//	n := 500000
		n := 1000
		txn := db.NewTransaction(true)
		for i := 0; i < n; {
			if err := txn.Set(data(i), data(i)); err != nil {
				require.NoError(t, txn.Commit())
				txn = db.NewTransaction(true)
			} else {
				i++
			}
		}
		require.NoError(t, txn.Commit())

		txn = db.NewTransaction(true)
		for i := 0; i < n; {
			if err := txn.Delete(data(i)); err != nil {
				require.NoError(t, txn.Commit())
				txn = db.NewTransaction(true)
			} else {
				i++
			}
		}
		require.NoError(t, txn.Commit())
	})
}

func TestForceCompactL0(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	opts := getTestOptions(dir)
	opts.ValueLogFileSize = 15 << 20
	db, err := OpenManaged(opts)
	require.NoError(t, err)

	data := func(i int) []byte {
		return []byte(fmt.Sprintf("%b", i))
	}
	n := 80
	m := 45 // Increasing would cause ErrTxnTooBig
	sz := 128
	v := make([]byte, sz)
	for i := 0; i < n; i += 2 {
		version := uint64(i)
		txn := db.NewTransactionAt(version, true)
		for j := 0; j < m; j++ {
			require.NoError(t, txn.SetEntry(&Entry{
				Key:   y.KeyWithTs(data(j), version+1),
				Value: v,
			}))
		}
		require.NoError(t, txn.Commit())
	}
	db.Close()

	db, err = OpenManaged(opts)
	defer db.Close()
	require.Equal(t, len(db.lc.levels[0].tables), 0)
}

// Put a lot of data to move some data to disk.
// WARNING: This test might take a while but it should pass!
func TestGetMore(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {

		data := func(i int) []byte {
			return []byte(fmt.Sprintf("%b", i))
		}
		n := 10000

		txn := db.NewTransaction(true)
		for i := 0; i < n; i++ {
			require.NoError(t, txn.Set(data(i), data(i)))
		}
		require.NoError(t, txn.Commit())

		require.NoError(t, db.validate())

		for i := 0; i < n; i++ {
			txn := db.NewTransaction(false)
			item, err := txn.Get(data(i))
			if err != nil {
				t.Errorf("key %v", data(i))
			}
			require.EqualValues(t, string(data(i)), string(getItemValue(t, item)))
			txn.Discard()
		}

		// Overwrite
		txn = db.NewTransaction(true)
		for i := 0; i < n; i++ {
			require.NoError(t, txn.Set(data(i),
				// Use a long value that will certainly exceed value threshold.
				[]byte(fmt.Sprintf("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz%9d", i))))
		}
		require.NoError(t, txn.Commit())

		require.NoError(t, db.validate())

		for i := 0; i < n; i++ {
			expectedValue := fmt.Sprintf("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz%9d", i)
			k := data(i)
			txn := db.NewTransaction(false)
			item, err := txn.Get(k)
			if err != nil {
				t.Error(err)
			}
			got := string(getItemValue(t, item))
			if expectedValue != got {

				vs := db.get(y.KeyWithTs(k, math.MaxUint64))
				fmt.Printf("wanted=%q Item: %s\n", k, item)
				fmt.Printf("on re-run, got version: %+v\n", vs)

				txn := db.NewTransaction(false)
				itr := txn.NewIterator(DefaultIteratorOptions)
				for itr.Seek(k); itr.Valid(); itr.Next() {
					item := itr.Item()
					fmt.Printf("item=%s\n", item)
					if !bytes.Equal(item.Key(), k) {
						break
					}
				}
				itr.Close()
				txn.Discard()
			}
			require.EqualValues(t, expectedValue, string(getItemValue(t, item)), "wanted=%q Item: %s\n", k, item)
			txn.Discard()
		}

		// MultiGet
		var multiGetKeys [][]byte
		var expectedValues []string
		for i := 0; i < n; i += 100 {
			multiGetKeys = append(multiGetKeys, data(i))
			// Set a long value to make sure we have enough sst tables.
			expectedValues = append(expectedValues, fmt.Sprintf("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz%9d", i))
		}
		txn1 := db.NewTransaction(false)
		items, err := txn1.MultiGet(multiGetKeys)
		require.NoError(t, err)
		for i, item := range items {
			val, err1 := item.Value()
			require.NoError(t, err1)
			require.Equal(t, expectedValues[i], string(val))
		}
		txn1.Discard()

		// "Delete" key.
		txn = db.NewTransaction(true)
		for i := 0; i < n; i++ {
			if (i % 10000) == 0 {
				fmt.Printf("Deleting i=%d\n", i)
			}
			require.NoError(t, txn.Delete(data(i)))
		}
		require.NoError(t, txn.Commit())

		db.validate()
		for i := 0; i < n; i++ {
			if (i % 10000) == 0 {
				// Display some progress. Right now, it's not very fast with no caching.
				fmt.Printf("Testing i=%d\n", i)
			}
			k := data(i)
			txn := db.NewTransaction(false)
			_, err := txn.Get([]byte(k))
			require.Equal(t, ErrKeyNotFound, err, "should not have found k: %q", k)
			txn.Discard()
		}
	})
}

// Put a lot of data to move some data to disk.
// WARNING: This test might take a while but it should pass!
func TestExistsMore(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		//	n := 500000
		n := 10000
		m := 45
		for i := 0; i < n; i += m {
			if (i % 1000) == 0 {
				t.Logf("Putting i=%d\n", i)
			}
			txn := db.NewTransaction(true)
			for j := i; j < i+m && j < n; j++ {
				require.NoError(t, txn.Set([]byte(fmt.Sprintf("%09d", j)),
					[]byte(fmt.Sprintf("%09d", j))))
			}
			require.NoError(t, txn.Commit())
		}
		db.validate()

		for i := 0; i < n; i++ {
			if (i % 1000) == 0 {
				fmt.Printf("Testing i=%d\n", i)
			}
			k := fmt.Sprintf("%09d", i)
			require.NoError(t, db.View(func(txn *Txn) error {
				_, err := txn.Get([]byte(k))
				require.NoError(t, err)
				return nil
			}))
		}
		require.NoError(t, db.View(func(txn *Txn) error {
			_, err := txn.Get([]byte("non-exists"))
			require.Error(t, err)
			return nil
		}))

		// "Delete" key.
		for i := 0; i < n; i += m {
			if (i % 1000) == 0 {
				fmt.Printf("Deleting i=%d\n", i)
			}
			txn := db.NewTransaction(true)
			for j := i; j < i+m && j < n; j++ {
				require.NoError(t, txn.Delete([]byte(fmt.Sprintf("%09d", j))))
			}
			require.NoError(t, txn.Commit())
		}
		db.validate()
		for i := 0; i < n; i++ {
			if (i % 10000) == 0 {
				// Display some progress. Right now, it's not very fast with no caching.
				fmt.Printf("Testing i=%d\n", i)
			}
			k := fmt.Sprintf("%09d", i)

			require.NoError(t, db.View(func(txn *Txn) error {
				_, err := txn.Get([]byte(k))
				require.Error(t, err)
				return nil
			}))
		}
		fmt.Println("Done and closing")
	})
}

func TestIterate2Basic(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {

		bkey := func(i int) []byte {
			return []byte(fmt.Sprintf("%09d", i))
		}
		bval := func(i int) []byte {
			return []byte(fmt.Sprintf("%025d", i))
		}

		// n := 500000
		n := 10000
		for i := 0; i < n; i++ {
			if (i % 1000) == 0 {
				t.Logf("Put i=%d\n", i)
			}
			txnSet(t, db, bkey(i), bval(i), byte(i%127))
		}

		txn := db.NewTransaction(false)
		it := txn.NewIterator(DefaultIteratorOptions)
		{
			var count int
			rewind := true
			t.Log("Starting first basic iteration")
			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				key := item.Key()
				if rewind && count == 5000 {
					// Rewind would skip /head/ key, and it.Next() would skip 0.
					count = 1
					it.Rewind()
					t.Log("Rewinding from 5000 to zero.")
					rewind = false
					continue
				}
				require.EqualValues(t, bkey(count), string(key))
				val := getItemValue(t, item)
				require.EqualValues(t, bval(count), string(val))
				require.Equal(t, []byte{byte(count % 127)}, item.UserMeta())
				count++
			}
			require.EqualValues(t, n, count)
		}

		{
			t.Log("Starting second basic iteration")
			idx := 5030
			for it.Seek(bkey(idx)); it.Valid(); it.Next() {
				item := it.Item()
				require.EqualValues(t, bkey(idx), string(item.Key()))
				require.EqualValues(t, bval(idx), string(getItemValue(t, item)))
				idx++
			}
		}
		it.Close()
	})
}

func TestLoad(t *testing.T) {
	testLoad := func(t *testing.T, opt Options) {
		dir, err := ioutil.TempDir("", "badger-test")
		require.NoError(t, err)
		defer os.RemoveAll(dir)
		opt.Dir = dir
		opt.ValueDir = dir
		n := 10000

		{
			kv, _ := Open(getTestOptions(dir))
			for i := 0; i < n; i++ {
				if (i % 10000) == 0 {
					fmt.Printf("Putting i=%d\n", i)
				}
				k := []byte(fmt.Sprintf("%09d", i))
				txnSet(t, kv, k, k, 0x00)
			}
			kv.Close()
		}

		kv, err := Open(getTestOptions(dir))
		require.NoError(t, err)
		require.Equal(t, uint64(10001), kv.orc.readTs())
		for i := 0; i < n; i++ {
			if (i % 10000) == 0 {
				fmt.Printf("Testing i=%d\n", i)
			}
			k := fmt.Sprintf("%09d", i)
			require.NoError(t, kv.View(func(txn *Txn) error {
				item, err := txn.Get([]byte(k))
				require.NoError(t, err)
				require.EqualValues(t, k, string(getItemValue(t, item)))
				return nil
			}))
		}

		kv.Close()
		summary := kv.lc.getSummary()

		// Check that files are garbage collected.
		idMap := getIDMap(dir)
		for fileID := range idMap {
			// Check that name is in summary.filenames.
			require.True(t, summary.fileIDs[fileID], "%d", fileID)
		}
		require.EqualValues(t, len(idMap), len(summary.fileIDs))

		var fileIDs []uint64
		for k := range summary.fileIDs { // Map to array.
			fileIDs = append(fileIDs, k)
		}
		sort.Slice(fileIDs, func(i, j int) bool { return fileIDs[i] < fileIDs[j] })
		fmt.Printf("FileIDs: %v\n", fileIDs)
	}

	t.Run("Without compression", func(t *testing.T) {
		opt := getTestOptions("")
		opt.TableBuilderOptions.CompressionPerLevel = getTestCompression(options.None)
		testLoad(t, opt)
	})
	t.Run("With compression", func(t *testing.T) {
		opt := getTestOptions("")
		testLoad(t, opt)
	})
}

func TestIterateDeleted(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		txnSet(t, db, []byte("Key1"), []byte("Value1"), 0x00)
		txnSet(t, db, []byte("Key2"), []byte("Value2"), 0x00)

		txn := db.NewTransaction(false)
		idxIt := txn.NewIterator(DefaultIteratorOptions)

		count := 0
		txn2 := db.NewTransaction(true)
		prefix := []byte("Key")
		for idxIt.Seek(prefix); idxIt.ValidForPrefix(prefix); idxIt.Next() {
			key := idxIt.Item().Key()
			count++
			newKey := make([]byte, len(key))
			copy(newKey, key)
			require.NoError(t, txn2.Delete(newKey))
		}
		require.Equal(t, 2, count)
		require.NoError(t, txn2.Commit())
		idxIt.Close()
		t.Run(fmt.Sprintf("Prefetch=%t", false), func(t *testing.T) {
			txn := db.NewTransaction(false)
			idxIt := txn.NewIterator(DefaultIteratorOptions)
			var estSize int64
			var idxKeys []string
			for idxIt.Seek(prefix); idxIt.Valid(); idxIt.Next() {
				item := idxIt.Item()
				key := item.Key()
				estSize += item.EstimatedSize()
				if !bytes.HasPrefix(key, prefix) {
					break
				}
				idxKeys = append(idxKeys, string(key))
				t.Logf("%+v\n", idxIt.Item())
			}
			require.Equal(t, 0, len(idxKeys))
			require.Equal(t, int64(0), estSize)
			idxIt.Close()
		})
	})
}

func TestDeleteWithoutSyncWrite(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	opt := DefaultOptions
	opt.Dir = dir
	opt.ValueDir = dir
	kv, err := Open(opt)
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	key := []byte("k1")
	// Set a value with size > value threshold so that its written to value log.
	txnSet(t, kv, key, []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789FOOBARZOGZOG"), 0x00)
	txnDelete(t, kv, key)
	kv.Close()

	// Reopen KV
	kv, err = Open(opt)
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	defer kv.Close()

	require.NoError(t, kv.View(func(txn *Txn) error {
		_, err := txn.Get(key)
		require.Equal(t, ErrKeyNotFound, err)
		return nil
	}))
}

func TestPidFile(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		// Reopen database
		_, err := Open(getTestOptions(db.opt.Dir))
		require.Error(t, err)
		require.Contains(t, err.Error(), "Another process is using this Badger database")
	})
}

func TestBigKeyValuePairs(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		bigK := make([]byte, maxKeySize+1)
		bigV := make([]byte, db.opt.ValueLogFileSize+1)
		small := make([]byte, 10)

		txn := db.NewTransaction(true)
		require.Regexp(t, regexp.MustCompile("Key.*exceeded"), txn.Set(bigK, small))
		require.Regexp(t, regexp.MustCompile("Value.*exceeded"), txn.Set(small, bigV))

		require.NoError(t, txn.Set(small, small))
		require.Regexp(t, regexp.MustCompile("Key.*exceeded"), txn.Set(bigK, bigV))

		require.NoError(t, db.View(func(txn *Txn) error {
			_, err := txn.Get(small)
			require.Equal(t, ErrKeyNotFound, err)
			return nil
		}))
	})
}

func TestSetIfAbsentAsync(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	kv, _ := Open(getTestOptions(dir))

	bkey := func(i int) []byte {
		return []byte(fmt.Sprintf("%09d", i))
	}

	n := 1000
	for i := 0; i < n; i++ {
		// if (i % 10) == 0 {
		// 	t.Logf("Put i=%d\n", i)
		// }
		txn := kv.NewTransaction(true)
		_, err = txn.Get(bkey(i))
		require.Equal(t, ErrKeyNotFound, err)
		require.NoError(t, txn.SetWithMeta(bkey(i), nil, byte(i%127)))
		require.NoError(t, txn.Commit())
	}

	require.NoError(t, kv.Close())
	kv, err = Open(getTestOptions(dir))
	require.NoError(t, err)

	txn := kv.NewTransaction(false)
	var count int
	it := txn.NewIterator(DefaultIteratorOptions)
	defer it.Close()
	{
		t.Log("Starting first basic iteration")
		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		require.EqualValues(t, n, count)
	}
	require.Equal(t, n, count)
	require.NoError(t, kv.Close())
}

func TestGetSetRace(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {

		data := make([]byte, 4096)
		_, err := rand.Read(data)
		require.NoError(t, err)

		var (
			numOp = 100
			wg    sync.WaitGroup
			keyCh = make(chan string)
		)

		// writer
		wg.Add(1)
		go func() {
			defer func() {
				wg.Done()
				close(keyCh)
			}()

			for i := 0; i < numOp; i++ {
				key := fmt.Sprintf("%d", i)
				txnSet(t, db, []byte(key), data, 0x00)
				keyCh <- key
			}
		}()

		// reader
		wg.Add(1)
		go func() {
			defer wg.Done()

			for key := range keyCh {
				require.NoError(t, db.View(func(txn *Txn) error {
					item, err := txn.Get([]byte(key))
					require.NoError(t, err)
					_, err = item.Value()
					require.NoError(t, err)
					return nil
				}))
			}
		}()

		wg.Wait()
	})
}

func randBytes(n int) []byte {
	recv := make([]byte, n)
	in, err := rand.Read(recv)
	if err != nil {
		log.Fatal(err)
	}
	return recv[:in]
}

var benchmarkData = []struct {
	key, value []byte
}{
	{randBytes(100), nil},
	{randBytes(1000), []byte("foo")},
	{[]byte("foo"), randBytes(1000)},
	{[]byte(""), randBytes(1000)},
	{nil, randBytes(1000000)},
	{randBytes(100000), nil},
	{randBytes(1000000), nil},
}

func TestLargeKeys(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	opts := new(Options)
	*opts = DefaultOptions
	opts.ValueLogFileSize = 1024 * 1024 * 1024
	opts.Dir = dir
	opts.ValueDir = dir

	db, err := Open(*opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 1000; i++ {
		tx := db.NewTransaction(true)
		for _, kv := range benchmarkData {
			k := make([]byte, len(kv.key))
			copy(k, kv.key)

			v := make([]byte, len(kv.value))
			copy(v, kv.value)
			if err := tx.Set(k, v); err != nil {
				// Skip over this record.
			}
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("#%d: batchSet err: %v", i, err)
		}
	}
}

func TestCreateDirs(t *testing.T) {
	dir, err := ioutil.TempDir("", "parent")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	opts := DefaultOptions
	dir = filepath.Join(dir, "badger")
	opts.Dir = dir
	opts.ValueDir = dir
	db, err := Open(opts)
	require.NoError(t, err)
	db.Close()
	_, err = os.Stat(dir)
	require.NoError(t, err)
}

func TestGetSetDeadlock(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	fmt.Println(dir)
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	opt := DefaultOptions
	opt.Dir = dir
	opt.ValueDir = dir
	opt.ValueLogFileSize = 1 << 20
	db, err := Open(opt)
	require.NoError(t, err)
	defer db.Close()

	val := make([]byte, 1<<19)
	key := []byte("key1")
	require.NoError(t, db.Update(func(txn *Txn) error {
		rand.Read(val)
		require.NoError(t, txn.Set(key, val))
		return nil
	}))

	timeout, done := time.After(10*time.Second), make(chan bool)

	go func() {
		db.Update(func(txn *Txn) error {
			item, err := txn.Get(key)
			require.NoError(t, err)
			_, err = item.Value() // This take a RLock on file
			require.NoError(t, err)

			rand.Read(val)
			require.NoError(t, txn.Set(key, val))
			require.NoError(t, txn.Set([]byte("key2"), val))
			return nil
		})
		done <- true
	}()

	select {
	case <-timeout:
		t.Fatal("db.Update did not finish within 10s, assuming deadlock.")
	case <-done:
		t.Log("db.Update finished.")
	}
}

func TestWriteDeadlock(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	fmt.Println(dir)
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	opt := DefaultOptions
	opt.Dir = dir
	opt.ValueDir = dir
	opt.ValueLogFileSize = 10 << 20
	db, err := Open(opt)
	require.NoError(t, err)
	defer db.Close()

	print := func(count *int) {
		*count++
		if *count%100 == 0 {
			fmt.Printf("%05d\r", *count)
		}
	}

	var count int
	val := make([]byte, 1000)
	require.NoError(t, db.Update(func(txn *Txn) error {
		for i := 0; i < 1500; i++ {
			key := fmt.Sprintf("%d", i)
			rand.Read(val)
			require.NoError(t, txn.Set([]byte(key), y.Copy(val)))
			print(&count)
		}
		return nil
	}))

	count = 0
	fmt.Println("\nWrites done. Iteration and updates starting...")
	err = db.Update(func(txn *Txn) error {
		it := txn.NewIterator(DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			// Using Value() would cause deadlock.
			// item.Value()
			out, err := item.ValueCopy(nil)
			require.NoError(t, err)
			require.Equal(t, len(val), len(out))

			key := y.Copy(item.Key())
			rand.Read(val)
			require.NoError(t, txn.Set(key, val))
			print(&count)
		}
		return nil
	})
	require.NoError(t, err)
}

func TestReadOnly(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	opts := getTestOptions(dir)

	// Create the DB
	db, err := Open(opts)
	require.NoError(t, err)
	for i := 0; i < 10000; i++ {
		txnSet(t, db, []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("value%d", i)), 0x00)
	}

	// Attempt a read-only open while it's open read-write.
	opts.ReadOnly = true
	_, err = Open(opts)
	require.Error(t, err)
	if err == ErrWindowsNotSupported {
		return
	}
	require.Contains(t, err.Error(), "Another process is using this Badger database")
	db.Close()

	// Open one read-only
	opts.ReadOnly = true
	kv1, err := Open(opts)
	require.NoError(t, err)
	defer kv1.Close()

	// Open another read-only
	kv2, err := Open(opts)
	require.NoError(t, err)
	defer kv2.Close()

	// Attempt a read-write open while it's open for read-only
	opts.ReadOnly = false
	_, err = Open(opts)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Another process is using this Badger database")

	// Get a thing from the DB
	txn1 := kv1.NewTransaction(true)
	v1, err := txn1.Get([]byte("key1"))
	require.NoError(t, err)
	b1, err := v1.Value()
	require.NoError(t, err)
	require.Equal(t, b1, []byte("value1"))
	err = txn1.Commit()
	require.NoError(t, err)

	// Get a thing from the DB via the other connection
	txn2 := kv2.NewTransaction(true)
	v2, err := txn2.Get([]byte("key2000"))
	require.NoError(t, err)
	b2, err := v2.Value()
	require.NoError(t, err)
	require.Equal(t, b2, []byte("value2000"))
	err = txn2.Commit()
	require.NoError(t, err)

	// Attempt to set a value on a read-only connection
	txn := kv1.NewTransaction(true)
	err = txn.SetWithMeta([]byte("key"), []byte("value"), 0x00)
	require.Error(t, err)
	require.Contains(t, err.Error(), "No sets or deletes are allowed in a read-only transaction")
	err = txn.Commit()
	require.NoError(t, err)
}

func TestLSMOnly(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	opts := LSMOnlyOptions
	opts.Dir = dir
	opts.ValueDir = dir

	dopts := DefaultOptions
	require.NotEqual(t, dopts.ValueThreshold, opts.ValueThreshold)
	require.NotEqual(t, dopts.ValueLogFileSize, opts.ValueLogFileSize)

	dopts.ValueThreshold = 1 << 16
	_, err = Open(dopts)
	require.Equal(t, ErrValueThreshold, err)

	db, err := Open(opts)
	require.NoError(t, err)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 5000; i++ {
		value := make([]byte, 64000)
		_, err = rand.Read(value)
		require.NoError(t, err)

		txnSet(t, db, []byte(fmt.Sprintf("key%d", i)), value, 0x00)
	}
}

func TestMinReadTs(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		for i := 0; i < 10; i++ {
			require.NoError(t, db.Update(func(txn *Txn) error {
				return txn.Set([]byte("x"), []byte("y"))
			}))
		}
		time.Sleep(time.Second)
		require.Equal(t, uint64(10), db.orc.readTs())
		min := db.getCompactSafeTs()
		require.Equal(t, uint64(9), min)

		readTxn := db.NewTransaction(false)
		for i := 0; i < 10; i++ {
			require.NoError(t, db.Update(func(txn *Txn) error {
				return txn.Set([]byte("x"), []byte("y"))
			}))
		}
		require.Equal(t, uint64(20), db.orc.readTs())
		time.Sleep(time.Second)
		require.Equal(t, min, db.getCompactSafeTs())
		readTxn.Discard()
		time.Sleep(time.Second)
		require.Equal(t, uint64(19), db.getCompactSafeTs())
		// // The minReadTS can only be increase by newer txn done.
		// readTxn = db.NewTransaction(false)
		// readTxn.Discard()
		// time.Sleep(time.Second)
		// require.Equal(t, uint64(20), db.getCompactSafeTs())

		for i := 0; i < 10; i++ {
			db.View(func(txn *Txn) error {
				return nil
			})
		}
		time.Sleep(time.Second)
		require.Equal(t, uint64(20), db.getCompactSafeTs())
	})
}

type testFilter struct{}

var (
	userMetaDrop   = []byte{0, 0}
	userMetaDelete = []byte{0}
)

func (f *testFilter) Filter(key, val, userMeta []byte) Decision {
	if bytes.Equal(userMeta, userMetaDrop) {
		return DecisionDrop
	} else if bytes.Equal(userMeta, userMetaDelete) {
		return DecisionMarkTombstone
	}
	return DecisionKeep
}

func TestCompactionFilter(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	opts := getTestOptions(dir)
	opts.ValueThreshold = 8 * 1024
	opts.TableBuilderOptions.MaxTableSize = 32 * 1024
	opts.MaxMemTableSize = 32 * 1024
	opts.NumMemtables = 2
	opts.NumLevelZeroTables = 1
	opts.NumLevelZeroTablesStall = 2
	opts.CompactionFilterFactory = func(targetLevel int, smallest, biggest []byte) CompactionFilter {
		return &testFilter{}
	}
	// This case depend on level's size, so disable compression for now.
	opts.TableBuilderOptions.CompressionPerLevel = getTestCompression(options.None)
	db, err := Open(opts)
	require.NoError(t, err)
	defer db.Close()
	val := make([]byte, 1024*4)
	// Insert 50 entries that will be kept.
	for i := 0; i < 50; i++ {
		err = db.Update(func(txn *Txn) error {
			key := []byte(fmt.Sprintf("key%d", i))
			// Entries without userMeta will result in DecisionKeep in testFilter.
			return txn.Set(key, val)
		})
		require.NoError(t, err)
	}
	// Insert keys for delete decision and drop decision.
	for i := 0; i < 100; i++ {
		db.Update(func(txn *Txn) error {
			key := []byte(fmt.Sprintf("key%d", i))
			if i%2 == 0 {
				// Entries with userMetaDelete will result in DecisionMarkTombstone in testFilter.
				txn.SetWithMetaSlice(key, val, userMetaDelete)
			} else {
				// Entries with userMetaDrop will result in DecisionDrop in testFilter.
				txn.SetWithMetaSlice(key, val, userMetaDrop)
			}
			return nil
		})
	}
	var deleteNotFoundCount, dropAppearOldCount int
	err = db.View(func(txn *Txn) error {
		for i := 0; i < 50; i++ {
			key := []byte(fmt.Sprintf("key%d", i))
			item, _ := txn.Get(key)
			if i%2 == 0 {
				// For delete decision, the old value can not be read.
				if item == nil {
					deleteNotFoundCount++
				}
			} else {
				// For dropped entry, since no tombstone left, the old value appear again.
				if item != nil && len(item.UserMeta()) == 0 {
					dropAppearOldCount++
				}
			}
		}
		return nil
	})
	require.True(t, deleteNotFoundCount > 0)
	// Since compaction always pick upper level first, the never has chance to reappear old value.
	// require.True(t, dropAppearOldCount > 0)
}

func (f *testFilter) Guards() []Guard {
	return []Guard{
		{
			Prefix:   nil,
			MatchLen: 1,
			MinSize:  64,
		},
		{
			Prefix:   []byte("b"),
			MatchLen: 4,
			MinSize:  128,
		},
		{
			Prefix:   []byte("bx"),
			MatchLen: 5,
			MinSize:  128,
		},
		{
			Prefix:   []byte("c"),
			MatchLen: 3,
			MinSize:  128,
		},
	}
}

func TestSearchGuard(t *testing.T) {
	filter := &testFilter{}
	guards := filter.Guards()
	tests := []struct {
		key         string
		guardPrefix string
	}{
		{"0", ""},
		{"br", "b"},
		{"bn", "b"},
		{"bx", "bx"},
		{"crz", "c"},
	}
	for _, tt := range tests {
		guard := searchGuard([]byte(tt.key), guards)
		require.Equal(t, string(guard.Prefix), tt.guardPrefix)
	}
}

func TestShouldFinishFile(t *testing.T) {
	tests1 := []struct {
		key     []byte
		lastKey []byte
		finish  bool
	}{
		{[]byte("k2"), nil, false},
		{[]byte("k2"), []byte("k1"), true},
		{[]byte("k12"), []byte("k1"), true},
		{[]byte("k234"), []byte("k233"), false},
		{[]byte("k241"), []byte("k233"), true},
		{[]byte("l233"), []byte("k233"), true},
	}
	guard := &Guard{Prefix: []byte("k"), MatchLen: 3, MinSize: 64}
	for _, tt := range tests1 {
		finish := shouldFinishFile(y.KeyWithTs(tt.key, 0), y.KeyWithTs(tt.lastKey, 0), guard, 100, 100)
		require.Equal(t, tt.finish, finish)
	}
	// A guard prefix change always finish the file even if the MinSize has not been reached.
	require.Equal(t, shouldFinishFile(y.KeyWithTs([]byte("l11"), 0), y.KeyWithTs([]byte("k11"), 0), guard, 1, 100), true)
	// guard prefix match, but matchLen changed, must reach MinSize to finish file.
	require.Equal(t, shouldFinishFile(y.KeyWithTs([]byte("k12"), 0), y.KeyWithTs([]byte("k11"), 0), guard, 1, 100), false)
	require.Equal(t, shouldFinishFile(y.KeyWithTs([]byte("k12"), 0), y.KeyWithTs([]byte("k11"), 0), guard, 65, 100), true)
	// table max size has reached always finish the file.
	require.Equal(t, shouldFinishFile(y.KeyWithTs([]byte("k111"), 0), y.KeyWithTs([]byte("k110"), 0), guard, 33, 32), true)
}

func TestIterateVLog(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	opts := getTestOptions(dir)
	opts.TableBuilderOptions.MaxTableSize = 1 << 20
	opts.MaxMemTableSize = 1 << 20
	opts.ValueLogFileSize = 1 << 20
	opts.ValueThreshold = 1000
	opts.ValueLogMaxNumFiles = 1000
	db, err := Open(opts)
	require.NoError(t, err)
	defer db.Close()
	for i := 0; i < 3000; i++ {
		err = db.Update(func(txn *Txn) error {
			key := []byte(fmt.Sprintf("key%d", i))
			val := make([]byte, 1024)
			return txn.Set(key, val)
		})
		require.NoError(t, err)
	}

	var iterKeys [][]byte
	var iterVals [][]byte
	err = db.IterateVLog(0, func(e Entry) {
		key := y.SafeCopy(nil, e.Key.UserKey)
		iterKeys = append(iterKeys, key)
		iterVals = append(iterVals, y.SafeCopy(nil, e.Value))
	})
	require.Nil(t, err)
	require.Equal(t, 3000, len(iterKeys))
	expectedVal := make([]byte, 1024)
	for i, key := range iterKeys {
		require.Equal(t, fmt.Sprintf("key%d", i), string(key))
		require.Equal(t, expectedVal, iterVals[i])
	}
	offset := db.GetVLogOffset()
	for i := 3000; i < 5000; i++ {
		err = db.Update(func(txn *Txn) error {
			key := []byte(fmt.Sprintf("key%d", i))
			return txn.Set(key, make([]byte, 999))
		})
		require.NoError(t, err)
	}
	iterKeys = iterKeys[:0]
	iterVals = iterVals[:0]
	err = db.IterateVLog(offset, func(e Entry) {
		key := y.SafeCopy(nil, e.Key.UserKey)
		iterKeys = append(iterKeys, key)
		iterVals = append(iterVals, y.SafeCopy(nil, e.Value))
	})
	require.Len(t, iterKeys, 2000)
	expectedVal = make([]byte, 999)
	for i, key := range iterKeys {
		require.Equal(t, fmt.Sprintf("key%d", i+3000), string(key))
		require.Equal(t, expectedVal, iterVals[i])
	}
}

func TestMultiGet(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	opts := getTestOptions(dir)
	opts.ValueThreshold = 512
	db, err := Open(opts)
	require.NoError(t, err)
	defer db.Close()
	var keys [][]byte
	for i := 0; i < 1000; i++ {
		keys = append(keys, []byte(fmt.Sprintf("key%d", i)))
	}
	for i := 0; i < 1000; i++ {
		err = db.Update(func(txn *Txn) error {
			val := make([]byte, 513)
			return txn.SetWithMetaSlice(keys[i], val, make([]byte, 16))
		})
		require.NoError(t, err)
	}
	txn := db.NewTransaction(false)
	defer txn.Discard()
	items, err := txn.MultiGet(keys)
	require.NoError(t, err)
	for i := range keys {
		require.NotNil(t, items[i])
	}
	// Update more data to trigger compaction.
	for i := 0; i < 1000; i++ {
		err = db.Update(func(txn *Txn) error {
			val := make([]byte, 500)
			return txn.Set(keys[i], val)
		})
		require.NoError(t, err)
	}
	// items can be safely accessed.
	total := 0
	for _, item := range items {
		total += int(item.UserMeta()[15])
	}
	require.Equal(t, 0, total)
}

func buildSst(t *testing.T, keys [][]byte, vals [][]byte) *os.File {
	filename := fmt.Sprintf("%s%s%d.sst", os.TempDir(), string(os.PathSeparator), rand.Int63())
	f, err := y.OpenSyncedFile(filename, true)
	require.NoError(t, err)
	builder := sstable.NewExternalTableBuilder(f, nil, DefaultOptions.TableBuilderOptions, options.ZSTD)

	for i, k := range keys {
		err := builder.Add(y.KeyWithTs(k, 0), y.ValueStruct{Value: vals[i], Meta: 0, UserMeta: []byte{0}})
		require.NoError(t, err)
	}
	_, err = builder.Finish()
	require.NoError(t, err)
	return f
}

func TestIngestSimple(t *testing.T) {
	var ingestKeys [][]byte
	for i := 1000; i < 2000; i++ {
		ingestKeys = append(ingestKeys, []byte(fmt.Sprintf("key%04d", i)))
	}
	f := buildSst(t, ingestKeys, ingestKeys)
	defer os.Remove(f.Name())

	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	opts := getTestOptions(dir)
	opts.ValueThreshold = 512
	db, err := Open(opts)
	require.NoError(t, err)

	var keys [][]byte
	for i := 0; i < 1000; i++ {
		keys = append(keys, []byte(fmt.Sprintf("key%d", i)))
	}
	for i := 0; i < 1000; i++ {
		err = db.Update(func(txn *Txn) error {
			return txn.SetWithMetaSlice(keys[i], keys[i], []byte{0})
		})
		require.NoError(t, err)
	}

	cnt, err := db.IngestExternalFiles([]ExternalTableSpec{{f.Name()}})
	require.NoError(t, err)
	require.Equal(t, 1, cnt)

	txn := db.NewTransaction(false)
	for _, k := range append(keys, ingestKeys...) {
		item, err := txn.Get(k)
		require.NoError(t, err)
		v, err := item.Value()
		require.NoError(t, err)
		require.Equal(t, k, v)
	}
}

func TestIngestOverwrite(t *testing.T) {
	var ingestKeys, ingestVals [][]byte
	for i := 0; i < 1000; i++ {
		ingestKeys = append(ingestKeys, []byte(fmt.Sprintf("key%07d", i)))
		ingestVals = append(ingestVals, []byte(fmt.Sprintf("val%07d", i)))
	}
	ingestKeys = append(ingestKeys, []byte(fmt.Sprintf("key%07d", 9000)))
	ingestVals = append(ingestVals, []byte(fmt.Sprintf("val%07d", 9000)))
	f := buildSst(t, ingestKeys, ingestVals)
	defer os.Remove(f.Name())

	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	opts := getTestOptions(dir)
	opts.ValueThreshold = 512
	opts.NumLevelZeroTables = 1
	opts.NumLevelZeroTablesStall = 2
	opts.LevelOneSize *= 5
	db, err := Open(opts)
	require.NoError(t, err)

	for i := 1000; i < 20000; i++ {
		if i == 9000 {
			continue
		}
		key := []byte(fmt.Sprintf("key%07d", i))
		err = db.Update(func(txn *Txn) error {
			return txn.SetWithMetaSlice(key, key, []byte{0})
		})
		require.NoError(t, err)
	}

	cnt, err := db.IngestExternalFiles([]ExternalTableSpec{{f.Name()}})
	require.NoError(t, err)
	require.Equal(t, 1, cnt)

	txn := db.NewTransaction(false)
	for i, k := range ingestKeys {
		item, err := txn.Get(k)
		require.NoError(t, err)
		v, err := item.Value()
		require.NoError(t, err)
		require.Equal(t, ingestVals[i], v)
	}
}

func TestIngestWhileWrite(t *testing.T) {
	var ingestKeys, ingestVals [][][]byte
	var files []*os.File
	for n := 0; n < 10; n++ {
		var keys, vals [][]byte
		for i := n * 1000; i < (n+1)*1000; i++ {
			keys = append(keys, []byte(fmt.Sprintf("key%05d", i)))
			vals = append(vals, []byte(fmt.Sprintf("val%05d", i)))
		}
		f := buildSst(t, keys, vals)
		files = append(files, f)
		ingestKeys = append(ingestKeys, keys)
		ingestVals = append(ingestVals, vals)
	}
	defer func() {
		for _, f := range files {
			os.Remove(f.Name())
		}
	}()

	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	opts := getTestOptions(dir)
	opts.ValueThreshold = 512
	db, err := Open(opts)
	require.NoError(t, err)

	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				close(done)
				return
			default:
			}

			err = db.Update(func(txn *Txn) error {
				for n := 0; n < 10; n++ {
					key := []byte(fmt.Sprintf("key%05d", n*1000+1))
					if err := txn.Set(key, key); err != nil {
						return err
					}
				}
				return nil
			})
			require.NoError(t, err)
		}
	}()

	specs := make([]ExternalTableSpec, len(files))
	for i := range specs {
		specs[i] = ExternalTableSpec{
			Filename: files[i].Name(),
		}
	}
	cnt, err := db.IngestExternalFiles(specs)
	require.NoError(t, err)
	require.Equal(t, len(files), cnt)
	close(stop)
	<-done

	txn := db.NewTransaction(false)
	for i, keys := range ingestKeys {
		vals := ingestVals[i]
		for j, k := range keys {
			val := vals[j]
			item, err := txn.Get(k)
			require.NoError(t, err)
			v, err := item.Value()
			require.NoError(t, err)
			if !bytes.Equal(val, v) {
				require.Equal(t, k, v)
			}
		}
	}
}

func TestIngestSplit(t *testing.T) {
	var ingestKeys [][]byte
	var files []*os.File
	{
		keys := [][]byte{[]byte("c"), []byte("d")}
		ingestKeys = append(ingestKeys, keys...)
		files = append(files, buildSst(t, keys, keys))
	}
	{
		keys := [][]byte{[]byte("e"), []byte("h")}
		ingestKeys = append(ingestKeys, keys...)
		files = append(files, buildSst(t, keys, keys))
	}
	{
		keys := [][]byte{[]byte("l"), []byte("o")}
		ingestKeys = append(ingestKeys, keys...)
		files = append(files, buildSst(t, keys, keys))
	}
	defer func() {
		for _, f := range files {
			os.Remove(f.Name())
		}
	}()

	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	opts := getTestOptions(dir)
	opts.ValueThreshold = 512
	opts.NumLevelZeroTables = 10
	opts.NumLevelZeroTablesStall = 20
	db, err := Open(opts)
	require.NoError(t, err)

	keys := [][]byte{[]byte("a"), []byte("b"), []byte("i"), []byte("k"), []byte("x"), []byte("z")}
	err = db.Update(func(txn *Txn) error {
		for _, k := range keys {
			if err := txn.Set(k, k); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	wg := db.flushMemTable()
	wg.Wait()

	l0 := db.lc.levels[0]
	l1 := db.lc.levels[1]
	l0.Lock()
	l1.Lock()
	l1.tables = append(l1.tables, l0.tables[0])
	l0.tables[0] = nil
	l0.tables = l0.tables[:0]
	l0.Unlock()
	l1.Unlock()

	specs := make([]ExternalTableSpec, len(files))
	for i := range specs {
		specs[i] = ExternalTableSpec{
			Filename: files[i].Name(),
		}
	}
	cnt, err := db.IngestExternalFiles(specs)
	require.NoError(t, err)
	require.Equal(t, 3, cnt)

	txn := db.NewTransaction(false)
	for _, k := range append(keys, ingestKeys...) {
		item, err := txn.Get(k)
		require.NoError(t, err, string(k))
		v, err := item.Value()
		require.NoError(t, err)
		require.Equal(t, k, v)
	}

	l1.RLock()
	tblCnt := 0
	for _, t := range l1.tables {
		if t.(*sstable.Table).HasGlobalTs() {
			tblCnt++
		}
	}
	l1.RUnlock()
	require.Equal(t, 2, tblCnt)
}

func TestDeleteRange(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		data := func(i int) []byte {
			return []byte(fmt.Sprintf("%06d", i))
		}
		n := 20000

		txn := db.NewTransaction(true)
		for i := 0; i < n; i++ {
			require.NoError(t, txn.Set(data(i), make([]byte, 128)))
		}
		require.NoError(t, txn.Commit())
		require.NoError(t, db.validate())

		db.DeleteFilesInRange(data(0), data(n/2))

		// wait for compaction.
		time.Sleep(2 * time.Second)

		txn = db.NewTransaction(false)
		for i := 0; i < n/4; i++ {
			_, err := txn.Get(data(i))
			require.Equal(t, ErrKeyNotFound, err)
		}
		for i := n / 2; i < n; i++ {
			_, err := txn.Get(data(i))
			require.NoError(t, err)
		}
	})
}

func ExampleOpen() {
	dir, err := ioutil.TempDir("", "badger")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	opts := DefaultOptions
	opts.Dir = dir
	opts.ValueDir = dir
	db, err := Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.View(func(txn *Txn) error {
		_, err := txn.Get([]byte("key"))
		// We expect ErrKeyNotFound
		fmt.Println(err)
		return nil
	})

	if err != nil {
		log.Fatal(err)
	}

	txn := db.NewTransaction(true) // Read-write txn
	err = txn.Set([]byte("key"), []byte("value"))
	if err != nil {
		log.Fatal(err)
	}
	err = txn.Commit()
	if err != nil {
		log.Fatal(err)
	}

	err = db.View(func(txn *Txn) error {
		item, err := txn.Get([]byte("key"))
		if err != nil {
			return err
		}
		val, err := item.Value()
		if err != nil {
			return err
		}
		fmt.Printf("%s\n", string(val))
		return nil
	})

	if err != nil {
		log.Fatal(err)
	}

	// Output:
	// Key not found
	// value
}

func ExampleTxn_NewIterator() {
	dir, err := ioutil.TempDir("", "badger")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	opts := DefaultOptions
	opts.Dir = dir
	opts.ValueDir = dir

	db, err := Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	bkey := func(i int) []byte {
		return []byte(fmt.Sprintf("%09d", i))
	}
	bval := func(i int) []byte {
		return []byte(fmt.Sprintf("%025d", i))
	}

	txn := db.NewTransaction(true)

	// Fill in 1000 items
	n := 1000
	for i := 0; i < n; i++ {
		err := txn.Set(bkey(i), bval(i))
		if err != nil {
			log.Fatal(err)
		}
	}

	err = txn.Commit()
	if err != nil {
		log.Fatal(err)
	}

	// Iterate over 1000 items
	var count int
	err = db.View(func(txn *Txn) error {
		it := txn.NewIterator(DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Counted %d elements", count)
	// Output:
	// Counted 1000 elements
}

func TestRemoteCompaction(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	remoteAddr := "127.0.0.1:4080"
	compactionServer, err := NewCompactionServer(remoteAddr)
	require.NoError(t, err)
	go compactionServer.Run()
	defer compactionServer.Close()
	opts := getTestOptions(dir)
	opts.ValueThreshold = 0
	opts.RemoteCompactionAddr = remoteAddr
	opts.TableBuilderOptions.MaxTableSize = 32 * 1024
	opts.MaxMemTableSize = 32 * 1024
	opts.NumMemtables = 2
	opts.NumLevelZeroTables = 1
	opts.NumLevelZeroTablesStall = 2
	// This case depend on level's size, so disable compression for now.
	opts.TableBuilderOptions.CompressionPerLevel = getTestCompression(options.None)
	db, err := Open(opts)
	require.NoError(t, err)
	defer db.Close()
	for i := 0; i < 128; i++ {
		err = db.Update(func(txn *Txn) error {
			key := []byte(fmt.Sprintf("key%03d", i))
			val := make([]byte, 1024)
			copy(val, key)
			return txn.Set(key, val)
		})
		require.NoError(t, err)
	}

	for i := 0; i < 512; i++ {
		err = db.Update(func(txn *Txn) error {
			key := []byte(fmt.Sprintf("key%03d", rand.Intn(128)))
			val := make([]byte, 1024*4)
			copy(val, key)
			return txn.Set(key, val)
		})
		require.NoError(t, err)
	}
	err = db.View(func(txn *Txn) error {
		it := txn.NewIterator(DefaultIteratorOptions)
		defer it.Close()
		var i int
		for it.Rewind(); it.Valid(); it.Next() {
			require.EqualValues(t, string(it.Item().Key()), fmt.Sprintf("key%03d", i))
			require.True(t, bytes.HasPrefix(it.Item().vptr, it.Item().Key()))
			i++
		}
		return nil
	})
}

func TestNonDirectIO(t *testing.T) {
	// if the dir is a tmpfs (or other file system which doesn't support directio), badger
	// should still work
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	remoteAddr := "127.0.0.1:4080"
	compactionServer, err := NewCompactionServer(remoteAddr)
	require.NoError(t, err)
	go compactionServer.Run()
	defer compactionServer.Close()
	opts := getTestOptions(dir)
	db, err := Open(opts)
	require.NoError(t, err)
	defer db.Close()

	for i := 0; i < 512; i++ {
		err = db.Update(func(txn *Txn) error {
			key := []byte(fmt.Sprintf("key%03d", rand.Intn(128)))
			val := make([]byte, 1024*4)
			copy(val, key)
			return txn.Set(key, val)
		})
		require.NoError(t, err)
	}
	// flushing memTable shouldn't hang forever even on file system which doesn't support directio
	db.flushMemTable().Wait()
}
