package badger

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBlob(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	opts := getTestOptions(dir)
	opts.ValueThreshold = 20
	opts.TableBuilderOptions.MaxTableSize = 4 * 1024
	opts.MaxMemTableSize = 4 * 1024
	opts.NumMemtables = 2
	opts.NumLevelZeroTables = 1
	opts.NumLevelZeroTablesStall = 2
	db, err := Open(opts)
	require.NoError(t, err)
	val := make([]byte, 128)
	// Insert 50 entries that will be kept.
	for i := 0; i < 100; i++ {
		err = db.Update(func(txn *Txn) error {
			key := []byte(fmt.Sprintf("key%d", i))
			return txn.Set(key, val)
		})
		require.NoError(t, err)
	}
	err = db.View(func(txn *Txn) error {
		for i := 0; i < 100; i++ {
			key := []byte(fmt.Sprintf("key%d", i))
			item, err := txn.Get(key)
			require.Nil(t, err)
			v, err := item.Value()
			require.Nil(t, err)
			require.Len(t, val, len(v))
			require.Equal(t, val, v, i)
		}
		return nil
	})
	require.Nil(t, err)
	db.Close()
	db, err = Open(opts)
	err = db.View(func(txn *Txn) error {
		for i := 0; i < 100; i++ {
			key := []byte(fmt.Sprintf("key%d", i))
			item, err := txn.Get(key)
			require.Nil(t, err)
			v, err := item.Value()
			require.Equal(t, v, val)
		}
		return nil
	})
	require.Nil(t, err)
	db.Close()
}

func TestBlobGC(t *testing.T) {
	minCandidateValidSize = 4 * 1024
	maxCandidateValidSize = minCandidateValidSize * 4
	maxCandidateDiscardSize = 1024 * 1024
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	opts := getTestOptions(dir)
	opts.ValueThreshold = 20
	opts.TableBuilderOptions.MaxTableSize = 6 * 1024
	opts.MaxMemTableSize = 6 * 1024
	opts.NumMemtables = 2
	opts.NumLevelZeroTables = 1
	opts.NumLevelZeroTablesStall = 2

	var db *DB
	expectedMap := make(map[string]string)
	for c := 0; c < 10; c++ {
		db, err = Open(opts)
		require.NoError(t, err)
		for i := 0; i < 100; i++ {
			err = db.Update(func(txn *Txn) error {
				key := []byte(fmt.Sprintf("key%d", rand.Intn(100)))
				val := make([]byte, 128)
				_, _ = rand.Read(val)
				expectedMap[string(key)] = fmt.Sprintf("%x", val)
				return txn.Set(key, val)
			})
			require.Nil(t, err)
		}
		validateValue(t, db, expectedMap)
		require.Nil(t, db.Close())
	}
}

func validateValue(t *testing.T, db *DB, expectedMap map[string]string) {
	err := db.View(func(txn *Txn) error {
		for expectedKey, expectedVal := range expectedMap {
			item, err := txn.Get([]byte(expectedKey))
			require.Nil(t, err)
			val, err := item.Value()
			require.Equal(t, expectedVal, fmt.Sprintf("%x", val), "%s", expectedKey)
		}
		return nil
	})
	require.Nil(t, err)
}
