package badger

import (
	"io/ioutil"
	"net/http"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	_ "net/http/pprof"
)

func TestShardingDB(t *testing.T) {
	go func() {
		http.ListenAndServe(":9291", nil)
	}()
	dir, err := ioutil.TempDir("", "sharding")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	opts := getTestOptions(dir)
	db, err := OpenShardingDB(opts)
	require.NoError(t, err)
	defer db.Close()
}
