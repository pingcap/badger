package badger

import "github.com/pingcap/badger/y"

func (sdb *ShardingDB) runCompactionLoop(c *y.Closer) {
	defer c.Done()
}
