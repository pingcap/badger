package l2

import (
	"os"
	"path/filepath"

	"github.com/coocood/badger/epoch"
	"github.com/coocood/badger/y"
	"github.com/dgraph-io/ristretto"
	"github.com/ngaut/log"
)

type Options struct {
	Size        int64
	AvgSize     int64
	CachePath   string
	StoragePath string
}

const savingSuffix = ".saving"

type Cache struct {
	cache       *ristretto.Cache
	resourceMgr *epoch.ResourceManager
	storage     storage
	opts        *Options
}

func NewCache(opts *Options, resourceMgr *epoch.ResourceManager, filenameToID func(string) uint64) (*Cache, error) {
	s, err := newDiskStorage(opts.StoragePath)
	if err != nil {
		return nil, err
	}

	c := &Cache{
		resourceMgr: resourceMgr,
		storage:     s,
		opts:        opts,
	}

	estimateCounts := opts.Size / opts.AvgSize
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: estimateCounts * 10,
		MaxCost:     opts.Size,
		BufferItems: 64,
		Metrics:     false,
		OnEvict:     c.onEvict,
	})
	if err != nil {
		return nil, err
	}
	c.cache = cache

	if err := c.recoverCache(filenameToID); err != nil {
		return nil, err
	}

	return c, nil
}

func (c *Cache) recoverCache(filenameToID func(string) uint64) error {
	return filepath.Walk(c.opts.CachePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || filepath.Ext(path) == savingSuffix {
			return nil
		}

		f, err := y.OpenExistingFile(path, 0)
		if err != nil {
			return err
		}
		c.cache.Set(filenameToID(info.Name()), f, info.Size())

		_, err = os.Stat(path + savingSuffix)
		if err == nil || !os.IsNotExist(err) {
			return err
		}
		return c.saveFileToStorage(info.Name())
	})
}

func (c *Cache) saveFileToStorage(name string) error {
	path := filepath.Join(c.opts.CachePath, name)
	tag := path + savingSuffix
	tagF, err := os.Create(tag)
	if err != nil && !os.IsExist(err) {
		return err
	}
	tagF.Close()

	go func() {
		// TODO: add retry and more
		if err := c.storage.Put(c.opts.CachePath, name); err != nil {
			log.Error(err)
		}
		os.Remove(tag)
	}()

	return nil
}

func (c *Cache) Add(id uint64, path string) (*os.File, error) {
	filename := filepath.Base(path)
	cacheFile := filepath.Join(c.opts.CachePath, filename)
	if err := os.Link(path, cacheFile); err != nil {
		return nil, err
	}

	if err := c.storage.Put(c.opts.CachePath, filename); err != nil {
		return nil, err
	}

	f, err := y.OpenExistingFile(cacheFile, 0)
	if err != nil {
		return nil, err
	}
	c.cache.Set(id, f, filesize(f))

	return f, nil
}

func (c *Cache) Get(id uint64, filename string) (*os.File, error) {
	fd, err := c.cache.GetOrCompute(id, func() (interface{}, int64, error) {
		f, err1 := c.get(filename)
		if err1 != nil {
			return nil, 0, err1
		}
		return f, filesize(f), nil
	})
	if err != nil {
		return nil, err
	}

	return fd.(*os.File), nil
}

func (c *Cache) Del(id uint64, filename string) error {
	job := &cleanupJob{
		filename:  filename,
		cache:     c,
		onlyCache: false,
	}

	old := c.cache.Del(id)
	if old != nil {
		job.f = old.(*os.File)
	}
	g := c.resourceMgr.Acquire()
	g.Delete([]epoch.Resource{job})
	g.Done()
	return nil
}

func (c *Cache) onEvict(id uint64, v interface{}) {
	fd := v.(*os.File)
	job := &cleanupJob{
		f:         fd,
		filename:  filepath.Base(fd.Name()),
		cache:     c,
		onlyCache: true,
	}
	g := c.resourceMgr.Acquire()
	g.Delete([]epoch.Resource{job})
	g.Done()
}

type cleanupJob struct {
	f         *os.File
	filename  string
	cache     *Cache
	onlyCache bool
}

func (job *cleanupJob) Delete() error {
	if job.f != nil {
		cachePath := job.f.Name()
		job.f.Close()
		os.Remove(cachePath)
	}
	if !job.onlyCache {
		job.cache.storage.Del(job.filename)
	}
	return nil
}

func (c *Cache) get(filename string) (*os.File, error) {
	cacheFile := filepath.Join(c.opts.CachePath, filename)
	f, err := y.OpenExistingFile(cacheFile, 0)
	if err == nil {
		return f, nil
	}
	if !os.IsNotExist(err) {
		return nil, err
	}
	if err := c.storage.Get(filename, c.opts.CachePath); err != nil {
		return nil, err
	}
	return y.OpenExistingFile(cacheFile, 0)
}

func filesize(f *os.File) int64 {
	stat, err := f.Stat()
	if err != nil {
		// TODO: error handling
		panic(err)
	}
	return stat.Size()
}
