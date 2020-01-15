package l2

import (
	"os"
	"path/filepath"
	"strconv"

	"github.com/bobotu/ristretto"
	"github.com/coocood/badger/epoch"
	"github.com/coocood/badger/y"
	"github.com/ngaut/log"
)

type Options struct {
	Size        int64
	AvgSize     int64
	CachePath   string
	StoragePath string
}

const savingSuffix = ".saving"

type Storage struct {
	cache       *ristretto.Cache
	resourceMgr *epoch.ResourceManager
	storage     storage
	opts        *Options
}

func NewCache(opts *Options, resourceMgr *epoch.ResourceManager) (*Storage, error) {
	s, err := newDiskStorage(opts.StoragePath)
	if err != nil {
		return nil, err
	}

	c := &Storage{
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

	if err := c.recoverCache(); err != nil {
		return nil, err
	}

	return c, nil
}

func (c *Storage) recoverCache() error {
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
		id, err := filenameToID(info.Name())
		if err != nil {
			panic(err)
		}
		c.cache.Set(id, f, info.Size())

		_, err = os.Stat(path + savingSuffix)
		if err == nil || !os.IsNotExist(err) {
			return err
		}
		return c.saveFileToStorage(info.Name())
	})
}

func (c *Storage) saveFileToStorage(name string) error {
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

func (c *Storage) Add(id uint64, path string) (*os.File, error) {
	filename := idToFilename(id)
	cacheFile := filepath.Join(c.opts.CachePath, filename)
	if err := os.Link(path, cacheFile); err != nil {
		return nil, err
	}

	if err := c.saveFileToStorage(filename); err != nil {
		return nil, err
	}

	f, err := y.OpenExistingFile(cacheFile, 0)
	if err != nil {
		return nil, err
	}
	c.cache.Set(id, f, filesize(f))

	return f, nil
}

func (c *Storage) Get(id uint64) (*os.File, error) {
	fd, err := c.cache.GetOrCompute(id, func() (interface{}, int64, error) {
		f, err1 := c.get(idToFilename(id))
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

func (c *Storage) Del(id uint64) error {
	job := &cleanupJob{
		filename:  idToFilename(id),
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

func (c *Storage) onEvict(id uint64, v interface{}) {
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
	cache     *Storage
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

func (c *Storage) get(filename string) (*os.File, error) {
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

func idToFilename(id uint64) string {
	return strconv.FormatUint(id, 16)
}

func filenameToID(filename string) (uint64, error) {
	return strconv.ParseUint(filename, 16, 64)
}
