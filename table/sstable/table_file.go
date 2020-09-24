package sstable

import (
	"io/ioutil"
	"os"
	"sync/atomic"
	"unsafe"

	"github.com/pingcap/badger/cache"
	"github.com/pingcap/badger/s3util"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
)

type TableFile interface {
	ReadBlock(offset, length int64) ([]byte, error)
	ReadIndex() (*TableIndex, error)
	LoadToMem() error
	Close()
	Delete()
}

// InMemFile keep data in memory.
type InMemFile struct {
	blocksData []byte
	index      *TableIndex
}

func NewInMemFile(blockData, indexData []byte) *InMemFile {
	index := NewTableIndex(indexData)
	return &InMemFile{
		blocksData: blockData,
		index:      index,
	}
}

func (r *InMemFile) ReadBlock(offset, length int64) ([]byte, error) {
	return r.blocksData[offset : offset+length], nil
}

func (r *InMemFile) ReadIndex() (*TableIndex, error) {
	return r.index, nil
}

func (r *InMemFile) Close() {
}

func (r *InMemFile) Delete() {
}

func (r *InMemFile) LoadToMem() error {
	return nil
}

func NewInMemFileFromFile(filename string) (*InMemFile, error) {
	blockData, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	indexData, err := ioutil.ReadFile(IndexFilename(filename))
	if err != nil {
		return nil, err
	}
	return NewInMemFile(blockData, indexData), nil
}

type MMapFile struct {
	*InMemFile

	fd    *os.File
	idxFd *os.File
}

func NewMMapFile(filename string) (TableFile, error) {
	fd, tblSize, err := getFdWithSize(filename)
	if err != nil {
		return nil, err
	}
	idxFd, idxSize, err := getFdWithSize(IndexFilename(filename))
	if err != nil {
		fd.Close()
		return nil, err
	}
	mmReader := &MMapFile{
		InMemFile: &InMemFile{},
		fd:        fd,
		idxFd:     fd,
	}
	mmReader.blocksData, err = y.Mmap(fd, false, tblSize)
	if err != nil {
		fd.Close()
		idxFd.Close()
		return nil, y.Wrapf(err, "Unable to map file")
	}
	var idxData []byte
	idxData, err = y.Mmap(idxFd, false, idxSize)
	if err != nil {
		fd.Close()
		idxFd.Close()
		return nil, y.Wrapf(err, "Unable to map index")
	}
	mmReader.index = NewTableIndex(idxData)
	return mmReader, nil
}

func getFdWithSize(filename string) (fd *os.File, size int64, err error) {
	fd, err = y.OpenExistingFile(filename, 0)
	if err != nil {
		return nil, 0, errors.WithStack(err)
	}
	fdi, err := fd.Stat()
	if err != nil {
		return nil, 0, errors.WithStack(err)
	}
	return fd, fdi.Size(), nil
}

func (r *MMapFile) Close() {
	y.Munmap(r.index.IndexData)
	y.Munmap(r.blocksData)
	r.idxFd.Close()
	r.fd.Close()
}

func (r *MMapFile) LoadToMem() error {
	return nil
}

func (r *MMapFile) Delete() {
	filename := r.fd.Name()
	r.Close()
	os.Remove(filename)
	os.Remove(IndexFilename(filename))
}

type LocalFile struct {
	blockCache *cache.Cache
	indexCache *cache.Cache
	fid        uint32
	fd         *os.File
	tblSize    uint32
	idxFd      *os.File
	idxSize    uint32
	memReader  unsafe.Pointer
}

func NewLocalFile(filename string, blockCache, indexCache *cache.Cache) (TableFile, error) {
	fid, ok := ParseFileID(filename)
	if !ok {
		return nil, errors.Errorf("Invalid filename: %s", filename)
	}
	fd, tblSize, err := getFdWithSize(filename)
	if err != nil {
		return nil, err
	}
	idxFd, idxSize, err := getFdWithSize(IndexFilename(filename))
	if err != nil {
		fd.Close()
		return nil, err
	}
	reader := &LocalFile{
		blockCache: blockCache,
		indexCache: indexCache,
		fid:        uint32(fid),
		fd:         fd,
		tblSize:    uint32(tblSize),
		idxFd:      idxFd,
		idxSize:    uint32(idxSize),
	}
	return reader, nil
}

func (r *LocalFile) ReadBlock(offset, length int64) ([]byte, error) {
	if ptr := atomic.LoadPointer(&r.memReader); ptr != nil {
		return (*InMemFile)(ptr).ReadBlock(offset, length)
	}
	blockData, err := r.blockCache.GetOrCompute(blockCacheKey(r.fid, uint32(offset)), func() (interface{}, int64, error) {
		data := make([]byte, length)
		_, err := r.fd.ReadAt(data, offset)
		return data, length, err
	})
	if err != nil {
		return nil, err
	}
	return blockData.([]byte), nil
}

func (r *LocalFile) ReadIndex() (*TableIndex, error) {
	if ptr := atomic.LoadPointer(&r.memReader); ptr != nil {
		return (*InMemFile)(ptr).ReadIndex()
	}
	index, err := r.indexCache.GetOrCompute(uint64(r.fid), func() (interface{}, int64, error) {
		data := make([]byte, r.idxSize)
		_, err := r.idxFd.ReadAt(data, 0)
		if err != nil {
			return nil, 0, err
		}
		return NewTableIndex(data), int64(r.idxSize), err
	})
	if err != nil {
		return nil, err
	}
	return index.(*TableIndex), nil
}

func (r *LocalFile) Close() {
	r.idxFd.Close()
	r.fd.Close()
}

func (r *LocalFile) Delete() {
	filename := r.fd.Name()
	r.Close()
	os.Remove(filename)
	os.Remove(IndexFilename(filename))
}

func (r *LocalFile) LoadToMem() error {
	memReader, err := NewInMemFileFromFile(r.fd.Name())
	if err != nil {
		return err
	}
	atomic.StorePointer(&r.memReader, unsafe.Pointer(memReader))
	return nil
}

func blockCacheKey(fid, offset uint32) uint64 {
	return uint64(fid)<<32 | uint64(offset)
}

type S3File struct {
	blockCache *cache.Cache
	idxCache   *cache.Cache
	fid        uint32
	filename   string
	s3c        *s3util.S3Client
	memReader  unsafe.Pointer
}

func NewS3File(filename string, blockCache, idxCache *cache.Cache, s3c *s3util.S3Client) (TableFile, error) {
	fid, _ := ParseFileID(filename)
	reader := &S3File{
		blockCache: blockCache,
		idxCache:   idxCache,
		filename:   filename,
		fid:        uint32(fid),
		s3c:        s3c,
	}
	return reader, nil
}

func (r *S3File) ReadBlock(offset, length int64) ([]byte, error) {
	if ptr := atomic.LoadPointer(&r.memReader); ptr != nil {
		return (*InMemFile)(ptr).ReadBlock(offset, length)
	}
	blockData, err := r.blockCache.GetOrCompute(blockCacheKey(r.fid, uint32(offset)), func() (interface{}, int64, error) {
		data, err := r.s3c.Get(r.s3c.BlockKey(r.fid), offset, length)
		return data, length, err
	})
	if err != nil {
		return nil, err
	}
	return blockData.([]byte), nil
}

func (r *S3File) ReadIndex() (*TableIndex, error) {
	if ptr := atomic.LoadPointer(&r.memReader); ptr != nil {
		return (*InMemFile)(ptr).ReadIndex()
	}
	index, err := r.idxCache.GetOrCompute(uint64(r.fid), func() (interface{}, int64, error) {
		data, err := r.s3c.Get(r.s3c.IndexKey(r.fid), 0, 0)
		if err != nil {
			return nil, 0, err
		}
		return NewTableIndex(data), int64(len(data)), err
	})
	if err != nil {
		return nil, err
	}
	return index.(*TableIndex), nil
}

func (r *S3File) Close() {
	// Is del all blocks from the cache worth?
	r.idxCache.Del(uint64(r.fid))
}

func (r *S3File) LoadToMem() error {
	blockData, err := r.s3c.Get(r.s3c.BlockKey(r.fid), 0, 0)
	if err != nil {
		return err
	}
	idx, err := r.ReadIndex()
	if err != nil {
		return err
	}
	memReader := NewInMemFile(blockData, idx.IndexData)
	atomic.StorePointer(&r.memReader, unsafe.Pointer(memReader))
	return nil
}

func (r *S3File) Delete() {
	r.s3c.Delete(r.s3c.BlockKey(r.fid))
	r.s3c.Delete(r.s3c.IndexKey(r.fid))
	os.Remove(r.filename)
	os.Remove(IndexFilename(r.filename))
}
