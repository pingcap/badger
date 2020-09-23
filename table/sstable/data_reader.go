package sstable

import (
	"io/ioutil"
	"os"

	"github.com/pingcap/badger/cache"
	"github.com/pingcap/badger/s3util"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
)

type DataReader interface {
	ReadBlock(offset, length int64) ([]byte, error)
	ReadIndex() (*TableIndex, error)
	Close()
}

// InMemReader keep data in memory.
type InMemReader struct {
	blocksData []byte
	index      *TableIndex
}

func NewInMemReader(blockData, indexData []byte) DataReader {
	index := NewTableIndex(indexData)
	return &InMemReader{
		blocksData: blockData,
		index:      index,
	}
}

func (r *InMemReader) ReadBlock(offset, length int64) ([]byte, error) {
	return r.blocksData[offset : offset+length], nil
}

func (r *InMemReader) ReadIndex() (*TableIndex, error) {
	return r.index, nil
}

func (r *InMemReader) Close() {
}

func NewInMemReaderFromFile(filename string) (DataReader, error) {
	blockData, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	indexData, err := ioutil.ReadFile(IndexFilename(filename))
	if err != nil {
		return nil, err
	}
	return NewInMemReader(blockData, indexData), nil
}

type MMapReader struct {
	*InMemReader

	fd    *os.File
	idxFd *os.File
}

func NewMMapReader(filename string) (DataReader, error) {
	fd, tblSize, err := getFdWithSize(filename)
	if err != nil {
		return nil, err
	}
	idxFd, idxSize, err := getFdWithSize(IndexFilename(filename))
	if err != nil {
		fd.Close()
		return nil, err
	}
	mmReader := &MMapReader{
		InMemReader: &InMemReader{},
		fd:          fd,
		idxFd:       fd,
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
		return nil, 0, err
	}
	fdi, err := fd.Stat()
	if err != nil {
		return nil, 0, err
	}
	return fd, fdi.Size(), nil
}

func (r *MMapReader) Close() {
	y.Munmap(r.index.IndexData)
	y.Munmap(r.blocksData)
	r.idxFd.Close()
	r.fd.Close()
}

type LocalFileBlockReader struct {
	blockCache *cache.Cache
	indexCache *cache.Cache
	fid        uint32
	fd         *os.File
	tblSize    uint32
	idxFd      *os.File
	idxSize    uint32
}

func NewLocalFileBlockReader(filename string, blockCache, indexCache *cache.Cache) (DataReader, error) {
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
	reader := &LocalFileBlockReader{
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

func (r *LocalFileBlockReader) ReadBlock(offset, length int64) ([]byte, error) {
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

func (r *LocalFileBlockReader) ReadIndex() (*TableIndex, error) {
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

func (r *LocalFileBlockReader) Close() {
	r.idxFd.Close()
	r.fd.Close()
}

func blockCacheKey(fid, offset uint32) uint64 {
	return uint64(fid)<<32 | uint64(offset)
}

type S3BlockReader struct {
	blockCache *cache.Cache
	idxCache   *cache.Cache
	instanceID uint32
	fid        uint32
	s3c        *s3util.S3Client
}

func NewS3BlockReader(instanceID, fid uint32, blockCache, idxCache *cache.Cache, s3c *s3util.S3Client) (DataReader, error) {
	reader := &S3BlockReader{
		blockCache: blockCache,
		idxCache:   idxCache,
		instanceID: instanceID,
		fid:        fid,
		s3c:        s3c,
	}
	return reader, nil
}

func (r *S3BlockReader) ReadBlock(offset, length int64) ([]byte, error) {
	blockData, err := r.blockCache.GetOrCompute(blockCacheKey(r.fid, uint32(offset)), func() (interface{}, int64, error) {
		data, err := r.s3c.Get(s3util.BlockKey(r.instanceID, r.fid), offset, length)
		return data, length, err
	})
	if err != nil {
		return nil, err
	}
	return blockData.([]byte), nil
}

func (r *S3BlockReader) ReadIndex() (*TableIndex, error) {
	index, err := r.idxCache.GetOrCompute(uint64(r.fid), func() (interface{}, int64, error) {
		data, err := r.s3c.Get(s3util.IndexKey(r.instanceID, r.fid), 0, 0)
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

func (r *S3BlockReader) Close() {
	// Is del all blocks from the cache worth?
	r.idxCache.Del(uint64(r.fid))
}
