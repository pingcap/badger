package l2

import (
	"os"
	"path/filepath"
)

type storage interface {
	Put(path, name string) error
	Get(name, path string) error
	Del(name string) error
}

type diskStorage struct {
	path string
}

func newDiskStorage(path string) (storage, error) {
	if err := os.MkdirAll(path, os.ModePerm); err != nil {
		return diskStorage{}, err
	}
	return diskStorage{
		path: path,
	}, nil
}

func (s diskStorage) Put(path, name string) error {
	src := filepath.Join(path, name)
	dst := filepath.Join(s.path, name)
	return os.Link(src, dst)
}

func (s diskStorage) Get(name, path string) error {
	src := filepath.Join(s.path, name)
	dst := filepath.Join(path, name)
	return os.Link(src, dst)
}

func (s diskStorage) Del(name string) error {
	return os.Remove(filepath.Join(s.path, name))
}
