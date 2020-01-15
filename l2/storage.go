package l2

import (
	"io"
	"os"
	"path/filepath"

	"github.com/coocood/badger/y"
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
	return s.copy(filepath.Join(s.path, name), filepath.Join(path, name))
}

func (s diskStorage) Get(name, path string) error {
	return s.copy(filepath.Join(path, name), filepath.Join(s.path, name))
}

func (s diskStorage) Del(name string) error {
	return os.Remove(filepath.Join(s.path, name))
}

func (s diskStorage) copy(dst, src string) error {
	r, err := y.OpenExistingFile(src, 0)
	if err != nil {
		return err
	}
	defer r.Close()
	w, err := y.OpenTruncFile(dst, false)
	if err != nil {
		return err
	}
	defer w.Close()
	_, err = io.Copy(w, r)
	return err
}
