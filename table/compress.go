package table

import (
	"github.com/klauspost/compress/zstd"
)

func compress(in []byte) ([]byte, error) {
	w, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, err
	}
	return w.EncodeAll(in, make([]byte, 0, len(in))), nil
}

func decompress(in []byte) ([]byte, error) {
	r, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return r.DecodeAll(in, nil)
}
