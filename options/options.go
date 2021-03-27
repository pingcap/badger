/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package options

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pingcap/badger/buffer"
)

// CompressionType specifies how a block should be compressed.
type CompressionType uint32

const (
	// None mode indicates that a block is not compressed.
	None CompressionType = 0
	// Snappy mode indicates that a block is compressed using Snappy algorithm.
	Snappy CompressionType = 1
	// ZSTD mode indicates that a block is compressed using ZSTD algorithm.
	ZSTD CompressionType = 2
)

func (c CompressionType) Compress(w io.Writer, data []byte) error {
	switch c {
	case None:
		_, err := w.Write(data)
		return err
	case Snappy:
		dst := buffer.GetBuffer(snappy.MaxEncodedLen(len(data)))
		res := snappy.Encode(dst, data)
		_, err := w.Write(res)
		buffer.PutBuffer(dst)
		return err
	case ZSTD:
		e, err := zstd.NewWriter(w)
		if err != nil {
			return err
		}
		_, err = e.Write(data)
		if err != nil {
			return err
		}
		return e.Close()
	}
	return errors.New("Unsupported compression type")
}

func (c CompressionType) Decompress(data []byte) ([]byte, error) {
	switch c {
	case None:
		return data, nil
	case Snappy:
		defer buffer.PutBuffer(data)
		length, err := snappy.DecodedLen(data)
		if err != nil {
			return nil, err
		}
		dst := buffer.GetBuffer(length)
		res, err := snappy.Decode(dst, data)
		if &res[0] != &dst[0] {
			buffer.PutBuffer(dst)
		}
		return res, err
	case ZSTD:
		defer buffer.PutBuffer(data)
		reader := bytes.NewBuffer(data)
		r, err := zstd.NewReader(reader)
		if err != nil {
			return nil, err
		}
		defer r.Close()
		return ioutil.ReadAll(r)
	}
	return nil, errors.New("Unsupported compression type")
}

func compress(in []byte) ([]byte, error) {
	w, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, err
	}
	return w.EncodeAll(in, make([]byte, 0, len(in))), nil
}

type TableBuilderOptions struct {
	HashUtilRatio       float32
	WriteBufferSize     int
	BytesPerSecond      int
	MaxLevels           int
	LevelSizeMultiplier int
	LogicalBloomFPR     float64
	BlockSize           int
	CompressionPerLevel []CompressionType
	SuRFStartLevel      int
	SuRFOptions         SuRFOptions
	MaxTableSize        int64
}

type SuRFOptions struct {
	HashSuffixLen  int
	RealSuffixLen  int
	BitsPerKeyHint int
}

type ValueLogWriterOptions struct {
	WriteBufferSize int
}
