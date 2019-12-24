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
}

type SuRFOptions struct {
	HashSuffixLen  int
	RealSuffixLen  int
	BitsPerKeyHint int
}

type ValueLogWriterOptions struct {
	WriteBufferSize int
}
