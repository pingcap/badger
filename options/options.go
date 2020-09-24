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

type TableBuilderOptions struct {
	HashUtilRatio       float32
	WriteBufferSize     int
	BytesPerSecond      int
	MaxLevels           int
	LevelSizeMultiplier int
	LogicalBloomFPR     float64
	BlockSize           int
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
