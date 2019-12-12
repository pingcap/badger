package surf

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func BenchmarkGet(b *testing.B) {
	forEachDataset(func(name string, data [][]byte) {
		b.Run(name, func(b *testing.B) {
			b.StopTimer()
			insert, vals, others := splitKeys(data)
			b.StartTimer()
			buildAndBenchSuRF(b, insert, vals, func(b *testing.B, surf *SuRF) {
				b.Run("exist", func(b *testing.B) {
					for n := 0; n < b.N; n++ {
						i := n % len(insert)
						surf.Get(insert[i])
					}
				})

				var total, fp int
				b.Run("nonexist", func(b *testing.B) {
					var localFp int
					for n := 0; n < b.N; n++ {
						i := n % len(others)
						if _, ok := surf.Get(others[i]); ok {
							localFp++
						}
					}
					fp += localFp
					total += b.N
				})

				b.Logf("\nSuRF size is %d bytes\nnumber of keys %d\nfalse positive rate is %.2f", surf.MarshalSize(), len(insert), float64(fp)/float64(total)*100)
			})
		})
	})
}

func BenchmarkSeek(b *testing.B) {
	forEachDataset(func(name string, data [][]byte) {
		b.Run(name, func(b *testing.B) {
			b.StopTimer()
			insert, vals, others := splitKeys(data)
			b.StartTimer()
			buildAndBenchSuRF(b, insert, vals, func(b *testing.B, surf *SuRF) {
				b.Run("exist", func(b *testing.B) {
					it := surf.NewIterator()
					for n := 0; n < b.N; n++ {
						i := n % len(insert)
						it.Seek(insert[i])
					}
				})

				var total, fp int
				b.Run("nonexist", func(b *testing.B) {
					var localFp int
					it := surf.NewIterator()
					for n := 0; n < b.N; n++ {
						i := n % len(others)
						it.Seek(others[i])
						if endian.Uint32(it.Value()) < endian.Uint32(vals[i]) {
							localFp++
						}
					}
					fp += localFp
					total += b.N
				})

				b.Logf("\nSuRF size is %d bytes\nnumber of keys %d\nfalse positive rate is %.2f", surf.MarshalSize(), len(insert), float64(fp)/float64(total)*100)
			})
		})
	})
}

func buildAndBenchSuRF(b *testing.B, keys, vals [][]byte, run func(t *testing.B, surf *SuRF)) {
	suffixLens := [][]uint32{
		{0, 0},
		{16, 0},
		{0, 16},
		{8, 8},
	}

	for _, sl := range suffixLens {
		builder := NewBuilder(4, sl[0], sl[1])

		builder.totalCount = len(keys)
		builder.buildNodes(keys, vals, 0, 0, 0)
		for i := 0; i < builder.treeHeight(); i++ {
			builder.sparseStartLevel = uint32(i)
			builder.ldLabels = builder.ldLabels[:0]
			builder.ldHasChild = builder.ldHasChild[:0]
			builder.ldIsPrefix = builder.ldIsPrefix[:0]
			builder.buildDense()

			surf := new(SuRF)
			surf.ld.Init(builder)
			surf.ls.Init(builder)

			b.ResetTimer()
			b.Run(fmt.Sprintf("cutoff=%d,hashLen=%d,realLen=%d", i, sl[0], sl[1]), func(b *testing.B) {
				run(b, surf)
			})
		}
	}
}

func forEachDataset(fn func(string, [][]byte)) {
	err := filepath.Walk("testdata", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() || filepath.Ext(info.Name()) != ".gz" {
			return nil
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		fn(loadData(f))
		return nil
	})
	if err != nil {
		panic(err)
	}
}

func loadData(f *os.File) (string, [][]byte) {
	decompressor, err := gzip.NewReader(f)
	if err != nil {
		panic(err)
	}
	sc := bufio.NewScanner(decompressor)
	sc.Split(dataSplitFunc)

	nameSize := strings.Split(strings.TrimSuffix(f.Name(), filepath.Ext(f.Name())), "_")
	size, _ := strconv.Atoi(nameSize[1])
	keys := make([][]byte, 0, size)
	for sc.Scan() {
		keys = append(keys, append([]byte{}, sc.Bytes()...))
	}

	return nameSize[0], keys
}

func dataSplitFunc(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if len(data) < 2 {
		return 0, nil, nil
	}
	l := int(binary.LittleEndian.Uint16(data[:2]))
	if len(data[2:]) < l {
		return 0, nil, nil
	}

	return 2 + l, data[2 : 2+l], nil
}
