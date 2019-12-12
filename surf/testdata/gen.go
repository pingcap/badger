package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"time"

	"github.com/brianvoe/gofakeit"
)

func main() {
	gofakeit.Seed(time.Now().Unix())

	fakeSet := []fakeData{
		{"street", func() string { return gofakeit.Address().Address }, 10000000},
		{"url", gofakeit.URL, 10000000},
		{"email", gofakeit.Email, 10000000},
		{"uuid", gofakeit.UUID, 10000000},
		{"ipv4", gofakeit.IPv4Address, 10000000},
		{"ipv6", gofakeit.IPv6Address, 20000000},
		{"username", gofakeit.Username, 4000000},
	}
	for _, w := range fakeSet {
		w.generate()
	}

	randSet := []randomData{
		{10000000, 10, 0},  // dense dataset
		{10000000, 100, 0}, // sparse dataset
		{100000, 100, 3},   // sparse prefix dataset
		{200, 5, 300},      // dense prefix dataset
	}
	for _, w := range randSet {
		w.generate()
	}
}

type fakeData struct {
	name string
	f    func() string
	n    int
}

func (w *fakeData) generate() {
	fmt.Printf("generating %d %s...", w.n, w.name)
	dedup := make(map[string]struct{}, w.n)
	progress := 10
	for len(dedup) < w.n {
		dedup[w.f()] = struct{}{}

		p := int(float64(len(dedup)) / float64(w.n) * 100.0)
		if p%10 == 0 && p >= progress {
			fmt.Printf("%d...", progress)
			progress += 10
		}
	}

	fmt.Print("sorting...")
	keys := make([][]byte, 0, w.n)
	for k := range dedup {
		keys = append(keys, []byte(k))
	}
	dedup = nil
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })

	fmt.Print("writing...")
	output(fmt.Sprintf("%s_%d.gz", w.name, w.n), keys)
	fmt.Println("done")
}

func output(filename string, data [][]byte) {
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_RDWR, os.ModePerm)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	buf := bufio.NewWriter(f)
	defer buf.Flush()
	compressed := gzip.NewWriter(buf)
	defer compressed.Close()

	for _, b := range data {
		if len(b) > 65535 {
			panic("key length overflow")
		}
		var lenBuf [2]byte
		binary.LittleEndian.PutUint16(lenBuf[:], uint16(len(b)))

		_, err := compressed.Write(lenBuf[:])
		if err != nil {
			panic(err)
		}
		_, err = compressed.Write(b)
		if err != nil {
			panic(err)
		}
	}
}

type randomData struct {
	initSize, initLen, round int
}

func (w *randomData) generate() {
	fmt.Printf("generating %d-%d-%d rand data...", w.initSize, w.initLen, w.round)
	start := time.Now()
	keys := make([][]byte, w.initSize)
	rand := rand.New(rand.NewSource(start.Unix()))
	fmt.Printf("init round...")
	for i := range keys {
		keys[i] = make([]byte, rand.Intn(w.initLen)+1)
		rand.Read(keys[i])
	}

	for r := 1; r <= w.round; r++ {
		for i := 0; i < w.initSize*r; i++ {
			k := make([]byte, len(keys[i])+rand.Intn(w.initLen)+1)
			copy(k, keys[i])
			rand.Read(k[len(keys[i]):])
			keys = append(keys, k)
		}
		fmt.Printf("round %d...", r)
	}

	fmt.Print("sorting...")
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})

	fmt.Print("dedup...")
	var prev []byte
	result := keys[:0]
	for _, k := range keys {
		if bytes.Equal(prev, k) {
			continue
		}
		prev = k
		result = append(result, k)
	}
	for i := len(result); i < len(keys); i++ {
		keys[i] = nil
	}

	fmt.Print("writing...")
	output(fmt.Sprintf("rand-%d-%d-%d_%d.gz", w.initSize, w.initLen, w.round, len(result)), result)
	fmt.Printf("done (size: %d)\n", len(result))
}
