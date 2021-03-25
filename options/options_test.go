package options

import (
	"bytes"
	"testing"
)

func TestCompressionType(t *testing.T) {
	types := []CompressionType{None, Snappy, ZSTD}
	data := make([]byte, 64)
	for i := 0; i < len(data); i++ {
		data[i] = byte(49)
	}
	for i := 0; i < len(types); i++ {
		typ := types[i]
		w := bytes.NewBuffer(nil)
		err := typ.Compress(w, data)
		if err != nil {
			t.Errorf("Compress type:%d,error:%s", typ, err.Error())
		}
		v, err := typ.Decompress(w.Bytes())
		if err != nil {
			t.Errorf("Decompress type:%d,error:%s", typ, err.Error())
		}
		if string(data) != string(v) {
			t.Errorf("Decompress src%s,decode%s", string(data), string(v))
		}
	}
}
