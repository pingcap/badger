package y

import (
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestFastWaterMark(t *testing.T) {
	fwm := NewFastWaterMark()
	node := fwm.Begin(100)
	fwm.Done(node)
	require.True(t, fwm.MinReadTS() == 100)

	t1 := fwm.Begin(103)
	t2 := fwm.Begin(102)
	require.True(t, t2.ReadTS == 103)
	t3 := fwm.Begin(105)
	fwm.Done(t2)
	require.True(t, fwm.MinReadTS() == 100)
	require.True(t, t2.next == unsafe.Pointer(t1))
	fwm.Done(t1)
	require.True(t, fwm.MinReadTS() == 103)
	fwm.Done(t3)
	require.True(t, fwm.MinReadTS() == 105)
	require.True(t, t3.next == nil)
}

var counter uint64

func BenchmarkFastWaterMark(b *testing.B) {
	b.ReportAllocs()
	fwm := NewFastWaterMark()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			node := fwm.Begin(atomic.AddUint64(&counter, 1))
			fwm.Done(node)
		}
	})
}

func BenchmarkWaterMark(b *testing.B) {
	b.ReportAllocs()
	wm := new(WaterMark)
	wm.Init()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			readTS := atomic.AddUint64(&counter, 1)
			wm.Begin(readTS)
			wm.Done(readTS)
		}
	})
}
