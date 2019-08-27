package fileutil

import (
	"context"
	"os"

	"golang.org/x/time/rate"
)

type BufferedFileWriter struct {
	f              *os.File
	buf            []byte
	bufSize        int64
	bytesPerSync   int64

	limiter *rate.Limiter
}

// NewBufferedFileWriter makes a new NewBufferedFileWriter.
// BufferedFileWriter use SyncFileRange internally, you can control the this sync by bytesPerSync.
// If the limiter is nil, this writer will not limit the write speed.
func NewBufferedFileWriter(f *os.File, bufSize int, limiter *rate.Limiter) *BufferedFileWriter {
	return &BufferedFileWriter{
		f:            f,
		buf:          make([]byte, 0, bufSize),
		bufSize:      int64(bufSize),
		limiter:      limiter,
	}
}

// Append data to buffer, flush buffer if needed.
// If bytesPerSync > 0, this call may flush some dirty page respect to bytesPerSync.
func (bw *BufferedFileWriter) Append(data []byte) error {
	if cap(bw.buf)-len(bw.buf) < len(data) && len(bw.buf) > 0 {
		if err := bw.Flush(); err != nil {
			return err
		}
	}
	if cap(bw.buf) >= len(data) {
		bw.buf = append(bw.buf, data...)
		return nil
	}
	return bw.write(data)
}

// Flush data in buffer to disk.
// If fsync is true, it will do a Fdatasync, otherwise it will try to do a SyncFileRange.
func (bw *BufferedFileWriter) Flush() error {
	if len(bw.buf) == 0 {
		return nil
	}
	if err := bw.write(bw.buf); err != nil {
		return err
	}
	bw.buf = bw.buf[:0]
	return nil
}

// Sync flush all OS caches to disk.
func (bw *BufferedFileWriter) Sync() error {
	return Fdatasync(bw.f)
}

// Reset this writer with new file.
func (bw *BufferedFileWriter) Reset(f *os.File) {
	bw.f = f
	bw.buf = bw.buf[:0]
}

func (bw *BufferedFileWriter) write(data []byte) error {
	bw.requestTokenForWrite(len(data))
	if _, err := bw.f.Write(data); err != nil {
		return err
	}
	return nil
}

func (bw *BufferedFileWriter) requestTokenForWrite(size int) {
	if bw.limiter == nil {
		return
	}
	if err := bw.limiter.WaitN(context.Background(), size); err != nil {
		panic(err)
	}
	return
}
