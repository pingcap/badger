package fileutil

import (
	"context"
	"os"

	"github.com/ncw/directio"
	"golang.org/x/time/rate"
)

// DirectWriter writes to a file opened with O_DIRECT flag.
// `Finish` must be called when the writing is done to truncate and sync the file.
type DirectWriter struct {
	writer
}

// BufferedWriter writes to a file with buffer.
type BufferedWriter struct {
	writer
}

type writer struct {
	fd       *os.File
	fileOff  int64
	writeBuf []byte // 4MB
	bufOff   int64
	limiter  *rate.Limiter
}

func NewBufferedWriter(fd *os.File, bufSize int, limiter *rate.Limiter) *BufferedWriter {
	return &BufferedWriter{
		writer: writer{
			fd:       fd,
			writeBuf: make([]byte, bufSize),
			limiter:  limiter,
		},
	}
}

func NewDirectWriter(fd *os.File, bufSize int, limiter *rate.Limiter) *DirectWriter {
	return &DirectWriter{
		writer: writer{
			fd:       fd,
			writeBuf: directio.AlignedBlock(bufSize),
			limiter:  limiter,
		},
	}
}

func (l *writer) Reset(fd *os.File) {
	l.fd = fd
	l.fileOff = 0
	l.bufOff = 0
}

func (l *writer) Append(val []byte) error {
	for {
		n := copy(l.writeBuf[l.bufOff:], val)
		if n == len(val) {
			l.bufOff += int64(len(val))
			return nil
		}
		// writeBuf is full.
		l.waitRateLimiter()
		_, err := l.fd.WriteAt(l.writeBuf, l.fileOff)
		if err != nil {
			return err
		}
		l.fileOff += int64(len(l.writeBuf))
		l.bufOff = 0
		val = val[n:]
	}
}

func (l *writer) waitRateLimiter() {
	if l.limiter != nil {
		err := l.limiter.WaitN(context.Background(), len(l.writeBuf))
		if err != nil {
			panic(err)
		}
	}
}

func (l *BufferedWriter) Flush() error {
	if l.bufOff == 0 {
		return nil
	}
	l.waitRateLimiter()
	_, err := l.fd.WriteAt(l.writeBuf[:l.bufOff], l.fileOff)
	if err != nil {
		return err
	}
	l.fileOff += l.bufOff
	l.bufOff = 0
	return nil
}

func (l *BufferedWriter) Sync() error {
	return Fdatasync(l.fd)
}

func (l *DirectWriter) Finish() error {
	if l.bufOff == 0 {
		return nil
	}
	writeLen := alignedSize(l.bufOff)
	l.waitRateLimiter()
	_, err := l.fd.WriteAt(l.writeBuf[:writeLen], l.fileOff)
	if err != nil {
		return err
	}
	err = l.fd.Truncate(l.fileOff + l.bufOff)
	if err != nil {
		return err
	}
	return Fdatasync(l.fd)
}

func alignedSize(n int64) int64 {
	return (n + directio.BlockSize - 1) / directio.BlockSize * directio.BlockSize
}
