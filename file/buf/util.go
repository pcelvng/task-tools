package buf

import (
	"sync/atomic"
)

// sizeWriter will perform a nop write and
// close. It will keep track of the total number
// of bytes written and provides a Size()
// method to know the total number of bytes written.
type sizeWriter struct {
	size int64
}

func (w *sizeWriter) Size() int64 {
	return atomic.LoadInt64(&w.size)
}

func (w *sizeWriter) Write(p []byte) (n int, err error) {
	atomic.AddInt64(&w.size, int64(len(p)))

	return len(p), nil
}
