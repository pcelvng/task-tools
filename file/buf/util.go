package buf

import "sync"

// sizeWriter will perform a nop write and
// close. It will keep track of the total number
// of bytes written and provides a Size()
// method to know the total number of bytes written.
type sizeWriter struct {
	size int64
	mu   sync.Mutex
}

func (w *sizeWriter) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.size
}

func (w *sizeWriter) Write(p []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.size = w.size + int64(len(p))
	return len(p), nil
}
