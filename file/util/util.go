package util

import (
	"bytes"
	"crypto/md5"
	"hash"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
)

func NewNopWriteCloser(w io.Writer) *NopCloser {
	return &NopCloser{w}
}

// NopCloser will turn a Writer into an io.WriteCloser
// Write will call the original write Write method.
// Close will do nothing.
type NopCloser struct {
	io.Writer
}

func (wc *NopCloser) Close() error {
	return nil
}

func NewMultiWriteCloser(writers []io.WriteCloser) *MultiWriteCloser {
	return &MultiWriteCloser{writers}
}

type MultiWriteCloser struct {
	writers []io.WriteCloser
}

func (mw *MultiWriteCloser) Write(p []byte) (n int, err error) {
	for _, w := range mw.writers {
		n, err = w.Write(p)
		if err != nil {
			return
		}
		if n != len(p) {
			err = io.ErrShortWrite
			return
		}
	}
	return len(p), nil
}

func (mw *MultiWriteCloser) Close() (err error) {
	for _, w := range mw.writers {
		wErr := w.Close()
		if wErr != nil {
			err = wErr
		}
	}
	return err
}

func NewHashReader(hshr hash.Hash, r io.Reader) *HashReader {
	return &HashReader{
		r:    r,
		Hshr: hshr,
	}
}

// HashReader executes the underlying reader Read call
// and will write the read bytes into the hasher.
// This is useful when you want the hasher to get at the
// raw bytes of the reader.
type HashReader struct {
	r    io.Reader
	Hshr hash.Hash
}

func (r *HashReader) Read(p []byte) (n int, err error) {
	// pass through read request to underlying reader
	n, err = r.r.Read(p)

	// write the read bytes to the hasher
	// writing nothing doesn't affect the final sum
	r.Hshr.Write(p[:n])
	return
}

// OpenTmp will open and create a temp file
// It will create necessary directories.
func OpenTmp(dir, prefix string) (absTmp string, f *os.File, err error) {
	// normalize dir path
	dir, _ = filepath.Abs(dir)

	err = os.MkdirAll(dir, 0700)
	if err != nil {
		return absTmp, f, err
	}

	f, err = ioutil.TempFile(dir, prefix)
	if f != nil {
		absTmp, _ = filepath.Abs(f.Name())
	}
	return absTmp, f, err
}

// RmTmp will remove a local tmp file.
func RmTmp(tmpPth string) error {
	if tmpPth == "" {
		return nil
	}

	return os.Remove(tmpPth)
}

// NewCloseBuf returns an instance of
// CloseBuf.
func NewCloseBuf() *CloseBuf {
	var buf *bytes.Buffer
	return &CloseBuf{Buf: buf}
}

// CloseBuf is a bytes.Buffer with
// a Close method.
type CloseBuf struct {
	Buf *bytes.Buffer
}

func (b CloseBuf) Close() error {
	return nil
}

func NewMD5Closer() *HashCloser {
	return &HashCloser{
		Hshr: md5.New(),
	}
}

type HashCloser struct {
	Hshr hash.Hash
}

func (h *HashCloser) Write(p []byte) (n int, err error) {
	return h.Hshr.Write(p)
}

func (h *HashCloser) Close() error {
	return nil
}

func NewSizeWriter() *SizeWriter {
	return &SizeWriter{}
}

// SizeWriter will perform a nop write and
// close. It will keep track of the total number
// of bytes written and provides a Size()
// method to know the total number of bytes written.
type SizeWriter struct {
	size int64
	mu   sync.Mutex
}

func (w *SizeWriter) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.size
}

func (w *SizeWriter) Write(p []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.size = w.size + int64(len(p))
	return len(p), nil
}

// FileInfo presents summary file information.
type FileInfo struct {

}


