package util

import (
	"hash"
	"io"
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
