package util

import (
	"bytes"
	"crypto/md5"
	"hash"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
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

// Ext will retrieve a non-compression related
// file extension. If there are multiple, it returns
// the first behind the compression extension. It
// is assumed the compression extension is last.
//
// Only supports '.gz' at the moment.
func Ext(p string) string {
	p = strings.Replace(p, ".gz", "", 1)
	return path.Ext(p)
}

// ParsePath will parse a path of the form:
// "{scheme}://{host}/{path/to/file.txt}
// and return the scheme, host and file path.
//
// Example:
// "s3://my-host/path/to/file.txt"
//
// Returns:
// scheme: "s3"
// host: "my-host"
// fPth: "path/to/file.txt"
func ParsePath(pth string) (scheme, host, fPth string) {
	pPth, _ := url.Parse(pth) // err is not possible since it's not via a request.
	scheme = pPth.Scheme
	host = pPth.Host
	fPth = strings.TrimLeft(pPth.Path, "/")

	return scheme, host, fPth
}
