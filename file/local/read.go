package local

import (
	"bufio"
	"compress/gzip"
	"crypto/md5"
	"hash"
	"io"
	"os"
	"path/filepath"

	"github.com/pcelvng/task-tools/file/stat"
)

func NewReader(pth string) (*Reader, error) {
	pth, _ = filepath.Abs(pth)

	// open
	f, err := os.Open(pth)
	if err != nil {
		return nil, err
	}

	// hash reader
	rHshr := &hashReader{
		r:    f,
		Hshr: md5.New(),
	}

	// compression
	var rBuf *bufio.Reader
	var rGzip *gzip.Reader
	if ext := filepath.Ext(pth); ext == ".gz" {
		rGzip, err = gzip.NewReader(rHshr)
		if err != nil {
			return nil, err // problem reading header
		}
		rBuf = bufio.NewReader(rGzip)
	} else {
		rBuf = bufio.NewReader(rHshr)
	}

	sts := stat.New()
	sts.SetPath(pth)
	sts.SetSize(fileSize(sts.Path))
	sts.SetCreated(fileCreated(sts.Path))

	return &Reader{
		f:     f,
		rHshr: rHshr,
		rBuf:  rBuf,
		rGzip: rGzip,
		sts:   sts,
	}, nil
}

// Reader
type Reader struct {
	f        *os.File
	rBuf     *bufio.Reader
	rGzip    *gzip.Reader
	rHshr    *hashReader
	sts      stat.Stat
	closed bool
}

func (r *Reader) ReadLine() (ln []byte, err error) {
	ln, err = r.rBuf.ReadBytes('\n')

	if len(ln) > 0 {
		r.sts.AddLine()

		// note that even '\n' bytes are
		// accounted for.
		r.sts.AddBytes(int64(len(ln)))

		if ln[len(ln)-1] == '\n' {
			return ln[:len(ln)-1], err
		}
	}
	return ln, err
}

func (r *Reader) Read(p []byte) (n int, err error) {
	n, err = r.rBuf.Read(p)
	r.sts.AddBytes(int64(n))
	return n, err
}

func (r *Reader) Stats() stat.Stat {
	return r.sts.Clone()
}

func (r *Reader) Close() (err error) {
	if r.closed {
		return nil
	}

	if r.rGzip != nil {
		r.rGzip.Close()
	}
	err = r.f.Close()

	// calculate checksum
	r.sts.SetCheckSum(r.rHshr.Hshr)

	r.closed = true
	return err
}

type hashReader struct {
	r    io.Reader
	Hshr hash.Hash
}

func (r *hashReader) Read(p []byte) (n int, err error) {
	// pass through read request to underlying reader
	n, err = r.r.Read(p)

	// write the read bytes to the hasher
	// writing nothing doesn't affect the final sum
	r.Hshr.Write(p[:n])
	return
}
