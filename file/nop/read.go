package nop

import (
	"io"

	"github.com/pcelvng/task-tools/file/stat"
)

func NewReader(pth string) *Reader {
	sts := stat.New()
	sts.SetPath(pth)

	return &Reader{
		sts: sts,
	}
}

type Reader struct {
	// Err is the err returned from method
	// calls.
	// Useful for mocking err scenarios.
	Err error

	sts stat.Stat
}

func (r *Reader) Read(p []byte) (n int, err error) {
	if r.Err == io.EOF || len(p) == 0 {
		return n, r.Err
	}

	p[0] = '1'
	n = 1
	r.sts.AddBytes(int64(1))
	return n, r.Err
}

func (r *Reader) ReadLine() (ln []byte, err error) {
	if r.Err == io.EOF {
		return ln, r.Err
	}

	r.sts.AddLine()
	r.sts.AddBytes(1)
	ln = []byte{'1'}
	return ln, r.Err
}

func (r *Reader) Stats() stat.Stat {
	return r.sts.Clone()
}

func (r *Reader) Close() (err error) {
	r.sts.SetSize(r.sts.ByteCnt)

	return r.Err
}
