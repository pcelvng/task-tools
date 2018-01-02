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

func (w *Reader) ReadLine() (ln []byte, err error) {
	if w.Err == io.EOF {
		return ln, w.Err
	}

	w.sts.AddLine()
	w.sts.AddBytes(1)
	ln = []byte{'1'}
	return ln, w.Err
}

func (w *Reader) Read(p []byte) (n int, err error) {
	if w.Err == io.EOF || len(p) == 0 {
		return n, w.Err
	}

	p[0] = '1'
	n = 1
	w.sts.AddBytes(int64(1))
	return n, w.Err
}

func (w *Reader) Stats() stat.Stat {
	return w.sts.Clone()
}

func (w *Reader) Close() (err error) {
	w.sts.SetSize(w.sts.ByteCnt)

	return w.Err
}
