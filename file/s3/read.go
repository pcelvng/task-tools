package s3

import (
	"github.com/pcelvng/task-tools/file/stat"
)

func NewReader(pth string, opt *Options) (*Reader, error) {
	sts := stat.New()
	sts.SetPath(pth)

	return &Reader{
		sts: sts,
		opt: opt,
	}, nil
}

type Reader struct {
	sts stat.Stat
	opt *Options
}

func (w *Reader) ReadLine() (ln []byte, err error) {
	return
}

func (w *Reader) Read(p []byte) (n int, err error) {
	return
}

func (w *Reader) Stats() stat.Stat {
	return w.sts.Clone()
}

func (w *Reader) Close() (err error) {
	return
}
