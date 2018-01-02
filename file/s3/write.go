package s3

import (
	"github.com/pcelvng/task-tools/file/stat"
)

func NewWriter(pth string, opt *Options) (*Writer, error) {
	sts := stat.New()
	sts.SetPath(pth)
	return &Writer{
		sts: sts,
		opt: opt,
	}, nil
}

type Writer struct {
	sts stat.Stat
	opt *Options
}

func (w *Writer) WriteLine(ln []byte) (err error) {
	return
}

func (w *Writer) Write(p []byte) (n int, err error) {
	return
}

func (w *Writer) Stats() stat.Stat {
	return w.sts.Clone()
}

func (w *Writer) Abort() (err error) {
	return
}

func (w *Writer) Close() (err error) {
	return
}
