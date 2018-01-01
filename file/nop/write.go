package nop

import (
	"errors"

	"github.com/pcelvng/task-tools/file/stat"
)

var (
	NopErr = errors.New("nop error")
)

func NewWriter(pth string) *Writer {
	sts := stat.New()
	sts.SetPath(pth)
	return &Writer{
		sts: sts,
	}
}

func NewErrWriter(pth string) *Writer {
	w := NewWriter(pth)
	w.isErr = true
	return w
}

// Writer is a no-operation writer. It doesn't do
// anything except keep byte count and line count.
// If isErr == true then all func calls return
// an err or type NopErr.
type Writer struct {
	isErr bool
	sts   stat.Stat
}

func (w *Writer) WriteLine(ln []byte) (err error) {
	_, err = w.Write(append(ln, '\n'))
	if err == nil {
		w.sts.AddLine()
	}
	return
}

func (w *Writer) Write(p []byte) (n int, err error) {
	if w.isErr {
		return 0, NopErr
	}
	w.sts.AddBytes(int64(len(p)))
	return len(p), nil
}

func (w *Writer) Stats() stat.Stat {
	return w.sts.Clone()
}

func (w *Writer) Abort() error {
	if w.isErr {
		return NopErr
	}
	return nil
}

func (w *Writer) Close() error {
	if w.isErr {
		return NopErr
	}
	w.sts.SetSize(w.sts.ByteCnt)
	return nil
}
