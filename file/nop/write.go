package nop

import (
	"github.com/pcelvng/task-tools/file/stat"
)

func NewWriter(pth string) *Writer {
	sts := stat.New()
	sts.SetPath(pth)
	return &Writer{
		sts: sts,
	}
}

// Writer is a no-operation writer. It doesn't do
// anything except keep byte count and line count.
// If isErr == true then all func calls return
// an err or type NopErr.
type Writer struct {
	// Err is the err returned from method
	// calls.
	// Useful for mocking err scenarios.
	Err error

	sts stat.Stat
}

func (w *Writer) WriteLine(ln []byte) (err error) {
	_, err = w.Write(append(ln, '\n'))
	if err == nil {
		w.sts.AddLine()
	}
	return
}

func (w *Writer) Write(p []byte) (n int, err error) {
	w.sts.AddBytes(int64(len(p)))
	return len(p), w.Err
}

func (w *Writer) Stats() stat.Stat {
	return w.sts.Clone()
}

func (w *Writer) Abort() error {
	return w.Err
}

func (w *Writer) Close() error {
	w.sts.SetSize(w.sts.ByteCnt)
	return w.Err
}
