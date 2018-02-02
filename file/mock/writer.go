package mock

import (
	"time"

	"github.com/pcelvng/task-tools/file/nop"
	"github.com/pkg/errors"
)

type writer struct {
	*nop.Writer
	WriteDelay time.Duration
}

func NewWriter(pth string, delay time.Duration) *writer {
	w, err := nop.NewWriter(pth)
	if err != nil {
		panic(errors.Wrap(err, "invalid mock writer"))
	}
	return &writer{
		Writer:     w,
		WriteDelay: delay,
	}
}

func (w *writer) Write(p []byte) (n int, err error) {
	time.Sleep(w.WriteDelay)
	return w.Writer.Write(p)
}

func (w *writer) WriteLine(ln []byte) (err error) {
	time.Sleep(w.WriteDelay)
	return w.Writer.WriteLine(ln)
}
