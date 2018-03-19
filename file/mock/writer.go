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

func NewWriter(pth string) *writer {
	w, err := nop.NewWriter(pth)
	if err != nil {
		panic(errors.Wrap(err, "invalid mock writer"))
	}
	return &writer{
		Writer:     w,
		WriteDelay: 0,
	}
}

func (w *writer) AddDelay(d time.Duration) *writer {
	w.WriteDelay = d
	return w
}

func (w *writer) Write(p []byte) (n int, err error) {
	time.Sleep(w.WriteDelay)
	return w.Writer.Write(p)
}

func (w *writer) WriteLine(ln []byte) (err error) {
	time.Sleep(w.WriteDelay)
	return w.Writer.WriteLine(ln)
}
