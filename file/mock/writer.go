package mock

import (
	"strings"
	"time"

	"github.com/pcelvng/task-tools/file/nop"
	"github.com/pkg/errors"
)

type Writer struct {
	*nop.Writer
	lines      []string
	WriteDelay time.Duration
}

func NewWriter(pth string) *Writer {
	w, err := nop.NewWriter(pth)
	if err != nil {
		panic(errors.Wrap(err, "invalid mock writer"))
	}
	return &Writer{
		Writer:     w,
		WriteDelay: 0,
		lines:      make([]string, 0),
	}
}

func (w *Writer) AddDelay(d time.Duration) *Writer {
	w.WriteDelay = d
	return w
}

func (w *Writer) Write(p []byte) (n int, err error) {
	time.Sleep(w.WriteDelay)
	w.lines = append(w.lines, strings.Split(string(p), "\n")...)
	return w.Writer.Write(p)
}

func (w *Writer) WriteLine(ln []byte) (err error) {
	time.Sleep(w.WriteDelay)
	if err := w.Writer.WriteLine(ln); err != nil {
		return err
	}
	w.lines = append(w.lines, string(ln))
	return nil
}

func (w *Writer) GetLines() []string {
	return w.lines
}
