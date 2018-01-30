package nop

import (
	"errors"
	"log"
	"net/url"
	"time"

	"github.com/pcelvng/task-tools/file/stat"
)

// MockWriteMode can be set in order to
// mock various return scenarios.
//
// MockWriteMode can be set directly on module
// or through the NewWriter initializer. The MockWriteMode
// value is the string value right after 'nop://'.
//
// Example Initializer Paths:
// "nop://init_err/" - MockWriteMode is set as 'init_err'
// "nop://err" - MockWriteMode is set as 'err'
// "nop://write_err/other/fake/path.txt" - MockWriteMode is set as 'write_err'
//
// Supported Values:
// - "init_err" - returns err on NewWriter
// - "err" - every method than can, returns an error
// - "write_err" - returns err on Writer.Write() call.
// - "writeline_err" - returns err on Writer.WriteLine() call.
// - "abort_err" - returns err on Writer.Abort() call.
// - "close_err" - returns non-nil error on Writer.Close() call.
//var MockWriteMode string

func NewWriter(pth string) (*Writer, error) {
	sts := stat.New()
	sts.SetPath(pth)

	w := &Writer{
		sts: sts,
	}
	// set mock write mode
	// Note: the parsed write mode value
	// will over-write pre-existing value.
	// Manually set MockWriteMode values
	// may need to be set after initialization.
	mockWriteMode, err := url.Parse(pth)
	if err != nil {
		log.Println(err)
	}
	if mockWriteMode != nil {
		w.MockWriteMode = mockWriteMode.Host
	}

	if w.MockWriteMode == "init_err" {
		return nil, errors.New(w.MockWriteMode)
	}

	return w, nil
}

// Writer is a no-operation writer useful for testing.
type Writer struct {
	sts           stat.Stats
	MockWriteMode string
}

func (w *Writer) Write(p []byte) (n int, err error) {
	if w.MockWriteMode == "write_err" || w.MockWriteMode == "err" {
		return 0, errors.New(w.MockWriteMode)
	}

	w.sts.AddBytes(int64(len(p)))
	return len(p), nil
}

func (w *Writer) WriteLine(ln []byte) (err error) {
	if w.MockWriteMode == "writeline_err" || w.MockWriteMode == "err" {
		return errors.New(w.MockWriteMode)
	}

	w.sts.AddBytes(int64(len(ln) + 1))
	w.sts.AddLine()
	return nil
}

func (w *Writer) Stats() stat.Stats {
	return w.sts.Clone()
}

func (w *Writer) Abort() error {
	if w.MockWriteMode == "abort_err" || w.MockWriteMode == "err" {
		return errors.New(w.MockWriteMode)
	}

	return nil
}

func (w *Writer) Close() error {
	if w.MockWriteMode == "close_err" || w.MockWriteMode == "err" {
		return errors.New(w.MockWriteMode)
	}

	w.sts.SetSize(w.sts.ByteCnt)
	w.sts.SetCreated(time.Now())
	return nil
}
