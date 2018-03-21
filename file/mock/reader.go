package mock

import (
	"io"
	"math"
	"strings"

	"github.com/pcelvng/task-tools/file/nop"
	"github.com/pkg/errors"
)

type reader struct {
	*nop.Reader
	lines     []string
	lineCount int
	linesRead int
	index     int
}

func NewReader(pth string) *reader {
	r, err := nop.NewReader(pth)
	if err != nil {
		panic(errors.Wrap(err, "invalid mock reader"))
	}

	return &reader{
		Reader:    r,
		lines:     make([]string, 0),
		lineCount: 1,
		linesRead: 0,
	}
}

func (r *reader) AddLines(lines ...string) *reader {
	r.lines = append(r.lines, lines...)
	if r.lineCount < len(r.lines) {
		r.lineCount = len(r.lines)
	}
	return r
}

func (r *reader) SetLineNumber(i int) *reader {
	r.lineCount = i
	return r
}

func (r *reader) Read(p []byte) (n int, err error) {
	switch strings.ToLower(r.MockReadMode) {
	case "read_err", "err":
		return 0, errors.New(r.MockReadMode)
	case "read_eof":
		return 0, io.EOF
	}

	if r.linesRead >= r.lineCount {
		return 0, io.EOF
	}

	line := r.nextLine()
	if line == "err" {
		return 0, errors.New("err")
	}
	ln := int(math.Min(float64(len(r.lines[r.index])), float64(len(p))))
	p = []byte(r.lines[r.index][:ln])
	return ln, nil
}

func (r *reader) nextLine() string {
	if len(r.lines) == 0 {
		r.linesRead++
		return "mock_string"
	}
	r.index = r.linesRead % len(r.lines)
	s := r.lines[r.index]
	r.linesRead++
	return s
}

func (r *reader) ReadLine() (ln []byte, err error) {
	switch strings.ToLower(r.MockReadMode) {
	case "readline_err", "err":
		return ln, errors.New(r.MockReadMode)
	case "readline_eof":
		return ln, io.EOF
	}

	if r.linesRead >= r.lineCount {
		return ln, io.EOF
	}

	line := r.nextLine()

	if line == "err" {
		return ln, errors.New("err")
	}

	return []byte(line), nil
}
