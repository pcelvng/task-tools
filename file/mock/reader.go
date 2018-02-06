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
	Lines     []string
	LineCount int
	i         int
}

func NewReader(pth string, data []string, count int) *reader {
	r, err := nop.NewReader(pth)
	if err != nil {
		panic(errors.Wrap(err, "invalid mock reader"))
	}

	if len(data) == 0 {
		data = []string{"mock line"}
	}
	return &reader{
		Reader:    r,
		Lines:     data,
		LineCount: count,
		i:         0,
	}
}

func (r *reader) Read(p []byte) (n int, err error) {
	switch strings.ToLower(r.MockReadMode) {
	case "read_err", "err":
		return 0, errors.New(r.MockReadMode)
	case "read_eof":
		return 0, io.EOF
	}

	if r.i >= r.LineCount {
		return 0, io.EOF
	}

	line := r.nextLine()
	if line == "err" {
		return 0, errors.New("err")
	}
	ln := int(math.Min(float64(len(r.Lines[r.i])), float64(len(p))))
	p = []byte(r.Lines[r.i][:ln])
	return ln, nil
}

func (r *reader) nextLine() string {
	index := r.i % len(r.Lines)
	s := r.Lines[index]
	r.i++
	return s
}

func (r *reader) ReadLine() (ln []byte, err error) {
	switch strings.ToLower(r.MockReadMode) {
	case "readline_err", "err":
		return ln, errors.New(r.MockReadMode)
	case "readline_eof":
		return ln, io.EOF
	}

	if r.i >= r.LineCount {
		return ln, io.EOF
	}

	line := r.nextLine()

	if line == "err" {
		return ln, errors.New("err")
	}

	return []byte(line), nil
}
