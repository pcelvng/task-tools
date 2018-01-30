package dedup

import (
	"errors"
	"io"
	"strings"

	"math"

	"github.com/pcelvng/task-tools/file/nop"
)

type mockReader struct {
	*nop.Reader
	Lines     []string
	LineCount int
	i         int
}

func newMockReader(pth string) (*mockReader, error) {
	r, err := nop.NewReader(pth)
	return &mockReader{
		Reader: r,
		Lines:  []string{"Mock Line"},
		i:      0,
	}, err
}

func (r *mockReader) Read(p []byte) (n int, err error) {
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

func (r *mockReader) nextLine() string {
	s := r.Lines[r.i]
	r.i += (r.i + 1) % len(r.Lines)
	return s
}

func (r *mockReader) ReadLine() (ln []byte, err error) {
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
