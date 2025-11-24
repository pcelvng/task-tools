package util

import (
	"errors"
	"io"
	"testing"

	"github.com/hydronica/trial"
)

func TestOpenTmp(t *testing.T) {
	fn := func(args trial.Input) (interface{}, error) {
		s, _, err := OpenTmp(args.Slice(0).String(), args.Slice(1).String())
		return s, err
	}
	cases := trial.Cases[trial.Input, any]{
		"normalize path": {
			Input:    trial.Args("/noexist/../tmp/", "test_"),
			Expected: "tmp/test_",
		},
		"no permission": {
			Input:     trial.Args("/root/bad", ""),
			ShouldErr: true,
		},
		"prefix with spaces": {
			Input:    trial.Args("/tmp/path", "test prefix"),
			Expected: "/tmp/path/test prefix",
		},
	}
	trial.New(fn, cases).Comparer(trial.Contains).Test(t)
}

func TestMultiWriteCloser(t *testing.T) {
	// showing:
	// - write err
	// - close err
	// - short write err

	// write, close err
	errW := new(errWriteCloser)
	writers := make([]io.WriteCloser, 1)
	writers[0] = errW
	w := NewMultiWriteCloser(writers)
if 	_, err := w.Write([]byte("test err")); err == nil {
	t.Error("Expected write to error") 
}
if 	err := w.Close(); err == nil {
	t.Error("Expected close to error") 
}
	

	// short write err
	errShort := new(shortWriteCloser)
	writers[0] = errShort
	w = NewMultiWriteCloser(writers)
	if _, err := w.Write([]byte("test short")); err == nil {
		t.Error("Expected error for short writer")
		// This checks that the byte count returns is the same as the text sent it. 
	}
	

	// Output:
	// error writing
	// error closing
	// short write
}

type errWriteCloser struct{}

func (w *errWriteCloser) Write(_ []byte) (int, error) {
	return 0, errors.New("error writing")
}

func (w *errWriteCloser) Close() error {
	return errors.New("error closing")
}

type shortWriteCloser struct{}

func (w *shortWriteCloser) Write(p []byte) (int, error) {
	return 0, nil
}

func (w *shortWriteCloser) Close() error {
	return nil
}
