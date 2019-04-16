package util

import (
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/jbsmith7741/trial"
)

func TestOpenTmp(t *testing.T) {
	fn := func(args ...interface{}) (interface{}, error) {
		s, _, err := OpenTmp(args[0].(string), args[1].(string))
		return s, err
	}
	cases := trial.Cases{
		"normalize path": {
			Input:    trial.Args("/noexist/../tmp/", "test_"),
			Expected: "tmp/test_",
		},
		"no permission": {
			Input:       trial.Args("/root/bad", ""),
			ExpectedErr: errors.New("permission denied"),
		},
		"prefix with spaces": {
			Input:    trial.Args("/tmp/path", "test prefix"),
			Expected: "/tmp/path/test prefix",
		},
	}
	trial.New(fn, cases).Comparer(trial.Contains).Test(t)
}

func ExampleMultiWriteCloser() {
	// showing:
	// - write err
	// - close err
	// - short write err

	// write, close err
	errW := new(errWriteCloser)
	writers := make([]io.WriteCloser, 1)
	writers[0] = errW
	w := NewMultiWriteCloser(writers)
	_, err := w.Write([]byte("test err"))
	fmt.Println(err) // error writing
	err = w.Close()
	fmt.Println(err) // error closing

	// short write err
	errShort := new(shortWriteCloser)
	writers[0] = errShort
	w = NewMultiWriteCloser(writers)
	_, err = w.Write([]byte("test short"))
	fmt.Println(err) // short write

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
