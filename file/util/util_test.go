package util

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
)

func Example_OpenTmp() {
	// showing:
	// - dir normalization
	// - tmp file prefix

	tmpPth, f, err := OpenTmp("/noexist/../tmp/", "test_")
	if err != nil {
		return
	}
	tmpDir, tmpF := path.Split(tmpPth)
	fmt.Println(tmpDir)
	fmt.Println(tmpF[0:5])

	if f != nil {
		f.Close()
	}
	err = os.Remove(tmpPth)
	fmt.Println(err)

	// Output:
	// /tmp/
	// test_
	// <nil>
}

func ExampleOpenTmp_err() {
	// showing:
	// closeF no err on nil f

	_, _, err := OpenTmp("/root/bad", "")
	isDenied := os.IsPermission(err)
	fmt.Println(isDenied) // output: true

	// Output:
	// true
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
