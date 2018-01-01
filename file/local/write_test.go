package local

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"testing"
)

func TestMain(m *testing.M) {
	exitCode := m.Run()

	// remove tmp dir
	os.RemoveAll("./tmp")
	os.Exit(exitCode)
}

func ExampleNewWriter() {
	// showing:
	// - basic writer (without tmp file)
	// - correct attribute assignments (no tmp file)
	// - basic writer (with tmp file)
	// - correct attribute assignments (with tmp file)
	// - write line to stdout
	// - write bytes to stdout

	pth := os.DevNull
	w, err := NewWriter(pth, nil)
	fmt.Println(err)
	fmt.Println(w.sts.Path)
	fmt.Println(w.tmpPth)

	// create tmp file
	opt := &Options{
		UseTmpFile: true,
		TmpDir:     "./tmp/",
		TmpPrefix:  "test_",
	}
	w, err = NewWriter(pth, opt)
	fmt.Println(err)
	fmt.Println(w.sts.Path)
	_, tmpF := path.Split(w.tmpPth)
	fmt.Println(tmpF[0:5])

	// remove tmp file and dir
	os.Remove(w.tmpPth)

	// write to stdout
	pth = "/dev/stdout"
	w, err = NewWriter(pth, nil)
	if err != nil {
		return
	}
	w.WriteLine([]byte("test line"))
	w.Close()
	fmt.Println(w.sts.LineCnt)
	fmt.Println(w.sts.ByteCnt)
	fmt.Println(w.sts.CheckSum)
	fmt.Println(w.Stats().CheckSum) // sanity check

	// Output:
	// <nil>
	// /dev/null
	//
	// <nil>
	// /dev/null
	// test_
	// test line
	// 1
	// 10
	// 4b3bbcd85a4c03a12b75bd1e70daa6c2
	// 4b3bbcd85a4c03a12b75bd1e70daa6c2
}

func ExampleWriter_copy() {
	// showing:
	// - copy from tmp file to stdout
	// - copy from tmp file to local file
	// - copy from tmp file to gzip local file

	// tmp file to stdout
	pth := "/dev/stdout"
	opt := &Options{
		UseTmpFile: true,
		TmpDir:     "./tmp",
		TmpPrefix:  "test_copy_",
	}
	w, _ := NewWriter(pth, opt)
	if w == nil {
		return
	}
	w.WriteLine([]byte("stdout line1"))
	w.WriteLine([]byte("stdout line2"))
	w.WriteLine([]byte("stdout line3"))
	w.Close()

	// tmp file to local file
	pth = "./tmp/test_copy.txt"
	opt = &Options{
		UseTmpFile: true,
		TmpDir:     "./tmp",
		TmpPrefix:  "test_copy_",
	}
	w, _ = NewWriter(pth, opt)
	if w == nil {
		return
	}
	w.WriteLine([]byte("local line1"))
	w.WriteLine([]byte("local line2"))
	w.WriteLine([]byte("local line3"))
	w.Close()
	b := make([]byte, w.sts.Size)
	r, _ := os.Open(w.sts.Path)
	io.ReadFull(r, b)
	fmt.Print(string(b))

	// tmp file to local gzip file
	pth = "./tmp/test_copy.gz"
	opt = &Options{
		UseTmpFile: true,
		TmpDir:     "./tmp",
		TmpPrefix:  "test_copy_",
	}
	w, _ = NewWriter(pth, opt)
	if w == nil {
		return
	}
	w.WriteLine([]byte("local gz line1"))
	w.WriteLine([]byte("local gz line2"))
	w.WriteLine([]byte("local gz line3"))
	w.Close()
	b = make([]byte, w.sts.ByteCnt)
	r, _ = os.Open(w.sts.Path)
	gr, _ := gzip.NewReader(r)
	io.ReadFull(gr, b)
	fmt.Print(string(b))

	// Output:
	// stdout line1
	// stdout line2
	// stdout line3
	// local line1
	// local line2
	// local line3
	// local gz line1
	// local gz line2
	// local gz line3
}

func ExampleWriter_copyerrmv() {
	// showing:
	// - mv err
	// - tmp open err
	// - openF err

	// tmp mv err
	pth := "/tmp/test.txt"
	w, _ := NewWriter(pth, nil)
	if w == nil {
		return
	}
	w.tmpPth = "/tmp/bad.txt" // doesn't exist
	n, err := w.copy()
	fmt.Println(n)   // 0
	fmt.Println(err) // rename /tmp/bad.txt /tmp/test.txt: no such file or directory

	// tmp open err
	pth = "/dev/null"
	w, _ = NewWriter(pth, nil)
	if w == nil {
		return
	}
	w.tmpPth = "/tmp/bad.txt" // doesn't exist
	n, err = w.copy()
	fmt.Println(n)   // 0
	fmt.Println(err) // rename /tmp/bad.txt /tmp/test.txt: no such file or directory

	// Output:
	// 0
	// rename /tmp/bad.txt /tmp/test.txt: no such file or directory
	// 0
	// open /tmp/bad.txt: no such file or directory
}

func ExampleOpenf_err() {
	// showing:
	// openf open dir err
	// openf open file err

	// dir bad perms
	_, _, err := openF("/bad/perms/dir/file.txt", false)
	fmt.Println(err)

	// file bad perms
	_, _, err = openF("/bad_perms.txt", false)
	fmt.Println(err)

	// Output:
	// mkdir /bad: permission denied
	// open /bad_perms.txt: permission denied
}

func ExampleClosef_err() {
	// showing:
	// closeF no err on nil f

	var nilF *os.File
	err := closeF("path.txt", nilF)
	fmt.Println(err)

	// Output:
	// <nil>
}

func ExampleOpenTmp_err() {
	// showing:
	// closeF no err on nil f

	_, _, err := openTmp("/root/bad", "")
	fmt.Println(err)

	// Output:
	// mkdir /root/bad: permission denied
}

func ExampleNewWriterErr() {
	// showing:
	// - writer is nil with an err
	// - err if pth is a dir
	// - err from bad write

	pth := "/dir/path/"
	w, err := NewWriter(pth, nil)
	fmt.Println(w)   // <nil>
	fmt.Println(err) // path /dir/path/: references a directory

	pth = "./test.txt"
	w, _ = NewWriter(pth, nil)
	if w == nil {
		return
	}
	var f *os.File
	var wc, wHshr io.WriteCloser

	wc = w.w
	wHshr = w.wHshr
	w.w = f
	w.wHshr = f
	err = w.WriteLine([]byte("bad write to nil"))
	fmt.Println(err) // invalid argument
	w.w = wc         // restore writer
	w.wHshr = wHshr  // restore write hasher
	err = w.Close()
	err = w.Abort()  // call Abort after close should return nil
	fmt.Println(err) // <nil>
	os.Remove(w.sts.Path)

	// Output:
	// <nil>
	// path /dir/path/: references a directory
	// invalid argument
	// <nil>
}

func ExampleNewWriter_Abort() {
	// showing:
	// - writer is nil with an err
	// - err if pth is a dir
	// - err from bad write

	pth := "./test.txt"
	w, _ := NewWriter(pth, nil)
	if w == nil {
		return
	}
	err := w.Abort()
	fmt.Println(err)
	w.Close() // call close after to test close following abort
	fmt.Println(err)

	// Output:
	// <nil>
	// <nil>
}

func ExampleNewWriterGzip() {
	// showing:
	// - gzip writer errs
	// - gzip writer

	pth := "./test.gz"
	w, err := NewWriter(pth, nil)
	if w == nil {
		return
	}
	fmt.Println(err)
	w.WriteLine([]byte("test line1"))
	w.WriteLine([]byte("test line2"))
	w.WriteLine([]byte("test line3"))
	w.Close()
	fmt.Println(w.sts.LineCnt)
	fmt.Println(w.sts.ByteCnt)
	fmt.Println(w.sts.CheckSum)
	fmt.Println(w.sts.Size)
	_, fName := path.Split(w.sts.Path)
	fmt.Println(fName)
	os.Remove(w.sts.Path)

	// Output:
	// <nil>
	// 3
	// 33
	// 632e558dab3dcdebcb4b2491ed3d7696
	// 61
	// test.gz
}

func Example_checkFile() {
	// showing:
	// - path normalization
	// - can use system files like '/dev/null'

	pth, err := checkFile("/")
	fmt.Println(pth)
	fmt.Println(err)
	fmt.Println("")

	pth, err = checkFile("//")
	fmt.Println(pth)
	fmt.Println(err)
	fmt.Println("")

	pth, err = checkFile("/test/dir/../../")
	fmt.Println(pth)
	fmt.Println(err)
	fmt.Println("")

	pth, err = checkFile("/test/dir/")
	fmt.Println(pth)
	fmt.Println(err)
	fmt.Println("")

	pth, err = checkFile("/dev/null")
	fmt.Println(pth)
	fmt.Println(err)
	fmt.Println("")

	// Output:
	// /
	// path /: references a directory
	//
	// /
	// path /: references a directory
	//
	// /
	// path /: references a directory
	//
	// /test/dir/
	// path /test/dir/: references a directory
	//
	// /dev/null
	// <nil>
	//
}

func Example_openTmp() {
	// showing:
	// - dir normalization
	// - tmp file prefix

	tmpPth, f, err := openTmp("/noexist/../tmp/", "test_")
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

func ExampleMultiWriteCloser() {
	// showing:
	// - write err
	// - close err
	// - short write err

	// write, close err
	errW := new(errWriteCloser)
	writers := make([]io.WriteCloser, 1)
	writers[0] = errW
	w := &multiWriteCloser{writers}
	_, err := w.Write([]byte("test err"))
	fmt.Println(err) // error writing
	err = w.Close()
	fmt.Println(err) // error closing

	// short write err
	errShort := new(shortWriteCloser)
	writers[0] = errShort
	w = &multiWriteCloser{writers}
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
