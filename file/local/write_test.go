package local

import (
	"fmt"
	"io"
	"os"
	"path"
)

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
	tmpDir, tmpF := path.Split(w.tmpPth)
	fmt.Println(tmpF[0:5])

	// remove tmp file and dir
	os.RemoveAll(tmpDir)

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

func ExampleNewWriterErr() {
	// showing:
	// - writer is nil with an err
	// - err if pth is a dir
	// - err from bad write

	pth := "/dir/path/"
	w, err := NewWriter(pth, nil)
	fmt.Println(w)
	fmt.Println(err)

	pth = "./test.txt"
	w, _ = NewWriter(pth, nil)
	if w == nil {
		return
	}
	var f *os.File
	var wc, wHshr io.WriteCloser

	wc = w.w
	w.w = f
	n, err := w.WriteLine([]byte("bad write to nil"))
	fmt.Println(n)
	fmt.Println(err)

	w.w = wc
	wHshr = w.wHshr
	w.wHshr = f
	n, err = w.WriteLine([]byte("bad hasher write to nil"))
	fmt.Println(n)
	fmt.Println(err)
	w.wHshr = wHshr
	err = w.Close()
	fmt.Println(err)
	err = w.Abort() // call Abort after close to test abort after close
	fmt.Println(err)
	os.Remove(w.sts.Path)

	// Output:
	// <nil>
	// path /dir/path/: references a directory
	// 0
	// invalid argument
	// 24
	// invalid argument
	// <nil>
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
