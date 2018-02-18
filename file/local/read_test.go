package local

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

func createFile(pth string) {
	w, _ := NewWriter(pth, nil)
	w.WriteLine([]byte("test line"))
	w.WriteLine([]byte("test line"))
	w.Close()
}

func ExampleNewReader() {
	pth := "./test/test.txt"
	createFile(pth)
	r, err := NewReader(pth)
	if r == nil {
		return
	}

	fmt.Println(err)                 // output: <nil>
	fmt.Println(r.sts.Path != "")    // output: true
	fmt.Println(r.sts.Size)          // output: 20
	fmt.Println(r.sts.Created != "") // output: true
	fmt.Println(r.f != nil)          // output: true
	fmt.Println(r.rBuf != nil)       // output: true
	fmt.Println(r.rGzip == nil)      // output: true
	fmt.Println(r.rHshr != nil)      // output: true
	fmt.Println(r.closed)            // output: false

	// cleanup
	os.Remove(pth)
	os.Remove("./test")

	// Output:
	// <nil>
	// true
	// 20
	// true
	// true
	// true
	// true
	// true
	// false
}

func ExampleNewReaderCompression() {
	pth := "./test/test.gz"
	createFile(pth)
	r, err := NewReader(pth)
	if r == nil {
		return
	}

	fmt.Println(err)                 // output: <nil>
	fmt.Println(r.sts.Path != "")    // output: true
	fmt.Println(r.sts.Size)          // output: 20
	fmt.Println(r.sts.Created != "") // output: true
	fmt.Println(r.f != nil)          // output: true
	fmt.Println(r.rBuf != nil)       // output: true
	fmt.Println(r.rGzip != nil)      // output: true
	fmt.Println(r.rHshr != nil)      // output: true
	fmt.Println(r.closed)            // output: false

	// cleanup
	os.Remove(pth)
	os.Remove("./test")

	// Output:
	// <nil>
	// true
	// 48
	// true
	// true
	// true
	// true
	// true
	// false
}

func ExampleNewReaderErr() {
	pth := "./does/not/exist.txt"
	r, err := NewReader(pth)
	if err == nil {
		return
	}

	notExists := os.IsNotExist(err)
	fmt.Println(r)         // output: <nil>
	fmt.Println(notExists) // output: true

	// Output:
	// <nil>
	// true
}

func ExampleNewReaderErrGzip() {
	oldPth := "./test/test.txt"
	pth := "./test/test.gz"
	createFile(oldPth)
	os.Rename(oldPth, pth)

	r, err := NewReader(pth)
	if err == nil {
		return
	}

	fmt.Println(r)   // output: <nil>
	fmt.Println(err) // output: gzip: invalid header

	os.Remove(pth)
	os.Remove("./test")

	// Output:
	// <nil>
	// gzip: invalid header
}

func ExampleReader_Read() {
	pth := "./test/test.txt"
	createFile(pth)
	r, _ := NewReader(pth)
	if r == nil {
		return
	}

	b1 := make([]byte, 20)
	b2 := make([]byte, 20)
	n1, err1 := r.Read(b1)
	n2, err2 := r.Read(b2)

	fmt.Print(string(b1))      // output: test line, test line
	fmt.Println(n1)            // output: 20
	fmt.Println(err1)          // output: <nil>
	fmt.Println(n2)            // output: 0
	fmt.Println(err2)          // output: EOF
	fmt.Println(r.sts.ByteCnt) // output: 20
	fmt.Println(r.sts.LineCnt) // output: 0

	// cleanup
	os.Remove(pth)
	os.Remove("./test")

	// Output:
	// test line
	// test line
	// 20
	// <nil>
	// 0
	// EOF
	// 20
	// 0
}

func ExampleReader_ReadGzip() {
	pth := "./test/test.gz"
	createFile(pth)
	r, _ := NewReader(pth)
	if r == nil {
		return
	}

	b1 := make([]byte, 20)
	b2 := make([]byte, 20)
	n1, err1 := r.Read(b1)
	n2, err2 := r.Read(b2)

	fmt.Print(string(b1))      // output: test line, test line
	fmt.Println(n1)            // output: 20
	fmt.Println(err1)          // output: <nil>
	fmt.Println(n2)            // output: 0
	fmt.Println(err2)          // output: EOF
	fmt.Println(r.sts.ByteCnt) // output: 20
	fmt.Println(r.sts.LineCnt) // output: 0

	// cleanup
	os.Remove(pth)
	os.Remove("./test")

	// Output:
	// test line
	// test line
	// 20
	// <nil>
	// 0
	// EOF
	// 20
	// 0
}

func ExampleReader_ReadLine() {
	pth := "./test/test.txt"
	createFile(pth)
	r, _ := NewReader(pth)
	if r == nil {
		return
	}

	ln1, err1 := r.ReadLine()
	ln2, err2 := r.ReadLine()
	ln3, err3 := r.ReadLine()

	fmt.Println(string(ln1))   // output: test line
	fmt.Println(string(ln2))   // output: test line
	fmt.Println(string(ln3))   // output:
	fmt.Println(err1)          // output: <nil>
	fmt.Println(err2)          // output: <nil>
	fmt.Println(err3)          // output: EOF
	fmt.Println(r.sts.ByteCnt) // output: 20
	fmt.Println(r.sts.LineCnt) // output: 2

	// cleanup
	os.Remove(pth)
	os.Remove("./test")

	// Output:
	// test line
	// test line
	//
	// <nil>
	// <nil>
	// EOF
	// 20
	// 2
}

func ExampleReader_ReadLineGzip() {
	pth := "./test/test.gz"
	createFile(pth)
	r, _ := NewReader(pth)
	if r == nil {
		return
	}

	ln1, err1 := r.ReadLine()
	ln2, err2 := r.ReadLine()
	ln3, err3 := r.ReadLine()

	fmt.Println(string(ln1))   // output: test line
	fmt.Println(string(ln2))   // output: test line
	fmt.Println(string(ln3))   // output:
	fmt.Println(err1)          // output: <nil>
	fmt.Println(err2)          // output: <nil>
	fmt.Println(err3)          // output: EOF
	fmt.Println(r.sts.ByteCnt) // output: 20
	fmt.Println(r.sts.LineCnt) // output: 2

	// cleanup
	os.Remove(pth)
	os.Remove("./test")

	// Output:
	// test line
	// test line
	//
	// <nil>
	// <nil>
	// EOF
	// 20
	// 2
}

func ExampleReader_Stats() {
	pth := "./test/test.txt"
	createFile(pth)
	r, _ := NewReader(pth)
	if r == nil {
		return
	}

	r.ReadLine()
	r.ReadLine()
	sts := r.Stats()

	fmt.Println(sts.ByteCnt) // output: 20
	fmt.Println(sts.LineCnt) // output: 2

	// cleanup
	os.Remove(pth)
	os.Remove("./test")

	// Output:
	// 20
	// 2
}

func ExampleReader_Close() {
	pth := "./test/test.txt"
	createFile(pth)
	r, _ := NewReader(pth)
	if r == nil {
		return
	}

	r.ReadLine()
	r.ReadLine()
	err := r.Close()

	fmt.Println(err)            // output: <nil>
	fmt.Println(r.sts.Checksum) // output: 54f30d75cf7374c7e524a4530dbc93c2
	fmt.Println(r.closed)       // output: true

	// cleanup
	os.Remove(pth)
	os.Remove("./test")

	// Output:
	// <nil>
	// 54f30d75cf7374c7e524a4530dbc93c2
	// true
}

func ExampleReader_CloseGzip() {
	pth := "./test/test.gz"
	createFile(pth)
	r, _ := NewReader(pth)
	if r == nil {
		return
	}

	r.ReadLine()
	r.ReadLine()
	err := r.Close()

	fmt.Println(err)            // output: <nil>
	fmt.Println(r.sts.Checksum) // output: 42e649f9834028184ec21940d13a300f
	fmt.Println(r.closed)       // output: true

	// cleanup
	os.Remove(pth)
	os.Remove("./test")

	// Output:
	// <nil>
	// 42e649f9834028184ec21940d13a300f
	// true
}

func ExampleReader_CloseAndClose() {
	pth := "./test/test.txt"
	createFile(pth)
	r, _ := NewReader(pth)
	if r == nil {
		return
	}

	r.ReadLine()
	r.ReadLine()
	r.Close()
	err := r.Close()

	fmt.Println(err)            // output: <nil>
	fmt.Println(r.sts.Checksum) // output: 54f30d75cf7374c7e524a4530dbc93c2
	fmt.Println(r.closed)       // output: true

	// cleanup
	os.Remove(pth)
	os.Remove("./test")

	// Output:
	// <nil>
	// 54f30d75cf7374c7e524a4530dbc93c2
	// true
}

func TestListFiles(t *testing.T) {
	// setup - create objects
	pths := []string{
		"./test/f1.txt",
		"./test/f2.txt",
		"./test/dir/f3.txt",
	}

	for _, pth := range pths {
		createFile(pth)
	}

	// test returns only files - no directories
	dirPth := "./test/"
	allSts, err := ListFiles(dirPth)
	if err != nil {
		t.Error(err)
	}

	if len(allSts) == 2 {
		sts1 := allSts[0]
		sts2 := allSts[1]

		// make sure stats are set
		if sts1.Created == "" {
			t.Error("file sts.Created not set")
		}
		if sts1.Size == 0 {
			t.Error("file sts.Size not set")
		}

		f1Txt := strings.Contains(sts1.Path, "f1.txt")
		if !f1Txt {
			f1Txt = strings.Contains(sts2.Path, "f1.txt")
		}

		f2Txt := strings.Contains(sts1.Path, "f2.txt")
		if !f2Txt {
			f2Txt = strings.Contains(sts2.Path, "f2.txt")
		}

		if !f1Txt {
			t.Error("f1.txt not returned")
		}

		if !f2Txt {
			t.Error("f2.txt not returned")
		}
	} else {
		t.Errorf("expected 2 files but got %v instead\n", len(allSts))
	}

	// cleanup
	for _, pth := range pths {
		os.Remove(pth)
	}
	os.Remove("./test/dir/")
	os.Remove("./test")
}
