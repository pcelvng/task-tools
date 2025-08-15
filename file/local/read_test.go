package local

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/pcelvng/task-tools/file/stat"
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
	sts := r.Stats()
	fmt.Println(err)               // <nil>
	fmt.Println(sts.Path != "")    // true
	fmt.Println(sts.Size)          // 20
	fmt.Println(sts.Created != "") // true
	fmt.Println(r.f != nil)        // true
	fmt.Println(r.rBuf != nil)     // true
	fmt.Println(r.rGzip == nil)    // true
	fmt.Println(r.rHshr != nil)    // true
	fmt.Println(r.closed)          // false

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

func ExampleNewReader_compression() {
	pth := "./test/test.gz"
	createFile(pth)
	r, err := NewReader(pth)
	if r == nil {
		return
	}
	sts := r.Stats()

	fmt.Println(err)               // <nil>
	fmt.Println(sts.Path != "")    // true
	fmt.Println(sts.Size)          // 20
	fmt.Println(sts.Created != "") // true
	fmt.Println(r.f != nil)        // true
	fmt.Println(r.rBuf != nil)     // true
	fmt.Println(r.rGzip != nil)    // true
	fmt.Println(r.rHshr != nil)    // true
	fmt.Println(r.closed)          // false

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

func ExampleNewReader_err() {
	pth := "./does/not/exist.txt"
	r, err := NewReader(pth)
	if err == nil {
		return
	}

	notExists := os.IsNotExist(err)
	fmt.Println(r)         // <nil>
	fmt.Println(notExists) // true

	// Output:
	// <nil>
	// true
}

func ExampleNewReader_errGzip() {
	oldPth := "./test/test.txt"
	pth := "./test/test.gz"
	createFile(oldPth)
	os.Rename(oldPth, pth)

	r, err := NewReader(pth)
	if err == nil {
		return
	}

	fmt.Println(r)   // <nil>
	fmt.Println(err) // gzip: invalid header

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

	fmt.Print(string(b1))      // test line, test line
	fmt.Println(n1)            // 20
	fmt.Println(err1)          // <nil>
	fmt.Println(n2)            // 0
	fmt.Println(err2)          // EOF
	fmt.Println(r.sts.ByteCnt) // 20
	fmt.Println(r.sts.LineCnt) // 0

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

func ExampleReader_Read_gzip() {
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

	fmt.Print(string(b1))      // test line, test line
	fmt.Println(n1)            // 20
	fmt.Println(err1)          // <nil>
	fmt.Println(n2)            // 0
	fmt.Println(err2)          // EOF
	fmt.Println(r.sts.ByteCnt) // 20
	fmt.Println(r.sts.LineCnt) // 0

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

	fmt.Println(string(ln1))   // test line
	fmt.Println(string(ln2))   // test line
	fmt.Println(string(ln3))   //
	fmt.Println(err1)          // <nil>
	fmt.Println(err2)          // <nil>
	fmt.Println(err3)          // EOF
	fmt.Println(r.sts.ByteCnt) // 20
	fmt.Println(r.sts.LineCnt) // 2

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

func ExampleReader_ReadLine_gzip() {
	pth := "./test/test.gz"
	createFile(pth)
	r, _ := NewReader(pth)
	if r == nil {
		return
	}

	ln1, err1 := r.ReadLine()
	ln2, err2 := r.ReadLine()
	ln3, err3 := r.ReadLine()

	fmt.Println(string(ln1))   // test line
	fmt.Println(string(ln2))   // test line
	fmt.Println(string(ln3))   //
	fmt.Println(err1)          // <nil>
	fmt.Println(err2)          // <nil>
	fmt.Println(err3)          // EOF
	fmt.Println(r.sts.ByteCnt) // 20
	fmt.Println(r.sts.LineCnt) // 2

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

	fmt.Println(sts.ByteCnt) // 20
	fmt.Println(sts.LineCnt) // 2

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

	fmt.Println(err)              // <nil>
	fmt.Println(r.sts.Checksum()) // 54f30d75cf7374c7e524a4530dbc93c2
	fmt.Println(r.closed)         // true

	// cleanup
	os.Remove(pth)
	os.Remove("./test")

	// Output:
	// <nil>
	// 54f30d75cf7374c7e524a4530dbc93c2
	// true
}

func ExampleReader_Close_gzip() {
	pth := "./test/test.gz"
	createFile(pth)
	r, _ := NewReader(pth)
	if r == nil {
		return
	}

	r.ReadLine()
	r.ReadLine()
	err := r.Close()

	fmt.Println(err)              // <nil>
	fmt.Println(r.sts.Checksum()) // 42e649f9834028184ec21940d13a300f
	fmt.Println(r.closed)         // true

	// cleanup
	os.Remove(pth)
	os.Remove("./test")

	// Output:
	// <nil>
	// 42e649f9834028184ec21940d13a300f
	// true
}

func ExampleReader_Close_afterClose() {
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

	fmt.Println(err)              // <nil>
	fmt.Println(r.sts.Checksum()) // 54f30d75cf7374c7e524a4530dbc93c2
	fmt.Println(r.closed)         // true

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

	// test returns all files and directories in the directory (non-recursive)
	dirPth := "./test/"
	allSts, err := ListFiles(dirPth)
	if err != nil {
		t.Error(err)
	}

	if len(allSts) != 3 {
		t.Fatalf("expected 3 items (1 directory + 2 files) but got %v instead\n", len(allSts))
	}
	m := make(map[string]stat.Stats)
	for _, f := range allSts {
		_, p := filepath.Split(f.Path)
		m[p] = f
		if !f.IsDir {
			if f.Created == "" {
				t.Errorf("%s should have created date", f.Path)
			}
			if f.Size == 0 {
				t.Errorf("%s should have size", f.Path)
			}
			if f.Checksum == "" {
				t.Errorf("%s should have checksum", f.Path)
			}

		}
	}
	f, ok := m["dir"]
	if !ok {
		t.Errorf("expected dir")
	}
	if !f.IsDir {
		t.Errorf("should be dir")
	}

	// cleanup
	for _, pth := range pths {
		os.Remove(pth)
	}
	os.Remove("./test/dir/")
	os.Remove("./test")
}
