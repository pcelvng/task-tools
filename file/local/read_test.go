package local

import (
	"fmt"
	"os"
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
	fmt.Println(r.sts.CheckSum) // output: 54f30d75cf7374c7e524a4530dbc93c2
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
	fmt.Println(r.sts.CheckSum) // output: 42e649f9834028184ec21940d13a300f
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
	fmt.Println(r.sts.CheckSum) // output: 54f30d75cf7374c7e524a4530dbc93c2
	fmt.Println(r.closed)       // output: true

	// cleanup
	os.Remove(pth)
	os.Remove("./test")

	// Output:
	// <nil>
	// 54f30d75cf7374c7e524a4530dbc93c2
	// true
}
