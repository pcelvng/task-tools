package minio

import (
	"fmt"
	"os"
	"strings"

	"github.com/pcelvng/task-tools/file/buf"
)

func ExampleNewWriter() {
	pth := fmt.Sprintf("mcs://%v/write/test.txt", testBucket)
	w, err := NewWriter(pth, testOption, nil)
	if w == nil {
		return
	}

	fmt.Println(err)             // output: <nil>
	fmt.Println(w.sts.Path())    // output: mcs://task-tools-test/write/test.txt
	fmt.Println(w.client != nil) // output: true
	fmt.Println(w.bfr != nil)    // output: true
	fmt.Println(w.bucket)        // output: task-tools-test
	fmt.Println(w.objPth)        // output: write/test.txt
	fmt.Println(w.tmpPth == "")  // output: true

	// Output:
	// <nil>
	// mcs://task-tools-test/write/test.txt
	// true
	// true
	// task-tools-test
	// write/test.txt
	// true
}

func ExampleNewWriterTmpFile() {
	pth := fmt.Sprintf("mcs://%v/write/test.txt", testBucket)
	opt := buf.NewOptions()
	opt.UseFileBuf = true
	opt.FileBufDir = "./test/tmp"
	opt.FileBufPrefix = "test_"
	w, err := NewWriter(pth, testOption, opt)
	if w == nil {
		return
	}

	fmt.Println(err)             // output: <nil>
	fmt.Println(w.sts.Path())    // output: mcs://task-tools-test/write/test.txt
	fmt.Println(w.client != nil) // output: true
	fmt.Println(w.bfr != nil)    // output: true
	fmt.Println(w.bucket)        // output: task-tools-test
	fmt.Println(w.objPth)        // output: write/test.txt
	fmt.Println(w.tmpPth != "")  // output: true

	// cleanup
	w.bfr.Cleanup()
	os.Remove("./test/tmp")
	os.Remove("./test")

	// Output:
	// <nil>
	// mcs://task-tools-test/write/test.txt
	// true
	// true
	// task-tools-test
	// write/test.txt
	// true
}

func ExampleNewWriterCompressed() {
	pth := fmt.Sprintf("mcs://%v/write/test.gz", testBucket)
	w, err := NewWriter(pth, testOption, nil)
	if w == nil {
		return
	}

	fmt.Println(err)             // output: <nil>
	fmt.Println(w.sts.Path())    // output: mcs://task-tools-test/write/test.gz
	fmt.Println(w.client != nil) // output: true
	fmt.Println(w.bfr != nil)    // output: true
	fmt.Println(w.bucket)        // output: task-tools-test
	fmt.Println(w.objPth)        // output: write/test.gz
	fmt.Println(w.tmpPth == "")  // output: true

	// Output:
	// <nil>
	// mcs://task-tools-test/write/test.gz
	// true
	// true
	// task-tools-test
	// write/test.gz
	// true
}

func ExampleNewWriterErrBuf() {
	pth := fmt.Sprintf("mcs://%v/%v/write/test.txt", testOption.Host, testBucket)
	opt := buf.NewOptions()
	opt.UseFileBuf = true
	opt.FileBufDir = "/private/bad/tmp/dir"
	opt.FileBufPrefix = "test_"
	w, err := NewWriter(pth, testOption, opt)
	if err == nil {
		return
	}

	isDenied := strings.Contains(err.Error(), "denied")
	fmt.Println(w)        // output: <nil>
	fmt.Println(isDenied) // output: true

	// Output:
	// <nil>
	// true
}

func ExampleWriter_Write() {
	pth := fmt.Sprintf("mcs://%v/write/test.txt", testBucket)
	w, _ := NewWriter(pth, testOption, nil)
	if w == nil {
		return
	}

	n, err := w.Write([]byte("test line"))

	fmt.Println(n)   // output: 9
	fmt.Println(err) // output: <nil>

	// Output:
	// 9
	// <nil>
}

func ExampleWriter_WriteLine() {
	pth := fmt.Sprintf("mcs://%v/write/test.txt", testBucket)
	w, _ := NewWriter(pth, testOption, nil)
	if w == nil {
		return
	}

	err := w.WriteLine([]byte("test line"))

	fmt.Println(err) // output: <nil>

	// Output:
	// <nil>
}

func ExampleWriter_Stats() {
	pth := fmt.Sprintf("mcs://%v/write/test.txt", testBucket)
	w, _ := NewWriter(pth, testOption, nil)
	if w == nil {
		return
	}

	w.WriteLine([]byte("test line"))
	w.WriteLine([]byte("test line"))
	sts := w.Stats()

	fmt.Println(sts.Path)          // output: mcs://task-tools-test/write/test.txt
	fmt.Println(sts.ByteCnt)       // output: 20
	fmt.Println(sts.LineCnt)       // output: 2
	fmt.Println(sts.Size)          // output: 0
	fmt.Println(sts.Checksum)      // output:
	fmt.Println(sts.Created != "") // output: true

	// Output:
	// mcs://task-tools-test/write/test.txt
	// 20
	// 2
	// 0
	//
	// true
}

func ExampleWriter_CloseStats() {
	pth := fmt.Sprintf("mcs://%v/write/test.txt", testBucket)
	w, _ := NewWriter(pth, testOption, nil)
	if w == nil {
		return
	}

	w.WriteLine([]byte("test line"))
	w.WriteLine([]byte("test line"))
	w.Close()
	sts := w.Stats()

	fmt.Println(sts.Path)          // output: mcs://task-tools-test/write/test.txt
	fmt.Println(sts.ByteCnt)       // output: 20
	fmt.Println(sts.LineCnt)       // output: 2
	fmt.Println(sts.Size)          // output: 20
	fmt.Println(sts.Checksum)      // output: 54f30d75cf7374c7e524a4530dbc93c2
	fmt.Println(sts.Created != "") // output: true

	// cleanup
	rmTestFile(pth)

	// Output:
	// mcs://task-tools-test/write/test.txt
	// 20
	// 2
	// 20
	// 54f30d75cf7374c7e524a4530dbc93c2
	// true
}

func ExampleWriter_Abort() {
	pth := fmt.Sprintf("mcs://%v/write/test.txt", testBucket)
	w, _ := NewWriter(pth, testOption, nil)
	if w == nil {
		return
	}

	w.WriteLine([]byte("test line"))

	fmt.Println(w.done) // output: false
	err := w.Abort()

	fmt.Println(err)    // output: <nil>
	fmt.Println(w.done) // output: true

	// Output:
	// false
	// <nil>
	// true
}

func ExampleWriter_AbortAndAbort() {
	pth := fmt.Sprintf("mcs://%v/write/test.txt", testBucket)
	w, _ := NewWriter(pth, testOption, nil)
	if w == nil {
		return
	}

	w.WriteLine([]byte("test line"))
	w.Abort()
	err := w.Abort()

	fmt.Println(err)    // output: <nil>
	fmt.Println(w.done) // output: true

	// Output:
	// <nil>
	// true
}

func ExampleWriter_Close() {
	pth := fmt.Sprintf("mcs://%v/write/test.txt", testBucket)
	w, _ := NewWriter(pth, testOption, nil)
	if w == nil {
		return
	}

	w.WriteLine([]byte("test line"))
	w.WriteLine([]byte("test line"))
	err := w.Close()

	fmt.Println(err)                      // output: <nil>
	fmt.Println(w.done)                   // output: true
	fmt.Println(w.Stats().Checksum != "") // output: true

	// cleanup
	rmTestFile(pth)

	// Output:
	// <nil>
	// true
	// true
}

func ExampleWriter_CloseErrCopy() {
	pth := fmt.Sprintf("mcs://%v/write/test.txt", testBucket)
	w, _ := NewWriter(pth, testOption, nil)
	if w == nil {
		return
	}

	w.WriteLine([]byte("test line"))
	w.WriteLine([]byte("test line"))
	w.bucket = ""
	w.objPth = ""
	err := w.Close()

	fmt.Println(err)                                                      // output: Bucket name cannot be empty
	fmt.Println(w.done)                                                   // output: true
	fmt.Println(w.Stats().Checksum == "54f30d75cf7374c7e524a4530dbc93c2") // output: true

	// Output:
	// Bucket name cannot be empty
	// true
	// true
}

func ExampleWriter_CloseAndClose() {
	pth := fmt.Sprintf("mcs://%v/write/test.txt", testBucket)
	w, _ := NewWriter(pth, testOption, nil)
	if w == nil {
		return
	}

	w.WriteLine([]byte("test line"))
	w.WriteLine([]byte("test line"))
	w.Close()
	err := w.Close()

	fmt.Println(err)                      // output: <nil>
	fmt.Println(w.done)                   // output: true
	fmt.Println(w.Stats().Checksum != "") // output: true

	// cleanup
	rmTestFile(pth)

	// Output:
	// <nil>
	// true
	// true
}

func ExampleWriter_AbortAndClose() {
	pth := fmt.Sprintf("mcs://%v/write/test.txt", testBucket)
	w, _ := NewWriter(pth, testOption, nil)
	if w == nil {
		return
	}

	w.WriteLine([]byte("test line"))
	w.Abort()
	err := w.Close()

	fmt.Println(err)                      // output: <nil>
	fmt.Println(w.done)                   // output: true
	fmt.Println(w.Stats().Checksum == "") // output: true

	// Output:
	// <nil>
	// true
	// true
}

func ExampleWriter_CloseAndAbort() {
	pth := fmt.Sprintf("mcs://%v/write/test.txt", testBucket)
	w, _ := NewWriter(pth, testOption, nil)
	if w == nil {
		return
	}

	w.WriteLine([]byte("test line"))
	w.WriteLine([]byte("test line"))
	w.Close()
	err := w.Abort()

	fmt.Println(err)                      // output: <nil>
	fmt.Println(w.done)                   // output: true
	fmt.Println(w.Stats().Checksum != "") // output: true

	// cleanup
	rmTestFile(pth)

	// Output:
	// <nil>
	// true
	// true
}

func ExampleWriter_CopyTmpFile() {
	pth := fmt.Sprintf("mcs://%v/write/test.txt", testBucket)
	opt := buf.NewOptions()
	opt.UseFileBuf = true
	opt.FileBufDir = "./test/tmp"
	opt.FileBufPrefix = "test_"
	w, _ := NewWriter(pth, testOption, opt)
	if w == nil {
		return
	}

	w.WriteLine([]byte("test line"))
	w.WriteLine([]byte("test line"))
	w.bfr.Close()
	n, err := w.copy()
	//w.setObjSts()
	w.bfr.Cleanup()
	sts := w.Stats()
	fmt.Println(n)                 // output: 20
	fmt.Println(err)               // output: <nil>
	fmt.Println(sts.Checksum)      // output: 54f30d75cf7374c7e524a4530dbc93c2
	fmt.Println(sts.Size)          // output: 20
	fmt.Println(sts.Path)          // output: mcs://task-tools-test/write/test.txt
	fmt.Println(sts.Created != "") // output: true

	// cleanup
	rmTestFile(pth)
	os.Remove("./test/tmp")
	os.Remove("./test")

	// Output:
	// 20
	// <nil>
	// 54f30d75cf7374c7e524a4530dbc93c2
	// 20
	// mcs://task-tools-test/write/test.txt
	// true
}

func ExampleWriter_CopyNoExtension() {
	pth := fmt.Sprintf("mcs://%v/write/test", testBucket)
	w, _ := NewWriter(pth, testOption, nil)
	if w == nil {
		return
	}

	w.WriteLine([]byte("test line"))
	w.WriteLine([]byte("test line"))
	w.bfr.Close()
	n, err := w.copy()
	//w.setObjSts()
	w.Close()
	sts := w.Stats()
	fmt.Println(n)                 // output: 20
	fmt.Println(err)               // output: <nil>
	fmt.Println(sts.Checksum)      // output: 54f30d75cf7374c7e524a4530dbc93c2
	fmt.Println(sts.Size)          // output: 20
	fmt.Println(sts.Path)          // output: mcs://task-tools-test/write/test
	fmt.Println(sts.Created != "") // output: true

	// cleanup
	rmTestFile(pth)

	// Output:
	// 20
	// <nil>
	// 54f30d75cf7374c7e524a4530dbc93c2
	// 20
	// mcs://task-tools-test/write/test
	// true
}
