package gs

import (
	"fmt"
	"os"
	"strings"
)

func ExampleNewWriter() {
	pth := fmt.Sprintf("gs://%v/write/test.txt", testBucket)
	w, err := NewWriter(pth, testAccessKey, testSecretKey, nil)
	if w == nil {
		return
	}

	fmt.Println(err)               // output: <nil>
	fmt.Println(w.sts.Path)        // output: gs://task-tools-gstest/write/test.txt
	fmt.Println(w.gsClient != nil) // output: true
	fmt.Println(w.bfr != nil)      // output: true
	fmt.Println(w.bucket)          // output: task-tools-gstest
	fmt.Println(w.objPth)          // output: write/test.txt
	fmt.Println(w.tmpPth == "")    // output: true

	// Output:
	// <nil>
	// gs://task-tools-gstest/write/test.txt
	// true
	// true
	// task-tools-gstest
	// write/test.txt
	// true
}

func ExampleNewWriterTmpFile() {
	pth := fmt.Sprintf("gs://%v/write/test.txt", testBucket)
	opt := NewOptions()
	opt.UseFileBuf = true
	opt.FileBufDir = "./test/tmp"
	opt.FileBufPrefix = "test_"
	w, err := NewWriter(pth, testAccessKey, testSecretKey, opt)
	if w == nil {
		return
	}

	fmt.Println(err)               // output: <nil>
	fmt.Println(w.sts.Path)        // output: gs://task-tools-gstest/write/test.txt
	fmt.Println(w.gsClient != nil) // output: true
	fmt.Println(w.bfr != nil)      // output: true
	fmt.Println(w.bucket)          // output: task-tools-gstest
	fmt.Println(w.objPth)          // output: write/test.txt
	fmt.Println(w.tmpPth != "")    // output: true

	// cleanup
	w.bfr.Cleanup()
	os.Remove("./test/tmp")
	os.Remove("./test")

	// Output:
	// <nil>
	// gs://task-tools-gstest/write/test.txt
	// true
	// true
	// task-tools-gstest
	// write/test.txt
	// true
}

func ExampleNewWriterCompressed() {
	pth := fmt.Sprintf("gs://%v/write/test.gz", testBucket)
	w, err := NewWriter(pth, testAccessKey, testSecretKey, nil)
	if w == nil {
		return
	}

	fmt.Println(err)               // output: <nil>
	fmt.Println(w.sts.Path)        // output: gs://task-tools-gstest/write/test.gz
	fmt.Println(w.gsClient != nil) // output: true
	fmt.Println(w.bfr != nil)      // output: true
	fmt.Println(w.bucket)          // output: task-tools-gstest
	fmt.Println(w.objPth)          // output: write/test.gz
	fmt.Println(w.tmpPth == "")    // output: true

	// Output:
	// <nil>
	// gs://task-tools-gstest/write/test.gz
	// true
	// true
	// task-tools-gstest
	// write/test.gz
	// true
}

func ExampleNewWriterErrBufGS() {
	pth := fmt.Sprintf("gs://%v/write/test.txt", testBucket)
	opt := NewOptions()
	opt.UseFileBuf = true
	opt.FileBufDir = "/private/bad/tmp/dir"
	opt.FileBufPrefix = "test_"
	w, err := NewWriter(pth, testAccessKey, testSecretKey, opt)
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

func ExampleNewWriterErrBadClient() {
	origHost := StoreHost
	StoreHost = "bad/endpoint/"
	w, err := NewWriter("", "", "", nil)
	if err == nil {
		return
	}

	fmt.Println(w)   // output: <nil>
	fmt.Println(err) // output: Endpoint: bad/endpoint/ does not follow ip address or domain name standards.

	// restore endpoint
	StoreHost = origHost

	// Output:
	// <nil>
	// Endpoint: bad/endpoint/ does not follow ip address or domain name standards.
}

func ExampleWriter_Write() {
	pth := fmt.Sprintf("gs://%v/write/test.txt", testBucket)
	w, _ := NewWriter(pth, testAccessKey, testSecretKey, nil)
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
	pth := fmt.Sprintf("gs://%v/write/test.txt", testBucket)
	w, _ := NewWriter(pth, testAccessKey, testSecretKey, nil)
	if w == nil {
		return
	}

	err := w.WriteLine([]byte("test line"))

	fmt.Println(err) // output: <nil>

	// Output:
	// <nil>
}

func ExampleWriter_Stats() {
	pth := fmt.Sprintf("gs://%v/write/test.txt", testBucket)
	w, _ := NewWriter(pth, testAccessKey, testSecretKey, nil)
	if w == nil {
		return
	}

	w.WriteLine([]byte("test line"))
	w.WriteLine([]byte("test line"))
	sts := w.Stats()

	fmt.Println(sts.Path)          // output: gs://task-tools-gstest/write/test.txt
	fmt.Println(sts.ByteCnt)       // output: 20
	fmt.Println(sts.LineCnt)       // output: 2
	fmt.Println(sts.Size)          // output: 0
	fmt.Println(sts.Checksum)      // output:
	fmt.Println(sts.Created == "") // output: true

	// Output:
	// gs://task-tools-gstest/write/test.txt
	// 20
	// 2
	// 0
	//
	// true
}

func ExampleWriter_CloseStats() {
	pth := fmt.Sprintf("gs://%v/write/test.txt", testBucket)
	w, _ := NewWriter(pth, testAccessKey, testSecretKey, nil)
	if w == nil {
		return
	}

	w.WriteLine([]byte("test line"))
	w.WriteLine([]byte("test line"))
	w.Close()
	sts := w.Stats()

	fmt.Println(sts.Path)          // output: gs://task-tools-gstest/write/test.txt
	fmt.Println(sts.ByteCnt)       // output: 20
	fmt.Println(sts.LineCnt)       // output: 2
	fmt.Println(sts.Size)          // output: 20
	fmt.Println(sts.Checksum)      // output: 54f30d75cf7374c7e524a4530dbc93c2
	fmt.Println(sts.Created != "") // output: true

	// cleanup
	rmTestFile(pth)

	// Output:
	// gs://task-tools-gstest/write/test.txt
	// 20
	// 2
	// 20
	// 54f30d75cf7374c7e524a4530dbc93c2
	// true
}

func ExampleWriter_Abort() {
	pth := fmt.Sprintf("gs://%v/write/test.txt", testBucket)
	w, _ := NewWriter(pth, testAccessKey, testSecretKey, nil)
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
	pth := fmt.Sprintf("gs://%v/write/test.txt", testBucket)
	w, _ := NewWriter(pth, testAccessKey, testSecretKey, nil)
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
	pth := fmt.Sprintf("gs://%v/write/test.txt", testBucket)
	w, _ := NewWriter(pth, testAccessKey, testSecretKey, nil)
	if w == nil {
		return
	}

	w.WriteLine([]byte("test line"))
	w.WriteLine([]byte("test line"))
	err := w.Close()

	fmt.Println(err)                     // output: <nil>
	fmt.Println(w.done)                  // output: true
	fmt.Println(w.objSts.Checksum != "") // output: true

	// cleanup
	rmTestFile(pth)

	// Output:
	// <nil>
	// true
	// true
}

func ExampleWriter_CloseErrCopy() {
	pth := fmt.Sprintf("gs://%v/write/test.txt", testBucket)
	w, _ := NewWriter(pth, testAccessKey, testSecretKey, nil)
	if w == nil {
		return
	}

	w.WriteLine([]byte("test line"))
	w.WriteLine([]byte("test line"))
	w.bucket = ""
	w.objPth = ""
	err := w.Close()

	fmt.Println(err)                     // output: Bucket name cannot be empty
	fmt.Println(w.done)                  // output: true
	fmt.Println(w.objSts.Checksum == "") // output: true

	// Output:
	// Bucket name cannot be empty
	// true
	// true
}

func ExampleWriter_CloseAndClose() {
	pth := fmt.Sprintf("gs://%v/write/test.txt", testBucket)
	w, _ := NewWriter(pth, testAccessKey, testSecretKey, nil)
	if w == nil {
		return
	}

	w.WriteLine([]byte("test line"))
	w.WriteLine([]byte("test line"))
	w.Close()
	err := w.Close()

	fmt.Println(err)                     // output: <nil>
	fmt.Println(w.done)                  // output: true
	fmt.Println(w.objSts.Checksum != "") // output: true

	// cleanup
	rmTestFile(pth)

	// Output:
	// <nil>
	// true
	// true
}

func ExampleWriter_AbortAndClose() {
	pth := fmt.Sprintf("gs://%v/write/test.txt", testBucket)
	w, _ := NewWriter(pth, testAccessKey, testSecretKey, nil)
	if w == nil {
		return
	}

	w.WriteLine([]byte("test line"))
	w.Abort()
	err := w.Close()

	fmt.Println(err)                     // output: <nil>
	fmt.Println(w.done)                  // output: true
	fmt.Println(w.objSts.Checksum == "") // output: true

	// Output:
	// <nil>
	// true
	// true
}

func ExampleWriter_CloseAndAbort() {
	pth := fmt.Sprintf("gs://%v/write/test.txt", testBucket)
	w, _ := NewWriter(pth, testAccessKey, testSecretKey, nil)
	if w == nil {
		return
	}

	w.WriteLine([]byte("test line"))
	w.WriteLine([]byte("test line"))
	w.Close()
	err := w.Abort()

	fmt.Println(err)                     // output: <nil>
	fmt.Println(w.done)                  // output: true
	fmt.Println(w.objSts.Checksum != "") // output: true

	// cleanup
	rmTestFile(pth)

	// Output:
	// <nil>
	// true
	// true
}

func ExampleWriter_CopyTmpFile() {
	pth := fmt.Sprintf("gs://%v/write/test.txt", testBucket)
	opt := NewOptions()
	opt.UseFileBuf = true
	opt.FileBufDir = "./test/tmp"
	opt.FileBufPrefix = "test_"
	w, _ := NewWriter(pth, testAccessKey, testSecretKey, opt)
	if w == nil {
		return
	}

	w.WriteLine([]byte("test line"))
	w.WriteLine([]byte("test line"))
	w.bfr.Close()
	n, err := w.copy()
	w.setObjSts()
	w.bfr.Cleanup()

	fmt.Println(n)                      // output: 20
	fmt.Println(err)                    // output: <nil>
	fmt.Println(w.objSts.Checksum)      // output: c57894a47304e4082679f4649441fa7a
	fmt.Println(w.objSts.Size)          // output: 20
	fmt.Println(w.objSts.Path)          // output: write/test.txt
	fmt.Println(w.objSts.Created != "") // output: true

	// cleanup
	rmTestFile(pth)
	os.Remove("./test/tmp")
	os.Remove("./test")

	// Output:
	// 20
	// <nil>
	// c57894a47304e4082679f4649441fa7a
	// 20
	// write/test.txt
	// true
}

func ExampleWriter_CopyNoExtension() {
	pth := fmt.Sprintf("gs://%v/write/test", testBucket)
	w, _ := NewWriter(pth, testAccessKey, testSecretKey, nil)
	if w == nil {
		return
	}

	w.WriteLine([]byte("test line"))
	w.WriteLine([]byte("test line"))
	w.bfr.Close()
	n, err := w.copy()
	w.setObjSts()

	fmt.Println(n)                      // output: 20
	fmt.Println(err)                    // output: <nil>
	fmt.Println(w.objSts.Checksum)      // output: 54f30d75cf7374c7e524a4530dbc93c2
	fmt.Println(w.objSts.Size)          // output: 20
	fmt.Println(w.objSts.Path)          // output: write/test
	fmt.Println(w.objSts.Created != "") // output: true

	// cleanup
	rmTestFile(pth)

	// Output:
	// 20
	// <nil>
	// 54f30d75cf7374c7e524a4530dbc93c2
	// 20
	// write/test
	// true
}

func ExampleWriter_SetObjSts() {
	pth := fmt.Sprintf("gs://%v/write/test.txt", testBucket)
	w, _ := NewWriter(pth, testAccessKey, testSecretKey, nil)
	if w == nil {
		return
	}

	w.WriteLine([]byte("test line"))
	w.WriteLine([]byte("test line"))
	w.bfr.Close()
	w.copy()
	err := w.setObjSts()

	fmt.Println(err)                    // output: <nil>
	fmt.Println(w.objSts.Checksum)      // output: c57894a47304e4082679f4649441fa7a
	fmt.Println(w.objSts.Size)          // output: 20
	fmt.Println(w.objSts.Path)          // output: write/test.txt
	fmt.Println(w.objSts.Created != "") // output: true

	// cleanup
	rmTestFile(pth)

	// Output:
	// <nil>
	// c57894a47304e4082679f4649441fa7a
	// 20
	// write/test.txt
	// true
}

func ExampleWriter_SetObjStsErr() {
	pth := fmt.Sprintf("gs://%v/write/test.txt", testBucket)
	w, _ := NewWriter(pth, testAccessKey, testSecretKey, nil)
	if w == nil {
		return
	}

	w.WriteLine([]byte("test line"))
	w.WriteLine([]byte("test line"))
	w.bucket = ""
	w.objPth = ""
	err := w.setObjSts()

	fmt.Println(err)                    // output: Bucket name cannot be empty
	fmt.Println(w.objSts.Checksum)      // output:
	fmt.Println(w.objSts.Size)          // output: 0
	fmt.Println(w.objSts.Path)          // output:
	fmt.Println(w.objSts.Created == "") // output: true

	// Output:
	// Bucket name cannot be empty
	//
	// 0
	//
	// true
}
