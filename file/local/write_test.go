package local

import (
	"fmt"
	"os"
	"strings"
)

func ExampleNewWriter() {
	w, err := NewWriter("./test/test.txt", nil)
	if w == nil {
		return
	}

	fmt.Println(strings.HasSuffix(w.sts.Path, "/test/test.txt")) // output: true
	fmt.Println(err)                                             // output: <nil>

	os.Remove("./test") // cleanup test dir

	// Output:
	// true
	// <nil>
}

func ExampleNewWriterCompression() {
	w, err := NewWriter("./test/test.gz", nil)
	if w == nil {
		return
	}

	fmt.Println(strings.HasSuffix(w.sts.Path, "/test/test.gz")) // output: true
	fmt.Println(w.tmpPth)                                       // output:
	fmt.Println(err)                                            // output: <nil>

	os.Remove("./test") // cleanup test dir

	// Output:
	// true
	//
	// <nil>
}

func ExampleNewWriterWTmpFile() {
	opt := NewOptions()
	opt.UseFileBuf = true
	opt.FileBufDir = "./test/tmp/"
	opt.FileBufPrefix = "prefix_"
	w, err := NewWriter("./test/test.txt", opt)
	if w == nil {
		return
	}

	hasPath := strings.HasSuffix(w.sts.Path, "/test/test.txt")
	hasTmp := strings.Contains(w.tmpPth, "/test/tmp/prefix_")

	fmt.Println(hasPath) // output: true
	fmt.Println(hasTmp)  // output: true
	fmt.Println(err)     // output: <nil>

	os.Remove(w.tmpPth)     // cleanup tmp file
	os.Remove("./test/tmp") // cleanup tmp dir
	os.Remove("./test")     // cleanup test dir

	// Output:
	// true
	// true
	// <nil>
}

func ExampleNewWriterBufErr() {
	opt := NewOptions()
	opt.UseFileBuf = true
	opt.FileBufDir = "/bad/tmp/dir"
	w, err := NewWriter("./test/test.txt", opt)
	if err == nil {
		return
	}

	hasDenied := strings.Contains(err.Error(), "permission denied")

	fmt.Println(w)         // output: <nil>
	fmt.Println(hasDenied) // output: true

	// Output:
	// <nil>
	// true
}

func ExampleNewWriterPthCheckErr() {
	w, err := NewWriter("/bad/path/test.txt", nil)
	if err == nil {
		return
	}

	hasDenied := strings.Contains(err.Error(), "permission denied")

	fmt.Println(w)         // output: <nil>
	fmt.Println(hasDenied) // output: true

	// Output:
	// <nil>
	// true
}

func ExampleWriter_Write() {
	w, _ := NewWriter("./test/test.txt", nil)
	if w == nil {
		return
	}

	n, err := w.Write([]byte("test line"))

	fmt.Println(n)   // output: 9
	fmt.Println(err) // output: <nil>

	os.Remove("./test") // cleanup test dir

	// Output:
	// 9
	// <nil>
}

func ExampleWriter_WriteLine() {
	w, _ := NewWriter("./test/test.txt", nil)
	if w == nil {
		return
	}

	err := w.WriteLine([]byte("test line"))

	fmt.Println(err) // output: <nil>

	os.Remove("./test") // cleanup test dir

	// Output:
	// <nil>
}

func ExampleWriter_Stats() {
	w, _ := NewWriter("./test/test.txt", nil)
	if w == nil {
		return
	}
	w.sts.Path = "test path"
	w.sts.Created = "test created"

	sts := w.Stats()
	fmt.Println(sts.Path)    // output: test path
	fmt.Println(sts.Created) // output: test created

	os.Remove("./test") // cleanup test dir

	// Output:
	// test path
	// test created
}

func ExampleWriter_Abort() {
	w, _ := NewWriter("./test/test.txt", nil)
	if w == nil {
		return
	}
	err := w.Abort()

	fmt.Println(err) // output: <nil>

	os.Remove("./test") // cleanup test dir

	// Output:
	// <nil>
}

func ExampleWriter_Close() {
	opt := NewOptions()
	opt.UseFileBuf = true
	opt.FileBufDir = "./test/tmp"
	opt.FileBufPrefix = "prefix_"
	w, _ := NewWriter("./test/test.txt", opt)
	if w == nil {
		return
	}
	w.WriteLine([]byte("test line"))
	w.WriteLine([]byte("test line"))
	err := w.Close()
	sts := w.Stats()

	// check that tmp file was cleaned up
	_, tmpErr := os.Open(w.tmpPth)
	if tmpErr == nil {
		return
	}
	removed := strings.Contains(tmpErr.Error(), "no such file or directory")

	fmt.Println(err)               // output: <nil>
	fmt.Println(sts.Created != "") // output: true
	fmt.Println(sts.Checksum)      // output: 54f30d75cf7374c7e524a4530dbc93c2
	fmt.Println(removed)           // output: true

	// cleanup
	os.Remove("./test/tmp")
	os.Remove("./test/test.txt")
	os.Remove("./test")

	// Output:
	// <nil>
	// true
	// 54f30d75cf7374c7e524a4530dbc93c2
	// true
}

func ExampleWriter_AbortAfterClose() {
	w, _ := NewWriter("./test/test.txt", nil)
	if w == nil {
		return
	}
	w.WriteLine([]byte("test line"))
	w.WriteLine([]byte("test line"))
	w.Close()
	err := w.Abort()

	fmt.Println(err) // output: <nil>

	// cleanup
	os.Remove("./test")

	// Output:
	// <nil>
}

func ExampleWriter_CloseAfterAbort() {
	w, _ := NewWriter("./test/test.txt", nil)
	if w == nil {
		return
	}
	w.WriteLine([]byte("test line"))
	w.WriteLine([]byte("test line"))
	w.Abort()
	err := w.Close()

	fmt.Println(err) // output: <nil>

	// cleanup
	os.Remove("./test")

	// Output:
	// <nil>
}

func ExampleWriter_copyAndClean() {
	// memory buffer to file
	w, _ := NewWriter("./test/test.txt", nil)
	if w == nil {
		return
	}
	w.WriteLine([]byte("test line"))
	w.WriteLine([]byte("test line"))
	n, err := w.copyAndClean()

	// read file
	f, _ := os.Open(w.sts.Path)
	if f == nil {
		return
	}
	b := make([]byte, 20)
	rn, _ := f.Read(b)

	// check actual size
	fInfo, _ := os.Stat(w.sts.Path)
	if fInfo == nil {
		return
	}

	fmt.Println(n)            // output: 20
	fmt.Println(err)          // output: <nil>
	fmt.Print(string(b))      // output: test line, test line
	fmt.Println(rn)           // output: 20
	fmt.Println(fInfo.Size()) // output: 20

	// cleanup
	os.Remove("./test")

	// Output:
	// 20
	// <nil>
	// test line
	// test line
	// 20
	// 20
}

func ExampleWriter_copyAndCleanTmpFile() {
	// file buffer to file
	opt := NewOptions()
	opt.UseFileBuf = true
	opt.FileBufDir = "./test/tmp"
	w, _ := NewWriter("./test/test.txt", opt)
	if w == nil {
		return
	}
	w.WriteLine([]byte("test line"))
	w.WriteLine([]byte("test line"))
	n, err := w.copyAndClean()

	// read file
	f, _ := os.Open(w.sts.Path)
	if f == nil {
		return
	}
	b := make([]byte, 20)
	rn, _ := f.Read(b)

	// check actual size
	fInfo, _ := os.Stat(w.sts.Path)
	if fInfo == nil {
		return
	}

	// tmp file does not exist
	_, tmpErr := os.Open(w.tmpPth)
	if tmpErr == nil {
		return
	}
	removed := strings.Contains(tmpErr.Error(), "no such file or directory")

	fmt.Println(n)            // output: 20
	fmt.Println(err)          // output: <nil>
	fmt.Print(string(b))      // output: test line, test line
	fmt.Println(rn)           // output: 20
	fmt.Println(fInfo.Size()) // output: 20
	fmt.Println(removed)      // output: true

	// cleanup
	os.Remove("./test/tmp")
	os.Remove("./test/test.txt")
	os.Remove("./test")

	// Output:
	// 20
	// <nil>
	// test line
	// test line
	// 20
	// 20
	// true
}

func ExampleWriter_copyAndCleanTmpFileErr() {
	// file buffer to file
	opt := NewOptions()
	opt.UseFileBuf = true
	opt.FileBufDir = "./test/tmp"
	w, _ := NewWriter("./test/test.txt", opt)
	if w == nil {
		return
	}
	w.WriteLine([]byte("test line"))
	w.WriteLine([]byte("test line"))
	w.sts.Path = "/bad/file.txt"
	n, err := w.copyAndClean()
	if err == nil {
		return
	}
	notCopied := strings.Contains(err.Error(), "no such")

	// tmp file does not exist
	_, tmpErr := os.Open(w.tmpPth)
	if tmpErr == nil {
		return
	}
	tmpRemoved := strings.Contains(tmpErr.Error(), "no such file or directory")

	fmt.Println(n)          // output: 0
	fmt.Println(notCopied)  // output: true
	fmt.Println(tmpRemoved) // output: true

	// cleanup
	os.Remove("./test/tmp")
	os.Remove("./test")

	// Output:
	// 0
	// true
	// true
}

func ExampleWriter_copyAndCleanToDev() {
	// memory buffer to device file
	w, _ := NewWriter("/dev/stdout", nil)
	if w == nil {
		return
	}
	w.WriteLine([]byte("test line"))
	w.WriteLine([]byte("test line"))
	n, err := w.copyAndClean() // output: test line, test line

	fmt.Println(n)   // output: 20
	fmt.Println(err) // output: <nil>

	// Output:
	// test line
	// test line
	// 20
	// <nil>
}

func ExampleWriter_copyAndCleanTmpFileToDev() {
	// file buffer to device file
	opt := NewOptions()
	opt.UseFileBuf = true
	opt.FileBufDir = "./test/tmp"
	w, _ := NewWriter("/dev/stdout", opt)
	if w == nil {
		return
	}
	w.WriteLine([]byte("test line"))
	w.WriteLine([]byte("test line"))
	n, err := w.copyAndClean() // output: test line, test line

	// tmp file does not exist
	_, tmpErr := os.Open(w.tmpPth)
	if tmpErr == nil {
		return
	}
	removed := strings.Contains(tmpErr.Error(), "no such file or directory")

	fmt.Println(n)       // output: 20
	fmt.Println(err)     // output: <nil>
	fmt.Println(removed) // output: true

	// cleanup
	os.Remove("./test/tmp")
	os.Remove("./test")

	// Output:
	// test line
	// test line
	// 20
	// <nil>
	// true
}

func ExampleOpenf_ErrPerms() {
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

func ExampleOpenf_ErrDir() {
	// dir bad perms
	_, _, err := openF("/dir/path/", false)

	fmt.Println(err) // output: path /dir/path/: references a directory

	// Output:
	// path /dir/path/: references a directory
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
