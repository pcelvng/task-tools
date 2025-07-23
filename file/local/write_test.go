package local

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/pcelvng/task-tools/file/buf"
)

func ExampleNewWriter() {
	w, err := NewWriter("./test/test.txt", nil)
	if w == nil {
		return
	}

	fmt.Println(strings.HasSuffix(w.sts.Path(), "/test/test.txt")) // true
	fmt.Println(err)                                               // <nil>

	os.Remove("./test") // cleanup test dir

	// Output:
	// true
	// <nil>
}

func ExampleNewWriter_compression() {
	w, err := NewWriter("./test/test.gz", nil)
	if w == nil {
		return
	}

	fmt.Println(strings.HasSuffix(w.sts.Path(), "/test/test.gz")) // true
	fmt.Println(w.tmpPth)                                         // ''
	fmt.Println(err)                                              // <nil>

	os.Remove("./test") // cleanup test dir

	// Output:
	// true
	//
	// <nil>
}

func ExampleNewWriter_withTmpFile() {
	opt := &buf.Options{
		UseFileBuf:    true,
		FileBufDir:    "./test/tmp/",
		FileBufPrefix: "prefix_",
	}
	w, err := NewWriter("./test/test.txt", opt)
	if w == nil {
		return
	}

	hasPath := strings.HasSuffix(w.sts.Path(), "/test/test.txt")
	hasTmp := strings.Contains(w.tmpPth, "/test/tmp/prefix_")

	fmt.Println(hasPath) // true
	fmt.Println(hasTmp)  // true
	fmt.Println(err)     // <nil>

	os.Remove(w.tmpPth)     // cleanup tmp file
	os.Remove("./test/tmp") // cleanup tmp dir
	os.Remove("./test")     // cleanup test dir

	// Output:
	// true
	// true
	// <nil>
}

func ExampleNewWriter_bufErr() {
	opt := &buf.Options{UseFileBuf: true, FileBufDir: "/private/bad/tmp/dir"}

	w, err := NewWriter("./test/test.txt", opt)
	if err == nil {
		return
	}

	hasDenied := strings.Contains(err.Error(), "permission denied")

	fmt.Println(w)         // <nil>
	fmt.Println(hasDenied) // true

	// Output:
	// <nil>
	// true
}

func ExampleNewWriter_pthCheckErr() {
	w, err := NewWriter("/private/test.txt", nil)
	if err == nil {
		return
	}

	hasDenied := strings.Contains(err.Error(), "permission denied")

	fmt.Println(w)         // <nil>
	fmt.Println(hasDenied) // true

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

	fmt.Println(n)   // 9
	fmt.Println(err) // <nil>

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

	fmt.Println(err) // <nil>

	os.Remove("./test") // cleanup test dir

	// Output:
	// <nil>
}

func ExampleWriter_Abort() {
	w, _ := NewWriter("./test/test.txt", nil)
	if w == nil {
		return
	}
	err := w.Abort()

	fmt.Println(err) // <nil>

	os.Remove("./test") // cleanup test dir

	// Output:
	// <nil>
}

func ExampleWriter_Close() {
	opt := &buf.Options{UseFileBuf: true, FileBufDir: "./test/tmp", FileBufPrefix: "prefix_"}
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

	fmt.Println(err)               // <nil>
	fmt.Println(sts.Created != "") // true
	fmt.Println(sts.Checksum)      // 54f30d75cf7374c7e524a4530dbc93c2
	fmt.Println(removed)           // true

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

func ExampleWriter_Abort_afterClose() {
	w, _ := NewWriter("./test/test.txt", nil)
	if w == nil {
		return
	}
	w.WriteLine([]byte("test line"))
	w.WriteLine([]byte("test line"))
	w.Close()
	err := w.Abort()

	fmt.Println(err) // <nil>

	// cleanup
	os.Remove("./test")

	// Output:
	// <nil>
}

func ExampleWriter_Close_afterAbort() {
	w, _ := NewWriter("./test/test.txt", nil)
	if w == nil {
		return
	}
	w.WriteLine([]byte("test line"))
	w.WriteLine([]byte("test line"))
	w.Abort()
	err := w.Close()

	fmt.Println(err) // <nil>

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
	f, _ := os.Open(w.sts.Path())
	if f == nil {
		return
	}
	b := make([]byte, 20)
	rn, _ := f.Read(b)

	// check actual size
	fInfo, _ := os.Stat(w.sts.Path())
	if fInfo == nil {
		return
	}

	fmt.Println(n)            // 20
	fmt.Println(err)          // <nil>
	fmt.Print(string(b))      // test line, test line
	fmt.Println(rn)           // 20
	fmt.Println(fInfo.Size()) // 20

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
	opt := &buf.Options{UseFileBuf: true, FileBufDir: "./test/tmp"}
	w, _ := NewWriter("./test/test.txt", opt)
	if w == nil {
		return
	}
	w.WriteLine([]byte("test line"))
	w.WriteLine([]byte("test line"))
	n, err := w.copyAndClean()

	// read file
	f, _ := os.Open(w.sts.Path())
	if f == nil {
		return
	}
	b := make([]byte, 20)
	rn, _ := f.Read(b)

	// check actual size
	fInfo, _ := os.Stat(w.sts.Path())
	if fInfo == nil {
		return
	}

	// tmp file does not exist
	_, tmpErr := os.Open(w.tmpPth)
	if tmpErr == nil {
		return
	}
	removed := strings.Contains(tmpErr.Error(), "no such file or directory")

	fmt.Println(n)            // 20
	fmt.Println(err)          // <nil>
	fmt.Print(string(b))      // test line, test line
	fmt.Println(rn)           // 20
	fmt.Println(fInfo.Size()) // 20
	fmt.Println(removed)      // true

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
	opt := &buf.Options{UseFileBuf: true, FileBufDir: "./test/tmp"}
	w, _ := NewWriter("./test/test.txt", opt)
	if w == nil {
		return
	}
	w.WriteLine([]byte("test line"))
	w.WriteLine([]byte("test line"))
	w.sts.SetPath("/bad/file.txt")
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

	fmt.Println(n)          // 0
	fmt.Println(notCopied)  // true
	fmt.Println(tmpRemoved) // true

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
	n, err := w.copyAndClean() // test line, test line

	fmt.Println(n)   // 20
	fmt.Println(err) // <nil>

	// Output:
	// test line
	// test line
	// 20
	// <nil>
}

func ExampleWriter_copyAndCleanTmpFileToDev() {
	// file buffer to device file
	opt := &buf.Options{UseFileBuf: true, FileBufDir: "./test/tmp"}
	w, _ := NewWriter("/dev/stdout", opt)
	if w == nil {
		return
	}
	w.WriteLine([]byte("test line"))
	w.WriteLine([]byte("test line"))
	n, err := w.copyAndClean() // test line, test line

	// tmp file does not exist
	_, tmpErr := os.Open(w.tmpPth)
	if tmpErr == nil {
		return
	}
	removed := strings.Contains(tmpErr.Error(), "no such file or directory")

	fmt.Println(n)       // 20
	fmt.Println(err)     // <nil>
	fmt.Println(removed) // true

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

func TestOpenF_errPerms(t *testing.T) {
	// dir bad perms
	_, _, err := openF("/private/bad/perms/dir/file.txt", false)
	if err != nil && strings.Contains(err.Error(), "permission denied") {
		// Expected error
		return
	}
	t.Error("Expected permission denied error")
}

func TestOpenF_errDir(t *testing.T) {
	// dir bad perms
	_, _, err := openF("/dir/path/", false)
	if err == nil {
		t.Error("Expected error for directory path")
		return
	}
	if !strings.Contains(err.Error(), "references a directory") {
		t.Errorf("Expected directory error, got: %v", err)
	}
}

func TestCloseF_err(t *testing.T) {
	// showing: closeF no err on nil f
	var nilF *os.File
	err := closeF("path.txt", nilF)
	if err != nil {
		t.Errorf("Expected nil error, got: %v", err)
	}
}
