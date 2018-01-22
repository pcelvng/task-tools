package local

import (
	"fmt"
	"os"
	"time"

	"github.com/pcelvng/task-tools/file/util"
)

func ExampleNewOptions() {
	opt := NewOptions()
	if opt == nil {
		return
	}

	fmt.Println(opt.FileBufDir)    // output:
	fmt.Println(opt.UseFileBuf)    // output: false
	fmt.Println(opt.FileBufPrefix) // output:
	fmt.Println(opt.Compress)      // output: false

	// Output:
	//
	// false
	//
	// false
}

func ExampleFileSize() {
	pth, f, _ := util.OpenTmp("./test/", "")

	f.Write([]byte("test line\n"))
	f.Write([]byte("test line\n"))
	f.Close()

	size := fileSize(pth)

	fmt.Println(size) // output: 20

	// cleanup
	os.Remove(pth)
	os.Remove("./test")

	// Output:
	// 20
}

func ExampleFileSizeErr() {
	pth := "./does/not/exist.txt"
	size := fileSize(pth)

	fmt.Println(size) // output: 0

	// Output:
	// 0
}

func ExampleFileCreated() {
	os.Setenv("TZ", "UTC")
	pth, _, _ := util.OpenTmp("./test/", "")

	d := "2018-01-01T01:01:01Z"
	c, _ := time.Parse(time.RFC3339, d)
	os.Chtimes(pth, c, c)
	created := fileCreated(pth)

	fmt.Println(created.Format(time.RFC3339)) // output: 2018-01-01T01:01:01Z

	// cleanup
	os.Remove(pth)
	os.Remove("./test")
	os.Unsetenv("TZ")

	// Output:
	// 2018-01-01T01:01:01Z
}

func ExampleFileCreatedErr() {
	pth := "./does/not/exist.txt"
	created := fileCreated(pth)

	fmt.Println(created.IsZero()) // output: true

	// Output:
	// true
}
