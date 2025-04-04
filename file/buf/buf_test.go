package buf

import (
	"compress/gzip"
	"fmt"
	"os"
	"strings"
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

func ExampleNewBuffer() {
	bfr, err := NewBuffer(nil)
	if bfr == nil {
		return
	}

	fmt.Println(err)                     // output: <nil>
	fmt.Println(bfr.sts.Created() != "") // output: true
	fmt.Println(bfr.w != nil)            // output true
	fmt.Println(bfr.wGzip)               // output <nil>
	fmt.Println(bfr.wSize != nil)        // output true
	fmt.Println(bfr.bBuf != nil)         // output: true
	fmt.Println(bfr.fBuf)                // output <nil>
	fmt.Println(bfr.r != nil)            // output true
	fmt.Println(bfr.hshr != nil)         // output true

	// Output:
	// <nil>
	// true
	// true
	// <nil>
	// true
	// true
	// <nil>
	// true
	// true
}

func ExampleNewBuffer_TmpFile() {
	opt := NewOptions()
	opt.UseFileBuf = true
	opt.FileBufDir = "./tmp"
	opt.FileBufPrefix = "tmpprefix_"

	bfr, err := NewBuffer(opt)
	if bfr == nil {
		return
	}

	fmt.Println(err)                                                 // output: <nil>
	fmt.Println(bfr.sts.Created() != "")                             // output: true
	fmt.Println(strings.Contains(bfr.sts.Path(), "/tmp/tmpprefix_")) // output: true
	fmt.Println(bfr.w != nil)                                        // output true
	fmt.Println(bfr.wGzip)                                           // output <nil>
	fmt.Println(bfr.wSize != nil)                                    // output true
	fmt.Println(bfr.bBuf)                                            // output: <nil>
	fmt.Println(bfr.fBuf != nil)                                     // output true
	fmt.Println(bfr.r != nil)                                        // output true
	fmt.Println(bfr.hshr != nil)                                     // output true

	os.Remove(bfr.sts.Path()) // cleanup tmp file
	os.Remove("./tmp")        // remove dir

	// Output:
	// <nil>
	// true
	// true
	// true
	// <nil>
	// true
	// <nil>
	// true
	// true
	// true
}

func ExampleNewBuffer_TmpFileErr() {
	opt := NewOptions()
	opt.UseFileBuf = true
	opt.FileBufDir = "/private"
	opt.FileBufPrefix = "tmpprefix_"

	bfr, err := NewBuffer(opt)
	if err == nil {
		return
	}

	fmt.Println(bfr)                                                // output: <nil>
	fmt.Println(strings.Contains(err.Error(), "permission denied")) // output: true

	// Output:
	// <nil>
	// true

}

func ExampleNewBuffer_Compression() {
	opt := NewOptions()
	opt.Compress = true

	bfr, err := NewBuffer(opt)
	if bfr == nil {
		return
	}

	fmt.Println(err)                     // output: <nil>
	fmt.Println(bfr.sts.Created() != "") // output: true
	fmt.Println(bfr.w != nil)            // output true
	fmt.Println(bfr.wGzip != nil)        // output true
	fmt.Println(bfr.wSize != nil)        // output true
	fmt.Println(bfr.bBuf != nil)         // output: true
	fmt.Println(bfr.fBuf)                // output <nil>
	fmt.Println(bfr.r != nil)            // output true
	fmt.Println(bfr.hshr != nil)         // output true

	// Output:
	// <nil>
	// true
	// true
	// true
	// true
	// true
	// <nil>
	// true
	// true
}

func ExampleBuffer_Read() {
	bfr, _ := NewBuffer(nil)
	if bfr == nil {
		return
	}

	// write first
	bfr.Write([]byte("test line\n"))
	bfr.Write([]byte("test line\n"))

	// read
	b := make([]byte, 20)
	bfr.Read(b)

	fmt.Print(string(b)) // output: test line, test line

	// Output:
	// test line
	// test line
}

func ExampleBuffer_ReadCompressed() {
	opt := NewOptions()
	opt.Compress = true
	bfr, _ := NewBuffer(opt)
	if bfr == nil {
		return
	}

	// write first
	bfr.Write([]byte("test line\n"))
	bfr.Write([]byte("test line\n"))
	bfr.Close()

	// read with decompressor
	b := make([]byte, 20)
	r, _ := gzip.NewReader(bfr)
	r.Read(b)

	fmt.Print(string(b)) // output: test line, test line

	// Output:
	// test line
	// test line
}

func ExampleBuffer_ReadTmpFile() {
	opt := NewOptions()
	opt.UseFileBuf = true
	opt.FileBufDir = "./tmp"
	opt.FileBufPrefix = "tmpprefix_"

	bfr, _ := NewBuffer(opt)
	if bfr == nil {
		return
	}

	// write first
	bfr.Write([]byte("test line\n"))
	bfr.Write([]byte("test line\n"))
	bfr.Close()

	// read
	b := make([]byte, 20)
	bfr.Read(b)

	fmt.Print(string(b)) // output: test line, test line

	os.Remove(bfr.sts.Path()) // cleanup tmp file
	os.Remove("./tmp")        // remove dir

	// Output:
	// test line
	// test line
}

func ExampleBuffer_ReadTmpFileCompressed() {
	opt := NewOptions()
	opt.UseFileBuf = true
	opt.FileBufDir = "./tmp"
	opt.FileBufPrefix = "tmpprefix_"
	opt.Compress = true

	bfr, _ := NewBuffer(opt)
	if bfr == nil {
		return
	}

	// write first
	bfr.Write([]byte("test line\n"))
	bfr.Write([]byte("test line\n"))
	bfr.Close()

	// read with decompressor
	b := make([]byte, 20)
	r, _ := gzip.NewReader(bfr)
	r.Read(b)

	fmt.Print(string(b)) // output: test line, test line

	os.Remove(bfr.sts.Path()) // cleanup tmp file
	os.Remove("./tmp")        // remove dir

	// Output:
	// test line
	// test line
}

func ExampleBuffer_ReadAfterCleanup() {
	bfr, _ := NewBuffer(nil)
	if bfr == nil {
		return
	}

	// write first
	bfr.Write([]byte("test line\n"))
	bfr.Write([]byte("test line\n"))
	bfr.Close()

	// resets/removes the underlying buffer.
	// after calling Cleanup there is nothing
	// left to read. So read always returns EOF.
	bfr.Cleanup()

	// read
	b := make([]byte, 1)
	n, err := bfr.Read(b) // nothing should be read in

	fmt.Println(n)    // output: 0
	fmt.Println(err)  // output: EOF
	fmt.Println(b[0]) // output: 0

	// read again - same result
	b = make([]byte, 1)
	n, err = bfr.Read(b) // nothing should be read in

	fmt.Println(n)    // output: 0
	fmt.Println(err)  // output: EOF
	fmt.Println(b[0]) // output: 0

	// Output:
	// 0
	// EOF
	// 0
	// 0
	// EOF
	// 0
}

func ExampleBuffer_Write() {
	bfr, _ := NewBuffer(nil)
	if bfr == nil {
		return
	}

	// write first
	n1, err1 := bfr.Write([]byte("test line\n"))
	n2, err2 := bfr.Write([]byte("test line\n"))

	// read
	b := make([]byte, 20)
	bfr.Read(b)

	fmt.Println(n1)               // output: 10
	fmt.Println(err1)             // output: <nil>
	fmt.Println(n2)               // output: 10
	fmt.Println(err2)             // output: <nil>
	fmt.Print(string(b))          // output: test line, test line
	fmt.Println(bfr.sts.ByteCnt)  // output: 20
	fmt.Println(bfr.sts.LineCnt)  // output: 0
	fmt.Println(bfr.wSize.Size()) // output: 20

	// Output:
	// 10
	// <nil>
	// 10
	// <nil>
	// test line
	// test line
	// 20
	// 0
	// 20
}

func ExampleBuffer_WriteLine() {
	bfr, _ := NewBuffer(nil)
	if bfr == nil {
		return
	}

	// write first
	err1 := bfr.WriteLine([]byte("test line"))
	err2 := bfr.WriteLine([]byte("test line"))

	// read
	b := make([]byte, 20)
	bfr.Read(b)

	fmt.Println(err1)             // output: <nil>
	fmt.Println(err2)             // output: <nil>
	fmt.Print(string(b))          // output: test line, test line
	fmt.Println(bfr.sts.ByteCnt)  // output: 20
	fmt.Println(bfr.sts.LineCnt)  // output: 2
	fmt.Println(bfr.wSize.Size()) // output: 20

	// Output:
	// <nil>
	// <nil>
	// test line
	// test line
	// 20
	// 2
	// 20
}

func ExampleBuffer_Stats() {
	bfr, _ := NewBuffer(nil)
	if bfr == nil {
		return
	}

	// write first
	bfr.WriteLine([]byte("test line"))
	bfr.WriteLine([]byte("test line"))
	sts := bfr.Stats()

	fmt.Println(sts.ByteCnt) // output: 20
	fmt.Println(sts.LineCnt) // output: 2

	// Output:
	// 20
	// 2
}

func ExampleBuffer_Abort() {
	bfr, _ := NewBuffer(nil)
	if bfr == nil {
		return
	}

	// write first
	bfr.WriteLine([]byte("test line"))
	bfr.WriteLine([]byte("test line"))
	err := bfr.Abort()

	fmt.Println(err) // output: <nil>

	// Output:
	// <nil>
}

func ExampleBuffer_AbortCompression() {
	opt := NewOptions()
	opt.Compress = true

	bfr, _ := NewBuffer(opt)
	if bfr == nil {
		return
	}

	// write first
	bfr.WriteLine([]byte("test line"))
	bfr.WriteLine([]byte("test line"))

	err := bfr.Abort()
	fmt.Println(err) // output: <nil>

	// Output:
	// <nil>
}

func ExampleBuffer_Cleanup() {
	bfr, _ := NewBuffer(nil)
	if bfr == nil {
		return
	}

	// write first
	bfr.WriteLine([]byte("test line"))
	bfr.WriteLine([]byte("test line"))

	err := bfr.Cleanup()

	// bytes should be re-set
	// so reading from bBuf should return EOF
	b := make([]byte, 1)
	n, bErr := bfr.bBuf.Read(b)

	fmt.Println(err)  // output: <nil>
	fmt.Println(n)    // output: 0
	fmt.Println(bErr) // output: EOF

	// Output:
	// <nil>
	// 0
	// EOF
}

func ExampleBuffer_CleanupTmpFile() {
	opt := NewOptions()
	opt.UseFileBuf = true
	opt.FileBufDir = "./tmp"
	opt.FileBufPrefix = "tmpprefix_"

	bfr, _ := NewBuffer(opt)
	if bfr == nil {
		return
	}

	// write first
	bfr.WriteLine([]byte("test line"))
	bfr.WriteLine([]byte("test line"))

	err := bfr.Cleanup()

	// check if tmp file exists
	f, oErr := os.Open(bfr.sts.Path())
	if oErr == nil {
		return
	}

	fmt.Println(err) // output: <nil>
	fmt.Println(f)   // output: <nil>
	fmt.Println(strings.Contains(
		oErr.Error(),
		"no such file or directory")) // output: true

	os.Remove("./tmp") // cleanup dir

	// Output:
	// <nil>
	// <nil>
	// true
}

func ExampleBuffer_Close() {
	bfr, _ := NewBuffer(nil)
	if bfr == nil {
		return
	}

	// write first
	bfr.WriteLine([]byte("test line"))
	bfr.WriteLine([]byte("test line"))

	fmt.Println(bfr.sts.Checksum() == "") // output: true
	fmt.Println(bfr.sts.Size)             // output: 0

	err := bfr.Close()

	fmt.Println(err)                      // output: <nil>
	fmt.Println(bfr.sts.Checksum() != "") // output: true
	fmt.Println(bfr.sts.Size)             // output: 20

	// closing again has no effect
	err = bfr.Close()

	fmt.Println(err)                      // output: <nil>
	fmt.Println(bfr.sts.Checksum() != "") // output: true
	fmt.Println(bfr.sts.Size)             // output: 20

	// Output:
	// true
	// 0
	// <nil>
	// true
	// 20
	// <nil>
	// true
	// 20
}

func ExampleBuffer_CloseSizeCompressed() {
	opt := NewOptions()
	opt.Compress = true
	bfr, _ := NewBuffer(opt)
	if bfr == nil {
		return
	}

	// write first
	bfr.WriteLine([]byte("test line"))
	bfr.WriteLine([]byte("test line"))
	bfr.Close()

	// ByteCnt and Size are different
	// because of compression.
	fmt.Println(bfr.sts.ByteCnt) // output: 20
	fmt.Println(bfr.sts.Size)    // output: 48

	// Output:
	// 20
	// 48
}

func ExampleBuffer_CloseTmpFileSizeCompressed() {
	opt := NewOptions()
	opt.UseFileBuf = true
	opt.FileBufDir = "./tmp"
	opt.FileBufPrefix = "tmpprefix_"
	opt.Compress = true
	bfr, _ := NewBuffer(opt)
	if bfr == nil {
		return
	}

	// write first
	bfr.WriteLine([]byte("test line"))
	bfr.WriteLine([]byte("test line"))
	bfr.Close()

	// ByteCnt and Size are different
	// because of compression.
	fmt.Println(bfr.sts.ByteCnt) // output: 20
	fmt.Println(bfr.sts.Size)    // output: 48

	os.Remove(bfr.sts.Path()) // cleanup tmp file
	os.Remove("./tmp")        // remove dir

	// Output:
	// 20
	// 48
}

func ExampleBuffer_CloseChecksum() {
	bfr, _ := NewBuffer(nil)
	if bfr == nil {
		return
	}

	// write first
	bfr.WriteLine([]byte("test line"))
	bfr.WriteLine([]byte("test line"))
	bfr.Close()

	fmt.Println(bfr.sts.Checksum) // output: 54f30d75cf7374c7e524a4530dbc93c2

	// Output:
	// 54f30d75cf7374c7e524a4530dbc93c2
}

func ExampleBuffer_CloseChecksumCompressed() {
	opt := NewOptions()
	opt.Compress = true

	bfr, _ := NewBuffer(opt)
	if bfr == nil {
		return
	}

	// write first
	bfr.WriteLine([]byte("test line"))
	bfr.WriteLine([]byte("test line"))
	bfr.Close()

	fmt.Println(bfr.sts.Checksum) // output: 42e649f9834028184ec21940d13a300f

	// Output:
	// 42e649f9834028184ec21940d13a300f
}

func ExampleBuffer_CloseChecksumTmpFile() {
	opt := NewOptions()
	opt.UseFileBuf = true
	opt.FileBufDir = "./tmp"
	opt.FileBufPrefix = "tmpprefix_"

	bfr, _ := NewBuffer(opt)
	if bfr == nil {
		return
	}

	// write first
	bfr.WriteLine([]byte("test line"))
	bfr.WriteLine([]byte("test line"))
	bfr.Close()

	fmt.Println(bfr.sts.Checksum) // output: 54f30d75cf7374c7e524a4530dbc93c2

	os.Remove(bfr.sts.Path()) // cleanup tmp file
	os.Remove("./tmp")        // remove dir

	// Output:
	// 54f30d75cf7374c7e524a4530dbc93c2
}

func ExampleBuffer_CloseChecksumTmpFileCompressed() {
	opt := NewOptions()
	opt.UseFileBuf = true
	opt.FileBufDir = "./tmp"
	opt.FileBufPrefix = "tmpprefix_"
	opt.Compress = true

	bfr, _ := NewBuffer(opt)
	if bfr == nil {
		return
	}

	// write first
	bfr.WriteLine([]byte("test line"))
	bfr.WriteLine([]byte("test line"))
	bfr.Close()

	fmt.Println(bfr.sts.Checksum) // output: 42e649f9834028184ec21940d13a300f

	os.Remove(bfr.sts.Path()) // cleanup tmp file
	os.Remove("./tmp")        // remove dir

	// Output:
	// 42e649f9834028184ec21940d13a300f
}

func ExampleSizeWriter() {
	sw := &sizeWriter{}

	// write first
	n, err := sw.Write([]byte("test line"))

	fmt.Println(n)         // output: 9
	fmt.Println(err)       // output: <nil>
	fmt.Println(sw.Size()) // output: 9

	// Output:
	// 9
	// <nil>
	// 9
}
