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

	fmt.Println(opt.FileBufDir)
	fmt.Println(opt.UseFileBuf)
	fmt.Println(opt.FileBufPrefix)
	fmt.Println(opt.Compress)

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

	fmt.Println(err)
	fmt.Println(bfr.sts.Created() != "")
	fmt.Println(bfr.w != nil)
	fmt.Println(bfr.wGzip)
	fmt.Println(bfr.wSize != nil)
	fmt.Println(bfr.bBuf != nil)
	fmt.Println(bfr.fBuf)
	fmt.Println(bfr.r != nil)
	fmt.Println(bfr.hshr != nil)

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

func ExampleNewBuffer_withTmpFile() {
	opt := NewOptions()
	opt.UseFileBuf = true
	opt.FileBufDir = "./tmp"
	opt.FileBufPrefix = "tmpprefix_"

	bfr, err := NewBuffer(opt)
	if bfr == nil {
		return
	}

	fmt.Println(err)
	fmt.Println(bfr.sts.Created() != "")
	fmt.Println(strings.Contains(bfr.sts.Path(), "/tmp/tmpprefix_"))
	fmt.Println(bfr.w != nil)
	fmt.Println(bfr.wGzip)
	fmt.Println(bfr.wSize != nil)
	fmt.Println(bfr.bBuf)
	fmt.Println(bfr.fBuf != nil)
	fmt.Println(bfr.r != nil)
	fmt.Println(bfr.hshr != nil)

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

func ExampleNewBuffer_withTmpFileErr() {
	opt := NewOptions()
	opt.UseFileBuf = true
	opt.FileBufDir = "/private"
	opt.FileBufPrefix = "tmpprefix_"

	bfr, err := NewBuffer(opt)
	if err == nil {
		return
	}

	fmt.Println(bfr)
	fmt.Println(strings.Contains(err.Error(), "permission denied"))

	// Output:
	// <nil>
	// true

}

func ExampleNewBuffer_withCompression() {
	opt := NewOptions()
	opt.Compress = true

	bfr, err := NewBuffer(opt)
	if bfr == nil {
		return
	}

	fmt.Println(err)
	fmt.Println(bfr.sts.Created() != "")
	fmt.Println(bfr.w != nil)
	fmt.Println(bfr.wGzip != nil)
	fmt.Println(bfr.wSize != nil)
	fmt.Println(bfr.bBuf != nil)
	fmt.Println(bfr.fBuf)
	fmt.Println(bfr.r != nil)
	fmt.Println(bfr.hshr != nil)

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

	fmt.Print(string(b))

	// Output:
	// test line
	// test line
}

func ExampleBuffer_Read_compressed() {
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

	fmt.Print(string(b))

	// Output:
	// test line
	// test line
}

func ExampleBuffer_Read_tmpFile() {
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

	fmt.Print(string(b))

	os.Remove(bfr.sts.Path()) // cleanup tmp file
	os.Remove("./tmp")        // remove dir

	// Output:
	// test line
	// test line
}

func ExampleBuffer_Read_tmpFileCompressed() {
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

	fmt.Print(string(b))

	os.Remove(bfr.sts.Path()) // cleanup tmp file
	os.Remove("./tmp")        // remove dir

	// Output:
	// test line
	// test line
}

func ExampleBuffer_Read_afterCleanup() {
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

	fmt.Println(n)
	fmt.Println(err)
	fmt.Println(b[0])

	// read again - same result
	b = make([]byte, 1)
	n, err = bfr.Read(b) // nothing should be read in

	fmt.Println(n)
	fmt.Println(err)
	fmt.Println(b[0])

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

	fmt.Println(n1)
	fmt.Println(err1)
	fmt.Println(n2)
	fmt.Println(err2)
	fmt.Print(string(b))
	fmt.Println(bfr.sts.ByteCnt)
	fmt.Println(bfr.sts.LineCnt)
	fmt.Println(bfr.wSize.Size())

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

	fmt.Println(err1)
	fmt.Println(err2)
	fmt.Print(string(b))
	fmt.Println(bfr.sts.ByteCnt)
	fmt.Println(bfr.sts.LineCnt)
	fmt.Println(bfr.wSize.Size())

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

	fmt.Println(sts.ByteCnt)
	fmt.Println(sts.LineCnt)

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

	fmt.Println(err)

	// Output:
	// <nil>
}

func ExampleBuffer_Abort_compression() {
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
	fmt.Println(err)

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

	fmt.Println(err)
	fmt.Println(n)
	fmt.Println(bErr)

	// Output:
	// <nil>
	// 0
	// EOF
}

func ExampleBuffer_Cleanup_tmpFile() {
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

	fmt.Println(err)
	fmt.Println(f)
	fmt.Println(strings.Contains(
		oErr.Error(),
		"no such file or directory"))

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

	fmt.Println(bfr.sts.Checksum() == "")
	fmt.Println(bfr.sts.Size)

	err := bfr.Close()

	fmt.Println(err)
	fmt.Println(bfr.sts.Checksum() != "")
	fmt.Println(bfr.sts.Size)

	// closing again has no effect
	err = bfr.Close()

	fmt.Println(err)
	fmt.Println(bfr.sts.Checksum() != "")
	fmt.Println(bfr.sts.Size)

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

func ExampleBuffer_Close_sizeCompressed() {
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
	fmt.Println(bfr.sts.ByteCnt)
	fmt.Println(bfr.sts.Size)

	// Output:
	// 20
	// 48
}

func ExampleBuffer_Close_tmpFileSizeCompressed() {
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
	fmt.Println(bfr.sts.ByteCnt)
	fmt.Println(bfr.sts.Size)

	os.Remove(bfr.sts.Path()) // cleanup tmp file
	os.Remove("./tmp")        // remove dir

	// Output:
	// 20
	// 48
}

func ExampleBuffer_Close_checksum() {
	bfr, _ := NewBuffer(nil)
	if bfr == nil {
		return
	}

	// write first
	bfr.WriteLine([]byte("test line"))
	bfr.WriteLine([]byte("test line"))
	bfr.Close()

	fmt.Println(bfr.sts.Checksum())

	// Output:
	// 54f30d75cf7374c7e524a4530dbc93c2
}

func ExampleBuffer_Close_checksumCompressed() {
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

	fmt.Println(bfr.sts.Checksum())

	// Output:
	// 42e649f9834028184ec21940d13a300f
}

func ExampleBuffer_Close_checksumTmpFile() {
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

	fmt.Println(bfr.sts.Checksum())

	os.Remove(bfr.sts.Path()) // cleanup tmp file
	os.Remove("./tmp")        // remove dir

	// Output:
	// 54f30d75cf7374c7e524a4530dbc93c2
}

func ExampleBuffer_Close_checksumTmpFileCompressed() {
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

	fmt.Println(bfr.sts.Checksum())

	os.Remove(bfr.sts.Path()) // cleanup tmp file
	os.Remove("./tmp")        // remove dir

	// Output:
	// 42e649f9834028184ec21940d13a300f
}

func ExamplesizeWriter() {
	sw := &sizeWriter{}

	// write first
	n, err := sw.Write([]byte("test line"))

	fmt.Println(n)
	fmt.Println(err)
	fmt.Println(sw.Size())

	// Output:
	// 9
	// <nil>
	// 9
}
