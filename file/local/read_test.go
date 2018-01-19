package local

//import (
//	"compress/gzip"
//	"crypto/md5"
//	"fmt"
//	"os"
//
//	"github.com/pcelvng/task-tools/file/stat"
//)
//
//func ExampleReader_ReadLine() {
//	// showing:
//	// - reading a few lines
//	// - last line has newline character at end
//	// - repeat ReadLine call after EOF
//	// - sts.LineCnt is correct
//	// - sts.ByteCnt is correct
//
//	w, _ := NewWriter("./tmp/readtest1.txt", nil)
//	w.WriteLine([]byte("read test line1"))
//	w.WriteLine([]byte("read test line2"))
//	err := w.Close()
//	if err != nil {
//		return
//	}
//
//	r, err := NewReader(w.sts.Path)
//	fmt.Println(err) // <nil>
//	if r == nil {
//		return
//	}
//	ln1, err := r.ReadLine()
//	fmt.Println(string(ln1))   // read test line1
//	fmt.Println(err)           // <nil>
//	fmt.Println(r.sts.ByteCnt) // 16
//	fmt.Println(r.sts.LineCnt) // 1
//
//	ln2, err := r.ReadLine()
//	fmt.Println(string(ln2))   // read test line2
//	fmt.Println(err)           // <nil>
//	fmt.Println(r.sts.ByteCnt) // 32
//	fmt.Println(r.sts.LineCnt) // 2
//
//	// note that extra calls to ReadLine
//	// does not increment LineCnt
//	// or BytesCnt.
//	ln3, err := r.ReadLine()
//	fmt.Println(string(ln3))   //
//	fmt.Println(err)           // EOF
//	fmt.Println(r.sts.ByteCnt) // 32
//	fmt.Println(r.sts.LineCnt) // 2
//
//	ln4, err := r.ReadLine()
//	fmt.Println(string(ln4))   //
//	fmt.Println(err)           // EOF
//	fmt.Println(r.sts.ByteCnt) // 32
//	fmt.Println(r.sts.LineCnt) // 2
//
//	os.Remove(w.sts.Path)
//
//	// Output:
//	// <nil>
//	// read test line1
//	// <nil>
//	// 16
//	// 1
//	// read test line2
//	// <nil>
//	// 32
//	// 2
//	//
//	// EOF
//	// 32
//	// 2
//	//
//	// EOF
//	// 32
//	// 2
//}
//
//func ExampleReader_ReadLine_nonewlineeof() {
//	// showing:
//	// - reading a couple lines
//	// - last line does not have a newline character
//	// - repeat ReadLine call after EOF
//
//	w, _ := NewWriter("./tmp/readtest2.txt", nil)
//	w.WriteLine([]byte("read test line1"))
//	w.Write([]byte("read test line2")) // no newline at EOF
//	err := w.Close()
//	if err != nil {
//		return
//	}
//
//	r, err := NewReader(w.sts.Path)
//	fmt.Println(err) // <nil>
//	if r == nil {
//		return
//	}
//	ln1, err := r.ReadLine()
//	fmt.Println(string(ln1)) // read test line1
//	fmt.Println(err)         // <nil>
//
//	ln2, err := r.ReadLine()
//	fmt.Println(string(ln2)) // read test line2
//	fmt.Println(err)         // EOF
//
//	ln3, err := r.ReadLine()
//	fmt.Println(string(ln3)) //
//	fmt.Println(err)         // EOF
//
//	os.Remove(w.sts.Path)
//
//	// Output:
//	// <nil>
//	// read test line1
//	// <nil>
//	// read test line2
//	// EOF
//	//
//	// EOF
//}
//
//func ExampleReader_ReadLine_empty() {
//	// showing:
//	// - reading from empty file
//
//	w, _ := NewWriter("./tmp/readtest3.txt", nil)
//	err := w.Close()
//	if err != nil {
//		return
//	}
//
//	r, err := NewReader(w.sts.Path)
//	fmt.Println(err) // <nil>
//	if r == nil {
//		return
//	}
//	ln1, err := r.ReadLine()
//	fmt.Println(string(ln1)) //
//	fmt.Println(err)         // EOF
//
//	os.Remove(w.sts.Path)
//
//	// Output:
//	// <nil>
//	//
//	// EOF
//}
//
//func ExampleReader_ReadLine_longlines() {
//	// showing:
//	// - reading from file with multiple long lines
//
//	w, _ := NewWriter("./tmp/readtest4.txt", nil)
//	longLn1 := make([]byte, 100000)
//	longLn2 := make([]byte, 1000000)
//	w.WriteLine(longLn1)
//	w.WriteLine(longLn2)
//	err := w.Close()
//	if err != nil {
//		return
//	}
//
//	r, err := NewReader(w.sts.Path)
//	fmt.Println(err) // <nil>
//	if r == nil {
//		return
//	}
//	ln1, err := r.ReadLine()
//	fmt.Println(len(ln1)) // 100000
//	fmt.Println(err)      // <nil>
//
//	ln2, err := r.ReadLine()
//	fmt.Println(len(ln2)) // 1000000
//	fmt.Println(err)      // <nil>
//
//	os.Remove(w.sts.Path)
//
//	// Output:
//	// <nil>
//	// 100000
//	// <nil>
//	// 1000000
//	// <nil>
//}
//
//func ExampleReader_Read() {
//	// showing:
//	// - using a standard Read
//
//	w, _ := NewWriter("./tmp/readtest5.txt", nil)
//	w.WriteLine([]byte("read test line1"))
//	w.WriteLine([]byte("read test line2"))
//	err := w.Close()
//	if err != nil {
//		return
//	}
//
//	r, err := NewReader(w.sts.Path)
//	fmt.Println(err) // <nil>
//	if r == nil {
//		return
//	}
//	bBuf := make([]byte, 32)
//	n, err := r.Read(bBuf)
//	fmt.Println(n)             // 32
//	fmt.Println(r.sts.ByteCnt) // 32
//	fmt.Println(err)           // <nil>
//	fmt.Print(string(bBuf))    // read test line1\nread test line2\n
//
//	os.Remove(w.sts.Path)
//
//	// Output:
//	// <nil>
//	// 32
//	// 32
//	// <nil>
//	// read test line1
//	// read test line2
//}

//func ExampleNewReader() {
//	// showing:
//	// - returns err if unable to open file
//	// - sts.Path is set on initialization
//	// - sts.Size is set on initialization
//	// - r.f is not nil
//	// - r.rBuf is not nil
//	// - Close sets checksum
//	// - Close marks isClosed as true
//	// - Calling Close twice has err == nil the second time
//
//	// read file that does not exist
//	r, err := NewReader("/doesnot/exist.txt")
//	fmt.Println(err) // open /doesnot/exist.txt: no such file or directory
//	fmt.Println(r)   // <nil>
//
//	w, _ := NewWriter("/tmp/readtest6.txt", nil)
//	w.WriteLine([]byte("read test line1"))
//	w.WriteLine([]byte("read test line2"))
//	err = w.Close()
//	if err != nil {
//		return
//	}
//
//	// read file that does exist
//	r, err = NewReader(w.sts.Path)
//	fmt.Println(err) // <nil>
//	if r == nil {
//		return
//	}
//	fmt.Println(r.sts.LineCnt)  // 0
//	fmt.Println(r.sts.ByteCnt)  // 0
//	fmt.Println(r.sts.Size)     // 32
//	fmt.Println(r.sts.Path)     // /tmp/readtest6.txt
//	fmt.Println(r.sts.CheckSum) //
//	fmt.Println(r.isClosed)     // false
//
//	ln1, _ := r.ReadLine()
//	ln2, _ := r.ReadLine()
//	fmt.Println(string(ln1)) // read test line1
//	fmt.Println(string(ln2)) // read test line2
//	err = r.Close()
//	fmt.Println(r.isClosed) // true
//
//	// final stats
//	sts := r.Stats()
//
//	fmt.Println(sts.LineCnt)  // 2
//	fmt.Println(sts.ByteCnt)  // 32
//	fmt.Println(sts.Size)     // 32
//	fmt.Println(sts.Path)     // /tmp/readtest6.txt
//	fmt.Println(sts.CheckSum) // e7623bafac74621cd419e3e905768551
//	fmt.Println(r.Close())    // <nil>
//
//	os.Remove(w.sts.Path)
//
//	// Output:
//	// open /doesnot/exist.txt: no such file or directory
//	// <nil>
//	// <nil>
//	// 0
//	// 0
//	// 32
//	// /tmp/readtest6.txt
//	//
//	// false
//	// read test line1
//	// read test line2
//	// true
//	// 2
//	// 32
//	// 32
//	// /tmp/readtest6.txt
//	// e7623bafac74621cd419e3e905768551
//	// <nil>
//}

//func ExampleNewReader_gzip() {
//	// showing:
//	// - compressed file size
//	// - correct final read line count
//	// - stats size different than bytes read
//	// - correct checksum for compressed file not uncompressed bytes
//
//	w, _ := NewWriter("./tmp/readtest7.gz", nil)
//	if w == nil {
//		return
//	}
//	w.WriteLine([]byte("read gz test line1"))
//	w.WriteLine([]byte("read gz test line2"))
//	w.Close()
//
//	// read file that does exist
//	r, _ := NewReader(w.sts.Path)
//	if r == nil {
//		return
//	}
//
//	ln1, _ := r.ReadLine()
//	ln2, _ := r.ReadLine()
//	fmt.Println(string(ln1)) // read gz test line1
//	fmt.Println(string(ln2)) // read gz test line2
//	r.Close()
//
//	// final stats
//	sts := r.Stats()
//
//	fmt.Println(sts.LineCnt)  // 2
//	fmt.Println(sts.ByteCnt)  // 38
//	fmt.Println(sts.Size)     // 66
//	fmt.Println(sts.CheckSum) //
//
//	os.Remove(w.sts.Path)
//
//	// Output:
//	// read gz test line1
//	// read gz test line2
//	// 2
//	// 38
//	// 66
//	// adebedc23ab3b3bd843d431f85bb6dc4
//}
//
//func ExampleNewReader_gzip_error() {
//	// showing:
//	// - gz file headers problem
//
//	w, _ := NewWriter("./tmp/readtest8.txt", nil)
//	if w == nil {
//		return
//	}
//	w.WriteLine([]byte("read test line1"))
//	w.WriteLine([]byte("read test line2"))
//	w.Close()
//
//	// rename file as '.gz'
//	gzPth := "./tmp/readtest8.gz"
//	os.Rename(w.sts.Path, gzPth)
//
//	// read file that does exist
//	_, err := NewReader(gzPth)
//	fmt.Println(err) // gzip: invalid header
//
//	// os.Remove(gzPth)
//
//	// Output:
//	// gzip: invalid header
//}
//
//func ExampleReader_Close() {
//	// showing:
//	// - Close returns nil
//
//	// read file that does not exist
//	r, err := NewReader("/doesnot/exist.txt")
//	fmt.Println(err) // does not exist
//	fmt.Println(r)   // <nil>
//
//	w, _ := NewWriter("/tmp/readtest9.txt", nil)
//	w.WriteLine([]byte("read test line1"))
//	w.WriteLine([]byte("read test line2"))
//	err = w.Close()
//	if err != nil {
//		return
//	}
//
//	// read file that does exist
//	r, err = NewReader(w.sts.Path)
//	fmt.Println(err) // <nil>
//	if r == nil {
//		return
//	}
//	fmt.Println(r.sts.Size) // 32
//	fmt.Println(r.sts.Path) // /tmp/readtest9.txt
//
//	os.Remove(w.sts.Path)
//
//	// Output:
//	// open /doesnot/exist.txt: no such file or directory
//	// <nil>
//	// <nil>
//	// 32
//	// /tmp/readtest9.txt
//}
//
//func ExampleHashReader() {
//	// showing:
//	// - correct checksum when reading entire file at once
//
//	w, _ := NewWriter("./tmp/readsumtest1.txt", nil)
//	w.WriteLine([]byte("read test line1"))
//	w.WriteLine([]byte("read test line2"))
//	err := w.Close()
//	if err != nil {
//		return
//	}
//
//	f, _ := os.Open(w.sts.Path)
//	if f == nil {
//		return
//	}
//	rHshr := &hashReader{
//		r:    f,
//		Hshr: md5.New(),
//	}
//	bBuf := make([]byte, 32)
//	rHshr.Read(bBuf)
//	sts := stat.New()
//	sts.SetCheckSum(rHshr.Hshr)
//	fmt.Println(sts.CheckSum) // e7623bafac74621cd419e3e905768551
//
//	// os.Remove(w.sts.Path)
//
//	// Output:
//	// e7623bafac74621cd419e3e905768551
//}
//
//func ExampleHashReader_inchunks() {
//	// showing:
//	// - correct checksum when reading in smaller chunks
//	// - when read buffer is not filled up
//	// - correct checksum for gzipped files
//
//	w, _ := NewWriter("./tmp/readsumtest2.txt", nil)
//	w.WriteLine([]byte("read test line1"))
//	w.WriteLine([]byte("read test line2"))
//	err := w.Close()
//	if err != nil {
//		return
//	}
//
//	f, _ := os.Open(w.sts.Path)
//	if f == nil {
//		return
//	}
//	rHshr := &hashReader{
//		r:    f,
//		Hshr: md5.New(),
//	}
//	bBuf1 := make([]byte, 16)
//	bBuf2 := make([]byte, 16)
//	rHshr.Read(bBuf1)
//	rHshr.Read(bBuf2)
//	sts := stat.New()
//	sts.SetCheckSum(rHshr.Hshr)
//	fmt.Println(sts.CheckSum) // e7623bafac74621cd419e3e905768551
//
//	// os.Remove(w.sts.Path)
//
//	// Output:
//	// e7623bafac74621cd419e3e905768551
//}
//
//func ExampleHashReader_bigreadbuff() {
//	// showing:
//	// - correct checksum when read buffer is bigger than bytes read
//	// - correct checksum even when buffer has some non-zero bytes
//	// - correct checksum for gzipped files
//
//	w, _ := NewWriter("./tmp/readsumtest3.txt", nil)
//	w.WriteLine([]byte("read test line1"))
//	w.WriteLine([]byte("read test line2"))
//	err := w.Close()
//	if err != nil {
//		return
//	}
//
//	f, _ := os.Open(w.sts.Path)
//	if f == nil {
//		return
//	}
//	rHshr := &hashReader{
//		r:    f,
//		Hshr: md5.New(),
//	}
//
//	// len(bBuf) == 42, larger than needed
//	// bBuf has all non-zero values
//	bBuf := []byte("asdlkjfasfdqosdkfjasldkfjasdfiasdfasdfasdf")
//	n, err := rHshr.Read(bBuf)
//	fmt.Println(n)   // 32
//	fmt.Println(err) // <nil>
//	n, err = rHshr.Read(bBuf)
//	fmt.Println(n)   // 0
//	fmt.Println(err) // EOF
//	sts := stat.New()
//	sts.SetCheckSum(rHshr.Hshr)
//
//	fmt.Println(sts.CheckSum) // e7623bafac74621cd419e3e905768551
//
//	os.Remove(w.sts.Path)
//
//	// Output:
//	// 32
//	// <nil>
//	// 0
//	// EOF
//	// e7623bafac74621cd419e3e905768551
//}
//
//func ExampleHashReader_gzipped() {
//	// showing:
//	// - correct checksum for gzipped files
//	//
//	// NOTE: the stats.Stat.Checksum value represents
//	// the checksum of the underlying file not the
//	// bytes received. The two values can be different
//	// if the underlying file is compressed.
//
//	w, _ := NewWriter("./tmp/readsumtest4.gz", nil)
//	w.WriteLine([]byte("read gz test line1"))
//	w.WriteLine([]byte("read gz test line2"))
//	err := w.Close()
//	if err != nil {
//		return
//	}
//
//	f, _ := os.Open(w.sts.Path)
//	if f == nil {
//		return
//	}
//	rHshr := &hashReader{
//		r:    f,
//		Hshr: md5.New(),
//	}
//	bBuf := make([]byte, 100)
//	rHshr.Read(bBuf)
//
//	sts := stat.New()
//	sts.SetCheckSum(rHshr.Hshr)
//	fmt.Println(sts.CheckSum) // adebedc23ab3b3bd843d431f85bb6dc4
//
//	os.Remove(w.sts.Path)
//
//	// Output:
//	// adebedc23ab3b3bd843d431f85bb6dc4
//}
//
//func ExampleHashReader_withunzipper() {
//	// showing:
//	// - correct checksum for original file when
//	// using gzip to unzip file
//
//	w, _ := NewWriter("./tmp/readsumtest5.gz", nil)
//	w.WriteLine([]byte("read gz test line1"))
//	w.WriteLine([]byte("read gz test line2"))
//	err := w.Close()
//	if err != nil {
//		return
//	}
//
//	f, _ := os.Open(w.sts.Path)
//	if f == nil {
//		return
//	}
//	rHshr := &hashReader{
//		r:    f,
//		Hshr: md5.New(),
//	}
//	rGzip, err := gzip.NewReader(rHshr)
//	if rGzip == nil {
//		return
//	}
//	fmt.Println(err) // <nil>
//
//	bBuf := make([]byte, 100)
//	rGzip.Read(bBuf) // read all
//	rGzip.Close()
//
//	sts := stat.New()
//	sts.SetCheckSum(rHshr.Hshr)
//	fmt.Println(sts.CheckSum) // adebedc23ab3b3bd843d431f85bb6dc4
//
//	os.Remove(w.sts.Path)
//
//	// Output:
//	// <nil>
//	// adebedc23ab3b3bd843d431f85bb6dc4
//}
