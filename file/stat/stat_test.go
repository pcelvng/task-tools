package stat

import (
	"crypto/md5"
	"fmt"
	"testing"
	"time"
)

func ExampleNew() {
	sts := New()
	fmt.Println(sts.LineCnt)  // 0
	fmt.Println(sts.ByteCnt)  // 0
	fmt.Println(sts.Size)     // 0
	fmt.Println(sts.Checksum) //
	fmt.Println(sts.Path)     //
	fmt.Println(sts.Created)  //

	// Output:
	// 0
	// 0
	// 0
	//
	//
	//
}

func ExampleNewFromBytes() {
	sts := NewFromBytes([]byte(`{"linecnt":10,"bytecnt":100,"size":200,"checksum":"test checksum","path":"test path","created":"test created"}`))
	fmt.Println(sts.LineCnt)  // 10
	fmt.Println(sts.ByteCnt)  // 100
	fmt.Println(sts.Size)     // 200
	fmt.Println(sts.Checksum) // test checksum
	fmt.Println(sts.Path)     // test path
	fmt.Println(sts.Created)  // test created

	// Output:
	// 10
	// 100
	// 200
	// test checksum
	// test path
	// test created
}

func ExampleSafe_AddLine() {
	sts := Safe{}

	sts.AddLine()
	sts.AddLine()
	sts.AddLine()

	fmt.Println(sts.LineCnt) // 3

	// Output:
	// 3
}

func ExampleSafe_AddBytes() {
	sts := Safe{}

	sts.AddBytes(1)
	sts.AddBytes(10)
	sts.AddBytes(1100)

	fmt.Println(sts.ByteCnt) // 1111

	// Output:
	// 1111
}

func ExampleSafe_SetChecksum() {
	sts := Safe{}

	// checksum
	hsh := md5.New()
	hsh.Write([]byte("test message"))
	sts.SetChecksum(hsh)

	fmt.Println(sts.Checksum()) // c72b9698fa1927e1dd12d3cf26ed84b2
	// Output:
	// c72b9698fa1927e1dd12d3cf26ed84b2
}

func ExampleSafe_SetSize() {
	sts := Safe{}

	sts.SetSize(15)
	fmt.Println(sts.Size) // 15

	// Output:
	// 15
}

func ExampleSafe_SetPath() {
	sts := Safe{}

	sts.SetPath("path/to/file.txt")
	fmt.Println(sts.Path()) // path/to/file.txt

	// Output:
	// path/to/file.txt
}

func ExampleSafe_SetCreated() {
	sts := Safe{}

	created := "2017-01-02T03:04:05Z"
	t, _ := time.Parse(time.RFC3339, created)

	sts.SetCreated(t)
	fmt.Println(sts.Created()) // 2017-01-02T03:04:05Z

	// Output:
	// 2017-01-02T03:04:05Z
}

func ExampleStats_ParseCreated() {
	sts := New()

	sts.Created = "2017-01-02T03:04:05Z"

	t := sts.ParseCreated()
	fmt.Println(t.Format(time.RFC3339)) // 2017-01-02T03:04:05Z

	// Output:
	// 2017-01-02T03:04:05Z
}

func ExampleStats_ToSafe() {
	sts := Stats{
		LineCnt:  1,
		ByteCnt:  2,
		Size:     3,
		Checksum: "4",
		Path:     "5",
		Created:  "6",
	}

	cln := sts.ToSafe()
	fmt.Println(cln.LineCnt)
	fmt.Println(cln.ByteCnt)
	fmt.Println(cln.Size)
	fmt.Println(cln.Checksum())
	fmt.Println(cln.Path())
	fmt.Println(cln.Created())

	// Output:
	// 1
	// 2
	// 3
	// 4
	// 5
	// 6
}

func ExampleStats_JSONBytes() {
	sts := Stats{
		LineCnt:  10,
		ByteCnt:  100,
		Size:     200,
		Checksum: "test checksum",
		Path:     "test path",
		Created:  "test created",
	}

	b := sts.JSONBytes()
	fmt.Println(string(b)) // {"linecnt":10,"bytecnt":100,"size":200,"checksum":"test checksum","path":"test path","created":"test created"}

	// Output:
	// {"linecnt":10,"bytecnt":100,"size":200,"checksum":"test checksum","path":"test path","created":"test created"}
}

func ExampleStats_JSONString() {
	sts := Stats{
		LineCnt:  10,
		ByteCnt:  100,
		Size:     200,
		Checksum: "test checksum",
		Path:     "test path",
		Created:  "test created",
	}

	s := sts.JSONString()
	fmt.Println(s) // {"linecnt":10,"bytecnt":100,"size":200,"checksum":"test checksum","path":"test path","created":"test created"}

	// Output:
	// {"linecnt":10,"bytecnt":100,"size":200,"checksum":"test checksum","path":"test path","created":"test created"}
}

func BenchmarkAddLine(b *testing.B) {
	sts := Safe{}

	for i := 0; i < b.N; i++ {
		sts.AddLine()
	}
}

func BenchmarkAddBytes(b *testing.B) {
	sts := Safe{}

	for i := 0; i < b.N; i++ {
		sts.AddBytes(200)
	}
}

func BenchmarkTemplateParallel(b *testing.B) {
	sts := Safe{}
	hsh := md5.New()
	hsh.Write([]byte("test message"))

	// run test with '-race' flag to find race conditions
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sts.AddLine()
			sts.AddBytes(100)
			sts.SetSize(50)
			sts.SetChecksum(hsh)
			sts.SetPath("./test/path.txt")
			sts.SetPath("./tests/path.txt")
			_ = sts.Stats()
		}
	})
}
