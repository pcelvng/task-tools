package stat

import (
	"crypto/md5"
	"fmt"
	"testing"
	"time"
)

func ExampleNew() {
	sts := New()
	fmt.Println(sts.LineCnt)  // output: 0
	fmt.Println(sts.ByteCnt)  // output: 0
	fmt.Println(sts.Size)     // output: 0
	fmt.Println(sts.CheckSum) // output:
	fmt.Println(sts.Path)     // output:
	fmt.Println(sts.Created)  // output:

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
	fmt.Println(sts.LineCnt)  // output: 10
	fmt.Println(sts.ByteCnt)  // output: 100
	fmt.Println(sts.Size)     // output: 200
	fmt.Println(sts.CheckSum) // output: test checksum
	fmt.Println(sts.Path)     // output: test path
	fmt.Println(sts.Created)  // output: test created

	// Output:
	// 10
	// 100
	// 200
	// test checksum
	// test path
	// test created
}

func ExampleStat_AddLine() {
	sts := New()

	sts.AddLine()
	sts.AddLine()
	sts.AddLine()

	fmt.Println(sts.LineCnt) // output: 3

	// Output:
	// 3
}

func ExampleStat_AddBytes() {
	sts := New()

	sts.AddBytes(1)
	sts.AddBytes(10)
	sts.AddBytes(1100)

	fmt.Println(sts.ByteCnt) // output: 1111

	// Output:
	// 1111
}

func ExampleStat_SetChecksum() {
	sts := New()

	// checksum
	hsh := md5.New()
	hsh.Write([]byte("test message"))
	sts.SetCheckSum(hsh)

	fmt.Println(sts.CheckSum) // output: c72b9698fa1927e1dd12d3cf26ed84b2
	// Output:
	// c72b9698fa1927e1dd12d3cf26ed84b2
}

func ExampleStat_SetSize() {
	sts := New()

	sts.SetSize(15)
	fmt.Println(sts.Size) // output: 15

	// Output:
	// 15
}

func ExampleStat_SetPath() {
	sts := New()

	sts.SetPath("path/to/file.txt")
	fmt.Println(sts.Path) // output: path/to/file.txt

	// Output:
	// path/to/file.txt
}

func ExampleStat_SetCreated() {
	sts := New()

	created := "2017-01-02T03:04:05Z"
	t, _ := time.Parse(time.RFC3339, created)

	sts.SetCreated(t)
	fmt.Println(sts.Created) // output: 2017-01-02T03:04:05Z

	// Output:
	// 2017-01-02T03:04:05Z
}

func ExampleStat_ParseCreated() {
	sts := New()

	sts.Created = "2017-01-02T03:04:05Z"

	t := sts.ParseCreated()
	fmt.Println(t.Format(time.RFC3339)) // output: 2017-01-02T03:04:05Z

	// Output:
	// 2017-01-02T03:04:05Z
}

func ExampleStat_Clone() {
	sts := New()
	sts.LineCnt = 1
	sts.ByteCnt = 2
	sts.Size = 3
	sts.CheckSum = "4"
	sts.Path = "5"
	sts.Created = "6"

	cln := sts.Clone()
	fmt.Println(cln.LineCnt)
	fmt.Println(cln.ByteCnt)
	fmt.Println(cln.Size)
	fmt.Println(cln.CheckSum)
	fmt.Println(cln.Path)
	fmt.Println(cln.Created)

	// Output:
	// 1
	// 2
	// 3
	// 4
	// 5
	// 6
}

func ExampleStat_JSONBytes() {
	sts := New()
	sts.LineCnt = 10
	sts.ByteCnt = 100
	sts.Size = 200
	sts.CheckSum = "test checksum"
	sts.Path = "test path"
	sts.Created = "test created"

	b := sts.JSONBytes()
	fmt.Println(string(b)) // output: {"linecnt":10,"bytecnt":100,"size":200,"checksum":"test checksum","path":"test path","created":"test created"}

	// Output:
	// {"linecnt":10,"bytecnt":100,"size":200,"checksum":"test checksum","path":"test path","created":"test created"}
}

func ExampleStat_JSONString() {
	sts := New()
	sts.LineCnt = 10
	sts.ByteCnt = 100
	sts.Size = 200
	sts.CheckSum = "test checksum"
	sts.Path = "test path"
	sts.Created = "test created"

	s := sts.JSONString()
	fmt.Println(s) // output: {"linecnt":10,"bytecnt":100,"size":200,"checksum":"test checksum","path":"test path","created":"test created"}

	// Output:
	// {"linecnt":10,"bytecnt":100,"size":200,"checksum":"test checksum","path":"test path","created":"test created"}
}

func BenchmarkAddLine(b *testing.B) {
	sts := New()

	for i := 0; i < b.N; i++ {
		sts.AddLine()
	}
}

func BenchmarkAddBytes(b *testing.B) {
	sts := New()

	for i := 0; i < b.N; i++ {
		sts.AddBytes(200)
	}
}

func BenchmarkTemplateParallel(b *testing.B) {
	sts := New()
	hsh := md5.New()
	hsh.Write([]byte("test message"))

	// run test with '-race' flag to find race conditions
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sts.AddLine()
			sts.AddBytes(100)
			sts.SetSize(50)
			sts.SetCheckSum(hsh)
			sts.SetPath("./test/path.txt")
			sts.SetPath("./tests/path.txt")
			_ = sts.Clone()
		}
	})
}
