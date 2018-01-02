package stat

import (
	"crypto/md5"
	"fmt"
	"testing"
)

func ExampleNew() {
	sts := New()
	fmt.Println(sts.LineCnt)
	fmt.Println(sts.ByteCnt)
	fmt.Println(sts.Size)
	fmt.Println(sts.CheckSum) // empty string
	fmt.Println(sts.Path)     // empty string
	// Output:
	// 0
	// 0
	// 0
	//
	//
}

func ExampleLineCnt() {
	sts := New()

	sts.AddLine()
	sts.AddLine()
	sts.AddLine()

	fmt.Println(sts.LineCnt)
	fmt.Println(sts.ByteCnt)
	fmt.Println(sts.Size)
	fmt.Println(sts.CheckSum) // empty string
	fmt.Println(sts.Path)     // empty string
	// Output:
	// 3
	// 0
	// 0
	//
	//
}

func ExampleByteCnt() {
	sts := New()

	sts.AddBytes(1)
	sts.AddBytes(10)
	sts.AddBytes(1100)

	fmt.Println(sts.LineCnt)
	fmt.Println(sts.ByteCnt)
	fmt.Println(sts.Size)
	fmt.Println(sts.CheckSum) // empty string
	fmt.Println(sts.Path)     // empty string
	// Output:
	// 0
	// 1111
	// 0
	//
	//
}

func ExampleSize() {
	sts := New()

	// bad file
	sts.SetSizeFromPath("./bad_file.txt")
	fmt.Println(sts.Size)

	sts.SetSizeFromPath("./size_test.txt")
	fmt.Println(sts.Size)

	sts.SetSize(15)
	fmt.Println(sts.Size)

	fmt.Println(sts.LineCnt)
	fmt.Println(sts.ByteCnt)
	fmt.Println(sts.Size)
	fmt.Println(sts.CheckSum) // empty string
	fmt.Println(sts.Path)     // empty string
	// Output:
	// 0
	// 10
	// 15
	// 0
	// 0
	// 15
	//
	//
}

func ExampleChecksum() {
	sts := New()

	// checksum
	hsh := md5.New()
	hsh.Write([]byte("test message"))
	sts.SetCheckSum(hsh)

	fmt.Println(sts.LineCnt)
	fmt.Println(sts.ByteCnt)
	fmt.Println(sts.Size)
	fmt.Println(sts.CheckSum)
	fmt.Println(sts.Path) // empty string
	// Output:
	// 0
	// 0
	// 0
	// c72b9698fa1927e1dd12d3cf26ed84b2
	//
}

func ExamplePath() {
	sts := New()
	sts.SetPath("./test/path.txt")

	fmt.Println(sts.LineCnt)
	fmt.Println(sts.ByteCnt)
	fmt.Println(sts.Size)
	fmt.Println(sts.CheckSum)
	fmt.Println(sts.Path)
	// Output:
	// 0
	// 0
	// 0
	//
	// ./test/path.txt
}

func ExampleStat_Clone() {
	sts := New()
	sts.LineCnt = 1
	sts.ByteCnt = 2
	sts.Size = 3
	sts.CheckSum = "4"
	sts.Path = "5"

	cln := sts.Clone()
	fmt.Println(cln.LineCnt)
	fmt.Println(cln.ByteCnt)
	fmt.Println(cln.Size)
	fmt.Println(cln.CheckSum)
	fmt.Println(cln.Path)

	// Output:
	// 1
	// 2
	// 3
	// 4
	// 5
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
			sts.SetSizeFromPath("./size_test.txt")
			sts.SetCheckSum(hsh)
			sts.SetPath("./test/path.txt")
			sts.SetPath("./tests/path.txt")
			_ = sts.Clone()
		}
	})
}
