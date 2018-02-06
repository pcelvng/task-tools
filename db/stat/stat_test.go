package stat

import (
	"fmt"
	"time"
)

func ExampleNewFromBytes() {
	sts := NewFromBytes([]byte(`{"started":"teststarted","dur":"1s","db":"test-db-name","table":"test.table","removed":10,"inserted":100,"columns":5}`))
	fmt.Println(sts.Started)      // output: teststarted
	fmt.Println(sts.Dur.String()) // output: 1s
	fmt.Println(sts.DBName)       // output: test-db-name
	fmt.Println(sts.DBTable)      // output: test.table
	fmt.Println(sts.RemovedCnt)   // output: 10
	fmt.Println(sts.InsertCnt)    // output: 100
	fmt.Println(sts.ColumnCnt)    // output: 5

	// Output:
	// teststarted
	// 1s
	// test-db-name
	// test.table
	// 10
	// 100
	// 5
}

func ExampleStat_AddRow() {
	sts := New()

	sts.AddRow()
	sts.AddRow()
	sts.AddRow()

	fmt.Println(sts.InsertCnt) // output: 3

	// Output:
	// 3
}

func ExampleStat_SetStarted() {
	sts := New()

	created := "2017-01-02T03:04:05Z"
	t, _ := time.Parse(time.RFC3339, created)

	sts.SetStarted(t)
	fmt.Println(sts.Started) // output: 2017-01-02T03:04:05Z

	// Output:
	// 2017-01-02T03:04:05Z
}

func ExampleStat_ParseStarted() {
	sts := New()

	sts.Started = "2017-01-02T03:04:05Z"

	t := sts.ParseStarted()
	fmt.Println(t.Format(time.RFC3339)) // output: 2017-01-02T03:04:05Z

	// Output:
	// 2017-01-02T03:04:05Z
}

func ExampleStat_Clone() {
	sts := New()
	sts.Started = "teststarted"
	sts.Dur = Duration{time.Second}
	sts.DBName = "test-db-name"
	sts.DBTable = "test.table"
	sts.RemovedCnt = 10
	sts.InsertCnt = 100
	sts.ColumnCnt = 5

	cln := sts.Clone()
	fmt.Println(cln.Started)
	fmt.Println(cln.Dur.String())
	fmt.Println(cln.DBName)
	fmt.Println(cln.DBTable)
	fmt.Println(cln.RemovedCnt)
	fmt.Println(cln.InsertCnt)
	fmt.Println(cln.ColumnCnt)

	// Output:
	// teststarted
	// 1s
	// test-db-name
	// test.table
	// 10
	// 100
	// 5
}

func ExampleStat_JSONBytes() {
	sts := New()
	sts.Started = "teststarted"
	sts.Dur = Duration{time.Second}
	sts.DBName = "test-db-name"
	sts.DBTable = "test.table"
	sts.RemovedCnt = 10
	sts.InsertCnt = 100
	sts.ColumnCnt = 5

	b := sts.JSONBytes()
	fmt.Println(string(b)) // output: {"started":"teststarted","dur":"1s","db":"test-db-name","table":"test.table","removed":10,"inserted":100,"columns":5}

	// Output:
	// {"started":"teststarted","dur":"1s","db":"test-db-name","table":"test.table","removed":10,"inserted":100,"columns":5}
}

func ExampleStat_JSONString() {
	sts := New()
	sts.Started = "teststarted"
	sts.Dur = Duration{time.Second}
	sts.DBName = "test-db-name"
	sts.DBTable = "test.table"
	sts.RemovedCnt = 10
	sts.InsertCnt = 100
	sts.ColumnCnt = 5

	s := sts.JSONString()
	fmt.Println(s) // output: {"linecnt":10,"bytecnt":100,"size":200,"checksum":"test checksum","path":"test path","created":"test created"}

	// Output:
	// {"linecnt":10,"bytecnt":100,"size":200,"checksum":"test checksum","path":"test path","created":"test created"}
}

//func BenchmarkAddLine(b *testing.B) {
//	sts := New()
//
//	for i := 0; i < b.N; i++ {
//		sts.AddLine()
//	}
//}
//
//func BenchmarkAddBytes(b *testing.B) {
//	sts := New()
//
//	for i := 0; i < b.N; i++ {
//		sts.AddBytes(200)
//	}
//}
//
//func BenchmarkTemplateParallel(b *testing.B) {
//	sts := New()
//	hsh := md5.New()
//	hsh.Write([]byte("test message"))
//
//	// run test with '-race' flag to find race conditions
//	b.RunParallel(func(pb *testing.PB) {
//		for pb.Next() {
//			sts.AddLine()
//			sts.AddBytes(100)
//			sts.SetSize(50)
//			sts.SetChecksum(hsh)
//			sts.SetPath("./test/path.txt")
//			sts.SetPath("./tests/path.txt")
//			_ = sts.Clone()
//		}
//	})
//}
