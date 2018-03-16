package stat

import (
	"fmt"
	"time"
)

func ExampleNewFromBytes() {
	sts := NewFromBytes([]byte(`{"started":"teststarted","dur":"1s","table":"test.table","removed":10,"rows":100,"inserted":100,"cols":5}`))
	fmt.Println(sts.Started)      // output: teststarted
	fmt.Println(sts.Dur.String()) // output: 1s
	fmt.Println(sts.Table)        // output: test.table
	fmt.Println(sts.Removed)      // output: 10
	fmt.Println(sts.Rows)         // output: 100
	fmt.Println(sts.Inserted)     // output: 100
	fmt.Println(sts.Cols)         // output: 5

	// Output:
	// teststarted
	// 1s
	// test.table
	// 10
	// 100
	// 100
	// 5
}

func ExampleStat_AddRow() {
	sts := New()

	sts.AddRow()
	sts.AddRow()
	sts.AddRow()

	fmt.Println(sts.Inserted) // output: 3

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
	sts.Table = "test.table"
	sts.Removed = 10
	sts.Inserted = 100
	sts.Cols = 5

	cln := sts.Clone()
	fmt.Println(cln.Started)
	fmt.Println(cln.Dur.String())
	fmt.Println(cln.Table)
	fmt.Println(cln.Removed)
	fmt.Println(cln.Inserted)
	fmt.Println(cln.Cols)

	// Output:
	// teststarted
	// 1s
	// test.table
	// 10
	// 100
	// 5
}

func ExampleStat_JSONBytes() {
	sts := New()
	sts.Started = "teststarted"
	sts.Dur = Duration{time.Second}
	sts.Table = "test.table"
	sts.Removed = 10
	sts.Rows = 100
	sts.Inserted = 100
	sts.Cols = 5
	sts.BatchDate = "testbatchhour"

	b := sts.JSONBytes()
	fmt.Println(string(b)) // output: {"started":"teststarted","dur":"1s","table":"test.table","removed":10,"rows":100,"inserted":100,"cols":5,"batch_hour":"testbatchhour"}

	// Output:
	// {"started":"teststarted","dur":"1s","table":"test.table","removed":10,"rows":100,"inserted":100,"cols":5,"batch_hour":"testbatchhour"}
}

func ExampleStat_JSONString() {
	sts := New()
	sts.Started = "teststarted"
	sts.Dur = Duration{time.Second}
	sts.Table = "test.table"
	sts.Removed = 10
	sts.Rows = 100
	sts.Inserted = 100
	sts.Cols = 5
	sts.BatchDate = "testbatchhour"

	b := sts.JSONString()
	fmt.Println(string(b)) // output: {"started":"teststarted","dur":"1s","table":"test.table","removed":10,"rows":100,"inserted":100,"cols":5,"batch_hour":"testbatchhour"}

	// Output:
	// {"started":"teststarted","dur":"1s","table":"test.table","removed":10,"rows":100,"inserted":100,"cols":5,"batch_hour":"testbatchhour"}
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
