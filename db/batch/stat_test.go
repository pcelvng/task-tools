package batch

import (
	"fmt"
	"time"
)

func ExampleNewFromBytes() {
	sts := NewStatsFromBytes([]byte(`{"started":"teststarted","dur":"1s","table":"test.table","removed":10,"rows":100,"inserted":100,"cols":5}`))
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
	sts := NewStats()

	sts.AddRow()
	sts.AddRow()
	sts.AddRow()

	fmt.Println(sts.Inserted) // output: 3

	// Output:
	// 3
}

func ExampleStat_SetStarted() {
	sts := NewStats()

	created := "2017-01-02T03:04:05Z"
	t, _ := time.Parse(time.RFC3339, created)

	sts.SetStarted(t)
	fmt.Println(sts.Started) // output: 2017-01-02T03:04:05Z

	// Output:
	// 2017-01-02T03:04:05Z
}

func ExampleStat_ParseStarted() {
	sts := NewStats()

	sts.Started = "2017-01-02T03:04:05Z"

	t := sts.ParseStarted()
	fmt.Println(t.Format(time.RFC3339)) // output: 2017-01-02T03:04:05Z

	// Output:
	// 2017-01-02T03:04:05Z
}

func ExampleStat_Clone() {
	sts := NewStats()
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
	sts := NewStats()
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
	sts := NewStats()
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
