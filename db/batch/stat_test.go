package batch

import (
	"fmt"
	"time"
)

func ExampleNewStatsFromBytes() {
	sts := NewStatsFromBytes([]byte(`{"started":"teststarted","dur":"1s","table":"test.table","removed":10,"rows":100,"inserted":100,"cols":5}`))
	fmt.Println(sts.Started)
	fmt.Println(sts.Dur.String())
	fmt.Println(sts.Table)
	fmt.Println(sts.Removed)
	fmt.Println(sts.Rows)
	fmt.Println(sts.Inserted)
	fmt.Println(sts.Cols)

	// Output:
	// teststarted
	// 1s
	// test.table
	// 10
	// 100
	// 100
	// 5
}

func ExampleStats_AddRow() {
	sts := NewStats()

	sts.AddRow()
	sts.AddRow()
	sts.AddRow()

	fmt.Println(sts.Inserted)

	// Output:
	// 3
}

func ExampleStats_SetStarted() {
	sts := NewStats()

	created := "2017-01-02T03:04:05Z"
	t, _ := time.Parse(time.RFC3339, created)

	sts.SetStarted(t)
	fmt.Println(sts.Started)

	// Output:
	// 2017-01-02T03:04:05Z
}

func ExampleStats_ParseStarted() {
	sts := NewStats()

	sts.Started = "2017-01-02T03:04:05Z"

	t := sts.ParseStarted()
	fmt.Println(t.Format(time.RFC3339))

	// Output:
	// 2017-01-02T03:04:05Z
}

func ExampleStats_Clone() {
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

func ExampleStats_JSONBytes() {
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
	fmt.Println(string(b))

	// Output:
	// {"started":"teststarted","dur":"1s","table":"test.table","removed":10,"rows":100,"inserted":100,"cols":5,"batch_hour":"testbatchhour"}
}

func ExampleStats_JSONString() {
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
	fmt.Println(string(b))

	// Output:
	// {"started":"teststarted","dur":"1s","table":"test.table","removed":10,"rows":100,"inserted":100,"cols":5,"batch_hour":"testbatchhour"}
}
