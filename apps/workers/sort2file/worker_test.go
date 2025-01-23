package main

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/pcelvng/task/bus"

	"github.com/pcelvng/task-tools/file"
)

func ExampleDoTaskJSON() {
	os.Setenv("TZ", "UTC")
	defer os.Unsetenv("TZ")

	opts := options{}
	// initialize producer
	opts.Producer, _ = bus.NewProducer(bus.NewOptions("nop"))

	// write file
	w, _ := file.NewWriter("./test/test-20050405T201112.json", nil)
	w.WriteLine([]byte(`{"dateField":"2007-02-03T16:05:06Z"}`))
	w.WriteLine([]byte(`{"dateField":"2007-02-03T16:05:06Z"}`))
	w.WriteLine([]byte(`{"dateField":"2007-02-03T17:05:06Z"}`))
	w.WriteLine([]byte(`{"dateField":"2007-02-03T18:05:06Z"}`))
	w.Close()

	ctx, _ := context.WithCancel(context.Background())
	info := `./test/test-20050405T201112.json?date-field=dateField&dest-template=./test/{HH}-{SRC_TS}.json`
	wkr := opts.newWorker(info)
	result, msg := wkr.DoTask(ctx)

	fmt.Println(result) // output: complete
	fmt.Println(msg)    // output: wrote 4 lines over 3 files

	// cleanup
	os.Remove("./test/test-20050405T201112.json")
	os.Remove("./test/16-20050405T201112.json")
	os.Remove("./test/17-20050405T201112.json")
	os.Remove("./test/18-20050405T201112.json")
	os.Remove("./test")

	// Output:
	// complete
	// wrote 4 lines over 3 files
}

func ExampleDoTaskJSONBadRecord() {
	os.Setenv("TZ", "UTC")
	defer os.Unsetenv("TZ")

	opts := options{}
	opts.Producer, _ = bus.NewProducer(bus.NewOptions("nop"))

	// write file
	w, _ := file.NewWriter("./test/test.json", nil)
	w.WriteLine([]byte(`{"dateField":"2007-02-03T16:05:06Z"}`))
	w.WriteLine([]byte(`{"dateField":"2007-02-03T16:05:06Z"}`))
	w.WriteLine([]byte(`{"dateField":"2007-02-03T17:05:06Z"}`))
	w.WriteLine([]byte(`{"dateField":"2007-02-03T18:05:06Z"}`))
	w.WriteLine([]byte(`{"badField":"2007-02-03T18:05:06Z"}`))
	w.Close()

	ctx, _ := context.WithCancel(context.Background())
	info := `./test/test.json?date-field=dateField&dest-template=./test/{HH}.json`
	wkr := opts.newWorker(info)
	result, msg := wkr.DoTask(ctx)

	fmt.Println(result) // output: error
	fmt.Println(msg)    // output: issue at line 5: field "dateField" not in '{"badField":"2007-02-03T18:05:06Z"}' (./test/test.json)

	// cleanup
	os.Remove("./test/test.json")
	os.Remove("./test/16.json")
	os.Remove("./test/17.json")
	os.Remove("./test/18.json")
	os.Remove("./test")

	// Output:
	// error
	// issue at line 5: field "dateField" not in '{"badField":"2007-02-03T18:05:06Z"}' (./test/test.json)
}

func ExampleDoTaskJSONBadRecordDiscard() {
	os.Setenv("TZ", "UTC")
	defer os.Unsetenv("TZ")

	opts := options{}
	opts.Producer, _ = bus.NewProducer(bus.NewOptions("nop"))

	// write file
	w, _ := file.NewWriter("./test/test.json", nil)
	w.WriteLine([]byte(`{"dateField":"2007-02-03T16:05:06Z"}`))
	w.WriteLine([]byte(`{"dateField":"2007-02-03T16:05:06Z"}`))
	w.WriteLine([]byte(`{"dateField":"2007-02-03T17:05:06Z"}`))
	w.WriteLine([]byte(`{"dateField":"2007-02-03T18:05:06Z"}`))
	w.WriteLine([]byte(`{"badField":"2007-02-03T18:05:06Z"}`))
	w.WriteLine([]byte(`{"badField":"2007-02-03T18:05:06Z"}`))
	w.Close()

	ctx, _ := context.WithCancel(context.Background())
	info := `./test/test.json?date-field=dateField&dest-template=./test/{HH}.json&discard=true`
	wkr := opts.newWorker(info)
	result, msg := wkr.DoTask(ctx)

	fmt.Println(result) // output: complete
	fmt.Println(msg)    // output: output: wrote 4 lines over 3 files (2 discarded)

	// cleanup
	os.Remove("./test/test.json")
	os.Remove("./test/16.json")
	os.Remove("./test/17.json")
	os.Remove("./test/18.json")
	os.Remove("./test")

	// Output:
	// complete
	// wrote 4 lines over 3 files (2 discarded)
}

func ExampleDoTaskCSV() {
	os.Setenv("TZ", "UTC")
	defer os.Unsetenv("TZ")

	opts := options{}
	opts.Producer, _ = bus.NewProducer(bus.NewOptions("nop"))

	// write file
	w, _ := file.NewWriter("./test/test.csv", nil)
	w.WriteLine([]byte(`2007-02-03T16:05:06Z`))
	w.WriteLine([]byte(`2007-02-03T16:05:06Z`))
	w.WriteLine([]byte(`2007-02-03T17:05:06Z`))
	w.WriteLine([]byte(`2007-02-03T18:05:06Z`))
	w.Close()

	ctx, _ := context.WithCancel(context.Background())
	info := "./test/test.csv?date-field=0&dest-template=./test/{HH}.csv&sep=,"
	wkr := opts.newWorker(info)
	result, msg := wkr.DoTask(ctx)

	fmt.Println(result) // output: complete
	fmt.Println(msg)    // output: wrote 4 lines over 3 files

	// cleanup
	os.Remove("./test/test.csv")
	os.Remove("./test/16.csv")
	os.Remove("./test/17.csv")
	os.Remove("./test/18.csv")
	os.Remove("./test")

	// Output:
	// complete
	// wrote 4 lines over 3 files
}

func ExampleDoTaskWCloseErr() {
	os.Setenv("TZ", "UTC")
	defer os.Unsetenv("TZ")

	// write file
	w, _ := file.NewWriter("./test/test.csv", nil)
	w.WriteLine([]byte(`2007-02-03T16:05:06Z`))
	w.Close()
	opts := options{}
	ctx, _ := context.WithCancel(context.Background())
	info := "./test/test.csv?date-field=0&dest-template=nop://close_err/{HH}.csv&sep=,"
	wkr := opts.newWorker(info)
	result, msg := wkr.DoTask(ctx)

	fmt.Println(result) // output: error
	fmt.Println(msg)    // output: close_err

	// cleanup
	os.Remove("./test/test.csv")
	os.Remove("./test/16.csv")
	os.Remove("./test")

	// Output:
	// error
	// close_err
}

func ExampleDoTaskReadLineErr() {
	os.Setenv("TZ", "UTC")
	defer os.Unsetenv("TZ")
	opts := options{}
	ctx, _ := context.WithCancel(context.Background())
	info := `nop://readline_err?date-field=0&dest-template=nop://{HH}.csv&sep=,`
	wkr := opts.newWorker(info)
	result, msg := wkr.DoTask(ctx)

	fmt.Println(result) // output: error
	fmt.Println(msg)    // output: issue at line 1: readline_err (nop://readline_err)

	// Output:
	// error
	// issue at line 1: readline_err (nop://readline_err)
}

func ExampleWorker_DoTaskDirSrc() {
	os.Setenv("TZ", "UTC")
	defer os.Unsetenv("TZ")

	opts := options{}
	opts.Producer, _ = bus.NewProducer(bus.NewOptions("nop"))

	// src file 1
	w1, _ := file.NewWriter("./test/dir/test1.csv", nil)
	w1.WriteLine([]byte(`2007-02-03T16:05:06Z`))
	w1.WriteLine([]byte(`2007-02-03T16:05:06Z`))
	w1.WriteLine([]byte(`2007-02-03T17:05:06Z`))
	w1.WriteLine([]byte(`2007-02-03T18:05:06Z`))
	w1.Close()

	// src file 2
	w2, _ := file.NewWriter("./test/dir/test2.csv", nil)
	w2.WriteLine([]byte(`2007-02-03T16:05:06Z`))
	w2.WriteLine([]byte(`2007-02-03T16:05:06Z`))
	w2.WriteLine([]byte(`2007-02-03T17:05:06Z`))
	w2.WriteLine([]byte(`2007-02-03T20:05:06Z`))
	w2.Close()

	// src file 3
	w3, _ := file.NewWriter("./test/dir/test3.csv", nil)
	w3.WriteLine([]byte(`2007-02-03T16:05:06Z`))
	w3.WriteLine([]byte(`2007-02-03T16:05:06Z`))
	w3.WriteLine([]byte(`2007-02-03T17:05:06Z`))
	w3.WriteLine([]byte(`2007-02-03T19:05:06Z`))
	w3.Close()

	ctx, _ := context.WithCancel(context.Background())
	info := "./test/dir?date-field=0&dest-template=./test/{HH}.csv&sep=,"
	wkr := opts.newWorker(info)
	result, msg := wkr.DoTask(ctx)

	fmt.Println(result) // output: complete
	fmt.Println(msg)    // output: wrote 8 lines over 3 files

	// cleanup
	os.Remove("./test/dir/test1.csv")
	os.Remove("./test/dir/test2.csv")
	os.Remove("./test/dir/test3.csv")
	os.Remove("./test/16.csv")
	os.Remove("./test/17.csv")
	os.Remove("./test/18.csv")
	os.Remove("./test/19.csv")
	os.Remove("./test/20.csv")
	os.Remove("./test/dir")
	os.Remove("./test")

	// Output:
	// complete
	// wrote 12 lines over 5 files
}

func TestParseTmpl(t *testing.T) {
	type tCase struct {
		srcPth   string // source file
		tmpl     string // template string
		expected string // expected parsed template
	}
	cases := []tCase{
		// {SRC_FILE} cases
		{srcPth: "dir/path/file.txt.gz", tmpl: "srcf-{SRC_FILE}", expected: "srcf-file.txt.gz"},
		{srcPth: "dir/path/file.txt", tmpl: "srcf-{SRC_FILE}", expected: "srcf-file.txt"},
		{srcPth: "file.txt", tmpl: "srcf-{SRC_FILE}", expected: "srcf-file.txt"},
		{srcPth: "", tmpl: "srcf-{SRC_FILE}", expected: "srcf-{SRC_FILE}"},
		{srcPth: "dir/path/", tmpl: "srcf-{SRC_FILE}", expected: "srcf-{SRC_FILE}"},
		{srcPth: "dir/path", tmpl: "srcf-{SRC_FILE}", expected: "srcf-path"},
		{srcPth: "dir/path/file.txt", tmpl: "srcf-{src_file}", expected: "srcf-{src_file}"},

		// {SRC_TS} cases
		{srcPth: "dir/path/file-20070203T160101.json.gz", tmpl: "srcts-{SRC_TS}", expected: "srcts-20070203T160101"},
		{srcPth: "dir/path/file-20070203T160101.json.gz", tmpl: "srcts-{SRC_TS}.json.gz", expected: "srcts-20070203T160101.json.gz"},
		{srcPth: "dir/path/file-20070203T160101.json.gz", tmpl: "srcts-{src_ts}.json.gz", expected: "srcts-{src_ts}.json.gz"},
		{srcPth: "file-20070203T160101.json.gz", tmpl: "srcts-{SRC_TS}.json.gz", expected: "srcts-20070203T160101.json.gz"},
		{srcPth: "dir/path/", tmpl: "srcts-{SRC_TS}.json.gz", expected: "srcts-{SRC_TS}.json.gz"},
		{srcPth: "", tmpl: "srcts-{SRC_TS}.json.gz", expected: "srcts-{SRC_TS}.json.gz"},
		{srcPth: "dir/path/2017/02/03/04/file.txt", tmpl: "srcts-{SRC_TS}.txt", expected: "srcts-20170203T040000.txt"},
		{srcPth: "dir/path/2017/02/03/file.txt", tmpl: "srcts-{SRC_TS}.txt", expected: "srcts-20170203T000000.txt"},
		{srcPth: "dir/path/2017/02/file.txt", tmpl: "srcts-{SRC_TS}.txt", expected: "srcts-20170201T000000.txt"},
		{srcPth: "dir/path/2017/file.txt", tmpl: "srcts-{SRC_TS}.txt", expected: "srcts-{SRC_TS}.txt"},

		// {SRC_TS} with {SRC_FILE} cases
		{srcPth: "dir/path/file-20070203T160101.json.gz", tmpl: "{SRC_TS}-{SRC_FILE}", expected: "20070203T160101-file-20070203T160101.json.gz"},
	}

	for _, tc := range cases {
		got := parseTmpl(tc.srcPth, tc.tmpl)
		if tc.expected != got {
			t.Errorf("for srcPth:'%v' and tmpl:'%v' expected '%v' but got '%v'", tc.srcPth, tc.tmpl, tc.expected, got)
		}
	}
}
