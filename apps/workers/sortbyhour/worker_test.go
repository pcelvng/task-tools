package main

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task/bus"
)

func ExampleNewInfoOptions() {
	iOpt, err := newInfoOptions("")

	fmt.Println(err)                     // output: <nil>
	fmt.Println(iOpt.SrcPath == "")      // output: true
	fmt.Println(iOpt.RecordType == "")   // output: true
	fmt.Println(iOpt.DateField == "")    // output: true
	fmt.Println(iOpt.DateFieldIndex)     // output: 0
	fmt.Println(iOpt.DateFormat == "")   // output: true
	fmt.Println(iOpt.CSVSep == "")       // output: true
	fmt.Println(iOpt.DestTemplate == "") // output: true
	fmt.Println(iOpt.Discard)            // output: false
	fmt.Println(iOpt.UseFileBuffer)      // output: false

	// Output:
	// <nil>
	// true
	// true
	// true
	// 0
	// true
	// true
	// true
	// false
	// false
}

func ExampleNewInfoOptionsJSON() {
	info := `nop://source/file.json?record-type=json&date-field=testDateField&date-format=testFormat&dest-template=testTemplate&discard=true&use-file-buffer=true`
	iOpt, err := newInfoOptions(info)

	fmt.Println(err)                 // output: <nil>
	fmt.Println(iOpt.SrcPath)        // output: nop://source/file.json
	fmt.Println(iOpt.RecordType)     // output: json
	fmt.Println(iOpt.DateField)      // output: testDateField
	fmt.Println(iOpt.DateFieldIndex) // output: 0
	fmt.Println(iOpt.DateFormat)     // output: testFormat
	fmt.Println(iOpt.CSVSep)         // output:
	fmt.Println(iOpt.DestTemplate)   // output: testTemplate
	fmt.Println(iOpt.Discard)        // output: true
	fmt.Println(iOpt.UseFileBuffer)  // output: true

	// Output:
	// <nil>
	// nop://source/file.json
	// json
	// testDateField
	// 0
	// testFormat
	//
	// testTemplate
	// true
	// true
}

func ExampleNewInfoOptionsCSV() {
	info := `nop://source/file.csv?record-type=csv&date-field-index=1&date-format=testFormat&sep=testSep&dest-template=testTemplate&discard=true&use-file-buffer=true`
	iOpt, err := newInfoOptions(info)

	fmt.Println(err)                 // output: <nil>
	fmt.Println(iOpt.SrcPath)        // output: nop://source/file.csv
	fmt.Println(iOpt.RecordType)     // output: csv
	fmt.Println(iOpt.DateField)      // output:
	fmt.Println(iOpt.DateFieldIndex) // output: 0
	fmt.Println(iOpt.DateFormat)     // output: testFormat
	fmt.Println(iOpt.CSVSep)         // output: testSep
	fmt.Println(iOpt.DestTemplate)   // output: testTemplate
	fmt.Println(iOpt.Discard)        // output: true
	fmt.Println(iOpt.UseFileBuffer)  // output: true

	// Output:
	// <nil>
	// nop://source/file.csv
	// csv
	//
	// 1
	// testFormat
	// testSep
	// testTemplate
	// true
	// true
}

func ExampleNewInfoOptionsFieldMatching() {
	info := `nop://source/file.csv?Record-Type=csv`
	iOpt, err := newInfoOptions(info)

	fmt.Println(err)             // output: <nil>
	fmt.Println(iOpt.RecordType) // output:
	fmt.Println(iOpt.SrcPath)    // output: nop://source/file.csv

	// Output:
	// <nil>
	//
	// nop://source/file.csv
}

func ExampleNewInfoOptions_ValidateJSON() {
	info := `nop://source/file.json?record-type=json&date-field=testDateField&dest-template=testTemplate`
	iOpt, _ := newInfoOptions(info)
	err := iOpt.validate()

	fmt.Println(err) // output: <nil>

	// Output:
	// <nil>
}

func ExampleNewInfoOptions_ValidateJSONErr() {
	info := `nop://source/file.json?record-type=json`
	iOpt, _ := newInfoOptions(info)
	err := iOpt.validate()

	fmt.Println(err) // output: date-field required

	// Output:
	// date-field required
}

func ExampleNewInfoOptions_ValidateDestTemplateErr() {
	info := `nop://source/file.json?record-type=json&date-field=testDateField`
	iOpt, _ := newInfoOptions(info)
	err := iOpt.validate()

	fmt.Println(err) // output: dest-template required

	// Output:
	// dest-template required
}

func ExampleNewInfoOptions_ValidateCSV() {
	info := `nop://source/file.json?record-type=csv&dest-template=testTemplate`
	iOpt, _ := newInfoOptions(info)
	err := iOpt.validate()

	fmt.Println(err) // output: <nil>

	// Output:
	// <nil>
}

func ExampleNewInfoOptions_ValidateRecordTypeErr() {
	info := `nop://source/file.json?record-type=badType`
	iOpt, _ := newInfoOptions(info)
	err := iOpt.validate()

	fmt.Println(err) // output: record-type must be "csv" or "json"

	// Output:
	// record-type must be "csv" or "json"
}

func ExampleMakeWorkerJSON() {
	ctx, cncl := context.WithCancel(context.Background())
	cncl()
	info := `nop://source/file.json?record-type=json&date-field=testDateField&dest-template=testTemplate`
	wkr := MakeWorker(info)
	result, msg := wkr.DoTask(ctx)

	fmt.Println(result) // output: error
	fmt.Println(msg)    // output: task interrupted

	// Output:
	// error
	// task interrupted
}

func ExampleMakeWorkerValidateErr() {
	ctx, cncl := context.WithCancel(context.Background())
	cncl()
	info := `nop://source/file.json?record-type=json&dest-template=testTemplate`
	wkr := MakeWorker(info)
	result, msg := wkr.DoTask(ctx)

	fmt.Println(result) // output: error
	fmt.Println(msg)    // output: date-field required

	// Output:
	// error
	// date-field required
}

func ExampleMakeWorkerReaderErr() {
	ctx, cncl := context.WithCancel(context.Background())
	cncl()
	info := `nop://init_err/file.json?record-type=json&date-field=testDateField&dest-template=testTemplate`
	wkr := MakeWorker(info)
	result, msg := wkr.DoTask(ctx)

	fmt.Println(result) // output: error
	fmt.Println(msg)    // output: init_err

	// Output:
	// error
	// init_err
}

func ExampleMakeWorkerCSV() {
	ctx, cncl := context.WithCancel(context.Background())
	cncl()
	info := `nop://source/file.csv?record-type=csv&date-field-index=1&date-format=testFormat&sep=testSep&dest-template=testTemplate&discard=true&use-file-buffer=true`
	wkr := MakeWorker(info)
	result, msg := wkr.DoTask(ctx)

	fmt.Println(result) // output: error
	fmt.Println(msg)    // output: task interrupted

	// Output:
	// error
	// task interrupted
}

func ExampleDoTaskJSON() {
	os.Setenv("TZ", "UTC")
	defer os.Unsetenv("TZ")

	// initialize producer
	producer, _ = bus.NewProducer(bus.NewOptions("nop"))

	// write file
	w, _ := file.NewWriter("./test/test-20050405T201112.json", nil)
	w.WriteLine([]byte(`{"dateField":"2007-02-03T16:05:06Z"}`))
	w.WriteLine([]byte(`{"dateField":"2007-02-03T16:05:06Z"}`))
	w.WriteLine([]byte(`{"dateField":"2007-02-03T17:05:06Z"}`))
	w.WriteLine([]byte(`{"dateField":"2007-02-03T18:05:06Z"}`))
	w.Close()

	ctx, _ := context.WithCancel(context.Background())
	info := `./test/test-20050405T201112.json?record-type=json&date-field=dateField&dest-template=./test/{HH}-{SRC_TS}.json`
	wkr := MakeWorker(info)
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

	// initialize producer
	producer, _ = bus.NewProducer(bus.NewOptions("nop"))

	// write file
	w, _ := file.NewWriter("./test/test.json", nil)
	w.WriteLine([]byte(`{"dateField":"2007-02-03T16:05:06Z"}`))
	w.WriteLine([]byte(`{"dateField":"2007-02-03T16:05:06Z"}`))
	w.WriteLine([]byte(`{"dateField":"2007-02-03T17:05:06Z"}`))
	w.WriteLine([]byte(`{"dateField":"2007-02-03T18:05:06Z"}`))
	w.WriteLine([]byte(`{"badField":"2007-02-03T18:05:06Z"}`))
	w.Close()

	ctx, _ := context.WithCancel(context.Background())
	info := `./test/test.json?record-type=json&date-field=dateField&dest-template=./test/{HH}.json`
	wkr := MakeWorker(info)
	result, msg := wkr.DoTask(ctx)

	fmt.Println(result) // output: error
	fmt.Println(msg)    // output: issue at line 5: field "dateField" not in '{"badField":"2007-02-03T18:05:06Z"}'

	// cleanup
	os.Remove("./test/test.json")
	os.Remove("./test/16.json")
	os.Remove("./test/17.json")
	os.Remove("./test/18.json")
	os.Remove("./test")

	// Output:
	// error
	// issue at line 5: field "dateField" not in '{"badField":"2007-02-03T18:05:06Z"}'
}

func ExampleDoTaskJSONBadRecordDiscard() {
	os.Setenv("TZ", "UTC")
	defer os.Unsetenv("TZ")

	// initialize producer
	producer, _ = bus.NewProducer(bus.NewOptions("nop"))

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
	info := `./test/test.json?record-type=json&date-field=dateField&dest-template=./test/{HH}.json&discard=true`
	wkr := MakeWorker(info)
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

	// initialize producer
	producer, _ = bus.NewProducer(bus.NewOptions("nop"))

	// write file
	w, _ := file.NewWriter("./test/test.csv", nil)
	w.WriteLine([]byte(`2007-02-03T16:05:06Z`))
	w.WriteLine([]byte(`2007-02-03T16:05:06Z`))
	w.WriteLine([]byte(`2007-02-03T17:05:06Z`))
	w.WriteLine([]byte(`2007-02-03T18:05:06Z`))
	w.Close()

	ctx, _ := context.WithCancel(context.Background())
	info := `./test/test.csv?record-type=csv&date-field-index=0&dest-template=./test/{HH}.csv`
	wkr := MakeWorker(info)
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

	ctx, _ := context.WithCancel(context.Background())
	info := `./test/test.csv?record-type=csv&date-field-index=0&dest-template=nop://close_err/{HH}.csv`
	wkr := MakeWorker(info)
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

	ctx, _ := context.WithCancel(context.Background())
	info := `nop://readline_err/?record-type=csv&date-field-index=0&dest-template=nop://{HH}.csv`
	wkr := MakeWorker(info)
	result, msg := wkr.DoTask(ctx)

	fmt.Println(result) // output: error
	fmt.Println(msg)    // output: issue at line 1: readline_err

	// Output:
	// error
	// issue at line 1: readline_err
}

func TestParseTmpl(t *testing.T) {
	type tCase struct {
		srcPth   string // source file
		tmpl     string // template string
		expected string // expected parsed template
	}
	cases := []tCase{
		// {SRC_FILE} cases
		tCase{srcPth: "dir/path/file.txt.gz", tmpl: "srcf-{SRC_FILE}", expected: "srcf-file.txt.gz"},
		tCase{srcPth: "dir/path/file.txt", tmpl: "srcf-{SRC_FILE}", expected: "srcf-file.txt"},
		tCase{srcPth: "file.txt", tmpl: "srcf-{SRC_FILE}", expected: "srcf-file.txt"},
		tCase{srcPth: "", tmpl: "srcf-{SRC_FILE}", expected: "srcf-{SRC_FILE}"},
		tCase{srcPth: "dir/path/", tmpl: "srcf-{SRC_FILE}", expected: "srcf-{SRC_FILE}"},
		tCase{srcPth: "dir/path", tmpl: "srcf-{SRC_FILE}", expected: "srcf-path"},
		tCase{srcPth: "dir/path/file.txt", tmpl: "srcf-{src_file}", expected: "srcf-{src_file}"},

		// {SRC_TS} cases
		tCase{srcPth: "dir/path/file-20070203T160101.json.gz", tmpl: "srcts-{SRC_TS}", expected: "srcts-20070203T160101"},
		tCase{srcPth: "dir/path/file-20070203T160101.json.gz", tmpl: "srcts-{SRC_TS}.json.gz", expected: "srcts-20070203T160101.json.gz"},
		tCase{srcPth: "dir/path/file-20070203T160101.json.gz", tmpl: "srcts-{src_ts}.json.gz", expected: "srcts-{src_ts}.json.gz"},
		tCase{srcPth: "file-20070203T160101.json.gz", tmpl: "srcts-{SRC_TS}.json.gz", expected: "srcts-20070203T160101.json.gz"},
		tCase{srcPth: "dir/path/", tmpl: "srcts-{SRC_TS}.json.gz", expected: "srcts-{SRC_TS}.json.gz"},
		tCase{srcPth: "", tmpl: "srcts-{SRC_TS}.json.gz", expected: "srcts-{SRC_TS}.json.gz"},

		// {SRC_TS} with {SRC_FILE} cases
		tCase{srcPth: "dir/path/file-20070203T160101.json.gz", tmpl: "{SRC_TS}-{SRC_FILE}", expected: "20070203T160101-file-20070203T160101.json.gz"},
	}

	for _, tc := range cases {
		got := parseTmpl(tc.srcPth, tc.tmpl)
		if tc.expected != got {
			t.Errorf("for srcPth:'%v' and tmpl:'%v' expected '%v' but got '%v'", tc.srcPth, tc.tmpl, tc.expected, got)
		}
	}
}
