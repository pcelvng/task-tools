package file

import (
	"fmt"
	"log"
	"os"
)

func ExampleNewWriteByHour() {
	destTmpl := "./test/{HH}.csv"
	csvExtractor := CSVDateExtractor("", "", 0)
	wBy := NewWriteByHour(destTmpl, csvExtractor, nil)
	if wBy == nil {
		return
	}

	fmt.Println(wBy.opt != nil)     // output: true
	fmt.Println(wBy.destTmpl)       // output: ./test/{HH}.csv
	fmt.Println(wBy.writers != nil) // output: true

	// Output:
	// true
	// ./test/{HH}.csv
	// true
}

func ExampleNewWriteByHourJSON() {
	destTmpl := "./test/{HH}.json"
	jsonExtractor := JSONDateExtractor("dateField", "")
	wBy := NewWriteByHour(destTmpl, jsonExtractor, nil)
	if wBy == nil {
		return
	}

	fmt.Println(wBy.opt != nil)     // output: true
	fmt.Println(wBy.destTmpl)       // output: ./test/{HH}.json
	fmt.Println(wBy.writers != nil) // output: true

	// Output:
	// true
	// ./test/{HH}.json
	// true
}

func ExampleWriteByHour_WriteLine() {
	os.Setenv("TZ", "UTC")

	destTmpl := "./test/{HH}.csv"
	csvExtractor := CSVDateExtractor("", "", 0)
	wBy := NewWriteByHour(destTmpl, csvExtractor, nil)
	if wBy == nil {
		return
	}

	ln1 := []byte("2007-02-03T16:05:06Z,test field")
	err := wBy.WriteLine(ln1)
	if err != nil {
		return
	}
	wBy.Close()

	// read from file
	pth := "./test/16.csv"
	f, _ := os.Open(pth)
	b := make([]byte, 32)
	f.Read(b)

	fmt.Println(err)                 // output: <nil>
	fmt.Print(string(b))             // output: 2007-02-03T16:05:06Z,test field
	fmt.Println(wBy.lineCnt.LineCnt) // output: 1

	// cleanup
	os.Remove(pth)
	os.Remove("./test")
	os.Unsetenv("TZ")

	// Output:
	// <nil>
	// 2007-02-03T16:05:06Z,test field
	// 1
}

func ExampleWriteByHour_WriteLineMulti() {
	os.Setenv("TZ", "UTC")

	destTmpl := "./test/{HH}.csv"
	csvExtractor := CSVDateExtractor("", "", 0)
	wBy := NewWriteByHour(destTmpl, csvExtractor, nil)
	if wBy == nil {
		return
	}

	ln1 := []byte("2007-03-04T16:05:06Z,test field")
	ln2 := []byte("2007-04-05T17:05:06Z,test field")
	ln3 := []byte("2007-03-04T16:05:06Z,test field") // same hour as ln1
	wBy.WriteLine(ln1)
	err := wBy.WriteLine(ln2)
	if err != nil {
		return
	}
	err = wBy.WriteLine(ln3)
	if err != nil {
		return
	}
	wBy.Close()

	// read from file 1
	pth1 := "./test/16.csv"
	f1, err := os.Open(pth1)
	if err != nil {
		log.Println(err)
		return
	}
	b1 := make([]byte, 64)
	f1.Read(b1)

	// read from file 2
	pth2 := "./test/17.csv"
	f2, err := os.Open(pth2)
	if err != nil {
		log.Println(err)
		return
	}
	b2 := make([]byte, 32)
	f2.Read(b2)

	fmt.Print(string(b1))            // output: 2007-03-04T16:05:06Z,test field\n2007-03-04T16:05:06Z,test field\n
	fmt.Print(string(b2))            // output: 2007-04-05T17:05:06Z,test field\n
	fmt.Println(wBy.lineCnt.LineCnt) // output: 3

	// cleanup
	os.Remove(pth1)
	os.Remove(pth2)
	os.Remove("./test")
	os.Unsetenv("TZ")

	// Output:
	// 2007-03-04T16:05:06Z,test field
	// 2007-03-04T16:05:06Z,test field
	// 2007-04-05T17:05:06Z,test field
	// 3
}

func ExampleWriteByHour_WriteLineErrExtractDate() {
	os.Setenv("TZ", "UTC")

	destTmpl := "nop://{HH}.csv"
	csvExtractor := CSVDateExtractor("", "", 0)
	wBy := NewWriteByHour(destTmpl, csvExtractor, nil)
	if wBy == nil {
		return
	}

	ln1 := []byte("not date,test field")
	err := wBy.WriteLine(ln1)

	fmt.Println(err) // output: parsing time "not date" as "2006-01-02T15:04:05Z07:00": cannot parse "not date" as "2006"

	// cleanup
	os.Unsetenv("TZ")

	// Output:
	// parsing time "not date" as "2006-01-02T15:04:05Z07:00": cannot parse "not date" as "2006"
}

func ExampleWriteByHour_WriteLineErrNewWriter() {
	os.Setenv("TZ", "UTC")

	destTmpl := "nop://init_err/"
	csvExtractor := CSVDateExtractor("", "", 0)
	wBy := NewWriteByHour(destTmpl, csvExtractor, nil)
	if wBy == nil {
		return
	}

	ln1 := []byte("2007-02-03T16:05:06Z,test field")
	err := wBy.WriteLine(ln1)

	fmt.Println(err) // output: init_err

	// cleanup
	os.Unsetenv("TZ")

	// Output:
	// init_err
}

func ExampleWriteByHour_WriteLineErrWriteLine() {
	os.Setenv("TZ", "UTC")

	destTmpl := "nop://writeline_err/"
	csvExtractor := CSVDateExtractor("", "", 0)
	wBy := NewWriteByHour(destTmpl, csvExtractor, nil)
	if wBy == nil {
		return
	}

	ln1 := []byte("2007-02-03T16:05:06Z,test field")
	err := wBy.WriteLine(ln1)

	fmt.Println(err) // output: writeline_err

	// cleanup
	os.Unsetenv("TZ")

	// Output:
	// writeline_err
}

func ExampleWriteByHour_LineCnt() {
	os.Setenv("TZ", "UTC")

	destTmpl := "./test/{HH}.csv"
	csvExtractor := CSVDateExtractor("", "", 0)
	wBy := NewWriteByHour(destTmpl, csvExtractor, nil)
	if wBy == nil {
		return
	}

	ln := []byte("2007-02-03T16:05:06Z,test field")
	wBy.WriteLine(ln)

	fmt.Println(wBy.LineCnt()) // output: 1

	// cleanup
	os.Remove("./test")
	os.Unsetenv("TZ")

	// Output:
	// 1
}

func ExampleWriteByHour_Stats() {
	os.Setenv("TZ", "UTC")

	destTmpl := "./test/{HH}.csv"
	csvExtractor := CSVDateExtractor("", "", 0)
	wBy := NewWriteByHour(destTmpl, csvExtractor, nil)
	if wBy == nil {
		return
	}

	wBy.WriteLine([]byte("2007-02-03T16:05:06Z,test field"))
	wBy.WriteLine([]byte("2007-02-03T17:05:06Z,test field"))
	wBy.WriteLine([]byte("2007-02-03T18:05:06Z,test field"))
	allSts := wBy.Stats()

	for _, sts := range allSts {
		fmt.Println(sts.LineCnt) // output: 1
		fmt.Println(sts.ByteCnt) // output: 32
	}
	fmt.Println(len(allSts)) // output: 3

	// cleanup
	os.Remove("./test")
	os.Unsetenv("TZ")

	// Output:
	// 1
	// 32
	// 1
	// 32
	// 1
	// 32
	// 3
}

func ExampleWriteByHour_Abort() {
	os.Setenv("TZ", "UTC")

	destTmpl := "./test/{HH}.csv"
	csvExtractor := CSVDateExtractor("", "", 0)
	wBy := NewWriteByHour(destTmpl, csvExtractor, nil)
	if wBy == nil {
		return
	}

	wBy.WriteLine([]byte("2007-02-03T16:05:06Z,test field"))
	wBy.WriteLine([]byte("2007-02-03T17:05:06Z,test field"))
	wBy.WriteLine([]byte("2007-02-03T18:05:06Z,test field"))
	err := wBy.Abort()

	fmt.Println(err) // output: <nil>

	// cleanup
	os.Remove("./test")
	os.Unsetenv("TZ")

	// Output:
	// <nil>
}

func ExampleWriteByHour_AbortErr() {
	os.Setenv("TZ", "UTC")

	destTmpl := "nop://abort_err/"
	csvExtractor := CSVDateExtractor("", "", 0)
	wBy := NewWriteByHour(destTmpl, csvExtractor, nil)
	if wBy == nil {
		return
	}

	wBy.WriteLine([]byte("2007-02-03T16:05:06Z,test field"))
	wBy.WriteLine([]byte("2007-02-03T17:05:06Z,test field"))
	wBy.WriteLine([]byte("2007-02-03T18:05:06Z,test field"))
	err := wBy.Abort()

	fmt.Println(err) // output: abort_err

	// cleanup
	os.Unsetenv("TZ")

	// Output:
	// abort_err
}

func ExampleWriteByHour_CloseErr() {
	os.Setenv("TZ", "UTC")

	destTmpl := "nop://close_err/{HH}.txt"
	csvExtractor := CSVDateExtractor("", "", 0)
	wBy := NewWriteByHour(destTmpl, csvExtractor, nil)
	if wBy == nil {
		return
	}

	wBy.WriteLine([]byte("2007-02-03T16:05:06Z,test field"))
	wBy.WriteLine([]byte("2007-02-03T17:05:06Z,test field"))
	wBy.WriteLine([]byte("2007-02-03T18:05:06Z,test field"))
	err := wBy.Close()

	fmt.Println(err) // output: close_err

	// cleanup
	os.Unsetenv("TZ")

	// Output:
	// close_err
}

func ExampleCSVDateExtractor() {
	os.Setenv("TZ", "UTC")

	csvExtract := CSVDateExtractor("", "", 1)

	t, err := csvExtract([]byte("test field,2007-02-03T16:05:06Z"))

	fmt.Println(t.IsZero()) // output: false
	fmt.Println(err)        // output: <nil>

	// cleanup
	os.Unsetenv("TZ")

	// Output:
	// false
	// <nil>
}

func ExampleCSVDateExtractorNegativeIndex() {
	os.Setenv("TZ", "UTC")

	csvExtract := CSVDateExtractor("", "", -1)

	t, err := csvExtract([]byte("2007-02-03T16:05:06Z,test field"))

	fmt.Println(t.IsZero()) // output: false
	fmt.Println(err)        // output: <nil>

	// cleanup
	os.Unsetenv("TZ")

	// Output:
	// false
	// <nil>
}

func ExampleCSVDateExtractorIndexOutOfRange() {
	os.Setenv("TZ", "UTC")

	csvExtract := CSVDateExtractor("", "", 2)

	t, err := csvExtract([]byte("test field,2007-02-03T16:05:06Z"))

	fmt.Println(t.IsZero()) // output: true
	fmt.Println(err)        // output: index 2 not in 'test field,2007-02-03T16:05:06Z'

	// cleanup
	os.Unsetenv("TZ")

	// Output:
	// true
	// index 2 not in 'test field,2007-02-03T16:05:06Z'
}

func ExampleCSVDateExtractorIndexOutOfRange2() {
	os.Setenv("TZ", "UTC")

	csvExtract := CSVDateExtractor("", "", 3)

	t, err := csvExtract([]byte("test field,2007-02-03T16:05:06Z"))

	fmt.Println(t.IsZero()) // output: true
	fmt.Println(err)        // output: index 3 not in 'test field,2007-02-03T16:05:06Z'

	// cleanup
	os.Unsetenv("TZ")

	// Output:
	// true
	// index 3 not in 'test field,2007-02-03T16:05:06Z'
}

func ExampleCSVDateExtractorTabSeparated() {
	os.Setenv("TZ", "UTC")

	csvExtract := CSVDateExtractor("\t", "", 1)

	// line contains \t literal
	t, err := csvExtract([]byte("test field	2007-02-03T16:05:06Z"))

	fmt.Println(t.IsZero()) // output: false
	fmt.Println(err)        // output: <nil>

	// cleanup
	os.Unsetenv("TZ")

	// Output:
	// false
	// <nil>
}

func ExampleCSVDateExtractorTabSeparated2() {
	os.Setenv("TZ", "UTC")

	csvExtract := CSVDateExtractor("\t", "", 1)
	t, err := csvExtract([]byte("test field\t2007-02-03T16:05:06Z"))

	fmt.Println(t.IsZero()) // output: false
	fmt.Println(err)        // output: <nil>

	// cleanup
	os.Unsetenv("TZ")

	// Output:
	// false
	// <nil>
}

func ExampleCSVDateExtractorTabSeparated3() {
	os.Setenv("TZ", "UTC")

	// sep is \t literal
	csvExtract := CSVDateExtractor("	", "", 1)
	t, err := csvExtract([]byte("test field\t2007-02-03T16:05:06Z"))

	fmt.Println(t.IsZero()) // output: false
	fmt.Println(err)        // output: <nil>

	// cleanup
	os.Unsetenv("TZ")

	// Output:
	// false
	// <nil>
}

func ExampleCSVDateExtractorCustomFormat() {
	os.Setenv("TZ", "UTC")

	// sep is \t literal
	format := "20060102T150405Z0700"
	csvExtract := CSVDateExtractor("", format, 1)
	t, err := csvExtract([]byte("test field,20070203T160506Z"))

	fmt.Println(err)        // output: <nil>
	fmt.Println(t.IsZero()) // output: false
	fmt.Println(t.Year())   // output: 2007
	fmt.Println(t.Month())  // output: February
	fmt.Println(t.Day())    // output: 3
	fmt.Println(t.Hour())   // output: 16
	fmt.Println(t.Minute()) // output: 5
	fmt.Println(t.Second()) // output: 6

	// cleanup
	os.Unsetenv("TZ")

	// Output:
	// <nil>
	// false
	// 2007
	// February
	// 3
	// 16
	// 5
	// 6
}

func ExampleCSVDateExtractorEmptyField() {
	os.Setenv("TZ", "UTC")

	csvExtract := CSVDateExtractor("", "", 2)

	t, err := csvExtract([]byte(",,2007-02-03T16:05:06Z"))

	fmt.Println(t.IsZero()) // output: false
	fmt.Println(err)        // output: <nil>

	// cleanup
	os.Unsetenv("TZ")

	// Output:
	// false
	// <nil>
}

func ExampleJSONDateExtractor() {
	os.Setenv("TZ", "UTC")

	jsonExtract := JSONDateExtractor("date-field", "")

	t, err := jsonExtract([]byte(`{"date-field":"2007-02-03T16:05:06Z","other-field":"other-value"}`))

	fmt.Println(err)        // output: <nil>
	fmt.Println(t.IsZero()) // output: false
	fmt.Println(t.Year())   // output: 2007
	fmt.Println(t.Month())  // output: February
	fmt.Println(t.Day())    // output: 3
	fmt.Println(t.Hour())   // output: 16
	fmt.Println(t.Minute()) // output: 5
	fmt.Println(t.Second()) // output: 6

	// cleanup
	os.Unsetenv("TZ")

	// Output:
	// <nil>
	// false
	// 2007
	// February
	// 3
	// 16
	// 5
	// 6
}

func ExampleJSONDateExtractorDateFieldNotFound() {
	os.Setenv("TZ", "UTC")

	jsonExtract := JSONDateExtractor("date-field", "")

	t, err := jsonExtract([]byte(`{"other-field":"other-value"}`))

	fmt.Println(err)        // output: <nil>
	fmt.Println(t.IsZero()) // output: true

	// cleanup
	os.Unsetenv("TZ")

	// Output:
	// field "date-field" not in '{"other-field":"other-value"}'
	// true
}

func ExampleJSONDateExtractorDateFieldTwice() {
	os.Setenv("TZ", "UTC")

	jsonExtract := JSONDateExtractor("date-field", "")

	t, err := jsonExtract([]byte(`{"date-field":"2007-02-03T16:05:06Z","date-field":"2007-02-03T16:05:06Z"}`))

	fmt.Println(err)        // output: <nil>
	fmt.Println(t.IsZero()) // output: false
	fmt.Println(t.Year())   // output: 2007
	fmt.Println(t.Month())  // output: February
	fmt.Println(t.Day())    // output: 3
	fmt.Println(t.Hour())   // output: 16
	fmt.Println(t.Minute()) // output: 5
	fmt.Println(t.Second()) // output: 6

	// cleanup
	os.Unsetenv("TZ")

	// Output:
	// <nil>
	// false
	// 2007
	// February
	// 3
	// 16
	// 5
	// 6
}

func ExampleJSONDateExtractorCustomFormat() {
	os.Setenv("TZ", "UTC")

	format := "20060102T150405Z0700"
	jsonExtract := JSONDateExtractor("date-field", format)

	t, err := jsonExtract([]byte(`{"date-field":"20070203T160506Z"}`))

	fmt.Println(err)        // output: <nil>
	fmt.Println(t.IsZero()) // output: false
	fmt.Println(t.Year())   // output: 2007
	fmt.Println(t.Month())  // output: February
	fmt.Println(t.Day())    // output: 3
	fmt.Println(t.Hour())   // output: 16
	fmt.Println(t.Minute()) // output: 5
	fmt.Println(t.Second()) // output: 6

	// cleanup
	os.Unsetenv("TZ")

	// Output:
	// <nil>
	// false
	// 2007
	// February
	// 3
	// 16
	// 5
	// 6
}

func ExampleJSONDateExtractorEmptyDateField() {
	os.Setenv("TZ", "UTC")

	jsonExtract := JSONDateExtractor("", "")

	t, err := jsonExtract([]byte(`{"date-field":"2007-02-03T16:05:06Z","date-field":"2007-02-03T16:05:06Z"}`))

	fmt.Println(err)        // output: field "" not in '{"date-field":"2007-02-03T16:05:06Z","date-field":"2007-02-03T16:05:06Z"}'
	fmt.Println(t.IsZero()) // output: true

	// cleanup
	os.Unsetenv("TZ")

	// Output:
	// field "" not in '{"date-field":"2007-02-03T16:05:06Z","date-field":"2007-02-03T16:05:06Z"}'
	// true
}
