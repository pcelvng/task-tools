package main

import (
	"context"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/hydronica/trial"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task/bus"

	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task-tools/file/stat"
)

func TestOptions_Validate(t *testing.T) {
	fn := func(opt infoOptions) (interface{}, error) {
		return nil, (&opt).validate()
	}
	cases := trial.Cases[infoOptions, any]{
		"valid options": {
			Input: infoOptions{
				Fields:       []string{"apple"},
				DestTemplate: "nop://",
			},
		},
		"missing destination": {
			Input: infoOptions{
				Fields: []string{"apple"},
			},
			ShouldErr: true,
		},
		"integer fields": {
			Input: infoOptions{
				Fields:       []string{"1", "2", "3"},
				Sep:          "\t",
				DestTemplate: "nop://",
			},
		},
		"integer range": {
			Input: infoOptions{
				Fields:       []string{"1-5", "7", "10-12"},
				Sep:          "\t",
				DestTemplate: "nop://",
			},
		},
		"invalid ints": {
			Input: infoOptions{
				Fields:       []string{"1-a5", "7", "10-12"},
				Sep:          "\t",
				DestTemplate: "nop://",
			},
			ShouldErr: true,
		},
	}
	trial.New(fn, cases).Test(t)
}

func TestWorker_DoTask(t *testing.T) {
	// setup
	os.Mkdir("./test", 0700)
	nopProducer, _ := bus.NewProducer(bus.NewOptions("nop"))
	bOpt := bus.NewOptions("file")
	fileTopic := "./test/files_stats.json"
	fileProducer, _ := bus.NewProducer(bOpt)

	pths := []string{
		"./test/1/dups.json",

		"./test/2/file-20160101T000000.json",
		"./test/2/file-20170101T000000.json",
		"./test/2/file-20180101T000000.json",

		"./test/3/file-20160101T000000.json",
		"./test/3/file-20170101T000000.json",
		"./test/3/file-20180101T000000.json",

		"./test/4/file1.json",
		"./test/4/file2.json",
		"./test/4/file3.json",

		"./test/5/dups-20160101T000000.csv",

		"./test/6/file-20160101T000000.csv",
		"./test/6/file-20170101T000000.csv",
		"./test/6/file-20180101T000000.csv",

		"./test/7/2017/01/02/03/test.json",

		"./test/8/2017/01/02/test.json",

		"./test/9/2017/01/test.json",
	}

	createdDates := []time.Time{
		time.Date(2016, 01, 01, 00, 00, 00, 00, time.UTC),
		time.Date(2017, 01, 01, 00, 00, 00, 00, time.UTC),
		time.Date(2018, 01, 01, 00, 00, 00, 00, time.UTC),
	}
	// line sets
	lineSets := [][]string{
		// set 1 - dups in set
		{
			`{"f1":"v1","f2":"v1","f3":"v1"}`,
			`{"f1":"v2","f2":"v1","f3":"v2"}`,
			`{"f1":"v1","f2":"v1","f3":"v3"}`,
			`{"f1":"v3","f2":"v1","f3":"v4"}`,
			`{"f1":"v2","f2":"v2","f3":"v5"}`,
			`{"f1":"v2","f2":"v2","f3":"v6"}`,
		},

		// set 2a - no dups in set (across one field key)
		{
			`{"f1":"v1","f2":"v1","f3":"v1"}`,
			`{"f1":"v2","f2":"v1","f3":"v2"}`,
			`{"f1":"v3","f2":"v1","f3":"v3"}`,
		},

		// set 2b - no dups in set (across one field key)
		{
			`{"f1":"v1","f2":"v1","f3":"v4"}`,
			`{"f1":"v2","f2":"v1","f3":"v5"}`,
			`{"f1":"v3","f2":"v1","f3":"v6"}`,
		},

		// set 2c - no dups in set (across one field key)
		{
			`{"f1":"v1","f2":"v1","f3":"v7"}`,
		},

		// set 3a - single record (across two field keys)
		{
			`{"f1":"v1","f2":"v1","f3":"v1"}`,
		},

		// set 3b - single record (across two field keys)
		{
			`{"f1":"v1","f2":"v1","f3":"v2"}`,
		},

		// set 3c - single record (across two field keys)
		{
			`{"f1":"v1","f2":"v1","f3":"v3"}`,
		},

		// set 4 - dups in set (csv - tab separated)
		{
			"f1v1	f2v1	f3v1",
			`f1v2	f2v1	f3v2`,
			`f1v3	f2v1	f3v3`,
			`f1v1	f2v1	f3v4`,
			`f1v2	f2v2	f3v5`,
			`f1v2	f2v2	f3v6`,
		},

		// set 5a - (csv - comma separated)
		{
			`f1v1,f2v1`,
			`f1v2,f2v2`,
			`f1v3,f2v3`,
		},

		// set 5b - (csv - comma separated)
		{
			`f1v1,f2v4`,
			`f1v4,f2v5`,
			`f1v5,f2v6`,
		},

		// set 5c - (csv - comma separated)
		{
			`f1v1,f2v7`,
			`f1v4,f2v8`,
			`f1v6,f2v9`,
		},
	}

	// scenario 1 file
	createFile(lineSets[0], pths[0], createdDates[0])

	// scenario 2 files
	createFile(lineSets[1], pths[1], createdDates[0])
	createFile(lineSets[2], pths[2], createdDates[0])
	createFile(lineSets[3], pths[3], createdDates[0])

	// scenario 3 files
	createFile(lineSets[4], pths[4], createdDates[0])
	createFile(lineSets[5], pths[5], createdDates[0])
	createFile(lineSets[6], pths[6], createdDates[0])

	// scenario 4 files
	createFile(lineSets[4], pths[7], createdDates[2])
	createFile(lineSets[5], pths[8], createdDates[0])
	createFile(lineSets[6], pths[9], createdDates[1])

	// scenario 5 file - csv
	createFile(lineSets[7], pths[10], createdDates[0])

	// scenario 6 files - csv
	createFile(lineSets[8], pths[11], createdDates[0])
	createFile(lineSets[9], pths[12], createdDates[0])
	createFile(lineSets[10], pths[13], createdDates[0])

	// scenario 7 file
	createFile(lineSets[0], pths[14], createdDates[0])

	// scenario 8 file
	createFile(lineSets[0], pths[15], createdDates[0])

	// scenario 9 file
	createFile(lineSets[0], pths[16], createdDates[0])

	// case1: single file with duplicates
	type scenario struct {
		appOpt         *options
		producer       bus.Producer
		info           string
		expectedResult task.Result
		expectedMsg    string
	}
	scenarios := []scenario{
		// scenario 1: single file input deduping file lines
		{
			appOpt:         &options{},
			producer:       nopProducer,
			info:           `./test/1/dups.json?dest-template=./test/1/dedup.json&fields=f1,f2`,
			expectedResult: task.CompleteResult,
			expectedMsg:    `read 6 lines from 1 files and wrote 4 lines`,
		},

		// scenario 2: multiple input files deduping across files
		{
			appOpt:         &options{},
			producer:       nopProducer,
			info:           `./test/2?dest-template=./test/2/dedup/dedup.json&fields=f1`,
			expectedResult: task.CompleteResult,
			expectedMsg:    `read 7 lines from 3 files and wrote 3 lines`,
		},

		// scenario 3: lines over-writing in the correct file order - by file ts date
		{
			appOpt:         &options{},
			producer:       nopProducer,
			info:           `./test/3/?dest-template=./test/3/dedup/dedup.json&fields=f1,f2`,
			expectedResult: task.CompleteResult,
			expectedMsg:    `read 3 lines from 3 files and wrote 1 lines`,
		},

		// scenario 4: lines over-writing in the correct file order - by file created date
		{
			appOpt:         &options{},
			producer:       fileProducer,
			info:           `./test/4?dest-template=./test/4/dedup/dedup.json&fields=f1,f2`,
			expectedResult: task.CompleteResult,
			expectedMsg:    `read 3 lines from 3 files and wrote 1 lines`,
		},

		// scenario 5: csv - tab separated
		{
			appOpt:         &options{},
			producer:       nopProducer,
			info:           "./test/5/dups-20160101T000000.csv?dest-template=./test/5/dedup/dedup.csv&sep=%09&fields=0,1",
			expectedResult: task.CompleteResult,
			expectedMsg:    `read 6 lines from 1 files and wrote 4 lines`,
		},

		// scenario 6: csv -multiple input files deduping across files
		{
			appOpt:         &options{},
			producer:       nopProducer,
			info:           "./test/6?dest-template=./test/6/dedup/dedup.csv&fields=0&sep=,",
			expectedResult: task.CompleteResult,
			expectedMsg:    `read 9 lines from 3 files and wrote 6 lines`,
		},

		// scenario 7: ts from hour slug in src dir
		{
			appOpt:         &options{},
			producer:       nopProducer,
			info:           "./test/7/2017/01/02/03/test.json?dest-template=./test/7/{HOUR_SLUG}/{TS}.json&fields=f1&",
			expectedResult: task.CompleteResult,
			expectedMsg:    `read 6 lines from 1 files and wrote 3 lines`,
		},

		// scenario 8: ts from day slug in src dir
		{
			appOpt:         &options{},
			producer:       nopProducer,
			info:           "./test/8/2017/01/02/test.json?dest-template=./test/8/{DAY_SLUG}/{TS}.json&fields=f1&",
			expectedResult: task.CompleteResult,
			expectedMsg:    `read 6 lines from 1 files and wrote 3 lines`,
		},

		// scenario 9: ts from month slug in src dir
		{
			appOpt:         &options{},
			producer:       nopProducer,
			info:           "./test/9/2017/01/test.json?dest-template=./test/9/{MONTH_SLUG}/{TS}.json&fields=f1&",
			expectedResult: task.CompleteResult,
			expectedMsg:    `read 6 lines from 1 files and wrote 3 lines`,
		},
	}

	for sNum, s := range scenarios {
		appOpt := s.appOpt
		appOpt.FileTopic = fileTopic
		appOpt.Producer = s.producer
		wkr := appOpt.newWorker(s.info)
		gotRslt, gotMsg := wkr.DoTask(context.Background())

		// check result
		if gotRslt != s.expectedResult {
			t.Errorf("scenario %v expected result '%v' but got '%v'", sNum+1, s.expectedResult, gotRslt)
		}

		// check msg
		if gotMsg != s.expectedMsg {
			t.Errorf("scenario %v expected msg '%v' but got '%v'", sNum+1, s.expectedMsg, gotMsg)
		}
	}

	// scenario 3 special check
	// match written line to expected
	expected := lineSets[6][0]
	b := make([]byte, len(expected))
	f, _ := os.Open("./test/3/dedup/dedup.json")
	f.Read(b)
	got := string(b)
	if expected != got {
		t.Errorf("got '%v' from file but expected '%v'", got, expected)
	}

	// scenario 4 special check
	// match written line to expected
	expected = lineSets[4][0]
	b = make([]byte, len(expected))
	f, _ = os.Open("./test/4/dedup/dedup.json")
	f.Read(b)
	got = string(b)
	if expected != got {
		t.Errorf("got '%v' from file but expected '%v'", got, expected)
	}

	// scenario 4 special check
	// verify file producer output
	expected = `dedup.json` // contains
	b = make([]byte, len(expected))
	r, _ := file.NewReader("./test/files_stats.json", nil)
	ln, _ := r.ReadLine()
	gotPth := stat.NewFromBytes(ln).Path
	if !strings.HasSuffix(gotPth, expected) {
		t.Errorf("got '%v' from stats file but expected '%v'", gotPth, expected)
	}

	// cleanup
	os.RemoveAll("./test/")
}

func TestWorker_DoTask_Err(t *testing.T) {
	// setup
	nopProducer, _ := bus.NewProducer(bus.NewOptions("nop"))
	cnclCtx, cncl := context.WithCancel(context.Background())
	cncl()

	pths := []string{
		"./test/test.json",
	}

	createdDates := []time.Time{
		time.Date(2016, 01, 01, 00, 00, 00, 00, time.UTC),
		time.Date(2017, 01, 01, 00, 00, 00, 00, time.UTC),
		time.Date(2018, 01, 01, 00, 00, 00, 00, time.UTC),
	}
	// line sets
	lineSets := [][]string{
		{
			`{"f1":"v1","f2":"v1","f3":"v1"}`,
			`{"f1":"v2","f2":"v1","f3":"v2"}`,
		},
	}

	// scenario 1 file
	createFile(lineSets[0], pths[0], createdDates[0])

	// case1: single file with duplicates
	type scenario struct {
		appOpt         *options
		producer       bus.Producer
		ctx            context.Context
		info           string
		expectedResult task.Result
		expectedMsg    string
	}
	scenarios := []scenario{
		// scenario 1: bad info (no fields)
		{
			appOpt:         &options{},
			producer:       nopProducer,
			ctx:            context.Background(),
			info:           ``,
			expectedResult: task.ErrResult,
			expectedMsg:    `fields required`,
		},

		// scenario 2: bad info (no dest-template)
		{
			appOpt:         &options{},
			producer:       nopProducer,
			ctx:            context.Background(),
			info:           `?fields=f1`,
			expectedResult: task.ErrResult,
			expectedMsg:    `dest-template required`,
		},

		// scenario 3: bad info (bad sep fields)
		{
			appOpt:         &options{},
			producer:       nopProducer,
			ctx:            context.Background(),
			info:           `?fields=f1&dest-template=./test/test.json&sep=,`,
			expectedResult: task.ErrResult,
			expectedMsg:    `invalid field f1 for csv file`,
		},

		// scenario 4: empty src dir
		{
			appOpt:         &options{},
			producer:       nopProducer,
			ctx:            context.Background(),
			info:           "./test/empty_dir/?fields=0&dest-template=./test/test.json&sep=,",
			expectedResult: task.ErrResult,
			expectedMsg:    `no such file or directory`,
		},

		// scenario 5: file does not exist
		{
			appOpt:         &options{},
			producer:       nopProducer,
			ctx:            context.Background(),
			info:           "./test/doesnotexist.json?fields=0&dest-template=./test/test.json&sep=,",
			expectedResult: task.ErrResult,
			expectedMsg:    `no such file or directory`,
		},

		// scenario 6: trouble reading file
		{
			appOpt:         &options{},
			producer:       nopProducer,
			ctx:            context.Background(),
			info:           "nop://readline_err/test.json?fields=f1&dest-template=./test/test.json",
			expectedResult: task.ErrResult,
			expectedMsg:    `issue at line 1: readline_err (nop://readline_err/test.json)`,
		},

		// scenario 7: cancelled by context
		{
			appOpt:         &options{},
			producer:       nopProducer,
			ctx:            cnclCtx, // already cancelled
			info:           "./test/test.json?fields=f1&dest-template=./test/output.json",
			expectedResult: task.ErrResult,
			expectedMsg:    `task interrupted`,
		},

		// scenario 8: err closing writer
		{
			appOpt:         &options{},
			producer:       nopProducer,
			ctx:            context.Background(),
			info:           "./test/test.json?fields=f1&dest-template=nop://close_err/test.json",
			expectedResult: task.ErrResult,
			expectedMsg:    `close_err`,
		},

		// scenario 9: err writer init
		{
			appOpt:         &options{},
			producer:       nopProducer,
			ctx:            context.Background(),
			info:           "./test/test.json?fields=f1&dest-template=nop://init_err/test.json",
			expectedResult: task.ErrResult,
			expectedMsg:    `init_err`,
		},
	}

	for sNum, s := range scenarios {
		appOpt := s.appOpt
		appOpt.Producer = s.producer
		wkr := appOpt.newWorker(s.info)
		gotRslt, gotMsg := wkr.DoTask(s.ctx)

		// check result
		if gotRslt != s.expectedResult {
			t.Errorf("scenario %v expected result '%v' but got '%v'", sNum+1, s.expectedResult, gotRslt)
		}

		// check msg
		if !strings.Contains(gotMsg, s.expectedMsg) {
			t.Errorf("scenario %v expected msg '%v' but got '%v'", sNum+1, s.expectedMsg, gotMsg)
		}
	}

	// cleanup
	os.RemoveAll("./test/")
}

func TestStatsReaders_Sort(t *testing.T) {
	// 1,2,3 have different dates
	sts1 := stat.New()
	sts1.Created = "2016-01-01T00:00:00Z" // oldest
	sts1.Path = "sts1"                    // set to identify
	sts2 := stat.New()
	sts2.Created = "2017-01-01T00:00:00Z"
	sts2.Path = "sts2"
	sts3 := stat.New()
	sts3.Created = "2018-01-01T00:00:00Z" // youngest
	sts3.Path = "sts3"
	// 4,5,6 have same date
	sts4 := stat.New()
	sts4.Created = "2016-01-01T00:00:00Z"
	sts4.Path = "sts4"
	sts5 := stat.New()
	sts5.Created = "2016-01-01T00:00:00Z"
	sts5.Path = "sts5"
	sts6 := stat.New()
	sts6.Created = "2016-01-01T00:00:00Z"
	sts6.Path = "sts6"

	// oldest to youngest
	pthTime1 := time.Date(2016, 01, 01, 00, 00, 00, 00, time.UTC)
	pthTime2 := time.Date(2017, 01, 01, 00, 00, 00, 00, time.UTC)
	pthTime3 := time.Date(2018, 01, 01, 00, 00, 00, 00, time.UTC)

	type scenario struct {
		stsRdrs       StatsFiles
		expectedOrder []string
	}

	scenarios := []scenario{
		// scenario 1: pthTime is the same, sts.Created are different
		{
			stsRdrs: StatsFiles{
				StatsFile{Stats: sts3, pthTime: pthTime1},
				StatsFile{Stats: sts1, pthTime: pthTime1},
				StatsFile{Stats: sts2, pthTime: pthTime1},
			},
			expectedOrder: []string{
				"sts1",
				"sts2",
				"sts3",
			},
		},

		// scenario 2: pthTime is different, sts.Created are same
		{
			stsRdrs: StatsFiles{
				StatsFile{Stats: sts6, pthTime: pthTime3},
				StatsFile{Stats: sts4, pthTime: pthTime1},
				StatsFile{Stats: sts5, pthTime: pthTime2},
			},
			expectedOrder: []string{
				"sts4",
				"sts5",
				"sts6",
			},
		},
	}

	for sNum, s := range scenarios {
		sort.Sort(s.stsRdrs)
		for i, expected := range s.expectedOrder {
			got := s.stsRdrs[i].Path
			if expected != got {
				t.Errorf("scenario %v expected %v but got %v", sNum, expected, got)
			}
		}
	}
}

// createFile creates file with lines at pth with created time as
// the created date.
func createFile(lines []string, pth string, created time.Time) {
	w, _ := file.NewWriter(pth, nil)

	for _, ln := range lines {
		w.WriteLine([]byte(ln))
	}
	w.Close()
	pth = w.Stats().Path // full path

	// set created date
	os.Chtimes(pth, created, created)
}
