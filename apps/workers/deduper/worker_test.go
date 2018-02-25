package main

import (
	"context"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task-tools/file/stat"
	"github.com/pcelvng/task/bus"
)

func TestNewWorker(t *testing.T) {
	// setup
	nopProducer, _ := bus.NewProducer(bus.NewOptions("nop"))
	//bOpt := bus.NewOptions("file")
	//bOpt.OutFile = "./test/files_stats.json"
	//fileProducer, _ := bus.NewProducer(bOpt)

	pths := []string{
		"./test/1/dups.json",
		"./test/2/file-20160101T000000.json",
		"./test/2/file-20170101T000000.json",
		"./test/2/file-20180101T000000.json",
	}

	createdDates := []time.Time{
		time.Date(2016, 01, 01, 00, 00, 00, 00, time.UTC),
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

		//// set 3 - no dups in set (across two field keys)
		//{
		//	`{"f1":"v1","f2":"v1","f3":"v7"}`,
		//	`{"f1":"v2","f2":"v1","f3":"v8"}`,
		//	`{"f1":"v3","f2":"v1","f3":"v9"}`,
		//	`{"f1":"v1","f2":"v2","f3":"v10"}`,
		//	`{"f1":"v2","f2":"v2","f3":"v11"}`,
		//	`{"f1":"v3","f2":"v2","f3":"v12"}`,
		//},
	}

	// scenario 1 file
	createFile(lineSets[0], pths[0], createdDates[0])

	// scenario 2 files
	createFile(lineSets[1], pths[1], createdDates[0])
	createFile(lineSets[2], pths[2], createdDates[0])
	createFile(lineSets[3], pths[3], createdDates[0])

	// case1: single file with duplicates
	type scenario struct {
		appOpt         *options
		producer       bus.Producer
		info           string
		expectedResult task.Result
		expectedMsg    string
	}
	scenarios := []scenario{
		// scenario1: single file input deduping file lines
		{
			appOpt:         newOptions(),
			producer:       nopProducer,
			info:           `./test/1/dups.json?dest-template=./test/1/deduped.json&fields=f1,f2`,
			expectedResult: task.CompleteResult,
			expectedMsg:    `read 6 lines from 1 files and wrote 4 lines`,
		},

		// scenario2: multiple input files deduping across files
		{
			appOpt:         newOptions(),
			producer:       nopProducer,
			info:           `./test/2?dest-template=./test/2/dedup/dedup.json&fields=f1`,
			expectedResult: task.CompleteResult,
			expectedMsg:    `read 7 lines from 3 files and wrote 3 lines`,
		},
	}

	for sNum, s := range scenarios {
		appOpt = s.appOpt
		producer = s.producer
		wkr := NewWorker(s.info)
		gotRslt, gotMsg := wkr.DoTask(context.Background())

		// check result
		if gotRslt != s.expectedResult {
			t.Errorf("scenario %v expected result '%v' but got '%v'", sNum, s.expectedResult, gotRslt)
		}

		// check msg
		if gotMsg != s.expectedMsg {
			t.Errorf("scenario %v expected msg '%v' but got '%v'", sNum, s.expectedMsg, gotMsg)
		}
	}

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
		stsRdrs       StatsReaders
		expectedOrder []string
	}

	scenarios := []scenario{
		// scenario 1: pthTime is the same, sts.Created are different
		{
			stsRdrs: StatsReaders{
				&StatsReader{sts: &sts3, pthTime: pthTime1},
				&StatsReader{sts: &sts1, pthTime: pthTime1},
				&StatsReader{sts: &sts2, pthTime: pthTime1},
			},
			expectedOrder: []string{
				"sts1",
				"sts2",
				"sts3",
			},
		},

		// scenario 2: pthTime is different, sts.Created are same
		{
			stsRdrs: StatsReaders{
				&StatsReader{sts: &sts6, pthTime: pthTime3},
				&StatsReader{sts: &sts4, pthTime: pthTime1},
				&StatsReader{sts: &sts5, pthTime: pthTime2},
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
			got := s.stsRdrs[i].sts.Path
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
