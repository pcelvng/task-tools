package main

import (
	"bufio"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/hydronica/trial"
)

func TestRootPath(t *testing.T) {
	fn := func(in trial.Input) (string, error) {
		return rootPath(in.Slice(0).String(), in.Slice(1).Interface().(time.Time)), nil
	}
	trial.New(fn, trial.Cases[trial.Input, string]{
		"full time slug": {
			Input:    trial.Args("/mnt/data/folder/2018/04/05/15.json.gz", trial.TimeHour("2018-04-05T15")),
			Expected: "/mnt/data/folder/",
		},
		"day slug": {
			Input:    trial.Args("/mnt/data/folder/2018/04/05", trial.TimeHour("2018-04-05T15")),
			Expected: "/mnt/data/folder/",
		},
		"month slug": {
			Input:    trial.Args("/mnt/data/folder/2018/04", trial.TimeHour("2018-04-05T15")),
			Expected: "/mnt/data/folder/",
		},
		"static file": {
			Input:    trial.Args("s3://bucket/path/to/static/file/data.json.gz", time.Now()),
			Expected: "s3://bucket/path/to/static/file/data",
		},
	}).Test(t)
}

func TestDoneTopic(t *testing.T) {
	fn := func(in string) ([]string, error) {

		r := strings.NewReader(in)
		scanner := bufio.NewScanner(r)
		s := doneTopic(scanner)
		sort.Strings(s)
		return s, nil
	}
	cases := trial.Cases[string, []string]{
		"task without job": {
			Input:    `{"type":"test1","info":"?date=2020-01-02","result":"complete"}`,
			Expected: []string{"test1\n\tmin: 0s max 0s avg:0s\n\tComplete   1  2020/01/02"},
		},
		"task with job meta": {
			Input:    `{"type":"test2","info":"?date=2020-01-02","result":"complete","meta":"job=part1"}`,
			Expected: []string{"test2:part1\n\tmin: 0s max 0s avg:0s\n\tComplete   1  2020/01/02"},
		},
		"2 task with job meta": {
			Input: `{"type":"test3","info":"?date=2020-01-02","result":"complete","meta":"job=part1"}
{"type":"test3","info":"?date=2020-01-02","result":"complete","meta":"job=part2"}`,
			Expected: []string{"test3:part1\n\tmin: 0s max 0s avg:0s\n\tComplete   1  2020/01/02",
				"test3:part2\n\tmin: 0s max 0s avg:0s\n\tComplete   1  2020/01/02"},
		},
	}
	trial.New(fn, cases).Test(t)
}
