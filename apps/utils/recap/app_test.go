package main

import (
	"bufio"
	"strings"
	"testing"
	"time"

	"github.com/hydronica/trial"
)

func TestPrintDates(t *testing.T) {
	f := "2006/01/02T15"
	fn := func(in trial.Input) (interface{}, error) {
		s := printDates(in.Interface().([]time.Time))
		return s, nil
	}
	trial.New(fn, map[string]trial.Case{
		"simple series": {
			Input:    trial.Times(f, "2018/04/09T03", "2018/04/09T04", "2018/04/09T05"),
			Expected: "2018/04/09T03-2018/04/09T05",
		},
		"group of dates": {
			Input:    trial.Times(f, "2018/04/10T14", "2018/04/10T14", "2018/04/10T00", "2018/04/09T00", "2018/04/10T00", "2018/04/11T00"),
			Expected: "2018/04/09T00,2018/04/10T00,2018/04/10T14,2018/04/11T00",
		},
		"missing dates in middle": {
			Input:    trial.Times(f, "2018/04/09T03", "2018/04/09T04", "2018/04/09T05", "2018/04/09T07", "2018/04/09T08", "2018/04/09T09", "2018/04/09T11"),
			Expected: "2018/04/09T03-2018/04/09T05,2018/04/09T07-2018/04/09T09,2018/04/09T11",
		},
		"daily records": {
			Input:    trial.Times(f, "2018/04/09T00", "2018/04/10T00", "2018/04/11T00", "2018/04/12T00"),
			Expected: "2018/04/09-2018/04/12",
		},
		"daily records with gaps": {
			Input:    trial.Times(f, "2018/04/09T00", "2018/04/10T00", "2018/04/11T00", "2018/04/12T00", "2018/04/15T00", "2018/04/16T00", "2018/04/17T00"),
			Expected: "2018/04/09-2018/04/12,2018/04/15-2018/04/17",
		},
	}).Test(t)

}

func TestRootPath(t *testing.T) {
	fn := func(in trial.Input) (interface{}, error) {
		return rootPath(in.Slice(0).String(), in.Slice(1).Interface().(time.Time)), nil
	}
	trial.New(fn, map[string]trial.Case{
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
	fn := func(in trial.Input) (interface{}, error) {

		r := strings.NewReader(in.String())
		scanner := bufio.NewScanner(r)
		return doneTopic(scanner), nil

	}
	cases := trial.Cases{
		"task without job": {
			Input:    `{"type":"test","info":"?date=2020-01-02","result":"complete"}`,
			Expected: []string{"test\n\tmin: 0s max 0s avg:0s\n\tComplete   1  2020/01/02"},
		},
		"task with job meta": {
			Input:    `{"type":"test","info":"?date=2020-01-02","result":"complete","meta":"job=part1"}`,
			Expected: []string{"test:part1\n\tmin: 0s max 0s avg:0s\n\tComplete   1  2020/01/02"},
		},
		"2 task with job meta": {
			Input: `{"type":"test","info":"?date=2020-01-02","result":"complete","meta":"job=part1"}
{"type":"test","info":"?date=2020-01-02","result":"complete","meta":"job=part2"}`,
			Expected: []string{"test:part1\n\tmin: 0s max 0s avg:0s\n\tComplete   1  2020/01/02",
				"test:part2\n\tmin: 0s max 0s avg:0s\n\tComplete   1  2020/01/02"},
		},
	}
	trial.New(fn, cases).Test(t)
}
