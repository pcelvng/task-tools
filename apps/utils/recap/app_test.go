package main

import (
	"testing"
	"time"

	"github.com/jbsmith7741/go-tools/trial"
)

func TestPrintDates(t *testing.T) {
	f := "2006/01/02T15"
	fn := func(args ...interface{}) (interface{}, error) {
		s := printDates(args[0].([]time.Time))
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
	fn := func(args ...interface{}) (interface{}, error) {
		return rootPath(args[0].(string), args[1].(time.Time)), nil
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
