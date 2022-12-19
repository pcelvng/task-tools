package main

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/hydronica/trial"
	"github.com/pcelvng/task"
)

func TestBackloader(t *testing.T) {
	type result struct {
		Tasks []task.Task
		Count int
	}
	tm := &taskMaster{}
	fn := func(req request) (result, error) {
		t, err := tm.backload(req)

		// only keep the first and last for long lists
		if count := len(t); count > 2 {
			return result{Tasks: []task.Task{t[0], t[count-1]}, Count: count}, err
		}
		return result{Tasks: t, Count: len(t)}, err
	}
	cases := trial.Cases[request, result]{
		"now": {
			Input: request{
				Task:     "sql",
				Job:      "load",
				Template: "./file.txt?ts={YYYY}-{MM}-{DD}",
			},
			Expected: result{
				Tasks: []task.Task{
					{Type: "sql", Meta: "job=load", Info: time.Now().Format("./file.txt?ts=2006-01-02")},
				},
				Count: 1,
			},
		},
		"hourly": {
			Input: request{
				Task:     "task1",
				Template: "?day={YYYY}-{MM}-{DD}T{HH}",
				From:     "2020-01-01T00",
				To:       "2020-01-02T23",
				By:       "hour",
			},
			Expected: result{
				Tasks: []task.Task{
					{Type: "task1", Info: "?day=2020-01-01T00"}, // first
					{Type: "task1", Info: "?day=2020-01-02T23"}, // last
				},
				Count: 48,
			},
		},
		"daily": {
			Input: request{
				Task:     "task2",
				Template: "?date={YYYY}-{MM}-{DD}",
				From:     "2020-01-01",
				To:       "2020-02-01",
				By:       "day",
			},
			Expected: result{
				Tasks: []task.Task{
					{Type: "task2", Info: "?date=2020-01-01"},
					{Type: "task2", Info: "?date=2020-02-01"},
				},
				Count: 32,
			},
		},

		"monthly": {
			Input: request{
				Task:     "month",
				Template: "?table=exp.tbl_{YYYY}_{MM}",
				From:     "2020-01-01",
				To:       "2020-12-12",
				By:       "month",
			},
			Expected: result{
				Tasks: []task.Task{
					{Type: "month", Info: "?table=exp.tbl_2020_01"},
					{Type: "month", Info: "?table=exp.tbl_2020_12"},
				},
				Count: 12,
			},
		},

		"meta_template": {
			Input: request{
				Task:     "meta",
				Template: "{meta:file}?date={YYYY}-{mm}-{dd}&value={meta:value}",
				At:       "2020-02-20",
				Meta:     map[string]string{"file": "s3://task-bucket/data/f.txt", "value": "apple"},
			},
			Expected: result{
				Tasks: []task.Task{
					{Type: "meta", Info: "s3://task-bucket/data/f.txt?date=2020-02-20&value=apple", Meta: "file=s3://task-bucket/data/f.txt&value=apple"},
				},
				Count: 1,
			},
		},
		/*	"phase_not_found": {}, */
	}
	trial.New(fn, cases).Comparer(trial.EqualOpt(
		trial.IgnoreAllUnexported,
		//trial.IgnoreFields()
		ignoreTask,
	)).SubTest(t)
}

func ignoreTask(interface{}) cmp.Option {
	t := task.Task{}
	return cmpopts.IgnoreFields(t, "Created", "ID")
}
