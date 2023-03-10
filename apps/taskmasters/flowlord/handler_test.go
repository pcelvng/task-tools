package main

import (
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/hydronica/trial"
	"github.com/pcelvng/task"

	"github.com/pcelvng/task-tools/workflow"
)

func TestBackloader(t *testing.T) {
	cache, err := workflow.New("../../../internal/test/workflow/f3.toml", nil)
	if err != nil {
		t.Fatal(err)
	}
	tm := &taskMaster{
		Cache: cache,
	}
	fn := func(req request) (response, error) {
		resp := tm.backload(req)

		if resp.code >= 400 {
			return response{}, errors.New(resp.Status)
		}

		// only keep the first and last for long lists
		if resp.Count > 2 {
			resp.Tasks = []task.Task{resp.Tasks[0], resp.Tasks[resp.Count-1]}
		}
		return resp, nil
	}
	cases := trial.Cases[request, response]{
		"now": {
			Input: request{
				Task:     "sql",
				Job:      "load",
				Template: "./file.txt?ts={YYYY}-{MM}-{DD}",
			},
			Expected: response{
				Tasks: []task.Task{
					{Type: "sql", Meta: "job=load", Info: time.Now().Format("./file.txt?ts=2006-01-02")},
				},
				Count: 1,
			},
		},
		"from_cache": {
			Input: request{
				Task: "task1",
				At:   "2022-06-12",
				// Template: "?date={yyyy}-{mm}-{dd}" // from f3.toml file
			},
			Expected: response{
				Count: 1,
				Tasks: []task.Task{{Type: "task1", Info: "?date=2022-06-12", Meta: "workflow=f3.toml"}},
			},
		},
		"hourly": {
			Input: request{
				Task:     "hourly",
				Template: "?day={YYYY}-{MM}-{DD}T{HH}",
				From:     "2020-01-01T00",
				To:       "2020-01-02T23",
				By:       "hour",
			},
			Expected: response{
				Tasks: []task.Task{
					{Type: "hourly", Info: "?day=2020-01-01T00"}, // first
					{Type: "hourly", Info: "?day=2020-01-02T23"}, // last
				},
				Count: 48,
			},
		},
		"daily": {
			Input: request{
				Task:     "daily",
				Template: "?date={YYYY}-{MM}-{DD}",
				From:     "2020-01-01",
				To:       "2020-02-01",
				By:       "day",
			},
			Expected: response{
				Tasks: []task.Task{
					{Type: "daily", Info: "?date=2020-01-01"},
					{Type: "daily", Info: "?date=2020-02-01"},
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
			Expected: response{
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
			Expected: response{
				Tasks: []task.Task{
					{Type: "meta", Info: "s3://task-bucket/data/f.txt?date=2020-02-20&value=apple", Meta: "file=s3://task-bucket/data/f.txt&value=apple"},
				},
				Count: 1,
			},
		},
		"phase_not_found": {
			Input: request{
				Task: "unknown",
			},
			ShouldErr: true,
		},
		"invalid_time": {
			Input: request{Task: "task1", At: "2022-120-01"},
			Expected: response{
				Count: 1,
				Tasks: []task.Task{
					{Type: "task1", Info: time.Now().Format("?date=2006-01-02"), Meta: "workflow=f3.toml"},
				}},
		},
		"to only": {
			Input: request{Task: "task1", To: "2022-12-01"},
			Expected: response{
				Count: 1,
				Tasks: []task.Task{
					{Type: "task1", Info: "?date=2022-12-01", Meta: "workflow=f3.toml"},
				}},
		},
		"from only": {
			Input: request{Task: "task1", From: "2022-12-01"},
			Expected: response{
				Count: 1,
				Tasks: []task.Task{
					{Type: "task1", Info: "?date=2022-12-01", Meta: "workflow=f3.toml"},
				}},
		},
		"backwards": {
			Input: request{
				Task:     "month",
				Template: "?table=exp.tbl_{YYYY}_{MM}",
				From:     "2021-01-01",
				To:       "2020-10-01",
				By:       "month",
			},
			Expected: response{
				Tasks: []task.Task{
					{Type: "month", Info: "?table=exp.tbl_2021_01"},
					{Type: "month", Info: "?table=exp.tbl_2020_10"},
				},
				Count: 4,
			},
		},
	}
	trial.New(fn, cases).Comparer(trial.EqualOpt(
		trial.IgnoreAllUnexported,
		trial.IgnoreFields("Status"),
		ignoreTask,
	)).SubTest(t)
}

func ignoreTask(interface{}) cmp.Option {
	return cmpopts.IgnoreFields(task.Task{}, "Created", "ID")
}
