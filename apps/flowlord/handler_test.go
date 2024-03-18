package main

import (
	"errors"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"testing"
	"time"

	"github.com/hydronica/trial"
	"github.com/pcelvng/task"

	"github.com/pcelvng/task-tools/workflow"
)

const testPath = "../../internal/test"

func TestBackloader(t *testing.T) {
	cache, err := workflow.New(testPath+"/workflow/f3.toml", nil)
	today := time.Now().Format("2006-01-02")
	toHour := time.Now().Format(DateHour)
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
					{Type: "sql", Meta: "cron=" + toHour + "&job=load", Info: "./file.txt?ts=" + today},
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
				Tasks: []task.Task{{Type: "task1", Info: "?date=2022-06-12", Meta: "cron=2022-06-12T00&workflow=f3.toml"}},
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
					{Type: "hourly", Info: "?day=2020-01-01T00", Meta: "cron=2020-01-01T00"}, // first
					{Type: "hourly", Info: "?day=2020-01-02T23", Meta: "cron=2020-01-02T23"}, // last
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
					{Type: "daily", Info: "?date=2020-01-01", Meta: "cron=2020-01-01T00"},
					{Type: "daily", Info: "?date=2020-02-01", Meta: "cron=2020-02-01T00"},
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
					{Type: "month", Info: "?table=exp.tbl_2020_01", Meta: "cron=2020-01-01T00"},
					{Type: "month", Info: "?table=exp.tbl_2020_12", Meta: "cron=2020-12-01T00"},
				},
				Count: 12,
			},
		},
		"meta_template": {
			Input: request{
				Task:     "meta",
				Template: "{meta:file}?date={YYYY}-{mm}-{dd}&value={meta:value}",
				At:       "2020-02-20",
				Meta:     Meta{"file": {"s3://task-bucket/data/f.txt"}, "value": {"apple"}},
			},
			Expected: response{
				Tasks: []task.Task{
					{Type: "meta", Info: "s3://task-bucket/data/f.txt?date=2020-02-20&value=apple", Meta: "cron=2020-02-20T00&file=s3://task-bucket/data/f.txt&value=apple"},
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
					{Type: "task1", Info: time.Now().Format("?date=2006-01-02"), Meta: "cron=" + toHour + "&workflow=f3.toml"},
				}},
		},
		"to only": {
			Input: request{Task: "task1", To: "2022-12-01"},
			Expected: response{
				Count: 1,
				Tasks: []task.Task{
					{Type: "task1", Info: "?date=2022-12-01", Meta: "cron=2022-12-01T00&workflow=f3.toml"},
				}},
		},
		"from only": {
			Input: request{Task: "task1", From: "2022-12-01"},
			Expected: response{
				Count: 1,
				Tasks: []task.Task{
					{Type: "task1", Info: "?date=2022-12-01", Meta: "cron=2022-12-01T00&workflow=f3.toml"},
				},
			},
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
					{Type: "month", Info: "?table=exp.tbl_2021_01", Meta: "cron=2021-01-01T00"},
					{Type: "month", Info: "?table=exp.tbl_2020_10", Meta: "cron=2020-10-01T00"},
				},
				Count: 4,
			},
		},
		"meta-file": {
			Input: request{
				Task:     "mfile",
				Template: "?president={meta:name}&start={meta:start}&end={meta:end}",
				Metafile: "./test/presidents.json",
			},
			Expected: response{
				Tasks: []task.Task{
					{
						Type: "mfile", Info: "?president=george washington&start=1789&end=1797",
						Meta: "cron=" + toHour + "&end=1797&name=george washington&start=1789"},
					{
						Type: "mfile",
						Info: "?president=james madison&start=1809&end=1817",
						Meta: "cron=" + toHour + "&end=1817&name=james madison&start=1809",
					},
				},
				Count: 4,
			},
		},
		"override-file": {
			Input: request{
				Task:     "b-meta",
				Metafile: "test/kv.json",
			},
			Expected: response{
				Tasks: []task.Task{
					{Type: "b-meta", Info: "?key=fruit&val=apple", Meta: "cron=" + toHour + "&key=fruit&val=apple&workflow=f3.toml"},
					{Type: "b-meta", Info: "?key=animal&val=dog", Meta: "cron=" + toHour + "&key=animal&val=dog&workflow=f3.toml"},
				},
				Count: 2,
			},
		},
		"override-meta": {
			Input: request{
				Task: "batch-president",
				Meta: Meta{"name": {"bob", "albert"}, "start": {"1111", "1120"}, "end": {"1120", "1130"}},
			},
			Expected: response{
				Tasks: []task.Task{
					{
						Type: "batch-president",
						Info: "?president=bob&start=1111&end=1120",
						Meta: "cron=" + toHour + "&end=1120&name=bob&start=1111&workflow=f3.toml",
					},
					{
						Type: "batch-president",
						Info: "?president=albert&start=1120&end=1130",
						Meta: "cron=" + toHour + "&end=1130&name=albert&start=1120&workflow=f3.toml",
					},
				},
				Count: 2,
			},
		},
	}
	trial.New(fn, cases).Comparer(trial.EqualOpt(
		trial.IgnoreAllUnexported,
		trial.IgnoreFields("Status"),
		ignoreTask,
	)).SubTest(t)
}

func TestMeta_UnmarshalJSON(t *testing.T) {
	fn := func(d string) (Meta, error) {
		m := make(Meta)
		err := m.UnmarshalJSON([]byte(d))
		return m, err
	}
	cases := trial.Cases[string, Meta]{
		"map_string": {
			Input:    `{"key":"value","k2":"v2"}`,
			Expected: Meta{"key": []string{"value"}, "k2": []string{"v2"}},
		},
		"map_slice": {
			Input:    `{"key":["v1","v2"],"k2":["v3","v4"]}`,
			Expected: Meta{"key": []string{"v1", "v2"}, "k2": []string{"v3", "v4"}},
		},
		"mixed": {
			Input:     `{"key":["1"], "k2":"v"}`,
			ShouldErr: true,
		},
	}
	trial.New(fn, cases).SubTest(t)
}

func ignoreTask(interface{}) cmp.Option {
	return cmpopts.IgnoreFields(task.Task{}, "Created", "ID")
}
