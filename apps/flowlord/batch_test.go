package main

import (
	"testing"
	"time"

	"github.com/hydronica/trial"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task-tools/tmpl"
)

// batchResponse mirrors the response struct from handler_test.go
type batchResponse struct {
	Count int
	Tasks []task.Task
}

func TestBatch_Batch(t *testing.T) {
	today := time.Now().Format("2006-01-02")
	toHour := time.Now().Format("2006-01-02T15")

	fn := func(batch Batch) (batchResponse, error) {
		tasks, err := batch.Batch(time.Time{}, nil)
		if err != nil {
			return batchResponse{}, err
		}

		resp := batchResponse{
			Count: len(tasks),
			Tasks: tasks,
		}

		// only keep the first and last for long lists
		if resp.Count > 2 {
			resp.Tasks = []task.Task{resp.Tasks[0], resp.Tasks[resp.Count-1]}
		}

		return resp, nil
	}

	cases := trial.Cases[Batch, batchResponse]{
		"now": {
			Input: Batch{
				Template: "./file.txt?ts={YYYY}-{MM}-{DD}",
				Topic:    "sql",
				Job:      "load",
				Start:    time.Now(),
				End:      time.Now(),
			},
			Expected: batchResponse{
				Count: 1,
				Tasks: []task.Task{
					{Type: "sql", Job: "load", Meta: "cron=" + toHour + "&job=load", Info: "./file.txt?ts=" + today},
				},
			},
		},
		"hourly": {
			Input: Batch{
				Template: "?day={YYYY}-{MM}-{DD}T{HH}",
				Topic:    "hourly",
				Start:    trial.TimeHour("2020-01-01T00"),
				End:      trial.TimeHour("2020-01-02T23"),
				By:       "hour",
			},
			Expected: batchResponse{
				Count: 48,
				Tasks: []task.Task{
					{Type: "hourly", Info: "?day=2020-01-01T00", Meta: "cron=2020-01-01T00"}, // first
					{Type: "hourly", Info: "?day=2020-01-02T23", Meta: "cron=2020-01-02T23"}, // last
				},
			},
		},
		"daily": {
			Input: Batch{
				Template: "?date={YYYY}-{MM}-{DD}",
				Topic:    "daily",
				Start:    trial.TimeDay("2020-01-01"),
				End:      trial.TimeDay("2020-02-01"),
				By:       "day",
			},
			Expected: batchResponse{
				Count: 32,
				Tasks: []task.Task{
					{Type: "daily", Info: "?date=2020-01-01", Meta: "cron=2020-01-01T00"},
					{Type: "daily", Info: "?date=2020-02-01", Meta: "cron=2020-02-01T00"},
				},
			},
		},
		"monthly": {
			Input: Batch{
				Template: "?table=exp.tbl_{YYYY}_{MM}",
				Topic:    "month",
				Start:    trial.TimeDay("2020-01-01"),
				End:      trial.TimeDay("2020-12-12"),
				By:       "month",
			},
			Expected: batchResponse{
				Count: 12,
				Tasks: []task.Task{
					{Type: "month", Info: "?table=exp.tbl_2020_01", Meta: "cron=2020-01-01T00"},
					{Type: "month", Info: "?table=exp.tbl_2020_12", Meta: "cron=2020-12-01T00"},
				},
			},
		},
		"weekly": {
			Input: Batch{
				Template: "?date={YYYY}-{MM}-{DD}",
				Topic:    "week",
				Start:    trial.TimeDay("2020-01-01"),
				End:      trial.TimeDay("2020-02-01"),
				By:       "week",
			},
			Expected: batchResponse{
				Count: 5,
				Tasks: []task.Task{
					{Type: "week", Info: "?date=2020-01-01", Meta: "cron=2020-01-01T00"},
					{Type: "week", Info: "?date=2020-01-29", Meta: "cron=2020-01-29T00"},
				},
			},
		},
		"meta_template": {
			Input: Batch{
				Template: "{meta:file}?date={YYYY}-{mm}-{dd}&value={meta:value}",
				Topic:    "meta",
				Start:    trial.TimeDay("2020-02-20"),
				End:      trial.TimeDay("2020-02-20"),
				Meta:     map[string][]string{"file": {"s3://task-bucket/data/f.txt"}, "value": {"apple"}},
			},
			Expected: batchResponse{
				Count: 1,
				Tasks: []task.Task{
					{Type: "meta", Info: "s3://task-bucket/data/f.txt?date=2020-02-20&value=apple", Meta: "cron=2020-02-20T00&file=s3://task-bucket/data/f.txt&value=apple"},
				},
			},
		},
		"backwards": {
			Input: Batch{
				Template: "?table=exp.tbl_{YYYY}_{MM}",
				Topic:    "month",
				Start:    trial.TimeDay("2021-01-01"),
				End:      trial.TimeDay("2020-10-01"),
				By:       "month",
			},
			Expected: batchResponse{
				Count: 4,
				Tasks: []task.Task{
					{Type: "month", Info: "?table=exp.tbl_2021_01", Meta: "cron=2021-01-01T00"},
					{Type: "month", Info: "?table=exp.tbl_2020_10", Meta: "cron=2020-10-01T00"},
				},
			},
		},
		"with_workflow": {
			Input: Batch{
				Template: "?date={YYYY}-{MM}-{DD}",
				Topic:    "task1",
				Workflow: "f3.toml",
				Start:    trial.TimeDay("2022-12-01"),
				End:      trial.TimeDay("2022-12-01"),
			},
			Expected: batchResponse{
				Count: 1,
				Tasks: []task.Task{
					{Type: "task1", Info: "?date=2022-12-01", Meta: "cron=2022-12-01T00&workflow=f3.toml"},
				},
			},
		},
		"with_job": {
			Input: Batch{
				Template: "?date={YYYY}-{MM}-{DD}",
				Topic:    "task1",
				Job:      "load",
				Start:    trial.TimeDay("2022-12-01"),
				End:      trial.TimeDay("2022-12-01"),
			},
			Expected: batchResponse{
				Count: 1,
				Tasks: []task.Task{
					{Type: "task1", Job: "load", Info: "?date=2022-12-01", Meta: "cron=2022-12-01T00&job=load"},
				},
			},
		},
		"meta_multiple_values": {
			Input: Batch{
				Template: "?president={meta:name}&start={meta:start}&end={meta:end}",
				Topic:    "batch-president",
				Start:    time.Now(),
				End:      time.Now(),
				Meta:     map[string][]string{"name": {"bob", "albert"}, "start": {"1111", "1120"}, "end": {"1120", "1130"}},
			},
			Expected: batchResponse{
				Count: 2,
				Tasks: []task.Task{
					{Type: "batch-president", Info: "?president=bob&start=1111&end=1120", Meta: "cron=" + toHour + "&end=1120&name=bob&start=1111"},
					{Type: "batch-president", Info: "?president=albert&start=1120&end=1130", Meta: "cron=" + toHour + "&end=1130&name=albert&start=1120"},
				},
			},
		},
		"single_task": {
			Input: Batch{
				Template: "?date={YYYY}-{MM}-{DD}",
				Topic:    "single",
				Start:    trial.TimeDay("2020-01-01"),
				End:      trial.TimeDay("2020-01-01"),
			},
			Expected: batchResponse{
				Count: 1,
				Tasks: []task.Task{
					{Type: "single", Info: "?date=2020-01-01", Meta: "cron=2020-01-01T00"},
				},
			},
		},
	}

	trial.New(fn, cases).Comparer(trial.EqualOpt(
		trial.IgnoreAllUnexported,
		ignoreTask,
	)).SubTest(t)
}

func TestBatch_Batch_WithFile(t *testing.T) {
	// Test with metafile - this would require a test file
	// For now, we'll test the error case when file doesn't exist
	batch := Batch{
		Template: "?president={meta:name}&start={meta:start}&end={meta:end}",
		Topic:    "mfile",
		Metafile: "./test/nonexistent.json",
		Start:    time.Now(),
		End:      time.Now(),
	}

	_, err := batch.Batch(time.Time{}, &file.Options{})
	if err == nil {
		t.Error("Expected error when metafile doesn't exist")
	}
}

func TestCreateMeta(t *testing.T) {
	fn := func(input map[string][]string) ([]tmpl.GetMap, error) {
		return createMeta(input)
	}

	cases := trial.Cases[map[string][]string, []tmpl.GetMap]{
		"simple": {
			Input: map[string][]string{
				"name": {"alice", "bob"},
				"age":  {"25", "30"},
			},
			Expected: []tmpl.GetMap{
				{"name": "alice", "age": "25"},
				{"name": "bob", "age": "30"},
			},
		},
		"single_value": {
			Input: map[string][]string{
				"key": {"value"},
			},
			Expected: []tmpl.GetMap{
				{"key": "value"},
			},
		},
		"empty": {
			Input:    map[string][]string{},
			Expected: []tmpl.GetMap{},
		},
	}

	trial.New(fn, cases).SubTest(t)
}

func TestCreateMeta_Error(t *testing.T) {
	// Test inconsistent lengths
	input := map[string][]string{
		"name": {"alice", "bob"},
		"age":  {"25"}, // Different length
	}

	_, err := createMeta(input)
	if err == nil {
		t.Error("Expected error for inconsistent lengths")
	}
}

/*
func ignoreTask(interface{}) cmp.Option {
	return cmpopts.IgnoreFields(task.Task{}, "Created", "ID")
}
*/
