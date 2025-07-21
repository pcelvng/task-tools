package main

import (
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/hydronica/trial"
	"github.com/pcelvng/task"

	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task-tools/tmpl"
	"github.com/pcelvng/task-tools/workflow"
)

func TestBatch_Range(t *testing.T) {
	today := time.Now().Format("2006-01-02")
	toHour := time.Now().Format("2006-01-02T15")

	type result struct {
		Count int
		Tasks []task.Task
	}

	fn := func(batch Batch) (result, error) {
		tasks, err := batch.Range(time.Now(), time.Now(), nil)
		if err != nil {
			return result{}, err
		}

		resp := result{
			Count: len(tasks),
			Tasks: tasks,
		}

		// only keep the first and last for long lists
		if resp.Count > 2 {
			resp.Tasks = []task.Task{resp.Tasks[0], resp.Tasks[resp.Count-1]}
		}

		return resp, nil
	}

	cases := trial.Cases[Batch, result]{
		"now": {
			Input: Batch{
				Template: "./file.txt?ts={YYYY}-{MM}-{DD}",
				Task:     "sql",
				Job:      "load",
			},
			Expected: result{
				Count: 1,
				Tasks: []task.Task{
					{Type: "sql", Job: "load", Meta: "cron=" + toHour + "&job=load", Info: "./file.txt?ts=" + today},
				},
			},
		},
		"hourly": {
			Input: Batch{
				Template: "?day={YYYY}-{MM}-{DD}T{HH}",
				Task:     "hourly",
				By:       "hour",
			},
			Expected: result{
				Count: 1, // Single time range now
				Tasks: []task.Task{
					{Type: "hourly", Info: "?day=" + toHour, Meta: "cron=" + toHour},
				},
			},
		},
		"daily": {
			Input: Batch{
				Template: "?date={YYYY}-{MM}-{DD}",
				Task:     "daily",
				By:       "day",
			},
			Expected: result{
				Count: 1,
				Tasks: []task.Task{
					{Type: "daily", Info: "?date=" + today, Meta: "cron=" + today + "T00"},
				},
			},
		},
		"monthly": {
			Input: Batch{
				Template: "?table=exp.tbl_{YYYY}_{MM}",
				Task:     "month",
				By:       "month",
			},
			Expected: result{
				Count: 1,
				Tasks: []task.Task{
					{Type: "month", Info: "?table=exp.tbl_" + time.Now().Format("2006_01"), Meta: "cron=" + time.Now().Format("2006-01-01T00")},
				},
			},
		},
		"weekly": {
			Input: Batch{
				Template: "?date={YYYY}-{MM}-{DD}",
				Task:     "week",
				By:       "week",
			},
			Expected: result{
				Count: 1,
				Tasks: []task.Task{
					{Type: "week", Info: "?date=" + today, Meta: "cron=" + today + "T00"},
				},
			},
		},
		"meta_template": {
			Input: Batch{
				Template: "{meta:file}?date={YYYY}-{mm}-{dd}&value={meta:value}",
				Task:     "meta",
				Meta:     map[string][]string{"file": {"s3://task-bucket/data/f.txt"}, "value": {"apple"}},
			},
			Expected: result{
				Count: 1,
				Tasks: []task.Task{
					{Type: "meta", Info: "s3://task-bucket/data/f.txt?date=" + time.Now().Format("2006-01-02") + "&value=apple", Meta: "cron=" + toHour + "&file=s3://task-bucket/data/f.txt&value=apple"},
				},
			},
		},
		"with_workflow": {
			Input: Batch{
				Template: "?date={YYYY}-{MM}-{DD}",
				Task:     "task1",
				Workflow: "f3.toml",
			},
			Expected: result{
				Count: 1,
				Tasks: []task.Task{
					{Type: "task1", Info: "?date=" + today, Meta: "cron=" + today + "T00&workflow=f3.toml"},
				},
			},
		},
		"with_job": {
			Input: Batch{
				Template: "?date={YYYY}-{MM}-{DD}",
				Task:     "task1",
				Job:      "load",
			},
			Expected: result{
				Count: 1,
				Tasks: []task.Task{
					{Type: "task1", Job: "load", Info: "?date=" + today, Meta: "cron=" + today + "T00&job=load"},
				},
			},
		},
		"meta_multiple_values": {
			Input: Batch{
				Template: "?president={meta:name}&start={meta:start}&end={meta:end}",
				Task:     "batch-president",
				Meta:     map[string][]string{"name": {"bob", "albert"}, "start": {"1111", "1120"}, "end": {"1120", "1130"}},
			},
			Expected: result{
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
				Task:     "single",
			},
			Expected: result{
				Count: 1,
				Tasks: []task.Task{
					{Type: "single", Info: "?date=" + today, Meta: "cron=" + today + "T00"},
				},
			},
		},
	}

	trial.New(fn, cases).Comparer(trial.EqualOpt(
		trial.IgnoreAllUnexported,
		ignoreTask,
	)).SubTest(t)
}

func TestBatchJob_For(t *testing.T) {
	today := "2024-01-15"
	toHour := "2024-01-15T00"
	tm := &taskMaster{}
	fn := func(ph workflow.Phase) ([]task.Task, error) {
		j, err := tm.NewJob(ph, "batch.toml")
		bJob, ok := j.(*batchJob)
		if !ok {
			return nil, errors.New("expected *batchjob")
		}
		if err != nil {
			return nil, err
		}
		batch := &Batch{
			Template: bJob.Template,
			Task:     bJob.Topic,
			Job:      bJob.Name,
			Workflow: bJob.Workflow,
			By:       bJob.By,
			Meta:     bJob.Meta,
			Metafile: bJob.Metafile,
		}
		return batch.For(trial.TimeDay(today).Add(bJob.Offset), bJob.For, &bJob.fOpts)
	}
	cases := trial.Cases[workflow.Phase, []task.Task]{
		/* NOT SUPPORTED
		"to_from": {
			Input: workflow.Phase{
				Task:     "batch-date",
				Rule:     "from=2024-01-01&to=2024-01-03&by=day",
				Template: "?day={yyyy}-{mm}-{dd}",
			},
			Expected: []task.Task{
				{Type: "batch-date", Info: "?day=2024-01-01", Meta: ""},
				{Type: "batch-date", Info: "?day=2024-01-02", Meta: ""},
				{Type: "batch-date", Info: "?day=2024-01-03", Meta: ""},
			},
		}, */
		"for -3": {
			Input: workflow.Phase{
				Task:     "batch-date",
				Rule:     "for=-48h",
				Template: "?day={yyyy}-{mm}-{dd}",
			},
			Expected: []task.Task{
				{Type: "batch-date", Info: "?day=2024-01-15", Meta: "cron=2024-01-15T00&workflow=batch.toml"},
				{Type: "batch-date", Info: "?day=2024-01-14", Meta: "cron=2024-01-14T00&workflow=batch.toml"},
				{Type: "batch-date", Info: "?day=2024-01-13", Meta: "cron=2024-01-13T00&workflow=batch.toml"},
			},
		},
		"for-48h +offset": {
			Input: workflow.Phase{
				Task:     "batch-date",
				Rule:     "for=-48h&offset=-48h",
				Template: "?day={yyyy}-{mm}-{dd}",
			},
			Expected: []task.Task{
				{Type: "batch-date", Info: "?day=2024-01-13", Meta: "cron=2024-01-13T00&workflow=batch.toml"},
				{Type: "batch-date", Info: "?day=2024-01-12", Meta: "cron=2024-01-12T00&workflow=batch.toml"},
				{Type: "batch-date", Info: "?day=2024-01-11", Meta: "cron=2024-01-11T00&workflow=batch.toml"},
			},
		},
		"metas": {
			Input: workflow.Phase{
				Task:     "meta-batch",
				Rule:     "meta=name:a,b,c|value:1,2,3",
				Template: "?name={meta:name}&value={meta:value}&day={yyyy}-{mm}-{dd}",
			},
			Expected: []task.Task{
				{Type: "meta-batch", Info: "?name=a&value=1&day=" + today, Meta: "cron=" + toHour + "&name=a&value=1&workflow=batch.toml"},
				{Type: "meta-batch", Info: "?name=b&value=2&day=" + today, Meta: "cron=" + toHour + "&name=b&value=2&workflow=batch.toml"},
				{Type: "meta-batch", Info: "?name=c&value=3&day=" + today, Meta: "cron=" + toHour + "&name=c&value=3&workflow=batch.toml"},
			},
		},
		"file": {
			Input: workflow.Phase{
				Task:     "batch-president",
				Rule:     "meta-file=test/presidents.json",
				Template: "?president={meta:name}&start={meta:start}&end={meta:end}",
			},
			Expected: []task.Task{
				{Type: "batch-president", Info: "?president=george washington&start=1789&end=1797", Meta: "cron=" + toHour + "&end=1797&name=george washington&start=1789&workflow=batch.toml"},
				{Type: "batch-president", Info: "?president=john adams&start=1797&end=1801", Meta: "cron=" + toHour + "&end=1801&name=john adams&start=1797&workflow=batch.toml"},
				{Type: "batch-president", Info: "?president=thomas jefferson&start=1801&end=1809", Meta: "cron=" + toHour + "&end=1809&name=thomas jefferson&start=1801&workflow=batch.toml"},
				{Type: "batch-president", Info: "?president=james madison&start=1809&end=1817", Meta: "cron=" + toHour + "&end=1817&name=james madison&start=1809&workflow=batch.toml"},
			},
		},
		"empty-file": {
			Input: workflow.Phase{
				Task:     "batch",
				Rule:     "meta-file=test/empty.json",
				Template: "?key={meta:key}",
			},
			Expected: []task.Task{},
		},
		"invalid-file": {
			Input: workflow.Phase{
				Task:     "batch",
				Rule:     "meta-file=test/invalid.json",
				Template: "?key={meta:key}",
			},
			ShouldErr: true,
		},
	}
	trial.New(fn, cases).Comparer(
		trial.EqualOpt(trial.IgnoreAllUnexported, trial.EquateEmpty, trial.IgnoreFields("ID", "Created"))).SubTest(t)
}

func TestBatch_Range_WithFile(t *testing.T) {
	// Test with metafile - this would require a test file
	// For now, we'll test the error case when file doesn't exist
	batch := Batch{
		Template: "?president={meta:name}&start={meta:start}&end={meta:end}",
		Task:     "mfile",
		Metafile: "./test/nonexistent.json",
	}

	_, err := batch.Range(time.Now(), time.Now(), &file.Options{})
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

func ignoreTask(interface{}) cmp.Option {
	return cmpopts.IgnoreFields(task.Task{}, "Created", "ID")
}
