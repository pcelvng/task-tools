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
	// TODO: This is currently being tested in TestBackloader in handler_test.go
}

func TestBatchJob_For(t *testing.T) {
	today := "2024-01-15"
	toHour := "2024-01-15T00"
	tm := &taskMaster{}
	fn := func(ph workflow.Phase) ([]task.Task, error) {
		j, err := tm.NewJob(ph, "batch.toml")
		if err != nil {
			return nil, err
		}
		bJob, ok := j.(*batchJob)
		if !ok {
			return nil, errors.New("expected *batchjob")
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
		t := trial.TimeDay(today).Add(bJob.Offset) // mimics bJob.Run()'s Offset handling
		return batch.For(t, bJob.For, &bJob.fOpts)
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
				Rule:     "cron=0 1 2 3 4&for=-48h",
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
				Rule:     "cron=0 1 2 3 4&for=-48h&offset=-48h",
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
				Rule:     "cron=0 1 2 3 4&meta=name:a,b,c|value:1,2,3",
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
				Rule:     "cron=0 1 2 3 4&meta-file=test/presidents.json",
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
				Rule:     "cron=0 1 2 3 4&meta-file=test/empty.json",
				Template: "?key={meta:key}",
			},
			Expected: []task.Task{},
		},
		"invalid-file": {
			Input: workflow.Phase{
				Task:     "batch",
				Rule:     "cron=0 1 2 3 4&meta-file=test/invalid.json",
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
