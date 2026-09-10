package main

import (
	"errors"
	"net/url"
	"testing"

	"github.com/hydronica/trial"
	"github.com/pcelvng/task"
)

func TestSanitizeRerunMeta(t *testing.T) {
	fn := func(in string) (string, error) {
		return sanitizeRerunMeta(in)
	}
	cases := trial.Cases[string, string]{
		"strips retry state": {
			Input:    "workflow=f1.toml&retry=3&retried=3&delayed=500ms&file=x",
			Expected: "file=x&rerun=manual&workflow=f1.toml",
		},
		"preserves template meta": {
			Input:    "cron=2022-06-12T00&workflow=f3.toml&key=fruit&val=apple",
			Expected: "cron=2022-06-12T00&key=fruit&rerun=manual&val=apple&workflow=f3.toml",
		},
		"empty meta": {
			Input:    "",
			Expected: "rerun=manual",
		},
		"invalid meta": {
			Input:       "%",
			ExpectedErr: errors.New("invalid URL escape \"%\""),
		},
	}
	trial.New(fn, cases).SubTest(t)
}

func TestTaskForRerun(t *testing.T) {
	orig := task.Task{
		Type:    "task1",
		Job:     "t2",
		Info:    "?date=2019-12-12",
		ID:      "UUID_task1",
		Meta:    "retry=2&retried=2&delayed=100ms&workflow=f1.toml",
		Result:  task.ErrResult,
		Msg:     "failed",
		Created: "2019-12-12T10:00:00Z",
	}

	got, err := taskForRerun(orig)
	if err != nil {
		t.Fatalf("taskForRerun: %v", err)
	}
	if got.Type != orig.Type || got.Job != orig.Job || got.Info != orig.Info || got.ID != orig.ID {
		t.Errorf("got type/job/info/id = %q/%q/%q/%q, want %q/%q/%q/%q",
			got.Type, got.Job, got.Info, got.ID, orig.Type, orig.Job, orig.Info, orig.ID)
	}
	if got.Meta != "rerun=manual&workflow=f1.toml" {
		t.Errorf("Meta = %q, want rerun=manual&workflow=f1.toml", got.Meta)
	}
	if got.Created == orig.Created {
		t.Errorf("Created = %q, want different from source %q", got.Created, orig.Created)
	}
	if got.Created == "" {
		t.Error("Created is empty")
	}
	if got.Result != "" {
		t.Errorf("Result = %q, want empty", got.Result)
	}
	if got.Msg != "" {
		t.Errorf("Msg = %q, want empty", got.Msg)
	}

	meta, err := url.ParseQuery(got.Meta)
	if err != nil {
		t.Fatalf("parse meta: %v", err)
	}
	if meta.Get("rerun") != "manual" {
		t.Errorf("rerun = %q, want manual", meta.Get("rerun"))
	}
	if meta.Get("retry") != "" || meta.Get("retried") != "" || meta.Get("delayed") != "" {
		t.Errorf("retry meta not stripped: %v", got.Meta)
	}
}
