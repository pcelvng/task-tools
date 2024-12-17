package main

import (
	"errors"
	"testing"

	"github.com/hydronica/trial"
	"github.com/pcelvng/task"
)

func TestNewWorker(t *testing.T) {
	fn := func(i string) (task.Worker, error) {
		o := &options{}
		w := o.NewWorker(i)
		if invalid, s := task.IsInvalidWorker(w); invalid {
			return nil, errors.New(s)
		}

		return w, nil
	}
	cases := trial.Cases[string, task.Worker]{
		"required fields": {
			Input:       "",
			ExpectedErr: errors.New("origin is required"),
		},
		"invalid destination": {
			Input:       "gs://file.json?dest_table=apple",
			ExpectedErr: errors.New("requires (project.dataset.table)"),
		},
		"missing insert rule": {
			Input:       "gs://file.json?dest_table=p.d.t",
			ExpectedErr: errors.New("insert rule required"),
		},
		"append": {
			Input: "gs://file.json?dest_table=p.d.t",
			Expected: &worker{
				Meta:      task.NewMeta(),
				File:      "gs://file.json",
				DestTable: Destination{"p", "d", "t"},
			},
		},
		"truncate": {
			Input: "gs://file.json?dest_table=p.d.t&truncate",
			Expected: &worker{
				Meta:      task.NewMeta(),
				File:      "gs://file.json",
				DestTable: Destination{"p", "d", "t"},
				Truncate:  true,
			},
		},
		"delete": {
			Input: "gs://file.json?dest_table=p.d.t&delete=id:10",
			Expected: &worker{
				Meta:      task.NewMeta(),
				File:      "gs://file.json",
				DestTable: Destination{"p", "d", "t"},
				delete:    true,
				DeleteMap: map[string]string{"id": "10"},
			},
		},
		"invalid delete": {
			Input:       "gs://file.json?dest_table=p.d.t&delete=id:10&truncate",
			ExpectedErr: errors.New("truncate and delete"),
		},
	}
	trial.New(fn, cases).Test(t)
}

func TestIsGCSExport(t *testing.T) {
	fn := func(s string) (bool, error) {
		return isGCSExport(s), nil
	}
	cases := trial.Cases[string, bool]{
		"simple file": {
			Input:    "gs://bucket/file.txt",
			Expected: false,
		},
		"nested file": {
			Input: "gs://bucket/f1/f2/file.json",
		},
		"gs://bucket/*.csv": {
			Input:    "gs://bucket/*.csv",
			Expected: true,
		},
		"star as ext": {
			Input:    "gs://bucket/data.*",
			Expected: true,
		},
		"gs nested *.csv": {
			Input:    "gs://bucket/f1/f2/f3/f4/*.json",
			Expected: true,
		},
		"not gcs": {
			Input:    "s3://bucket/*.txt",
			Expected: false,
		},
		"blank": {
			Expected: false,
		},
	}
	trial.New(fn, cases).Test(t)
}
