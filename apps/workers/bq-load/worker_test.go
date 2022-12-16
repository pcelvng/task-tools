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
			Input: "gs://file.json?dest_table=p.d.t&append",
			Expected: &worker{
				Meta:        task.NewMeta(),
				File:        "gs://file.json",
				Destination: Destination{"p", "d", "t"},
				Append:      true,
			},
		},
		"truncate": {
			Input: "gs://file.json?dest_table=p.d.t&truncate",
			Expected: &worker{
				Meta:        task.NewMeta(),
				File:        "gs://file.json",
				Destination: Destination{"p", "d", "t"},
				Truncate:    true,
			},
		},
		"delete": {
			Input: "gs://file.json?dest_table=p.d.t&delete=id:10",
			Expected: &worker{
				Meta:        task.NewMeta(),
				File:        "gs://file.json",
				Destination: Destination{"p", "d", "t"},
				delete:      true,
				Append:      true,
				DeleteMap:   map[string]string{"id": "10"},
			},
		},
		"invalid delete": {
			Input:       "gs://file.json?dest_table=p.d.t&delete=id:10&truncate",
			ExpectedErr: errors.New("truncate and delete"),
		},
	}
	trial.New(fn, cases).Test(t)
}
