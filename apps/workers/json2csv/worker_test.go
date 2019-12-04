package main

import (
	"errors"
	"testing"

	"github.com/hydronica/trial"
	"github.com/pcelvng/task"
)

func TestOptions_NewWorker(t *testing.T) {
	fn := func(in trial.Input) (interface{}, error) {
		opts := options{}
		w := opts.NewWorker(in.String())
		if isInvalid, s := task.IsInvalidWorker(w); isInvalid {
			return nil, errors.New(s)
		}
		return w, nil
	}
	cases := trial.Cases{
		"valid": {
			Input:    "?file=nop://in.txt&output=nop://file.csv",
			Expected: &worker{File: "nop://in.txt", Output: "nop://file.csv", Sep: ","},
		},
		"valid with fields": {
			Input:    "?file=nop://in.txt&output=nop://file.csv&field=abc,def,hij",
			Expected: &worker{File: "nop://in.txt", Output: "nop://file.csv", Sep: ",", Fields: []string{"abc", "def", "hij"}},
		},
		"missing file": {
			Input:       "?output=nop://file.csv",
			ExpectedErr: errors.New("file is required"),
		},
		"missing output": {
			Input:       "?file=nop://in.txt",
			ExpectedErr: errors.New("output is required"),
		},
	}
	trial.New(fn, cases).SubTest(t)
}
