package main

import (
	"testing"

	"github.com/hydronica/trial"
	"github.com/pcelvng/task"
)

func TestTaskTime(t *testing.T) {
	fn := func(in trial.Input) (interface{}, error) {
		t := task.Task{
			Info: in.String(),
		}
		return taskTime(t), nil
	}
	cases := trial.Cases{
		"date": {
			Input:    "./path/to/file.txt?date=2020-04-07",
			Expected: trial.TimeDay("2020-04-07"),
		},
		"date-map": {
			Input:    "./path/to/file.txt?map=date:2020-04-07",
			Expected: trial.TimeDay("2020-04-07"),
		},
		"day": {
			Input:    "?day=2021-03-11T00:00:00Z",
			Expected: trial.TimeDay("2021-03-11"),
		},
		"day-map": {
			Input:    "?map=day:2021-03-11T00:00:00Z",
			Expected: trial.TimeDay("2021-03-11"),
		},
		"hour": {
			Input:    "?hour=2021-03-06T11:00:00Z",
			Expected: trial.TimeHour("2021-03-06T11"),
		},
		"hour-map": {
			Input:    "?map=hour:2021-03-06T11",
			Expected: trial.TimeHour("2021-03-06T11"),
		},
	}
	trial.New(fn, cases).SubTest(t)
}
