package main

import (
	"testing"

	"github.com/hydronica/trial"
)

func TestDate(t *testing.T) {
	type input struct {
		date string
		dur  string // duration i.e., 48h
	}
	fn := func(i trial.Input) (interface{}, error) {
		in := i.Interface().(input)
		dt := truncDate(in.date)
		return date(dt, in.dur)
	}

	cases := trial.Cases{
		"one_day": {
			Input:    input{date: "2022-09-02", dur: "24h"},
			Expected: truncDate("2022-09-01"),
		},
		"two_days": {
			Input:    input{date: "2022-09-03", dur: "48h"},
			Expected: truncDate("2022-09-01"),
		},
		"bad_time": {
			Input:     input{date: "", dur: "48h"},
			ShouldErr: true,
		},
	}
	trial.New(fn, cases).SubTest(t)
}
