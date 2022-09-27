package main

import (
	"testing"
	"time"

	"github.com/hydronica/trial"
)

func TestDate(t *testing.T) {
	type input struct {
		date string
		dur  time.Duration // duration i.e., 48h
	}
	fn := func(i trial.Input) (interface{}, error) {

		in := i.Interface().(input)
		dt, _ := time.Parse("2006-01-02", in.date)
		return date(dt, in.dur)
	}

	cases := trial.Cases{
		"one_day": {
			Input:    input{date: "2022-09-02", dur: time.Hour * 24},
			Expected: trial.TimeDay("2022-09-01"),
		},
		"two_days": {
			Input:    input{date: "2022-09-03", dur: time.Hour * 48},
			Expected: trial.TimeDay("2022-09-01"),
		},
		"bad_time": {
			Input:     input{date: "", dur: time.Hour * 48},
			ShouldErr: true,
		},
	}
	trial.New(fn, cases).SubTest(t)
}
