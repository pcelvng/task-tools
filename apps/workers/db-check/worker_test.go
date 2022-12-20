package main

import (
	"testing"
	"time"

	"github.com/hydronica/trial"
)

func TestDate(t *testing.T) {
	type input struct {
		date time.Time
		dur  time.Duration // duration i.e., 48h
	}
	fn := func(in input) (time.Time, error) {
		return date(in.date, in.dur)
	}

	cases := trial.Cases[input, time.Time]{
		"one_day": {
			Input:    input{date: trial.TimeDay("2022-09-02"), dur: time.Hour * 24},
			Expected: trial.TimeDay("2022-09-01"),
		},
		"two_days": {
			Input:    input{date: trial.TimeDay("2022-09-03"), dur: time.Hour * 48},
			Expected: trial.TimeDay("2022-09-01"),
		},
		"bad_time": {
			Input:     input{date: time.Time{}, dur: time.Hour * 48},
			ShouldErr: true,
		},
	}
	trial.New(fn, cases).SubTest(t)
}
