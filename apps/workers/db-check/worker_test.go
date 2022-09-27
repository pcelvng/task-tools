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
		dt := truncDate(in.date)
		return date(dt, in.dur)
	}

	cases := trial.Cases{
		"one_day": {
			Input:    input{date: "2022-09-02", dur: time.Hour * 24},
			Expected: truncDate("2022-09-01"),
		},
		"two_days": {
			Input:    input{date: "2022-09-03", dur: time.Hour * 48},
			Expected: truncDate("2022-09-01"),
		},
		"bad_time": {
			Input:     input{date: "", dur: time.Hour * 48},
			ShouldErr: true,
		},
	}
	trial.New(fn, cases).SubTest(t)
}

func truncDate(date string) (t time.Time) {
	t, _ = time.Parse("2006-01-02", date)
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}
