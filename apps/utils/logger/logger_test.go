package main

import (
	"testing"
	"time"

	"github.com/jbsmith7741/trial"
)

func TestParse(t *testing.T) {
	type input struct {
		path  string
		topic string
		time  time.Time
	}
	fn := func(args ...interface{}) (interface{}, error) {
		in := args[0].(input)
		return Parse(in.path, in.topic, in.time), nil
	}
	cases := trial.Cases{
		"lower case topic": {
			Input: input{
				path:  "path/to/file/{topic}.json.gz",
				topic: "task1",
				time:  time.Now(),
			},
			Expected: "path/to/file/task1.json.gz",
		},
		"topic with time": {
			Input: input{
				path:  "path/to/file/{topic}_{TS}.json.gz",
				topic: "task1",
				time:  trial.TimeHour("2018-11-01T12"),
			},
			Expected: "path/to/file/task1_20181101T120000.json.gz",
		},
	}
	trial.New(fn, cases).Test(t)
}

func TestNextHour(t *testing.T) {
	fn := func(args ...interface{}) (interface{}, error) {
		return nextHour(args[0].(time.Time)), nil

	}
	cases := trial.Cases{
		"in 15 minutes": {
			Input:    trial.Time(time.RFC3339, "2018-11-01T12:44:50Z"),
			Expected: 15 * time.Minute,
		},
		"55 minutes": {
			Input:    trial.Time(time.RFC3339Nano, "2018-11-01T12:59:55Z"),
			Expected: 59*time.Minute + 55*time.Second,
		},
		"1 nano second": {
			Input:    trial.Time(time.RFC3339Nano, "2018-11-01T12:59:49.599999999Z"),
			Expected: time.Hour,
		},
		"start of the hour": {
			Input:    trial.Time(time.RFC3339, "2018-11-01T12:00:00Z"),
			Expected: 59*time.Minute + 50*time.Second,
		},
	}
	trial.New(fn, cases).Test(t)
}
