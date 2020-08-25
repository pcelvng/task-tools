package main

import (
	"errors"
	"testing"
	"time"

	"github.com/hydronica/trial"
	"github.com/pcelvng/task/bus"
)

func TestNewOptions(t *testing.T) {
	hours := func(s string) []bool {
		f, _ := parseHours(s)
		return f
	}
	now, _ := time.Parse(tFormat, time.Now().Truncate(time.Hour).Format(tFormat))
	fn := func(i trial.Input) (interface{}, error) {
		return loadOptions(i.Interface().(flags))
	}
	cases := trial.Cases{
		"default": {
			Input: flags{taskType: "test", from: "now"},
			Expected: &options{
				Bus: bus.Options{
					LookupdHosts: []string{""},
				},
				cache:        nil,
				start:        now,
				end:          now,
				taskType:     "test",
				taskTemplate: "",
				onHours:      hours(""),
				offHours:     hours(""),
			},
		},
		"complex case": {
			Input: flags{taskType: "test", from: "2010-05-06T12", to: "2020-05-10", everyXHours: 3},
			Expected: &options{
				Bus: bus.Options{
					LookupdHosts: []string{""},
				},
				cache:        nil,
				start:        trial.TimeHour("2010-05-06T12"),
				end:          trial.TimeDay("2020-05-10"),
				taskType:     "test",
				taskTemplate: "",
				everyXHours:  3,
				onHours:      hours(""),
				offHours:     hours(""),
			},
		},
		"no tasks": {
			Input:       flags{from: "now"},
			ExpectedErr: errors.New("flag '-type' or '-t' required"),
		},
		"at hours": {
			Input: flags{taskType: "test", at: "2010-02-12"},
			Expected: &options{
				Bus: bus.Options{
					LookupdHosts: []string{""},
				},
				cache:        nil,
				start:        trial.TimeDay("2010-02-12"),
				end:          trial.TimeDay("2010-02-12"),
				taskType:     "test",
				taskTemplate: "",
				onHours:      hours(""),
				offHours:     hours(""),
			},
		},
	}
	trial.New(fn, cases).SubTest(t)
}
