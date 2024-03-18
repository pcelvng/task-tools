package main

import (
	"errors"
	"testing"

	"github.com/hydronica/trial"
)

func TestOptions_Validate(t *testing.T) {
	fn := func(opts options) (interface{}, error) {
		return nil, opts.Validate()
	}
	cases := trial.Cases[options, any]{
		"valid rule": {
			Input: options{
				Rules: []Rule{
					{Topic: "test", CronRule: "* * * * * *"},
				},
			},
		},
		"no rules provided": {
			Input:       options{},
			ExpectedErr: errors.New("one rule is required"),
		},
		"topic required": {
			Input: options{
				Rules: []Rule{{}},
			},
			ExpectedErr: errors.New("topic is required"),
		},
		"invalid cronrule 1": {
			Input: options{
				Rules: []Rule{
					{Topic: "test", CronRule: "* * * *"},
				},
			},
			ExpectedErr: errors.New("invalid cron rule"),
		},
		"invalid cronrule2": {
			Input: options{
				Rules: []Rule{
					{Topic: "test", CronRule: "* /10 * * *"},
				},
			},
			ExpectedErr: errors.New("invalid cron rule"),
		},
	}
	trial.New(fn, cases).Test(t)
}
