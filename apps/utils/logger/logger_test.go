package main

import (
	"errors"
	"testing"
	"time"

	"github.com/hydronica/trial"
	"github.com/pcelvng/task/bus"
	"github.com/pcelvng/task/bus/nop"

	"github.com/pcelvng/task-tools/file"
)

func TestParse(t *testing.T) {
	type input struct {
		path  string
		topic string
		time  time.Time
	}
	fn := func(in input) (string, error) {
		return Parse(in.path, in.topic, in.time), nil
	}
	cases := trial.Cases[input, string]{
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

func TestCreateWriters(t *testing.T) {
	type input struct {
		logger *Logger
		opts   *file.Options
		dest   string
	}

	fn := func(in input) (interface{}, error) {
		l := in.logger
		return nil, l.CreateWriters(in.opts, in.dest)
	}

	cases := trial.Cases[input, any]{
		"good writer": {Input: input{
			opts:   &file.Options{},
			dest:   "nop://path1",
			logger: &Logger{topic: "topic", consumer: &nop.Consumer{}},
		}},
		"bad init writer": {Input: input{
			opts:   &file.Options{},
			dest:   "nop://init_err",
			logger: &Logger{topic: "topic", consumer: &nop.Consumer{}},
		},
			ExpectedErr: errors.New("init_err"),
		},
	}
	time.Sleep(time.Second)
	trial.New(fn, cases).Test(t)
}

func TestValidate(t *testing.T) {

	fn := func(in app) (interface{}, error) {
		return nil, in.Validate()
	}
	cases := trial.Cases[app, any]{
		"good validation": {
			Input: app{
				Bus: bus.Options{
					Bus:          "nsq",
					LookupdHosts: []string{"localhost:4161"},
				},
				LogPath: "nop://file",
			},
			ShouldErr: false,
		},
		"bad dest templates": {
			Input: app{
				Bus: bus.Options{
					Bus:          "nsq",
					LookupdHosts: []string{"localhost:4161"},
				},
			},
			ShouldErr: true,
		},
		"bad lookup hosts": {
			Input: app{
				Bus: bus.Options{
					Bus: "nsq",
				},
				LogPath: "nop://file",
			},
			ShouldErr: true,
		},
		"missing bus ": {
			Input: app{
				Bus:     bus.Options{},
				LogPath: "nop://file",
			},
			ShouldErr: true,
		},
		"pubsub- no project id ": {
			Input: app{
				Bus: bus.Options{
					Bus: "pubsub",
				},
				LogPath: "nop://file",
			},
			ShouldErr: true,
		},
	}

	trial.New(fn, cases).Test(t)
}
