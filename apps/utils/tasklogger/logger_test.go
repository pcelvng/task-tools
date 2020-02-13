package main

import (
	"errors"
	"testing"
	"time"

	"github.com/jbsmith7741/trial"
	nsq "github.com/nsqio/go-nsq"
	"github.com/pcelvng/task-tools/file"
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

func TestCreateWriters(t *testing.T) {
	type input struct {
		logger *Logger
		opts   *file.Options
		dests  []string
	}

	fn := func(args ...interface{}) (interface{}, error) {
		in := args[0].(input)
		l := in.logger
		return nil, l.CreateWriters(in.opts, in.dests)
	}

	cases := trial.Cases{
		"good writer": {Input: input{
			opts:   &file.Options{},
			dests:  []string{"nop://path1", "nop://path2"},
			logger: newlog("topic", &nsq.Consumer{}),
		}},
		"bad init writer": {Input: input{
			opts:   &file.Options{},
			dests:  []string{"nop://init_err", "nop://path2"},
			logger: newlog("topic", &nsq.Consumer{}),
		},
			ExpectedErr: errors.New("init_err"),
		},
	}
	time.Sleep(time.Second)
	trial.New(fn, cases).Test(t)
}

func TestHandleMessage(t *testing.T) {
	type input struct {
		logger *Logger
		opts   *file.Options
		dests  []string
		msg    *nsq.Message
	}

	fn := func(args ...interface{}) (interface{}, error) {
		in := args[0].(input)
		l := in.logger
		l.CreateWriters(in.opts, in.dests)
		return nil, l.HandleMessage(in.msg)
	}

	cases := trial.Cases{
		"good handle message": {Input: input{
			msg:    nsq.NewMessage([nsq.MsgIDLength]byte{}, []byte("test message")),
			opts:   &file.Options{},
			dests:  []string{"nop://path1", "nop://path2"},
			logger: newlog("topic", &nsq.Consumer{}),
		}},
		"bad writeline error": {Input: input{
			msg:    nsq.NewMessage([nsq.MsgIDLength]byte{}, []byte("test message")),
			opts:   &file.Options{},
			dests:  []string{"nop://writeline_err", "nop://close_err"},
			logger: newlog("topic", &nsq.Consumer{}),
		},
			ShouldErr: false, // HandleMessage only returns nil so it won't block, logging is where the error is displayed
		},
	}

	trial.New(fn, cases).Test(t)
}

func TestValidate(t *testing.T) {

	fn := func(args ...interface{}) (interface{}, error) {
		in := args[0].(app)
		return nil, in.Validate()
	}
	cases := trial.Cases{
		"good validation": {
			Input: app{
				Bus:           "nsq",
				LookupdHosts:  []string{"one", "two"},
				DestTemplates: []string{"path1", "path2"},
			},
			ShouldErr: false,
		},
		"bad dest templates": {
			Input: app{
				Bus:           "nsq",
				LookupdHosts:  []string{"one", "two"},
				DestTemplates: []string{},
			},
			ShouldErr: true,
		},
		"bad lookup hosts": {
			Input: app{
				Bus:           "nsq",
				LookupdHosts:  []string{},
				DestTemplates: []string{"path1", "path2"},
			},
			ShouldErr: true,
		},
	}

	trial.New(fn, cases).Test(t)
}
