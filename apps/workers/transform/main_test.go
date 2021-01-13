package main

import (
	"errors"
	"testing"
	"time"

	"github.com/hydronica/trial"
	"github.com/itchyny/gojq"
	"github.com/pcelvng/task"

	"github.com/pcelvng/task-tools/file/mock"
	"github.com/pcelvng/task-tools/file/nop"
)

const examplejson = `{"a":1,"b":12.345678901,"c":"apple","d":"dog"}`

func BenchmarkProcess(t *testing.B) {
	tm := &worker{}
	tm.writer, _ = nop.NewWriter("nop://")
	query, err := gojq.Parse("{a: .a,b: .b,c: .c, d: .d, e: (.e // 0) }")
	if err != nil {
		t.Fatal(err)
	}
	tm.code, _ = gojq.Compile(query)

	t.ResetTimer()

	for i := 0; i < t.N; i++ {
		tm.process([]byte(examplejson))
	}
}

func TestNewWorker(t *testing.T) {
	fn := func(in trial.Input) (interface{}, error) {
		o := &options{}
		w := o.newWorker(in.String())
		if b, err := task.IsInvalidWorker(w); b {
			return nil, errors.New(err)
		}
		return nil, nil
	}
	cases := trial.Cases{
		"valid": {
			Input: "nop://file.txt?dest=nop://output.txt&jq=nop://read_eof",
		},
		"no origin": {
			Input:       "?jq=nop://file.jq&dest=nop://output.txt",
			ExpectedErr: errors.New("origin is required"),
		},
		"no dest": {
			Input:       "nop://file.txt?jq=nop://read_eof",
			ExpectedErr: errors.New("dest is required"),
		},
		"no jq": {
			Input:       "nop://file.txt?dest=nop://output.txt",
			ExpectedErr: errors.New("jq is required"),
		},
		"invalid threads": {
			Input:       "nop://file.txt?dest=nop://output.txt&jq=nop://read_eof&threads=0",
			ExpectedErr: errors.New("threads"),
		},
	}
	trial.New(fn, cases).Timeout(3 * time.Second).Test(t)
}

func TestWorker_Process(t *testing.T) {
	type input struct {
		data string
		jq   string
	}
	fn := func(in trial.Input) (interface{}, error) {
		v := in.Interface().(input)

		// setup the worker
		w := mock.NewWriter("nop://")
		wrk := &worker{
			writer: w,
			code:   nil,
		}
		q, err := gojq.Parse(v.jq)
		if err != nil {
			return nil, err
		}
		wrk.code, err = gojq.Compile(q)
		if err != nil {
			return nil, err
		}
		// test the method
		err = wrk.process([]byte(v.data))

		// retrieve the data
		lines := w.GetLines()
		if len(lines) == 0 {
			return "", err
		}
		return lines[0], err
	}
	cases := trial.Cases{
		"passthrough": {
			Input: input{
				data: examplejson,
				jq:   ".",
			},
			Expected: examplejson,
		},
		"defaults": {
			Input: input{
				data: examplejson,
				jq:   "{e: (.e // 0)}",
			},
			Expected: `{"e":0}`,
		},
	}
	trial.New(fn, cases).Test(t)
}
