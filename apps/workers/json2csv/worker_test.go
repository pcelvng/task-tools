package main

import (
	"context"
	"errors"
	"testing"

	"github.com/hydronica/trial"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task/bus/nop"

	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task-tools/file/mock"
)

func TestOptions_NewWorker(t *testing.T) {
	fn := func(in trial.Input) (interface{}, error) {
		opts := options{}
		w := opts.NewWorker(in.String())
		if isInvalid, s := task.IsInvalidWorker(w); isInvalid {
			return nil, errors.New(s)
		}
		return w, nil
	}
	cases := trial.Cases{
		"valid": {
			Input:    "nop://file.txt?output=nop://file.csv",
			Expected: &worker{File: "nop://file.txt", Output: "nop://file.csv", Sep: ","},
		},
		"tab": {
			Input:    "nop://file.txt?output=nop://file.csv&sep=%09",
			Expected: &worker{File: "nop://file.txt", Output: "nop://file.csv", Sep: "\t"},
		},
		"valid with fields": {
			Input: "nop://file.txt?output=nop://file.csv&field=abc,def,hij",
			Expected: &worker{
				File:   "nop://file.txt",
				Output: "nop://file.csv",
				Sep:    ",",
				Fields: []string{"abc", "def", "hij"},
			},
		},
		"missing file": {
			Input:       "?output=nop://file.csv",
			ExpectedErr: errors.New("origin is required"),
		},
		"missing output": {
			Input:       "nop://file.txt",
			ExpectedErr: errors.New("output is required"),
		},
		"invalid input": {
			Input:       "nop://init_err?output=nop://file.txt",
			ExpectedErr: errors.New("new reader"),
		},
		"invalid output": {
			Input:       "nop://file.txt?output=nop://init_err",
			ExpectedErr: errors.New("new writer"),
		},
	}
	cmpfn := func(act, exp interface{}) (bool, string) {
		v := act.(*worker)
		v.writer = nil
		v.reader = nil
		return trial.Equal(act, exp)
	}
	trial.New(fn, cases).Comparer(cmpfn).SubTest(t)
}

func TestWorker_DoTask(t *testing.T) {
	type input struct {
		writePath string
		reader    file.Reader
		canceled  bool
		sep       string
	}
	fn := func(in trial.Input) (interface{}, error) {
		v := in.Interface().(input)
		if v.sep == "" {
			v.sep = ","
		}
		w := mock.NewWriter(v.writePath)
		p, _ := nop.NewProducer("")
		wkr := &worker{
			Sep:    v.sep,
			reader: v.reader,
			writer: w,
			options: options{
				producer: p,
			},
		}
		// cancel the context option
		ctx, cncl := context.WithCancel(context.Background())
		if v.canceled {
			cncl()
		}
		r, s := wkr.DoTask(ctx)
		// check for error cases
		if r == task.ErrResult {
			return nil, errors.New(s)
		}
		// test against data written
		return w.GetLines(), nil
	}
	cases := trial.Cases{
		"simple": {
			Input: input{
				reader: mock.NewReader("").AddLines(""),
			},
			Expected: []string{},
		},
		"cancel context": {
			Input: input{
				reader:   mock.NewReader("").AddLines(`{"key":"value"}`),
				canceled: true,
			},
			ExpectedErr: errors.New("context canceled"),
		},
		"read error": {
			Input: input{
				reader: mock.NewReader("").AddLines("err"),
			},
			ShouldErr: true,
		},
		"json error": {
			Input: input{
				reader: mock.NewReader(""),
			},
			ExpectedErr: errors.New("invalid json"),
		},
		"write close fail": {
			Input: input{
				writePath: "mock://err",
				reader:    mock.NewReader("").AddLines(`{"key":"value"}`),
			},
			ExpectedErr: errors.New("write close"),
		},
		"write fail": {
			Input: input{
				writePath: "mock://write_err",
				reader:    mock.NewReader("").AddLines(`{"key":"value"}`).SetLineCount(1000),
			},
			ExpectedErr: errors.New("write_err"),
		},
		"2 rows": {
			Input: input{
				reader: mock.NewReader("").AddLines(`{"name":"orange","count":1}`, `{"name":"apple","count":3}`),
			},
			Expected: []string{"count,name", "1,orange", "3,apple", ""},
		},
		"irregulars": {
			Input: input{
				reader: mock.NewReader("").AddLines(
					`{"name":"qu\"ote","value":"tab\t"}`,
					`{"name":"сайн байна уу?","value":"a,b,c"}`,
				),
			},
			Expected: []string{"name,value", `"qu""ote",tab	`, `сайн байна уу?,"a,b,c"`, ""},
		},
		"irregulars tab": {
			Input: input{
				reader: mock.NewReader("").AddLines(
					`{"name":"qu\"ote","value":"tab\t"}`,
					`{"name":"сайн байна уу?","value":"a,b,c"}`,
				),
				sep: "\t",
			},
			Expected: []string{"name\tvalue", `"qu""ote"	"tab	"`, `сайн байна уу?	a,b,c`, ""},
		},
	}
	trial.New(fn, cases).SubTest(t)
}

func TestGetFields(t *testing.T) {
	v := getFields(map[string]interface{}{"z": 123, "j": "apple", "a": "bcd"})
	if eq, diff := trial.Equal([]string{"a", "j", "z"}, v); !eq {
		t.Error(diff)
	}
}

func TestGetValues(t *testing.T) {
	v := getValues([]string{"a", "j", "z"}, map[string]interface{}{"z": 123, "j": "apple", "a": "bcd"})
	if eq, diff := trial.Equal([]string{"bcd", "apple", "123"}, v); !eq {
		t.Error(diff)
	}
}
