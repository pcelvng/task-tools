package main

/*
import (
	"context"
	"errors"
	"testing"

	"github.com/hydronica/trial"
	"github.com/pcelvng/task"

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
			Expected: &worker{Meta: task.Meta{}, File: "nop://file.txt", Output: "nop://file.csv", Sep: ","},
		},
		"templating": {
			Input:    "nop://2019/11/01/2019-11-01.json.gz?output=nop://{yyyy}-{mm}-{dd}.csv",
			Expected: &worker{Meta: task.Meta{}, File: "nop://2019/11/01/2019-11-01.json.gz", Output: "nop://2019-11-01.csv", Sep: ","},
		},
		"tab": {
			Input:    "nop://file.txt?output=nop://file.csv&sep=%09",
			Expected: &worker{Meta: task.Meta{}, File: "nop://file.txt", Output: "nop://file.csv", Sep: "\t"},
		},
		"valid with fields": {
			Input: "nop://file.txt?output=nop://file.csv&",
			Expected: &worker{
				File:   "nop://file.txt",
				Output: "nop://file.csv",
				Sep:    ",",
				Meta:   task.Meta{},
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
		omitnull  bool
		canceled  bool
		sep       string
	}
	header := "f1,f2,f3,f4,f5"
	fn := func(in trial.Input) (interface{}, error) {
		v := in.Interface().(input)
		if v.sep == "" {
			v.sep = ","
		}
		w := mock.NewWriter(v.writePath)
		wkr := &worker{
			Sep:      v.sep,
			reader:   v.reader,
			writer:   w,
			OmitNull: v.omitnull,
			Meta:     task.NewMeta(),
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
				reader:   mock.NewReader("").AddLines(header, "1,2,3,4,5").SetLineCount(100),
				canceled: true,
			},
			ExpectedErr: errors.New("context canceled"),
		},
		"read error": {
			Input: input{
				reader: mock.NewReader("").AddLines(header, "err"),
			},
			ShouldErr: true,
		},
		"write close fail": {
			Input: input{
				writePath: "mock://close_err",
				reader:    mock.NewReader("").AddLines(header, "1,2,3,4,5"),
			},
			ExpectedErr: errors.New("close_err"),
		},
		"write fail": {
			Input: input{
				writePath: "mock://writeline_err",
				reader:    mock.NewReader("").AddLines(header, "1,2,3,4,5").SetLineCount(10),
			},
			ExpectedErr: errors.New("writeline_err"),
		},
		"null fields": {
			Input: input{
				reader: mock.NewReader("").AddLines(header, ",,3,4,5"),
			},
			Expected: []string{`{"f1":null,"f2":null,"f3":3,"f4":4,"f5":5}`},
		},
		"omit null fields": {
			Input: input{
				reader:   mock.NewReader("").AddLines(header, ",,3,4,5"),
				omitnull: true,
			},
			Expected: []string{`{"f3":3,"f4":4,"f5":5}`},
		},
		"too many rows": {
			Input: input{
				reader: mock.NewReader("").AddLines(header, ",,3,4,5,6,7", "read_eof"),
			},
			ExpectedErr: errors.New("inconsistent length"),
		},
		"row with quote": {
			Input: input{
				reader: mock.NewReader("").AddLines(header, "1,\"a,b,c\",3,4,5"),
			},
			Expected: []string{`{"f1":1,"f2":"a,b,c","f3":3,"f4":4,"f5":5}`},
		},
	}
	trial.New(fn, cases).SubTest(t)
}
*/
