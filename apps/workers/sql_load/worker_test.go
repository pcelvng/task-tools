package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/hydronica/trial"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/file"
)

func TestNewWorker(t *testing.T) {
	type input struct {
		*options
		Info string
	}

	type output struct {
		Params  InfoOptions
		Invalid bool
		Msg     string
		Count   int
	}
	// create a test folder and file
	f := "./tmp/temp1.json"
	w, _ := file.NewWriter(f, &file.Options{})
	w.WriteLine([]byte(`{"test":"value1","testing":"value2","number":123}`))
	w.Close()

	f = "./tmp/temp2.json"
	w, _ = file.NewWriter(f, &file.Options{})
	w.WriteLine([]byte(`{"test":"value1","testing":"value2","number":123}`))
	w.Close()

	d1, _ := filepath.Abs(f)

	f2 := "./tmp1"
	os.Mkdir(f2, 0755)
	d2, _ := filepath.Abs(f2)

	fn := func(in trial.Input) (interface{}, error) {
		// set input
		i := in.Interface().(input)
		wrkr := i.options.newWorker(i.Info)
		o := output{}
		// if task is invalid set values
		o.Invalid, o.Msg = task.IsInvalidWorker(wrkr)

		if !o.Invalid {
			myw := wrkr.(*worker)
			o.Params = myw.Params
			o.Count = len(myw.flist)
		}

		return o, nil
	}

	// testing cases
	cases := trial.Cases{
		"valid_worker": {
			Input: input{options: &options{}, Info: d1 + "?table=schema.table_name"},
			Expected: output{
				Params: InfoOptions{
					FilePath: d1,
					Table:    "schema.table_name",
				},
				Invalid: false,
				Msg:     "",
				Count:   1,
			},
		},

		"table_required": {
			Input: input{options: &options{}, Info: "nothing"},
			Expected: output{
				Invalid: true,
				Msg:     "params uri.unmarshal: table is required",
			},
		},

		"invalid_path": {
			Input: input{options: &options{}, Info: "missingfile.json?table=schema.table_name"},
			Expected: output{
				Invalid: true,
				Msg:     "filepath os: stat missingfile.json: no such file or directory",
			},
		},

		"valid_worker?": {
			Input: input{options: &options{}, Info: d2 + "?table=schema.table_name"},
			Expected: output{
				Params:  InfoOptions{},
				Invalid: true,
				Msg:     "no files found in path " + d2,
				Count:   0,
			},
		},
	}

	trial.New(fn, cases).Test(t)
	os.Remove(f)
	os.Remove("./tmp")
	os.Remove("./tmp1")
}
