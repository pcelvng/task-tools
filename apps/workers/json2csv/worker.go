package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"sort"

	"github.com/jbsmith7741/uri"
	"github.com/pcelvng/task"
	"github.com/pkg/errors"

	"github.com/pcelvng/task-tools/file"
)

func (o options) NewWorker(info string) task.Worker {
	w := &worker{
		fOpts: o.File,
	}
	if err := uri.Unmarshal(info, w); err != nil {
		return task.InvalidWorker("uri %s", err)
	}
	return w
}

type worker struct {
	File   string   `uri:"file" required:"true"`
	Output string   `uri:"output" required:"true"`
	Fields []string `uri:"field"`
	Sep    string   `uri:"sep" default:","`

	fOpts *file.Options
}

func (w *worker) DoTask(ctx context.Context) (task.Result, string) {
	r, err := file.NewReader(w.File, w.fOpts)
	if err != nil {
		return task.Failed(errors.Wrapf(err, "new reader"))
	}
	out, err := file.NewWriter(w.Output, w.fOpts)
	if err != nil {
		return task.Failed(errors.Wrapf(err, "new writer"))
	}
	writer := csv.NewWriter(out)
	scanner := file.NewScanner(r)
	for i := 0; scanner.Scan(); i++ {

		data := map[string]string{}
		if err := json.Unmarshal(scanner.Bytes(), data); err != nil {
			task.Failed(errors.Wrapf(err, "invalid json: %s", scanner.Text()))
		}
		// first time
		if i == 0 {
			if len(w.Fields) == 0 {
				w.Fields = getFields(data)
			}
			//write header
			if err := writer.Write(w.Fields); err != nil {
				task.Failed(errors.Wrapf(err, "header write"))
			}
		}
		if err := writer.Write(getValues(data)); err != nil {
			task.Failed(errors.Wrapf(err, "line %d", i))
		}

	}
	if err := out.Close(); err != nil {
		task.Failed(errors.Wrapf(err, "write close"))
	}
	sts := out.Stats()
	return task.Completed("done", sts)
}

// getFields returns a sorted list of the header found in the map
func getFields(m map[string]string) (s []string) {
	for key := range m {
		s = append(s, key)
	}
	sort.Strings(s)
	return s
}

func getValues(m map[string]string) (s []string) {
	for _, v := range m {
		s = append(s, m[v])
	}
	return s
}
