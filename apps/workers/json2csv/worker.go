package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"sort"

	"github.com/jbsmith7741/uri"
	"github.com/pcelvng/task"
	"github.com/pkg/errors"

	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task-tools/tmpl"
)

func (o *options) NewWorker(info string) task.Worker {
	w := &worker{
		fileTopic: o.FileTopic,
		fOpts:     o.File,
		Meta:      task.NewMeta(),
	}
	err := uri.Unmarshal(info, w)
	if err != nil {
		return task.InvalidWorker("uri %s", err)
	}

	w.reader, err = file.NewReader(w.File, w.fOpts)
	if err != nil {
		return task.InvalidWorker("new reader %s", err)
	}
	tm := tmpl.PathTime(w.File)
	w.Output = tmpl.Parse(w.Output, tm)
	w.writer, err = file.NewWriter(w.Output, w.fOpts)
	if err != nil {
		return task.InvalidWorker("new writer %s", err)
	}
	return w
}

type worker struct {
	task.Meta
	File   string   `uri:"origin" required:"true"`
	Output string   `uri:"output" required:"true"`
	Fields []string `uri:"field"`
	Sep    string   `uri:"sep" default:","`

	reader    file.Reader
	writer    file.Writer
	fOpts     *file.Options
	fileTopic string
}

func (w *worker) DoTask(ctx context.Context) (task.Result, string) {
	writer := csv.NewWriter(w.writer)
	writer.Comma = rune(w.Sep[0])
	scanner := file.NewScanner(w.reader)
	for i := 0; scanner.Scan(); i++ {
		if task.IsDone(ctx) {
			return task.Failf("context canceled")
		}
		data := map[string]interface{}{}
		if err := json.Unmarshal(scanner.Bytes(), &data); err != nil {
			return task.Failf("invalid json: %s %s", err, scanner.Text())
		}
		// first time
		if i == 0 {
			if len(w.Fields) == 0 {
				w.Fields = getFields(data)
			}
			//write header
			if err := writer.Write(w.Fields); err != nil {
				return task.Failf("header write %s", err)
			}
		}
		if err := writer.Write(getValues(w.Fields, data)); err != nil {
			return task.Failf("line %d: %s", i, err)
		}
	}
	if scanner.Err() != nil {
		return task.Failed(scanner.Err())
	}
	writer.Flush()
	if err := w.writer.Close(); err != nil {
		return task.Failed(errors.Wrapf(err, "write close"))
	}
	sts := w.writer.Stats()
	if w.fileTopic != "" {
		if err := producer.Send(w.fileTopic, sts.JSONBytes()); err != nil {
			log.Println("file stats", err)
		}
	}
	w.SetMeta("file", sts.Path)
	return task.Completed("%d bytes writen to %s", sts.ByteCnt, sts.Path)
}

// getFields returns a sorted list of the header found in the map
func getFields(m map[string]interface{}) (s []string) {
	for key := range m {
		s = append(s, key)
	}
	sort.Strings(s)
	return s
}

func getValues(keys []string, m map[string]interface{}) (s []string) {
	for _, k := range keys {
		s = append(s, fmt.Sprintf("%v", m[k]))
	}
	return s
}
