package main

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"strings"

	"github.com/jbsmith7741/uri"
	"github.com/pcelvng/task"

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
	File     string `uri:"origin" required:"true"`
	Output   string `uri:"output" required:"true"`
	OmitNull bool   `uri:"omit_null"`
	Sep      string `uri:"sep" default:","`

	reader    file.Reader
	writer    file.Writer
	fOpts     *file.Options
	fileTopic string
}

func (w *worker) DoTask(ctx context.Context) (task.Result, string) {
	scanner := file.NewScanner(w.reader)
	// read header
	scanner.Scan()
	headers := strings.Split(scanner.Text(), w.Sep)
	for rIdx := 0; scanner.Scan(); rIdx++ {
		if task.IsDone(ctx) {
			return task.Failf("context canceled")
		}
		row := strings.Split(scanner.Text(), w.Sep)
		if len(row) != len(headers) {
			return task.Failf("inconsistent length on line %d, header:%d != row:%d", rIdx, len(headers), len(row))
		}
		data := map[string]interface{}{}
		for i, v := range row {
			h := headers[i]
			if v == "" {
				if w.OmitNull {
					continue
				}
				data[h] = nil
				continue
			}
			if i, err := strconv.Atoi(v); err == nil {
				data[h] = i
				continue
			}
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				data[h] = f
				continue
			}
			data[h] = v
		}
		b, err := json.Marshal(data)
		if err != nil {
			w.writer.Abort()
			return task.Failed(err)
		}
		if err := w.writer.WriteLine(b); err != nil {
			w.writer.Abort()
			return task.Failed(err)
		}
	}
	if scanner.Err() != nil {
		w.writer.Abort()
		return task.Failf("scanner %s", scanner.Err())
	}
	if err := w.writer.Close(); err != nil {
		return task.Failed(err)
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
