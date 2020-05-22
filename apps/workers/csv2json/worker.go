package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strconv"

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
	reader := csv.NewReader(w.reader)

	// read header

	headers, err := reader.Read()
	fmt.Println(headers)
	if err != nil {
		return task.Failed(err)
	}
	var row []string
	for row, err = reader.Read(); err == nil; row, err = reader.Read() {
		if task.IsDone(ctx) {
			return task.Failf("context canceled")
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
	if err != io.EOF {
		w.writer.Abort()
		return task.Failf("scanner %s", err)
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
