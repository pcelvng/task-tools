package main

import (
	"context"
	//	"encoding/json"
	"io/ioutil"
	"log"
	"sync"

	"github.com/dustin/go-humanize"
	"github.com/itchyny/gojq"
	"github.com/jbsmith7741/uri"
	jsoniter "github.com/json-iterator/go"
	"github.com/pcelvng/task"

	tools "github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/bootstrap"
	"github.com/pcelvng/task-tools/file"
)

const (
	taskType    = "transform"
	description = `modify json data using jq syntax

	info params:
 - origin: (required) - glob path to a file(s) to transform (extract) 
 - dest:   (required) - file path to where the resulting data will be written 
 - jq:     (required) - file path to a jq definition file
 - threads: - number of threads to process files (default: 2)

example 
{"task":"transform","info":"gs://path/to/file/*/*.gz?dest=gs://path/dest/output.gz&jq=./conf.jq"}`
)

type options struct {
	File file.Options
}

func main() {
	opts := &options{}

	app := bootstrap.NewWorkerApp(taskType, opts.newWorker, opts).
		Version(tools.String()).
		Description(description).
		Initialize()

	app.Run()
}

func (o *options) newWorker(info string) task.Worker {
	w := &worker{
		Meta:    task.NewMeta(),
		options: *o,
	}

	if err := uri.Unmarshal(info, w); err != nil {
		return task.InvalidWorker("uri error: %s", err)
	}

	if w.Threads < 1 {
		return task.InvalidWorker("invalid threads %d (min: 1)", w.Threads)
	}

	jqreader, err := file.NewReader(w.JqConfig, &o.File)
	if err != nil {
		return task.InvalidWorker("jq config: %s", err)
	}
	jqlogic, err := ioutil.ReadAll(jqreader)
	if err != nil {
		return task.InvalidWorker("jq config read: %s", err)
	}

	if w.reader, err = file.NewGlobReader(w.Path, &o.File); err != nil {
		return task.InvalidWorker("reader error: %s", err)
	}

	if w.writer, err = file.NewWriter(w.Dest, &o.File); err != nil {
		return task.InvalidWorker("writer error: %s", err)
	}

	query, err := gojq.Parse(string(jqlogic))
	if err != nil {
		return task.InvalidWorker("invalid jq: %s", err)
	}
	if w.code, err = gojq.Compile(query); err != nil {
		return task.InvalidWorker("invalid jq-compile: %s", err)
	}

	return w
}

func (o options) Validate() error {
	return nil
}

type worker struct {
	task.Meta

	Path     string `uri:"origin" required:"true"`
	Dest     string `uri:"dest" required:"true"`
	JqConfig string `uri:"jq" required:"true"`
	Threads  int    `uri:"threads" default:"2"`

	reader file.Reader
	writer file.Writer
	code   *gojq.Code

	options
}

func (w *worker) DoTask(ctx context.Context) (task.Result, string) {
	in := make(chan []byte, 200)
	errChan := make(chan error)
	log.Printf("threads: %d", w.Threads)

	var wg sync.WaitGroup
	for i := 0; i < w.Threads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for b := range in {
				if err := w.process(b); err != nil {
					errChan <- err
				}
			}
		}()
	}

	scanner := file.NewScanner(w.reader)
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			close(in)
			return task.Interrupted()
		case err := <-errChan:
			return task.Failed(err)
		default:
			in <- scanner.Bytes()
		}
	}
	close(in)
	wg.Wait()

	sts := w.writer.Stats()
	if sts.ByteCnt == 0 {
		w.writer.Abort()
		return task.Completed("no data to write")
	}
	if err := w.writer.Close(); err != nil {
		return task.Failed(err)
	}
	osts, _ := file.Stat(w.Dest, &w.File)

	w.SetMeta("file", w.Dest)
	return task.Completed("%d files processed with %d lines and %s", w.reader.Stats().Files, sts.LineCnt, humanize.IBytes(uint64(osts.Size)))
}

func (w *worker) process(line []byte) error {
	data := make(map[string]interface{})
	if err := jsoniter.Unmarshal(line, &data); err != nil {
		return err
	}
	result, ok := w.code.Run(data).Next()
	if !ok {
		return result.(error)
	}

	b, err := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(result)
	if err != nil {
		return err
	}
	if err := w.writer.WriteLine(b); err != nil {
		return err
	}
	return nil
}
