package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"sync"

	"github.com/dustin/go-humanize"
	"github.com/itchyny/gojq"
	"github.com/jbsmith7741/uri"
	"github.com/pcelvng/task"

	tools "github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/bootstrap"
	"github.com/pcelvng/task-tools/file"
)

type options struct {
	File    file.Options
	Threads int
}

func main() {
	opts := &options{
		Threads: 1,
	}

	app := bootstrap.NewWorkerApp("log-proc", opts.newWorker, opts).
		Version(tools.String()).
		Description("").
		Initialize()

	app.Run()
}

func (o *options) newWorker(info string) task.Worker {
	w := &worker{
		options: *o,
	}

	if err := uri.Unmarshal(info, w); err != nil {
		return task.InvalidWorker("uri error: %s", err)
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

	if w.query, err = gojq.Parse(string(jqlogic)); err != nil {
		return task.InvalidWorker("invalid jq: %s", err)
	}
	return w
}

func (o options) Validate() error {
	if o.Threads < 1 {
		return fmt.Errorf("threads > 0")
	}
	return nil
}

type worker struct {
	Path     string `uri:"origin" required:"true"`
	Dest     string `uri:"dest" required:"true"`
	JqConfig string `uri:"jq_file" required:"true"`

	reader file.Reader
	writer file.Writer
	query  *gojq.Query

	options
}

func (w *worker) DoTask(ctx context.Context) (task.Result, string) {
	in := make(chan []byte, w.Threads)
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

	return task.Completed("%d files processed with %d lines and %s", w.reader.Stats().Files, sts.LineCnt, humanize.IBytes(uint64(osts.Size)))
}

func (w *worker) process(line []byte) error {
	data := make(map[string]interface{})
	if err := json.Unmarshal(line, &data); err != nil {
		return err
	}
	v, ok := w.query.Run(data).Next()
	if !ok {
		return v.(error)
	}

	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if err := w.writer.WriteLine(b); err != nil {
		return err
	}
	return nil
}
