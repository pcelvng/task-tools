package main

import (
	tools "github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/bootstrap"
	"github.com/pcelvng/task-tools/file"
)

type options struct {
	File *file.Options
}

func (o options) Validate() error {
	return nil
}

const desc = ""

func main() {
	opts := &options{
		File: file.NewOptions(),
	}
	app := bootstrap.NewWorkerApp("json2csv", opts.NewWorker, opts).
		Description(desc).
		Version(tools.String()).
		Initialize()
	app.Run()
}
