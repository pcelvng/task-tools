package main

import (
	tools "github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/bootstrap"
)

const (
	taskType = "bq_load"
	desc     = ``
)

type options struct {
	BqAuth string `toml:"bq_auth" comment:"file path to service file"`
}

func (o *options) Validate() error {
	return nil
}

func main() {
	opts := &options{}
	app := bootstrap.NewWorkerApp(taskType, opts.NewWorker, opts).
		Description(desc).
		Version(tools.Version).Initialize()

	app.Run()
}
