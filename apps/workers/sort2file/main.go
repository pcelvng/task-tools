package main

import (
	"github.com/pcelvng/task/bus"

	_ "embed"

	"github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/bootstrap"
	"github.com/pcelvng/task-tools/file"
)

const taskType = "sort2file"

//go:embed readme.md
var description string

func main() {
	appOpt := &options{
		FileTopic: "files", // default

	}
	app := bootstrap.NewWorkerApp(taskType, appOpt.newWorker, appOpt).
		Version(tools.String()).
		Description(description)

	app.Initialize()
	appOpt.Producer = app.NewProducer()
	app.Run()
}

type options struct {
	FileTopic string `toml:"file_topic" commented:"true" comment:"topic to publish written file stats"` // topic to publish information about written files

	Fopt     file.Options
	Producer bus.Producer
}

func (o *options) Validate() error { return nil }
