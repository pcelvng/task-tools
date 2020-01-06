package main

import (
	tools "github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/bootstrap"
	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task/bus"
)

var producer, _ = bus.NewProducer(bus.NewOptions("nop"))

type options struct {
	File      *file.Options
	FileTopic string `toml:"file-topic"`
}

func (o options) Validate() error {
	return nil
}

const desc = `json2csv convert a json file to csv with a header 

Example: 
{"type":"json2csv","info":"gs://path/to/file.json.gz?output=gs://write/to/path/file.csv&field=f1,f2,f3"}
`

func main() {
	opts := &options{
		File: file.NewOptions(),
	}
	app := bootstrap.NewWorkerApp("json2csv", opts.NewWorker, opts).
		Description(desc).
		Version(tools.String()).
		Initialize()
	producer = app.NewProducer()
	app.Run()
}
