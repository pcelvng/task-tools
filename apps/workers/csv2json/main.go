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

const desc = `csv2json convert a csv with a header to json 

Example: 
{"type":"csv2json","info":"gs://path/to/file.csv?output=gs://write/to/path/file.json&omitnull=false"}
`

func main() {
	opts := &options{
		File: file.NewOptions(),
	}
	app := bootstrap.NewWorkerApp("csv2json", opts.NewWorker, opts).
		Description(desc).
		Version(tools.String()).
		Initialize()
	producer = app.NewProducer()
	app.Run()
}
