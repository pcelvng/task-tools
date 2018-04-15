package main

import (
	"github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/bootstrap"
	"github.com/pcelvng/task/bus"
)

var (
	appName     = "tm-http"
	description = `The http taskmaster creates tasks in response to http REST requests.

Useful for launching one-off and batches of tasks over http.
`

	appOpt      = &options{}
	producer, _ = bus.NewProducer(bus.NewOptions("nop"))
)

func main() {
	tm := &taskmaster{}
	app := bootstrap.NewTMApp(appName, tm, appOpt).
		Version(tools.String()).
		Description(description)
	app.Initialize()
	app.Run()
}

type options struct {
}

func (o *options) Validate() error { return nil }
