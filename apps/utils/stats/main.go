package main

import (
	"log"

	"github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/bootstrap"
)

const (
	name = "stats"
	desc = ``
)

func main() {
	log.SetFlags(log.Lshortfile)

	app := New()
	bootstrap.NewUtility(name, app).
		Description(desc).
		Version(tools.String()).
		Initialize()

	app.Start()
}
