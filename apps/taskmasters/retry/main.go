package main

import (
	"log"

	"github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/bootstrap"
	"github.com/pcelvng/task-tools/retry"
)

const (
	name        = "retry"
	description = ""
)

func main() {
	opts := retry.NewOptions()
	bootstrap.NewUtility(name, opts).Version(tools.String()).Description(description).Initialize()

	tm, err := retry.New(opts)
	if err != nil {
		log.Fatal(err)
	}
	if err := tm.Start(); err != nil {
		log.Println(err)
	}
}
