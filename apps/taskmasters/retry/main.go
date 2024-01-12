package main

import (
	"log"

	"github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/apps/taskmasters/retry/retry"
	"github.com/pcelvng/task-tools/bootstrap"
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
