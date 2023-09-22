package main

import (
	retry2 "github.com/pcelvng/task-tools/apps/taskmasters/retry/retry"
	"log"

	"github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/bootstrap"
)

const (
	name        = "retry"
	description = ""
)

func main() {
	opts := retry2.NewOptions()
	bootstrap.NewUtility(name, opts).Version(tools.String()).Description(description).Initialize()

	tm, err := retry2.New(opts)
	if err != nil {
		log.Fatal(err)
	}
	if err := tm.Start(); err != nil {
		log.Println(err)
	}
}
