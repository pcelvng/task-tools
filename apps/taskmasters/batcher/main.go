package main

import (
	"log"

	tools "github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/bootstrap"
	"github.com/pcelvng/task/bus"
)

const (
	taskType    = "batcher"
	description = `batcher creates a set of batch to be passed on to downstream processes

	type - the downstream task type (required)
	# - the downstream workers info template (required)
	from - the start time of the first task to be created (required)
	*** pick a duration modifier *** 
		to - the end time of the last task to be created
		for - the duration that should be run 

Example:
{"type":"batcher","info":"?task=topic&from=2006-01-02T15&for=-24h#s3://path/{yyyy}/{mm}/{dd}/{hh}.json.gz?options"}
`
)

type options struct {
	Bus bus.Options `toml:"bus"`
}

func main() {
	opt := &options{
		Bus: *bus.NewOptions(""),
	}

	bootstrap.NewHelper(taskType, opt).
		Version(tools.String()).
		Description(description).
		Initialize()

	tm, err := New(opt.Bus)
	if err != nil {
		log.Fatal(err)
	}
	tm.Start()
}
