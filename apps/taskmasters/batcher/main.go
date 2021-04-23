package main

import (
	tools "github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/bootstrap"
)

const (
	taskType    = "batcher"
	description = `batcher creates a set of batch to be passed on to downstream processes

	task-type - the downstream task type (required)
	from - the start time of the first task to be created (required)
	daily - run task for each day (every 24 hours)
	*** pick a duration modifier *** 
	 to - the end time of the last task to be created
	 for - the duration that should be run     
	 # - the downstream workers info template (required)
		

Example:
{"type":"batcher","info":"?type=topic&from=2006-01-02T15&for=-24h#s3://path/{yyyy}/{mm}/{dd}/{hh}.json.gz?options"}
{"type":"batcher","info":"?task-type=topic&daily&meta=job:test_job&from=2021-04-01T00&for=-2160h#gs://path/{yyyy}/{mm}/{dd}/none.json.gz?template_value=test"}
`
)

func main() {
	app := bootstrap.NewTaskMaster(taskType, New, nil).
		Version(tools.String()).
		Description(description)
	app.Initialize()
	app.Run()
}
