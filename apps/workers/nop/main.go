package main

import (
	"time"

	"github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/bootstrap2"
)

func main() {
	app := bootstrap2.NewWorkerApp(taskType, newWorker, appOpt).
		Version(tools.String()).
		Description(description)
	app.Initialize()
	app.Run()
}

type options struct {
	FailRate    int    `toml:"fail_rate" comment:"int between 0-100 representing a percent"`
	Dur         string `toml:"dur" comment:"how long the task will take to finish successfully as a time.Duration parseable string"`
	DurVariance string `toml:"dur_variance" comment:"random adjustment to the 'dur' value as a time.Duration parseable string"`

	dur         time.Duration // set during validation
	durVariance time.Duration // set during validation
}

func (o *options) Validate() (err error) {
	// dur
	if o.Dur != "" {
		o.dur, err = time.ParseDuration(o.Dur)
		if err != nil {
			return err
		}
	}

	// durVariance
	if o.DurVariance != "" {
		o.durVariance, err = time.ParseDuration(o.DurVariance)
		if err != nil {
			return err
		}
	}

	return nil
}
