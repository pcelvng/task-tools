package main

import (
	"context"
	"errors"
	"math/rand"
	"time"

	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/bootstrap"
)

func newWorker(_ string) task.Worker {
	return &worker{}
}

type worker struct{}

func (w *worker) DoTask(ctx context.Context) (result task.Result, msg string) {
	doneChan := make(chan interface{})
	go func() {
		result, msg = w.doTask()
		close(doneChan)
	}()

	select {
	case <-doneChan:
	case <-ctx.Done():
		return task.Interrupted()
	}

	return result, msg
}

func (w *worker) doTask() (task.Result, string) {
	// calc if failure
	isFail := checkFail(appOpt.FailRate)

	var dur time.Duration
	if isFail { // calc failDuration
		dur = failDuration(appOpt.dur, appOpt.durVariance)
	} else {
		dur = successDuration(appOpt.dur, appOpt.durVariance)
	}

	// wait for duration
	time.Sleep(dur)

	// complete task and return
	if isFail {
		return task.Failed(errors.New("failed"))
	} else {
		return task.Completed("finished")
	}
}

// successDuration will calculate how long the task
// will take to complete based on up to a random
// amount of variance.
//
// NOTE: the variance always adds to the base duration.
func successDuration(dur, durV time.Duration) time.Duration {
	// no variance so it's just the duration
	if durV == 0 {
		return dur
	}

	// generate a random variance
	seed := int64(time.Now().Nanosecond())
	randomizer := rand.New(rand.NewSource(seed))
	v := randomizer.Int63n(int64(durV))
	return dur + time.Duration(v)
}

// failDuration is any value up to the successDuration
// because it can fail at any time.
func failDuration(dur, durV time.Duration) time.Duration {
	maxDur := successDuration(dur, durV)
	if maxDur == 0 {
		return maxDur
	}

	// generate a random variance
	seed := int64(time.Now().Nanosecond())
	randomizer := rand.New(rand.NewSource(seed))
	v := randomizer.Int63n(int64(maxDur))
	return time.Duration(v)
}

// checkFail will return true if the task should
// be completed as an error and false otherwise.
// rate is assumed to be a value between 0-100.
// A value of 100 or more will always return true and
// a value of 0 or less will always return false.
func checkFail(rate int) bool {
	if rate <= 0 {
		return false
	}

	seed := int64(time.Now().Nanosecond())
	randomizer := rand.New(rand.NewSource(seed))
	if randomizer.Intn(100) <= rate {
		return true
	}

	return false
}

var (
	taskType    = "nop"
	description = `The nop worker does nothing except listen for tasks and mark the task
as a success or failure at random. The failure rate can be set at runtime. The
nop worker can simulate working on the task for a period of time by setting a 
task completion length or length range.

The nop worker is meant for staging task ecosystem interactions
and will ignore the "info" string.

Example task:
{"type":"does-not-matter" info":"it does not matter"}`

	appOpt = &options{}
)

func main() {
	app := bootstrap.NewWorkerApp(taskType, newWorker, appOpt).
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
