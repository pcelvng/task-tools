package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strconv"
	"time"

	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/bootstrap"
	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task-tools/tmpl"
	"github.com/pcelvng/task-tools/workflow"
	"github.com/pcelvng/task/bus"
	"github.com/pkg/errors"
	"github.com/robfig/cron/v3"
)

type taskMaster struct {
	initTime         time.Time
	path             string
	dur              string
	producer         bus.Producer
	consumer         bus.Consumer
	fOpts            *file.Options
	doneTopic        string
	retryFailedTopic string
	*workflow.Cache
	cron *cron.Cron
}

type stats struct {
	RunTime string       `json:"runtime"`
	Entries []cron.Entry `json:"entries"`
}

func New(app *bootstrap.TaskMaster) bootstrap.Runner {
	opts := app.AppOpt().(*options)
	return &taskMaster{
		initTime:         time.Now(),
		path:             opts.Workflow,
		doneTopic:        opts.DoneTopic,
		retryFailedTopic: opts.RetryFailedTopic,
		fOpts:            app.GetFileOpts(),
		producer:         app.NewProducer(),
		consumer:         app.NewConsumer(),
		cron:             cron.New(cron.WithSeconds()),
		dur:              opts.Refresh,
	}
}

func (tm *taskMaster) Info() interface{} {
	return stats{
		RunTime: time.Now().Sub(tm.initTime).String(),
		Entries: tm.cron.Entries(),
	}
}

func (tm *taskMaster) CacheUpdate(ctx context.Context) {
	d, err := time.ParseDuration(tm.dur)
	if err != nil {
		log.Println("error parsing cache duration", err)
		return
	}

	// starts a looping routine to update workflow file changes after a time duration
	go func(d time.Duration, ctx context.Context) {
		for now := range time.Tick(d) {
			fmt.Println("checking for workflow changes", now)
			if ctx.Err() != nil {
				fmt.Println("stopping cache update", ctx.Err())
				return
			}
			tm.Cache.Mutex.Lock()
			tm.Cache.Refresh()
			if tm.Cache.Reload {
				tm.cron.Stop()
				tm.cron = cron.New(cron.WithSeconds())
				err := tm.schedule()
				if err != nil {
					log.Println("error setting up cron schedule", err)
					return
				}
			}
			tm.Cache.Mutex.Unlock()
		}
	}(d, ctx)
}

func (tm *taskMaster) Run(ctx context.Context) (err error) {
	if tm.Cache, err = workflow.New(tm.path, tm.fOpts); err != nil {
		return errors.Wrapf(err, "workflow setup")
	}

	tm.CacheUpdate(ctx) // refresh the workflow if the file(s) have been changed
	if err := tm.schedule(); err != nil {
		return errors.Wrapf(err, "cron schedule")
	}

	go tm.read(ctx)
	for {
		select {
		case <-ctx.Done():
			log.Println("shutting down")
			return nil
		}
	}
}

// schedule the tasks and refresh the schedule when updated
func (tm *taskMaster) schedule() (err error) {
	for name, workflow := range tm.Workflows {
		for _, w := range workflow.Parent() {
			rules, _ := url.ParseQuery(w.Rule)
			if rules.Get("cron") == "" {
				log.Printf("skip: task:%s, rule:%s", w.Task, w.Rule)
				continue
			}

			j := &job{
				Workflow: name,
				Topic:    w.Task,
				Schedule: rules.Get("cron"),
				Template: w.Template,
				producer: tm.producer,
			}
			if s := rules.Get("offset"); s != "" {
				j.Offset, err = time.ParseDuration(s)
				if err != nil {
					return errors.Wrapf(err, "invalid duration %s", s)
				}
			}

			if _, err = tm.cron.AddJob(j.Schedule, j); err != nil {
				return errors.Wrapf(err, "invalid rule for %s:%s %s", name, w.Task, w.Rule)
			}
			log.Printf("cron: task:%s, rule:%s, info:%s", w.Task, j.Schedule, w.Template)
		}
	}
	tm.cron.Start()
	return nil
}

// Process the given task
// 1. check if the task needs to be retried
// 2. start any downstream tasks
// Log retries and failed retries to corrosponding topics
func (tm *taskMaster) Process(t *task.Task) error {
	meta, _ := url.ParseQuery(t.Meta)
	// attempt to return
	if t.Result == task.ErrResult {
		w := tm.Get(*t)
		r := meta.Get("retry")
		i, _ := strconv.Atoi(r)
		if w.Retry > i {
			t = task.NewWithID(t.Type, t.Info, t.ID)
			i++
			meta.Set("retry", strconv.Itoa(i))
			t.Meta = meta.Encode()
			if err := tm.producer.Send(t.Type, t.JSONBytes()); err != nil {
				return err
			}
		} else {
			// send to the retry failed topic if retries > w.Retry
			if tm.retryFailedTopic != "-" {
				meta.Set("retry", "failed")
				t.Meta = meta.Encode()
				tm.producer.Send(tm.retryFailedTopic, t.JSONBytes())
			}
		}
		return nil
	}

	// start off any children tasks
	if t.Result == task.CompleteResult {
		for _, w := range tm.Children(*t) {
			info := tmpl.Meta(w.Template, meta)
			child := task.NewWithID(w.Task, info, t.ID)
			child.Meta = "workflow=" + meta.Get("workflow")
			if err := tm.producer.Send(w.Task, child.JSONBytes()); err != nil {
				return err
			}
		}
		return nil
	}
	return fmt.Errorf("unknown result %q %s", t.Result, t.JSONString())
}

func (tm *taskMaster) read(ctx context.Context) {
	for {
		b, done, err := tm.consumer.Msg()
		if done || task.IsDone(ctx) {
			log.Println("stopping consumer")
			return
		}
		if err != nil {
			log.Println("consumer", err)
			return
		}
		t := &task.Task{}
		if err = json.Unmarshal(b, t); err != nil {
			log.Printf("unmarshal error %q: %s", string(b), err)
			continue
		}
		if err := tm.Process(t); err != nil {
			log.Println(err)
		}
	}
}
