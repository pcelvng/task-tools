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
	"github.com/pcelvng/task/bus"
	"github.com/pkg/errors"
	"github.com/robfig/cron"

	"github.com/pcelvng/task-tools/bootstrap"
	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task-tools/tmpl"
	"github.com/pcelvng/task-tools/workflow"
)

type taskMaster struct {
	initTime time.Time
	path     string
	dur      string
	producer bus.Producer
	consumer bus.Consumer
	fOpts    *file.Options
	*workflow.Cache
	cron *cron.Cron
}

type stats struct {
	RunTime string        `json:"runtime"`
	Entries []*cron.Entry `json:"entries"`
}

func New(app *bootstrap.TaskMaster) bootstrap.Runner {
	opts := app.AppOpt().(*options)
	return &taskMaster{
		initTime: time.Now(),
		path:     opts.Workflow,
		fOpts:    app.GetFileOpts(),
		producer: app.NewProducer(),
		consumer: app.NewConsumer(),
		cron:     cron.New(),
		dur:      opts.Refresh,
	}
}

func (tm *taskMaster) Info() interface{} {
	return stats{
		RunTime: time.Now().Sub(tm.initTime).String(),
		Entries: tm.cron.Entries(),
	}
}

func (tm *taskMaster) Run(ctx context.Context) (err error) {
	if tm.Cache, err = workflow.New(tm.path, tm.dur, tm.fOpts); err != nil {
		return errors.Wrapf(err, "workflow setup")
	}

	// todo: setup refresh for workflow cache
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
	for name, record := range tm.Workflows {
		for _, w := range record.Parent() {
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
			if err = tm.cron.AddJob(j.Schedule, j); err != nil {
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
