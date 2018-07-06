package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/jbsmith7741/uri"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/bootstrap"
	"github.com/pcelvng/task-tools/timeframe"
	"github.com/pcelvng/task-tools/tmpl"
	"github.com/pcelvng/task/bus"
)

type taskMaster struct {
	initTime time.Time
	producer bus.Producer
	consumer bus.Consumer
	stats
	done chan struct{}
}

type stats struct {
	RunTime      string         `json:"runtime"`
	LastReceived interface{}    `json:"last_receieved"`
	Requests     map[string]int `json:"requests"`
}

func New(app *bootstrap.TaskMaster) bootstrap.Runner {
	return &taskMaster{
		initTime: time.Now(),
		stats:    stats{Requests: make(map[string]int)},
		producer: app.NewProducer(),
		consumer: app.NewConsumer(),
		done:     make(chan struct{}),
	}
}

func (tm *taskMaster) Info() interface{} {
	tm.RunTime = time.Now().Sub(tm.initTime).String()
	return tm.stats
}

func (tm *taskMaster) Run(ctx context.Context) error {
	var waiting bool
	go tm.read(ctx)
	var timer = &time.Timer{}
	for {
		select {
		case <-ctx.Done():
			if !waiting {
				timer = time.NewTimer(5 * time.Second)
				waiting = true
			}
		case <-timer.C:
			return errors.New("force stop")
		case <-tm.done:
			return nil
		}
	}
}

func (tm *taskMaster) read(ctx context.Context) {
	var done bool
	var msg []byte
	var err error
	for !done {
		if task.IsDone(ctx) {
			break
		}
		msg, done, err = tm.consumer.Msg()
		if err != nil {
			log.Printf("consumer err: %s", err)
			continue
		}

		if msg != nil {
			tsk := task.Task{}
			if err := json.Unmarshal(msg, &tsk); err != nil {
				log.Printf("json unmarshal error %s", err)
				continue
			}
			if err = tm.generate(tsk.Info); err != nil {
				log.Println(err)
			}
		}
	}
	log.Println("done")
	close(tm.done)
}

type infoOpts struct {
	TaskType string   `uri:"task-type" required:"true"`
	Topic    string   `uri:"topic"`
	For      duration `uri:"for"`
	Template string   `uri:"fragment" required:"true"`
	timeframe.TimeFrame
}

func (tm *taskMaster) generate(info string) error {
	var iOpts infoOpts
	if err := uri.Unmarshal(info, &iOpts); err != nil {
		return err
	}
	if iOpts.End.IsZero() {
		if iOpts.For == 0 {
			return errors.New("end date required (see for/to)")
		}
		iOpts.End = iOpts.Start.Add(iOpts.For.Duration())
	}
	if iOpts.Topic == "" {
		iOpts.Topic = iOpts.TaskType
	}
	if err := iOpts.Validate(); err != nil {
		return err
	}
	tm.Requests[iOpts.Topic]++
	tm.LastReceived = info
	for _, t := range iOpts.Generate() {
		tsk := task.Task{
			Type:    iOpts.TaskType,
			Created: time.Now().UTC().Format(time.RFC3339),
			Info:    tmpl.Parse(iOpts.Template, t),
		}
		if err := tm.producer.Send(iOpts.Topic, tsk.JSONBytes()); err != nil {
			return err
		}
	}
	return nil
}
