package main

import (
	"encoding/json"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jbsmith7741/uri"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/timeframe"
	"github.com/pcelvng/task-tools/tmpl"
	"github.com/pcelvng/task/bus"
)

type taskMaster struct {
	producer bus.Producer
	consumer bus.Consumer
	done     chan struct{}
}

func New(bus bus.Options) (*taskMaster, error) {
	p, err := task.NewProducer(&bus)
	if err != nil {
		return nil, err
	}
	c, err := task.NewConsumer(&bus)
	if err != nil {
		return nil, err
	}
	return &taskMaster{
		producer: p,
		consumer: c,
		done:     make(chan struct{}),
	}, nil
}

func (tm *taskMaster) Start() {
	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
	go tm.read()

	select {
	case <-sigChan:
		return
	case <-tm.done:
		return
	}

}

func (tm *taskMaster) read() {
	for {
		msg, done, err := tm.consumer.Msg()
		if err != nil {
			log.Printf("consumer err: %s", err)
			goto donecheck
		}

		if msg != nil {
			tsk := task.Task{}
			if err := json.Unmarshal(msg, &tsk); err != nil {
				log.Printf("json unmarshal error %s", err)
				goto donecheck
			}
			if err = tm.generate(tsk.Info); err != nil {
				log.Println(err)
			}
		}

	donecheck:
		if done {
			log.Println("done")
			close(tm.done)
			return
		}
	}
}

type infoOpts struct {
	Type     string   `uri:"type" required:"true"`
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
		iOpts.Topic = iOpts.Type
	}
	if err := iOpts.Validate(); err != nil {
		return err
	}

	for _, t := range iOpts.Generate() {
		tsk := task.Task{
			Type:    iOpts.Type,
			Created: time.Now().UTC().Format(time.RFC3339),
			Info:    tmpl.Parse(iOpts.Template, t),
		}
		if err := tm.producer.Send(iOpts.Topic, tsk.JSONBytes()); err != nil {
			return err
		}
	}
	return nil
}
