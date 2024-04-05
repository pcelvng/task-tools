package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/pcelvng/task-tools/apps/tm-archive/batcher/timeframe"
	"github.com/pcelvng/task-tools/file"
	"log"
	"net/url"
	"strconv"
	"time"

	"github.com/jbsmith7741/uri"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/bootstrap"
	"github.com/pcelvng/task-tools/tmpl"
	"github.com/pcelvng/task/bus"
)

type taskMaster struct {
	initTime time.Time
	producer bus.Producer
	consumer bus.Consumer
	fOpts    file.Options
	stats
	done chan struct{}
}

type stats struct {
	RunTime      string         `json:"runtime"`
	LastReceived interface{}    `json:"last_receieved"`
	Requests     map[string]int `json:"requests"`
}

type infoOpts struct {
	FilePath string            `uri:"origin"`
	TaskType string            `uri:"task-type" required:"true"`
	Topic    string            `uri:"topic"`
	For      time.Duration     `uri:"for"`
	Template string            `uri:"fragment" required:"true"`
	Meta     map[string]string `uri:"meta"`
	timeframe.TimeFrame
}

func (o *options) New(app *bootstrap.TaskMaster) bootstrap.Runner {
	return &taskMaster{
		initTime: time.Now(),
		stats:    stats{Requests: make(map[string]int)},
		producer: app.NewProducer(),
		consumer: app.NewConsumer(),
		fOpts:    o.File,
		done:     make(chan struct{}),
	}
}

func (tm *taskMaster) Info() interface{} {
	tm.RunTime = time.Since(tm.initTime).String()
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
	close(tm.done)
}

func (tm *taskMaster) generate(info string) error {
	var iOpts infoOpts
	if err := uri.Unmarshal(info, &iOpts); err != nil {
		return fmt.Errorf("info uri: %w", err)
	}

	if iOpts.Meta == nil {
		iOpts.Meta = make(map[string]string)
	}

	var reader file.Reader
	if iOpts.FilePath != "" {
		var err error
		reader, err = file.NewReader(iOpts.FilePath, &tm.fOpts)
		if err != nil {
			return err
		}
	}

	iOpts.Meta["batcher"] = "true"
	if iOpts.TimeFrame.Start.IsZero() {
		iOpts.TimeFrame.Start = time.Now()
	}
	if iOpts.End.IsZero() {
		if iOpts.For == 0 && iOpts.FilePath == "" {
			return errors.New("end date required (see for/to)")
		}
		iOpts.End = iOpts.Start.Add(iOpts.For)
	}
	if iOpts.Topic == "" {
		iOpts.Topic = iOpts.TaskType
	}
	if err := iOpts.Validate(); err != nil {
		return err
	}
	tm.Requests[iOpts.Topic]++
	tm.LastReceived = info
	hours := iOpts.Generate()
	if len(hours) == 0 {
		hours = []time.Time{time.Now().Truncate(time.Hour)}
	}
	for _, t := range hours {
		if iOpts.FilePath == "" {
			tsk := task.New(iOpts.TaskType, tmpl.Parse(iOpts.Template, t))
			m := task.NewMeta()
			for k, v := range iOpts.Meta {
				m.SetMeta(k, v)
			}
			tsk.Meta = m.GetMeta().Encode()
			if err := tm.producer.Send(iOpts.Topic, tsk.JSONBytes()); err != nil {
				return err
			}
		} else {
			scanner := file.NewScanner(reader)
			for scanner.Scan() {
				data := make(map[string]any)
				if err := json.Unmarshal(scanner.Bytes(), &data); err != nil {
					return err
				}
				vals := make(url.Values)
				for k, v := range data {
					switch x := v.(type) {
					case string:
						vals.Add(k, x)
					case int:
						vals.Add(k, strconv.Itoa(x))
					case float64:
						vals.Add(k, strconv.FormatFloat(x, 'f', -1, 64))
					}
				}

				info := tmpl.Parse(iOpts.Template, t)
				info, _ = tmpl.Meta(info, vals)
				tsk := task.New(iOpts.TaskType, info)
				m := task.NewMeta()
				for k, v := range iOpts.Meta {
					m.SetMeta(k, v)
				}
				tsk.Meta = m.GetMeta().Encode()
				if err := tm.producer.Send(iOpts.Topic, tsk.JSONBytes()); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
