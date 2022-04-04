package main

import (
	"time"

	"github.com/pcelvng/task"
	"github.com/pcelvng/task/bus"

	"github.com/pcelvng/task-tools/tmpl"
)

type job struct {
	Name     string
	Workflow string
	Topic    string
	Schedule string
	Offset   time.Duration
	Template string
	producer bus.Producer
}

func (j *job) Run() {
	tm := time.Now().Add(j.Offset)
	info := tmpl.Parse(j.Template, tm)
	tsk := task.New(j.Topic, info)
	tsk.Meta = "workflow=" + j.Workflow
	tsk.Meta += "&cron=" + tm.Format(time.RFC3339)
	if j.Name != "" {
		tsk.Meta += "&job=" + j.Name
	}

	j.producer.Send(j.Topic, tsk.JSONBytes())
}
