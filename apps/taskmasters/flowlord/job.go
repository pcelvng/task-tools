package main

import (
	"time"

	"github.com/pcelvng/task"
	"github.com/pcelvng/task/bus"

	"github.com/pcelvng/task-tools/tmpl"
)

type job struct {
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

	j.producer.Send(j.Topic, tsk.JSONBytes())
}
