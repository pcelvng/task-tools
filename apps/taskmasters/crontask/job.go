package main

import (
	"fmt"
	"time"

	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/tmpl"
	"github.com/pcelvng/task/bus"
	"github.com/robfig/cron"
)

// makeCron will create the cron and setup all the cron jobs.
// It will not start the cron.
func makeCron(rules []Rule, producer bus.Producer) (*cron.Cron, error) {
	c := cron.New()
	for _, rule := range rules {
		job := newJob(rule, producer)
		if err := c.AddJob(rule.CronRule, job); err != nil {
			return nil, fmt.Errorf("cron: '%s' '%v'", rule.CronRule, err.Error())
		}
	}

	return c, nil
}

func newJob(rule Rule, p bus.Producer) *job {
	// normalize topic
	if rule.Topic == "" {
		rule.Topic = rule.TaskType
	}

	return &job{
		Rule: rule,
		p:    p,
	}
}

type job struct {
	Rule
	p bus.Producer
}

func (j *job) Run() {
	// make task
	info := tmpl.Parse(j.TaskTemplate, offsetDate(j.HourOffset))
	tsk := task.New(j.TaskType, info)

	j.p.Send(j.Topic, tsk.JSONBytes())
}

// offsetDate will return the time.Time value with the
// hour offset.
func offsetDate(offset int) time.Time {
	now := time.Now()
	return now.Add(time.Hour * time.Duration(offset))
}
