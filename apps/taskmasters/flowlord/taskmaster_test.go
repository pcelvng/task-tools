package main

import (
	"encoding/json"
	"errors"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/hydronica/trial"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/workflow"
	"github.com/pcelvng/task/bus/nop"
	"github.com/robfig/cron/v3"
)

func TestTaskMaster_Process(t *testing.T) {
	delayRegex := regexp.MustCompile(`delayed=(\d+.\d+)`)
	cache, err := workflow.New("../../../internal/test/workflow/f1.toml", nil)
	if err != nil {
		t.Fatal("cache init", err)
	}
	consumer, err := nop.NewConsumer("")
	if err != nil {
		t.Fatal("doneConsumer", err)
	}
	tm := taskMaster{doneConsumer: consumer, Cache: cache}
	fn := func(v trial.Input) (interface{}, error) {
		tsk := v.Interface().(task.Task)
		producer, err := nop.NewProducer("")
		if err != nil {
			return nil, err
		}
		tm.producer = producer
		nop.FakeMsg = tsk.JSONBytes()
		err = tm.Process(&tsk)
		time.Sleep(100 * time.Millisecond)
		tm.producer.Stop()
		result := make([]task.Task, 0)
		for _, msgs := range producer.Messages {
			for _, msg := range msgs {
				var v task.Task
				if err := json.Unmarshal([]byte(msg), &v); err != nil {
					return nil, err
				}
				v.Created = ""
				if s := delayRegex.FindStringSubmatch(v.Meta); len(s) > 1 {
					v.Meta = strings.Replace(v.Meta, s[1], "XX", 1)
				}
				result = append(result, v)
			}
		}
		sort.Slice(result, func(i, j int) bool {
			return result[i].Type < result[j].Type
		})
		return result, err
	}
	cases := trial.Cases{
		"task1 attempt 0": {
			Input: task.Task{
				Type:    "task1",
				Info:    "?date=2019-12-12",
				Result:  task.ErrResult,
				Started: "now",
				Ended:   "before",
				ID:      "UUID_task1_attempt0",
				Meta:    "workflow=f1.toml"},
			Expected: []task.Task{
				{
					Type: "task1",
					Info: "?date=2019-12-12",
					ID:   "UUID_task1_attempt0",
					Meta: "delayed=XXms&retry=1&workflow=f1.toml"},
			},
		},
		"task1 attempt 2": {
			Input: task.Task{
				Type:   "task1",
				Info:   "?date=2019-12-12",
				Result: task.ErrResult,
				ID:     "UUID_task1_attempt2",
				Meta:   "retry=2&workflow=f1.toml"},
			Expected: []task.Task{
				{
					Type: "task1",
					Info: "?date=2019-12-12",
					ID:   "UUID_task1_attempt2",
					Meta: "delayed=XXms&retry=3&workflow=f1.toml"},
			},
		},
		"task1 no retry": {
			Input: task.Task{
				Type:   "task1",
				Info:   "?date=2019-12-12",
				Result: task.ErrResult,
				ID:     "UUID_task1",
				Meta:   "retry=3&workflow=f1.toml"},
			Expected: []task.Task{
				{
					Type:   "task1",
					Info:   "?date=2019-12-12",
					ID:     "UUID_task1",
					Meta:   "retry=failed&workflow=f1.toml",
					Result: "error",
				},
			},
		},
		"task1 complete": {
			Input: task.Task{
				Type:   "task1",
				Info:   "?date=2019-12-12",
				Result: task.CompleteResult,
				ID:     "UUID_task1",
				Meta:   "workflow=f1.toml"},
			Expected: []task.Task{
				{
					Type: "task2",
					Info: "?time=2019-12-12",
					ID:   "UUID_task1",
					Meta: "workflow=f1.toml",
				},
			},
		},
		"task1:j4 complete": {
			Input: task.Task{
				Type:   "task1",
				Info:   "?date=2019-12-12",
				Result: task.CompleteResult,
				ID:     "UUID_task1",
				Meta:   "workflow=f1.toml&job=t2"},
			Expected: []task.Task{
				{
					Type: "task2",
					Info: "?time=2019-12-12",
					ID:   "UUID_task1",
					Meta: "workflow=f1.toml",
				},
				{
					Type: "task5",
					Info: "?year=2019",
					ID:   "UUID_task1",
					Meta: "workflow=f1.toml&job=t5",
				},
			},
		},
		"task1 unknown result": {
			Input: task.Task{
				Type: "task1",
				Info: "?date=2019-12-12",
				ID:   "UUID_task1",
				Meta: "retry=3&workflow=f1.toml"},
			ShouldErr: true,
		},
		"task2_complete": {
			Input: task.Task{
				Type:   "task2",
				ID:     "UUID_task1",
				Meta:   "workflow=f1.toml&file=metafile.txt",
				Result: task.CompleteResult,
			},
			Expected: []task.Task{
				{Type: "task3", Info: "metafile.txt", ID: "UUID_task1", Meta: "workflow=f1.toml"},
				{Type: "task4", Info: "metafile.txt", ID: "UUID_task1", Meta: "workflow=f1.toml"},
			},
		},
		"task6 requires file": {
			Input: task.Task{
				Type:   "task5",
				ID:     "ID",
				Meta:   "workflow=f1.toml&file=file.txt&job=t5",
				Result: task.CompleteResult,
			},
			Expected: []task.Task{
				{Type: "task6", Info: "file.txt", ID: "ID", Meta: "workflow=f1.toml"},
			},
		},
		"task6 requires not ready": {
			Input: task.Task{
				Type:   "task5",
				ID:     "ID",
				Meta:   "workflow=f1.toml",
				Result: task.CompleteResult,
			},
			Expected: []task.Task{},
		},
	}
	trial.New(fn, cases).Test(t)
}

func TestTaskMaster_Schedule(t *testing.T) {

	type expected struct {
		Jobs  []job
		Files []fileRule
	}
	fn := func(in trial.Input) (interface{}, error) {
		cache, err := workflow.New("../../../internal/test/"+in.String(), nil)
		if err != nil {
			return nil, err
		}
		tm := taskMaster{Cache: cache, cron: cron.New()}
		err = tm.schedule()
		exp := expected{
			Jobs:  make([]job, 0),
			Files: tm.files,
		}
		for _, e := range tm.cron.Entries() {
			j := e.Job.(*job)
			exp.Jobs = append(exp.Jobs, *j)
		}
		return exp, err
	}
	cases := trial.Cases{
		"f1.toml": {
			Input: "workflow/f1.toml",
			Expected: expected{
				Jobs: []job{
					{
						Name:     "t2",
						Workflow: "f1.toml",
						Topic:    "task1",
						Schedule: "0 * * * *",
						Offset:   -4 * time.Hour,
						Template: "?date={yyyy}-{mm}-{dd}T{hh}",
					},
					{
						Name:     "t4",
						Workflow: "f1.toml",
						Topic:    "task1",
						Schedule: "0 * * * *",
						Offset:   -4 * time.Hour,
						Template: "?date={yyyy}-{mm}-{dd}T{hh}",
					},
				},
			},
		},
		"f3.toml": {
			Input: "workflow/f3.toml",
			Expected: expected{
				Jobs: []job{
					{
						Workflow: "f3.toml",
						Topic:    "task1",
						Schedule: "0 0 * * *",
						Template: "?date={yyyy}-{mm}-{dd}",
					},
				},
				Files: []fileRule{
					{
						SrcPattern:   "./folder/*.txt",
						workflowFile: "f3.toml",
						Phase: workflow.Phase{
							Task:     "task3",
							Rule:     "files=./folder/*.txt",
							Template: "{meta:file}",
						},
					},
				},
			},
		},
	}

	trial.New(fn, cases).EqualFn(trial.EqualOpt(trial.IgnoreAllUnexported)).Test(t)
}

func TestIsReady(t *testing.T) {
	type input struct {
		rule string
		meta string
	}
	fn := func(i trial.Input) (interface{}, error) {
		in := i.Interface().(input)
		return isReady(in.rule, in.meta), nil
	}
	cases := trial.Cases{
		"no require": {
			Input:    input{"", ""},
			Expected: true,
		},
		"require 1": {
			Input:    input{"require={meta:file}", "file=file.txt"},
			Expected: true,
		},
		"require 2": {
			Input: input{
				rule: "require={meta:file}&require={meta:time}",
				meta: "file=file.txt&time=now",
			},
			Expected: true,
		},
		"require w/ comma": {
			Input: input{
				rule: "require={meta:file},{meta:time}",
				meta: "file=file.txt&time=now",
			},
			Expected: true,
		},
		"missing": {
			Input:    input{"require={meta:file}", "file1=file.txt"},
			Expected: false,
		},
		"missing 1": {
			Input: input{
				rule: "require={meta:file}&require={meta:time}",
				meta: "file=file.txt",
			},
			Expected: false,
		},
	}
	trial.New(fn, cases).Test(t)
}

func comparer(i1, i2 interface{}) (equal bool, diff string) {
	act := i1.(task.Task)
	exp := i2.(task.Task)
	if exp.Created == "match" {
		exp.Created = act.Created
	}
	return trial.Equal(act, exp)
}

func TestValidatePhase(t *testing.T) {
	fn := func(i trial.Input) (interface{}, error) {
		s := validatePhase(i.Interface().(workflow.Phase))
		if s != "" {
			return nil, errors.New(s)
		}
		return s, nil
	}
	cases := trial.Cases{
		"empty phase": {
			Input:       workflow.Phase{},
			ExpectedErr: errors.New("invalid phase"),
		},
		"valid cron phase": {
			Input: workflow.Phase{
				Rule: "cron=* * * * * *",
			},
			Expected: "",
		},
		"unknown rule": {
			Input:     workflow.Phase{Rule: "abcedfg"},
			ShouldErr: true,
		},
		"dependsOn and rule": {
			Input: workflow.Phase{
				Rule:      "cron=abc",
				DependsOn: "task1",
			},
			ShouldErr: true,
		},
	}
	trial.New(fn, cases).SubTest(t)
}
