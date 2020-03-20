package main

import (
	"encoding/json"
	"sort"
	"testing"

	"github.com/hydronica/trial"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/workflow"
	"github.com/pcelvng/task/bus/nop"
	"github.com/robfig/cron/v3"
)

func TestTaskMaster_Process(t *testing.T) {
	cache, err := workflow.New("../../../internal/test/workflow/f1.toml", nil)
	if err != nil {
		t.Fatal("cache init", err)
	}
	consumer, err := nop.NewConsumer("")
	if err != nil {
		t.Fatal("consumer", err)
	}
	tm := taskMaster{consumer: consumer, Cache: cache}
	fn := func(v trial.Input) (interface{}, error) {
		tsk := v.Interface().(task.Task)
		producer, err := nop.NewProducer("")
		if err != nil {
			return nil, err
		}
		tm.producer = producer
		nop.FakeMsg = tsk.JSONBytes()
		err = tm.Process(&tsk)
		result := make([]task.Task, 0)
		for _, msgs := range producer.Messages {
			for _, msg := range msgs {
				var v task.Task
				if err := json.Unmarshal([]byte(msg), &v); err != nil {
					return nil, err
				}
				v.Created = ""
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
					Meta: "retry=1&workflow=f1.toml"},
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
					Meta: "retry=3&workflow=f1.toml"},
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
				task.Task{
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
					Info: "?time={yyyy}-{mm}-{dd}", // todo: change after templating
					ID:   "UUID_task1",
					Meta: "workflow=f1.toml",
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
				Meta:   "workflow=f1.toml",
				Result: task.CompleteResult,
			},
			Expected: []task.Task{
				{Type: "task3", Info: "", ID: "UUID_task1", Meta: "workflow=f1.toml"},
				{Type: "task4", Info: "", ID: "UUID_task1", Meta: "workflow=f1.toml"},
			},
		},
	}
	trial.New(fn, cases).SubTest(t)
}

func TestTaskMaster_Schedule(t *testing.T) {
	cache, err := workflow.New("../../../internal/test/workflow/f1.toml", nil)
	if err != nil {
		t.Fatal("cache init", err)
	}
	consumer, err := nop.NewConsumer("")
	producer, err := nop.NewProducer("")
	if err != nil {
		t.Fatal("consumer", err)
	}
	tm := taskMaster{consumer: consumer, producer: producer, Cache: cache, cron: cron.New()}
	// verify task1 was scheduled correctly
	if err := tm.schedule(); err != nil {
		t.Fatal(err)
	}
	//
	for _, e := range tm.cron.Entries() {
		e.Job.Run()
	}
	if eq, diff := trial.Contains(producer.Messages["task1"], []string{`"type":"task1"`, `"meta":"workflow=f1.toml\u0026job=t2"`}); !eq {
		t.Error(diff)
	}
}

func comparer(i1, i2 interface{}) (equal bool, diff string) {
	act := i1.(task.Task)
	exp := i2.(task.Task)
	if exp.Created == "match" {
		exp.Created = act.Created
	}
	return trial.Equal(act, exp)
}
