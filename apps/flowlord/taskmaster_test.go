package main

import (
	"encoding/json"
	"errors"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hydronica/trial"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task/bus/nop"
	"github.com/robfig/cron/v3"

	"github.com/pcelvng/task-tools/apps/flowlord/cache"
	"github.com/pcelvng/task-tools/workflow"
)

const base_test_path string = "../../internal/test/"

func TestTaskMaster_Process(t *testing.T) {
	delayRegex := regexp.MustCompile(`delayed=(\d+.\d+)`)
	workflowCache, fatalErr := workflow.New(base_test_path+"workflow", nil)
	if fatalErr != nil {
		t.Fatal("cache init", fatalErr)
	}
	consumer, fatalErr := nop.NewConsumer("")
	if fatalErr != nil {
		t.Fatal("doneConsumer", fatalErr)
	}
	fn := func(tsk task.Task) ([]task.Task, error) {
		var alerts int64
		// Initialize taskCache for the test
		taskCache, err := cache.NewSQLite(time.Hour, ":memory:")
		if err != nil {
			return nil, err
		}
		tm := taskMaster{doneConsumer: consumer, Cache: workflowCache, taskCache: taskCache, failedTopic: "failed-topic", alerts: make(chan task.Task), slack: &Notification{}}
		producer, _ := nop.NewProducer("")
		tm.producer = producer
		nop.FakeMsg = tsk.JSONBytes()
		// handle alert messages
		go func() {
			for range tm.alerts {
				atomic.AddInt64(&alerts, 1)
			}
		}()
		if err := tm.Process(&tsk); err != nil {
			return nil, err
		}

		time.Sleep(100 * time.Millisecond)
		tm.producer.Stop()
		close(tm.alerts)
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
			if result[i].Type == result[j].Type {
				return result[i].Job < result[j].Job
			}
			return result[i].Type < result[j].Type
		})
		/* TODO: test alerts are sent
		if alerts > 0 {
			return result, fmt.Errorf("%d alerts", alerts)
		} */
		return result, nil
	}
	cases := trial.Cases[task.Task, []task.Task]{
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
					Job:  "t2",
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
					Job:  "t2",
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
					Meta:   "retried=3&retry=failed&workflow=f1.toml",
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
					Job:  "t5",
					Meta: "workflow=f1.toml&job=t5",
				},
			},
		},
		"job in phase": {
			Input: task.Task{
				Type:   "worker",
				Job:    "child2",
				ID:     "UUID2",
				Result: task.CompleteResult,
				Meta:   "workflow=jobs.toml",
			},
			Expected: []task.Task{
				{
					Type: "worker",
					Job:  "child3",
					Info: "?day={yyyy}-{mm}-{dd}",
					ID:   "UUID2",
					Meta: "workflow=jobs.toml&job=child3",
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
		"cron timestamp": {
			Input: task.Task{
				Type:   "task1",
				Meta:   "workflow=f1.toml&cron=2020-01-01T08",
				Result: task.CompleteResult,
			},
			Expected: []task.Task{
				{Type: "task2", Info: "?time=2020-01-01", Meta: "workflow=f1.toml&cron=2020-01-01T08"},
			},
		},
		// start a child worker with the job data in the rule
		"no meta job -> children ": {
			Input: task.Task{
				Type:   "worker",
				Job:    "parent_job",
				ID:     "parent_ID",
				Meta:   "workflow=jobs.toml&cron=2020-01-01T08",
				Result: "complete",
			},
			Expected: []task.Task{
				{
					Type: "worker",
					Job:  "child1",
					ID:   "parent_ID",
					Meta: "workflow=jobs.toml&cron=2020-01-01T08&job=child1",
					Info: "?date=2020-01-01T08",
				},
				{
					Type: "worker",
					Job:  "child2",
					ID:   "parent_ID",
					Meta: "workflow=jobs.toml&cron=2020-01-01T08&job=child2",
					Info: "?day=2020-01-01",
				},
			},
		},
	}
	trial.New(fn, cases).Timeout(time.Second).SubTest(t)
}

func TestTaskMaster_Schedule(t *testing.T) {

	type expected struct {
		Jobs  []Cronjob
		Files []fileRule
	}
	fn := func(in string) (expected, error) {
		cache, err := workflow.New(base_test_path+in, nil)
		if err != nil {
			return expected{}, err
		}
		tm := taskMaster{Cache: cache, cron: cron.New()}
		err = tm.schedule()
		exp := expected{
			Jobs:  make([]Cronjob, 0),
			Files: tm.files,
		}
		for _, e := range tm.cron.Entries() {
			j := e.Job.(*Cronjob)
			exp.Jobs = append(exp.Jobs, *j)
		}
		return exp, err
	}
	cases := trial.Cases[string, expected]{
		"f1.toml": {
			Input: "workflow/f1.toml",
			Expected: expected{
				Jobs: []Cronjob{
					{
						Name:     "t2",
						Workflow: "f1.toml",
						Topic:    "task1",
						Schedule: "0 0 * * * *",
						Offset:   -4 * time.Hour,
						Template: "?date={yyyy}-{mm}-{dd}T{hh}",
					},
					{
						Name:     "t4",
						Workflow: "f1.toml",
						Topic:    "task1",
						Schedule: "0 0 * * * *",
						Offset:   -4 * time.Hour,
						Template: "?date={yyyy}-{mm}-{dd}T{hh}",
					},
				},
			},
		},
		"f3.toml": {
			Input: "workflow/f3.toml",
			Expected: expected{
				Jobs: []Cronjob{
					{
						Workflow: "f3.toml",
						Topic:    "task1",
						Schedule: "0 0 0 * * *",
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

	trial.New(fn, cases).Comparer(trial.EqualOpt(trial.IgnoreAllUnexported)).SubTest(t)
}

func Test_NewJob(t *testing.T) {
	tm := &taskMaster{}
	fn := func(ph workflow.Phase) (cron.Job, error) {
		return tm.NewJob(ph, "")
	}
	cases := trial.Cases[workflow.Phase, cron.Job]{
		"simple cronjob": {
			Input: workflow.Phase{
				Task:     "task1",
				Rule:     "cron=15 35 13 * * *&offset=-24h",
				Template: "?date={yyyy}-{mm}-{dd}",
			},
			Expected: &Cronjob{
				Topic:    "task1",
				Schedule: "15 35 13 * * *",
				Offset:   -24 * time.Hour,
				Template: "?date={yyyy}-{mm}-{dd}",
			},
		},
		"invalid schedule": {
			Input: workflow.Phase{
				Task:     "invalid",
				Rule:     "cron=badcron",
				Template: "?day={yyyy}-{mm}-{dd}",
			},
			ShouldErr: true,
		},
		"5 digit schedule": {
			Input: workflow.Phase{
				Task:     "task5",
				Rule:     "cron=35 13 * * *",
				Template: "?date={yyyy}-{mm}-{dd}",
			},
			Expected: &Cronjob{
				Topic:    "task5",
				Schedule: "0 35 13 * * *",
				Template: "?date={yyyy}-{mm}-{dd}",
			},
		},
		"batch by meta": {
			Input: workflow.Phase{
				Task:     "meta-batch",
				Rule:     "cron=15 35 13 * * *&meta=name:a,b,c|value:1,2,3",
				Template: "?name={meta:name}&value={meta:value}&day={yyyy}-{mm}-{dd}",
			},
			Expected: &batchJob{
				Cronjob: Cronjob{
					Topic:    "meta-batch",
					Schedule: "15 35 13 * * *",
					Template: "?name={meta:name}&value={meta:value}&day={yyyy}-{mm}-{dd}",
				},
				Meta: map[string][]string{"name": {"a", "b", "c"}, "value": {"1", "2", "3"}},
			},
		},
		"batch by meta-file": {
			Input: workflow.Phase{
				Task:     "file-batch",
				Rule:     "cron=15 35 13 * * *&meta-file=test.json",
				Template: "?name={meta:name}&value={meta:value}&day={yyyy}-{mm}-{dd}",
			},
			Expected: &batchJob{
				Cronjob: Cronjob{
					Topic:    "file-batch",
					Schedule: "15 35 13 * * *",
					Template: "?name={meta:name}&value={meta:value}&day={yyyy}-{mm}-{dd}",
				},
				Metafile: "test.json",
			},
		},
		"batch by duration + meta": {
			Input: workflow.Phase{
				Task:     "combo-batch",
				Rule:     "cron=15 35 13 * * *&for=-48h&meta=name:a,b",
				Template: "?name={meta:name}&day={yyyy}-{mm}-{dd}",
			},
			Expected: &batchJob{
				Cronjob: Cronjob{
					Topic:    "combo-batch",
					Schedule: "15 35 13 * * *",
					Template: "?name={meta:name}&day={yyyy}-{mm}-{dd}",
				},
				For:  -48 * time.Hour,
				Meta: map[string][]string{"name": {"a", "b"}},
			},
		},
		"batch by duration + meta-file": {
			Input: workflow.Phase{
				Task:     "combo-batch",
				Rule:     "cron=15 35 13 * * *&for=-48h&meta-file=test.json",
				Template: "?name={meta:name}&day={yyyy}-{mm}-{dd}",
			},
			Expected: &batchJob{
				Cronjob: Cronjob{
					Topic:    "combo-batch",
					Schedule: "15 35 13 * * *",
					Template: "?name={meta:name}&day={yyyy}-{mm}-{dd}",
				},
				For:      -48 * time.Hour,
				Metafile: "test.json",
			},
		},
		"error: both meta and meta-file": {
			Input: workflow.Phase{
				Task:     "bad-batch",
				Rule:     "cron=15 35 13 * * *&meta=name:a,b&meta-file=test.json",
				Template: "?name={meta:name}&day={yyyy}-{mm}-{dd}",
			},
			ExpectedErr: errors.New("meta_file and meta can not be used at the same time"),
		},
		"batch by duration + by": {
			Input: workflow.Phase{
				Task:     "by-batch",
				Rule:     "cron=15 35 13 * * *&for=-48h&by=hour",
				Template: "?day={yyyy}-{mm}-{dd}",
			},
			Expected: &batchJob{
				Cronjob: Cronjob{
					Topic:    "by-batch",
					Schedule: "15 35 13 * * *",
					Template: "?day={yyyy}-{mm}-{dd}",
				},
				For: -48 * time.Hour,
				By:  "hour",
			},
		},
		"invalid rule": {
			Input: workflow.Phase{
				Task:     "invalid",
				Rule:     "%notvalid=%%%",
				Template: "?day={yyyy}-{mm}-{dd}",
			},
			ShouldErr: true,
		},
	}
	trial.New(fn, cases).Comparer(
		trial.EqualOpt(trial.IgnoreAllUnexported, trial.EquateEmpty)).SubTest(t)
}

func TestIsReady(t *testing.T) {
	type input struct {
		rule string
		meta string
	}
	fn := func(in input) (bool, error) {
		return isReady(in.rule, in.meta), nil
	}
	cases := trial.Cases[input, bool]{
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

func TestValidatePhase(t *testing.T) {
	fn := func(in workflow.Phase) (string, error) {
		s := validatePhase(in)
		if s != "" {
			return "", errors.New(s)
		}
		return s, nil
	}
	cases := trial.Cases[workflow.Phase, string]{
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
