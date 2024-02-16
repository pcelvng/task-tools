package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"

	"github.com/jbsmith7741/uri"

	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task-tools/tmpl"
	"github.com/pcelvng/task-tools/workflow"
	"github.com/pcelvng/task/bus"
	"github.com/robfig/cron/v3"
)

type Cronjob struct {
	Name     string        `uri:"-"`
	Workflow string        `uri:"-"`
	Topic    string        `uri:"-"`
	Schedule string        `uri:"cron"`
	Offset   time.Duration `uri:"offset"`
	Template string        `uri:"-"`

	// inherited from tm
	producer bus.Producer `uri:"-"`
}

func (j *Cronjob) Run() {
	tm := time.Now().Add(j.Offset)
	info := tmpl.Parse(j.Template, tm)
	tsk := task.New(j.Topic, info)
	tsk.Meta = "workflow=" + j.Workflow
	tsk.Meta += "&cron=" + tm.Format(time.DateTime)
	if j.Name != "" {
		tsk.Job = j.Name
		tsk.Meta += "&job=" + j.Name
	}

	j.producer.Send(j.Topic, tsk.JSONBytes())
}

func (tm *taskMaster) NewJob(ph workflow.Phase, path string) (cron.Job, error) {
	fOps := file.Options{}
	if tm.fOpts != nil {
		fOps = *tm.fOpts
	}
	bJob := &batchJob{
		Cronjob: Cronjob{
			Name:     ph.Job(),
			Workflow: path,
			Topic:    ph.Topic(),
			//Schedule: pull from uri,
			Template: ph.Template,
			producer: tm.producer,
		},
		alerts: tm.alerts,
		fOpts:  fOps,
	}

	u := url.URL{}
	u.RawQuery = ph.Rule
	if err := uri.Unmarshal(u.String(), bJob); err != nil {
		return nil, err
	}

	// return Cronjob if not batch params
	if bJob.For == 0 && bJob.FilePath == "" && len(bJob.Meta) == 0 {
		return &bJob.Cronjob, nil
	}

	if bJob.FilePath != "" && len(bJob.Meta) > 0 {
		return nil, errors.New("meta_file and meta can not be used at the same time")
	}

	return bJob, nil
}

type batchJob struct {
	Cronjob
	For      time.Duration       `uri:"for"`
	By       string              `uri:"by"`
	Meta     map[string][]string `uri:"meta"`
	FilePath string              `uri:"meta-file"`
	fOpts    file.Options

	alerts chan task.Task
}

// Run a batchJob
func (b *batchJob) Run() {
	t := time.Now().Add(b.Offset).Truncate(time.Hour)
	tasks, err := b.Batch(t)
	if err != nil {
		log.Println(err)
		tsk := *task.New(b.Topic, b.Template)
		tsk.Job = b.Name
		tsk.Result = task.ErrResult
		tsk.Msg = err.Error()
		b.alerts <- tsk //notify flowlord of issues
		return
	}
	for _, t := range tasks {
		if err := b.producer.Send(t.Type, t.JSONBytes()); err != nil {
			t.Result = task.ErrResult
			t.Msg = err.Error()
			b.alerts <- t //notify flowlord of issues
		}
	}
}

// Batch will create a range of jobs either by date or per line in a reference file
func (b *batchJob) Batch(t time.Time) ([]task.Task, error) {
	var err error
	start := t
	if start.IsZero() {
		start = time.Now().Truncate(time.Hour)
	}

	end := start.Add(b.For)

	var data []tmpl.GetMap
	if len(b.Meta) != 0 {
		if data, err = createMeta(b.Meta); err != nil {
			return nil, err
		}
	}

	if b.FilePath != "" {
		reader, err := file.NewGlobReader(b.FilePath, &b.fOpts)
		if err != nil {
			return nil, fmt.Errorf("file %q error %w", b.FilePath, err)
		}
		scanner := file.NewScanner(reader)

		for scanner.Scan() {
			row := make(tmpl.GetMap)
			if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
				return nil, err
			}
			data = append(data, row)
		}
	}
	// handle `by` iterator
	var byIter func(time.Time) time.Time
	switch strings.ToLower(b.By) {
	case "hour", "hourly":
		byIter = func(t time.Time) time.Time { return t.Add(time.Hour) }
	case "month", "monthly":
		byIter = func(t time.Time) time.Time { return t.AddDate(0, 1, 0) }
	default:
		fallthrough
	case "day", "daily":
		byIter = func(t time.Time) time.Time { return t.AddDate(0, 0, 1) }
	}

	var reverseTasks bool
	if end.Before(start) {
		reverseTasks = true
		t := end
		end = start
		start = t
	}
	// create the batch tasks
	tasks := make([]task.Task, 0)
	for t := start; end.Sub(t) >= 0; t = byIter(t) {
		info := tmpl.Parse(b.Template, t)
		for _, d := range data { // meta data tasks
			tsk := *task.New(b.Topic, tmpl.Meta(info, d))

			tsk.Meta = "workflow=" + b.Workflow
			if b.Name != "" {
				tsk.Job = b.Name
				tsk.Meta += "&job=" + b.Name
			}
			tasks = append(tasks, tsk)
		}
		if len(data) == 0 { // time only tasks
			tsk := *task.New(b.Topic, info)
			tsk.Meta = "workflow=" + b.Workflow
			if b.Name != "" {
				tsk.Job = b.Name
				tsk.Meta += "&job=" + b.Name
			}
			tasks = append(tasks, tsk)
		}
	}
	if reverseTasks {
		tmp := make([]task.Task, len(tasks))
		for i := 0; i < len(tasks); i++ {
			tmp[i] = tasks[len(tasks)-i-1]
		}
		tasks = tmp
	}

	return tasks, nil
}

func createMeta(data map[string][]string) ([]tmpl.GetMap, error) {
	var result []tmpl.GetMap
	for k, vals := range data {

		if result == nil {
			result = make([]tmpl.GetMap, len(vals))
		}

		if len(vals) != len(result) {
			log.Println("inconsistent lengths")
			return nil, fmt.Errorf("inconsistent lengths in meta %d != %d", len(vals), len(result))
		}
		for i, v := range vals {
			if result[i] == nil {
				result[i] = make(tmpl.GetMap)
			}
			result[i][k] = v
		}
	}
	return result, nil
}
