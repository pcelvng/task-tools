package main

import (
	"errors"
	"log"
	"net/url"
	"strings"
	"time"

	"github.com/jbsmith7741/uri"

	"github.com/pcelvng/task"
	"github.com/robfig/cron/v3"

	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task-tools/tmpl"
	"github.com/pcelvng/task-tools/workflow"
)

const DateHour = "2006-01-02T15"

type Cronjob struct {
	Name     string        `uri:"-"`
	Workflow string        `uri:"-"`
	Topic    string        `uri:"-"`
	Schedule string        `uri:"cron"`
	Offset   time.Duration `uri:"offset"`
	Template string        `uri:"-"`

	// inherited from tm
	sendFunc func(topic string, tsk *task.Task) error `uri:"-"`
	alerts   chan task.Task
}

func (j *Cronjob) Run() {
	tm := time.Now().Add(j.Offset)
	info := tmpl.Parse(j.Template, tm)
	tsk := task.New(j.Topic, info)
	tsk.Meta = "workflow=" + j.Workflow
	tsk.Meta += "&cron=" + tm.Format(DateHour)
	if j.Name != "" {
		tsk.Job = j.Name
		tsk.Meta += "&job=" + j.Name
	}

	if err := j.sendFunc(j.Topic, tsk); err != nil {
		tsk.Result = task.ErrResult
		tsk.Msg = err.Error()
		j.alerts <- *tsk
	}
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
			sendFunc: tm.taskCache.SendFunc(tm.producer),
			alerts:   tm.alerts,
		},
		fOpts: fOps,
	}

	u := url.URL{}
	u.RawQuery = ph.Rule
	if err := uri.Unmarshal(u.String(), bJob); err != nil {
		return nil, err
	}

	if fields := strings.Fields(bJob.Schedule); len(fields) == 5 {
		bJob.Schedule = "0 " + bJob.Schedule
	} else if len(fields) > 6 || len(fields) < 5 {
		return nil, errors.New("invalid schedule must be of pattern [second] minute day_of_month month day_of_week")
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
}

// Run a batchJob
func (b *batchJob) Run() {
	t := time.Now().Add(b.Offset).Truncate(time.Hour)
	tasks, err := b.Batch(t)
	if err != nil {
		log.Println(err)
		// TODO: Should this be different than a failed task?
		tsk := *task.New(b.Topic, b.Template)
		tsk.Job = b.Name
		tsk.Result = task.ErrResult
		tsk.Msg = err.Error()
		b.alerts <- tsk //notify flowlord of issues
		return
	}
	for _, t := range tasks {
		if err := b.sendFunc(t.Type, &t); err != nil {
			t.Result = task.ErrResult
			t.Msg = err.Error()
			b.alerts <- t //notify flowlord of issues
		}
	}
}

// Batch will create a range of jobs either by date or per line in a reference file
func (b *batchJob) Batch(t time.Time) ([]task.Task, error) {
	start := t
	if start.IsZero() {
		start = time.Now().Truncate(time.Hour)
	}
	end := start.Add(b.For)
	meta := b.Meta
	metafile := b.FilePath
	by := b.By
	if by == "" {
		by = "day"
	}
	batch := Batch{
		Template: b.Template,
		Topic:    b.Topic,
		Job:      b.Name,
		Workflow: b.Workflow,
		Start:    start,
		End:      end,
		By:       by,
		Meta:     meta,
		Metafile: metafile,
	}
	return batch.Batch(t, &b.fOpts)
}
