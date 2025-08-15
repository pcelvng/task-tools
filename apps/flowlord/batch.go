package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/pcelvng/task"

	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task-tools/tmpl"
)

// Batch defines the parameters for batch task expansion.
type Batch struct {
	Template string
	Task     string
	Job      string
	Workflow string

	By       string `uri:"by"` // month | day | hour // default by day,
	Meta     Meta   `json:"meta"`
	Metafile string `json:"meta-file"`
}

// For creates a number of tasks based on the start time and ranges through the specified duration.
func (b *Batch) For(start time.Time, For time.Duration, fOpts *file.Options) ([]task.Task, error) {
	end := start.Add(For)
	return b.Range(start, end, fOpts)
}

// At creates tasks for the specified time only
func (b *Batch) At(t time.Time, fOpts *file.Options) ([]task.Task, error) {
	return b.Range(t, t, fOpts)
}

// Range creates a number of tasks based on the start and end dates and meta combinations.
func (b *Batch) Range(start, end time.Time, fOpts *file.Options) ([]task.Task, error) {
	var data []tmpl.GetMap
	var err error
	if len(b.Meta) != 0 {
		data, err = createMeta(b.Meta)
		if err != nil {
			return nil, err
		}
	}
	if b.Metafile != "" {
		reader, err := file.NewGlobReader(b.Metafile, fOpts)
		if err != nil {
			return nil, fmt.Errorf("file %q error %w", b.Metafile, err)
		}
		scanner := file.NewScanner(reader)
		for scanner.Scan() {
			row := make(tmpl.GetMap)
			if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
				return nil, err
			}
			data = append(data, row)
		}
		// empty file with no date range when file is expected
		if len(data) == 0 && start.Equal(end) {
			return nil, nil
		}
	}

	// handle `by` iterator
	var byIter func(time.Time) time.Time
	switch strings.ToLower(b.By) {
	case "hour", "hourly":
		byIter = func(t time.Time) time.Time { return t.Add(time.Hour) }
	case "week", "weekly":
		byIter = func(t time.Time) time.Time { return t.AddDate(0, 0, 7) }
	case "month", "monthly":
		byIter = func(t time.Time) time.Time { return t.AddDate(0, 1, 0) }
	default:
		byIter = func(t time.Time) time.Time { return t.AddDate(0, 0, 1) } // day
	}
	var reverseTasks bool
	if end.Before(start) {
		reverseTasks = true
		t := end
		end = start
		start = t
	}
	tasks := make([]task.Task, 0)
	for t := start; !t.After(end); t = byIter(t) {
		info := tmpl.Parse(b.Template, t)
		tskMeta := make(url.Values)
		tskMeta.Set("cron", t.Format("2006-01-02T15"))
		if b.Workflow != "" {
			tskMeta.Set("workflow", b.Workflow)
		}
		if b.Job != "" {
			tskMeta.Set("job", b.Job)
		}
		for _, d := range data { // meta data tasks
			i, keys := tmpl.Meta(info, d)
			tsk := *task.New(b.Task, i)
			tsk.Job = b.Job
			for _, k := range keys {
				tskMeta.Set(k, d.Get(k))
			}
			tsk.Meta, _ = url.QueryUnescape(tskMeta.Encode())
			tasks = append(tasks, tsk)
		}
		if len(data) == 0 { // time only tasks
			tsk := *task.New(b.Task, info)
			tsk.Job = b.Job
			tsk.Meta, _ = url.QueryUnescape(tskMeta.Encode())
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

// createMeta expands a map of meta values into a slice of GetMap for templating.
func createMeta(data map[string][]string) ([]tmpl.GetMap, error) {
	var result []tmpl.GetMap
	for k, vals := range data {
		if result == nil {
			result = make([]tmpl.GetMap, len(vals))
		}
		if len(vals) != len(result) {
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
