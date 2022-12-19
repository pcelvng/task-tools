package main

import (
	"encoding/json"
	"net/http"
	"net/url"
	"time"

	"github.com/pcelvng/task"

	"github.com/pcelvng/task-tools/tmpl"
)

//	"github.com/go-chi/chi/v5"

func Info(w http.ResponseWriter, r *http.Request) {
}

func Refresh(w http.ResponseWriter, r *http.Request) {
}

type request struct {
	Task string
	Job  string
	From string // start
	To   string // end
	At   string // single time //todo: do we need At or can we infer with from and to?
	By   string // month | day | hour // default by day,

	Meta     map[string]string
	Template string // should pull from workflow if possible
	Execute  bool
}

func (tm *taskMaster) Backloader(w http.ResponseWriter, r *http.Request) {
	req := request{}
	json.NewDecoder(r.Body).Decode(&req)

}

func (tm *taskMaster) backload(req request) ([]task.Task, error) {
	// handle start and end date
	at := parseTime(req.At)
	start := parseTime(req.From)
	end := parseTime(req.To)
	if start.IsZero() && end.IsZero() && at.IsZero() {
		at = time.Now()
	}
	if !at.IsZero() {
		start = at
		end = at
	}

	// handle `by` iterator
	var byIter func(time.Time) time.Time
	switch req.By {
	case "hour":
		byIter = func(t time.Time) time.Time { return t.Add(time.Hour) }
	case "month":
		byIter = func(t time.Time) time.Time { return t.AddDate(0, 1, 0) }
	default:
		fallthrough
	case "day":
		byIter = func(t time.Time) time.Time { return t.AddDate(0, 0, 1) }
	}

	// handle meta data
	if req.Meta == nil {
		req.Meta = make(map[string]string)
	}
	if w := tm.Cache.Search(req.Task, req.Job); w != "" {
		req.Meta["workflow"] = w
	}
	if req.Job != "" {
		req.Meta["job"] = req.Job
	}
	vals := toUrlValues(req.Meta)
	req.Template = tmpl.Meta(req.Template, vals)
	meta, _ := url.QueryUnescape(vals.Encode())

	tasks := make([]task.Task, 0)
	for t := start; end.Sub(t) >= 0; t = byIter(t) {
		tsk := *task.New(req.Task, tmpl.Parse(req.Template, t))
		tsk.Meta = meta
		tasks = append(tasks, tsk)
	}
	return tasks, nil
}

func toUrlValues(m map[string]string) url.Values {
	u := make(url.Values)
	for k, v := range m {
		u[k] = []string{v}
	}
	return u
}

func parseTime(s string) time.Time {
	t, err := time.Parse("2006-01-02", s)
	if err == nil {
		return t
	}
	t, _ = time.Parse("2006-01-02T15", s)
	return t

}
