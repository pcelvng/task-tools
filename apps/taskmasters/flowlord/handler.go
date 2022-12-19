package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	gtools "github.com/jbsmith7741/go-tools"
	"github.com/pcelvng/task"

	tools "github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/tmpl"
	"github.com/pcelvng/task-tools/workflow"
)

func (tm *taskMaster) StartHandler() {
	router := chi.NewRouter()
	router.Get("/", tm.Info)
	router.Get("/info", tm.Info)
	router.Get("/refresh", tm.refreshHandler)
	router.Get("/backload", tm.Backloader)

	if tm.port == 0 {
		log.Println("flowlord router disabled")
		return
	}

	log.Printf("starting handler on :%v", tm.port)
	http.ListenAndServe(":"+strconv.Itoa(tm.port), router)
}

func (tm *taskMaster) Info(w http.ResponseWriter, r *http.Request) {
	sts := stats{
		AppName:    "flowlord",
		Version:    tools.Version,
		RunTime:    gtools.PrintDuration(time.Now().Sub(tm.initTime)),
		NextUpdate: tm.nextUpdate.Format("2006-01-02T15:04:05"),
		LastUpdate: tm.lastUpdate.Format("2006-01-02T15:04:05"),
		Workflow:   make(map[string]map[string]cEntry),
	}

	// create a copy of all workflows
	wCache := make(map[string]map[string]workflow.Phase) // [file][task:job]Phase
	for key, w := range tm.Cache.Workflows {
		phases := make(map[string]workflow.Phase)
		for _, j := range w.Phases {
			phases[pName(j.Topic(), j.Job())] = j
		}
		wCache[key] = phases
	}

	for _, e := range tm.cron.Entries() {
		j, ok := e.Job.(*job)
		if !ok {
			continue
		}
		ent := cEntry{
			Next:     &e.Next,
			Prev:     &e.Prev,
			Schedule: []string{j.Schedule + "?offset=" + gtools.PrintDuration(j.Offset)},
			Child:    make([]string, 0),
		}
		k := pName(j.Topic, j.Name)

		w, found := sts.Workflow[j.Workflow]
		if !found {
			w = make(map[string]cEntry)
			sts.Workflow[j.Workflow] = w
		}

		// check if for multi-scheduled entries
		if e, found := w[k]; found {
			if e.Prev.After(*ent.Prev) {
				ent.Prev = e.Prev // keep the last run time
			}
			if e.Next.Before(*ent.Next) {
				ent.Next = e.Next // keep the next run time
			}
			ent.Schedule = append(ent.Schedule, e.Schedule...)
		}
		// add children
		ent.Child = tm.getAllChildren(j.Topic, j.Workflow, j.Name)
		w[k] = ent

		// remove entries from wCache
		delete(wCache[j.Workflow], k)
		for _, child := range ent.Child {
			for _, v := range strings.Split(child, " ➞ ") {
				delete(wCache[j.Workflow], v)
			}
		}
	}

	// add files based tasks

	for _, f := range tm.files {
		wPath := f.workflowFile
		w, found := sts.Workflow[wPath]
		if !found {
			w = make(map[string]cEntry)
			sts.Workflow[wPath] = w
		}
		k := pName(f.Topic(), f.Job())
		ent := cEntry{
			Schedule: []string{f.SrcPattern},
			Child:    tm.getAllChildren(f.Topic(), f.workflowFile, f.Job()),
		}
		w[k] = ent

		// remove entries from wCache
		delete(wCache[f.workflowFile], k)
		for _, child := range ent.Child {
			for _, v := range strings.Split(child, " ➞ ") {
				delete(wCache[f.workflowFile], v)
			}
		}
	}

	// Add non cron based tasks
	for f, w := range wCache {
		for _, v := range w {
			k := pName(v.Topic(), v.Job())
			// check for parents
			for v.DependsOn != "" {
				if t, found := wCache[f][v.DependsOn]; found {
					k = v.DependsOn
					v = t
				} else {
					break
				}

			}

			children := tm.getAllChildren(v.Topic(), f, v.Job())
			// todo: remove children from Cache
			if _, found := sts.Workflow[f]; !found {
				sts.Workflow[f] = make(map[string]cEntry)
			}
			warning := validatePhase(v)
			if v.DependsOn != "" {
				warning += "parent task not found: " + v.DependsOn
			}
			sts.Workflow[f][k] = cEntry{
				Schedule: make([]string, 0),
				Warning:  warning,
				Child:    children,
			}
		}
	}

	w.Header().Add("Content-Type", "application/json")
	b, _ := json.MarshalIndent(sts, "", "  ")
	w.Write(b)
}

func (tm *taskMaster) refreshHandler(w http.ResponseWriter, _ *http.Request) {
	files, err := tm.refreshCache()
	w.Header().Add("Content-Type", "application/json")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	v := struct {
		Files   []string `json:",omitempty"`
		Updated time.Time
	}{
		Files:   files,
		Updated: tm.lastUpdate.UTC(),
	}
	b, _ := json.MarshalIndent(v, "", "  ")

	w.Write(b)
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
	b, _ := io.ReadAll(r.Body)
	json.Unmarshal(b, &req)

	tasks, _ := tm.backload(req)

	w.WriteHeader(http.StatusOK)
	w.Header().Add("Content-Type", "application/json")
	b, _ = json.Marshal(tasks)
	w.Write(b)

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
		//		req.Template = tm.Cache.Get()
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
