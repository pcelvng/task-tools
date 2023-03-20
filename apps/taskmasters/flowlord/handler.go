package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	gtools "github.com/jbsmith7741/go-tools"
	"github.com/jbsmith7741/go-tools/appenderr"
	"github.com/pcelvng/task"

	tools "github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task-tools/tmpl"
	"github.com/pcelvng/task-tools/workflow"
)

func (tm *taskMaster) StartHandler() {
	router := chi.NewRouter()
	router.Get("/", tm.Info)
	router.Get("/info", tm.Info)
	router.Get("/refresh", tm.refreshHandler)
	router.Post("/backload", tm.Backloader)
	router.Get("/workflow/*", tm.workflowFiles)
	router.Get("/task/{id}", tm.taskHandler)
	router.Get("/task", tm.recapHandler)

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
		RunTime:    gtools.PrintDuration(time.Since(tm.initTime)),
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
	entries := tm.cron.Entries()
	for i := 0; i < len(entries); i++ {
		e := entries[i]
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
		http.Error(w, err.Error(), http.StatusBadRequest)
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

func (tm *taskMaster) taskHandler(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	v := tm.taskCache.Get(id)
	b, _ := json.Marshal(v)
	w.Header().Add("Content-Type", "application/json")
	w.Write(b)
}

func (tm *taskMaster) recapHandler(w http.ResponseWriter, r *http.Request) {
	data := tm.taskCache.Recap()
	b, err := json.Marshal(data)
	if err != nil {
		log.Println(err)
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(b)
}

func (tm *taskMaster) workflowFiles(w http.ResponseWriter, r *http.Request) {
	f := chi.URLParam(r, "*")

	if strings.Contains(f, "../") {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	pth := tm.path + "/" + f

	sts, err := file.Stat(pth, tm.fOpts)
	if err != nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	if sts.IsDir {
		files, _ := file.List(pth, tm.fOpts)
		for _, f := range files {
			b, a, _ := strings.Cut(f.Path, tm.path)
			w.Write([]byte(b + a + "\n"))
		}
		w.WriteHeader(http.StatusOK)
		return
	}
	reader, err := file.NewReader(pth, tm.fOpts)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(err.Error()))
		return
	}
	ext := strings.TrimLeft(filepath.Ext(f), ".")
	switch ext {
	case "toml":
		w.Header().Set("Content-Type", "application/toml")
	case "json":
		w.Header().Set("Content-Type", "application/json")
	case "yaml", "yml":
		w.Header().Set("Context-Type", "text/x-yaml")
	}
	b, _ := io.ReadAll(reader)
	w.WriteHeader(http.StatusOK)
	w.Write(b)
}

type request struct {
	Task string
	Job  string
	From string // start
	To   string // end
	At   string // single time
	By   string // month | day | hour // default by day,

	Meta     map[string]string
	Template string // should pull from workflow if possible
	Execute  bool
}

func (tm *taskMaster) Backloader(w http.ResponseWriter, r *http.Request) {
	req := request{}
	b, _ := io.ReadAll(r.Body)
	if err := json.Unmarshal(b, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp := tm.backload(req)
	if resp.code >= 400 {
		http.Error(w, resp.Status, resp.code)
		return
	}

	if req.Execute {
		resp.Status = "Executed: " + resp.Status
		errs := appenderr.New()
		for _, t := range resp.Tasks {
			tm.taskCache.Add(t)
			errs.Add(tm.producer.Send(t.Type, t.JSONBytes()))
		}
		if errs.ErrOrNil() != nil {
			http.Error(w, "issue writing to producer "+errs.Error(), http.StatusInternalServerError)
		}
	} else {
		resp.Status = "DRY RUN ONLY: " + resp.Status
	}

	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	b, _ = json.Marshal(resp)
	w.Write(b)

}

type response struct {
	Status string
	Count  int
	Tasks  []task.Task

	code int
}

func (tm *taskMaster) backload(req request) response {
	// handle start and end date
	at := parseTime(req.At)
	start := parseTime(req.From)
	end := parseTime(req.To)
	msg := make([]string, 0)
	if start.IsZero() && !end.IsZero() {
		start = end
		msg = append(msg, "from value not set")
	}
	if !start.IsZero() && end.IsZero() {
		end = start
		msg = append(msg, "to value not set")
	}
	if start.IsZero() && end.IsZero() && at.IsZero() {
		msg = append(msg, "no time provided using today")
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
		msg = append(msg, "using default day iterator")
		fallthrough
	case "day":
		byIter = func(t time.Time) time.Time { return t.AddDate(0, 0, 1) }
	}

	// handle meta data
	if req.Meta == nil {
		req.Meta = make(map[string]string)
	}
	if w, template := tm.Cache.Search(req.Task, req.Job); w != "" {
		msg = append(msg, "phase found in "+w)
		req.Meta["workflow"] = w
		req.Template = template
	}
	if req.Template == "" {
		return response{Status: "no template found for " + req.Task, code: http.StatusBadRequest}
	}

	if req.Job != "" {
		req.Meta["job"] = req.Job
	}

	vals := toUrlValues(req.Meta)
	req.Template = tmpl.Meta(req.Template, vals)
	meta, _ := url.QueryUnescape(vals.Encode())

	tasks := make([]task.Task, 0)

	// reverse task order when end time comes before start
	var reverseTasks bool
	if end.Before(start) {
		reverseTasks = true
		t := end
		end = start
		start = t
	}

	for t := start; end.Sub(t) >= 0; t = byIter(t) {
		tsk := *task.New(req.Task, tmpl.Parse(req.Template, t))
		tsk.Meta = meta
		tasks = append(tasks, tsk)
	}

	if reverseTasks {
		tmp := make([]task.Task, len(tasks))
		for i := 0; i < len(tasks); i++ {
			tmp[i] = tasks[len(tasks)-i-1]
		}
		tasks = tmp
	}

	return response{Tasks: tasks, Count: len(tasks), Status: strings.Join(msg, ", ")}
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
