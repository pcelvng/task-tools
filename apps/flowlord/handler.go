package main

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
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
	"github.com/jbsmith7741/uri"
	"github.com/pcelvng/task"

	tools "github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/apps/flowlord/cache"
	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task-tools/slack"
	"github.com/pcelvng/task-tools/tmpl"
	"github.com/pcelvng/task-tools/workflow"
)

//go:embed handler/alert.tmpl
var AlertTemplate string

//go:embed handler/files.tmpl
var FilesTemplate string

//go:embed handler/task.tmpl
var TaskTemplate string

//go:embed handler/header.tmpl
var HeaderTemplate string

//go:embed handler/about.tmpl
var AboutTemplate string

func (tm *taskMaster) StartHandler() {
	router := chi.NewRouter()
	
	// Static file serving
	router.Handle("/static/*", http.StripPrefix("/static/", http.FileServer(http.Dir("handler/static"))))
	
	router.Get("/", tm.Info)
	router.Get("/info", tm.Info)
	router.Get("/refresh", tm.refreshHandler)
	router.Post("/backload", tm.Backloader)
	router.Get("/workflow/*", tm.workflowFiles)
	router.Get("/workflow", tm.workflowFiles)
	router.Get("/notify", func(w http.ResponseWriter, r *http.Request) {
		sts := stats{
			AppName: "flowlord",
			Version: tools.Version,
			RunTime: gtools.PrintDuration(time.Since(tm.initTime)),
		}
		b, _ := json.Marshal(sts)
		if err := tm.slack.Notify(string(b), slack.OK); err != nil {
			w.Write([]byte(err.Error()))
		}
	})
	router.Get("/status", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})
	router.Get("/task/{id}", tm.taskHandler)
	router.Get("/recap", tm.recapHandler)
	router.Get("/web/alert", tm.htmlAlert)
	router.Get("/web/files", tm.htmlFiles)
	router.Get("/web/task", tm.htmlTask)
	router.Get("/web/about", tm.htmlAbout)

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
		j, ok := e.Job.(*Cronjob)
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

	if r.Header.Get("Accept") == "application/json" {
		b, err := json.Marshal(data)
		if err != nil {
			log.Println(err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(b)
		return
	}

	var s string
	for k, v := range data {
		s += k + "\n\t" + v.String()
	}
	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte(s))

}

func (tm *taskMaster) workflowFiles(w http.ResponseWriter, r *http.Request) {
	fName := chi.URLParam(r, "*")

	if strings.Contains(fName, "../") {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	pth := tm.path
	// support directory and single file for workflow path lookup.
	if tm.Cache.IsDir() {
		pth += "/" + fName
	}

	sts, err := file.Stat(pth, tm.fOpts)
	if err != nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	if sts.IsDir {
		w.WriteHeader(http.StatusOK)
		files, _ := file.List(pth, tm.fOpts)
		for _, f := range files {
			b, a, _ := strings.Cut(f.Path, tm.path)
			w.Write([]byte(b + a + "\n"))
		}
		return
	}
	reader, err := file.NewReader(pth, tm.fOpts)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(err.Error()))
		return
	}
	ext := strings.TrimLeft(filepath.Ext(fName), ".")
	switch ext {
	case "toml":
		w.Header().Set("Content-Type", "application/toml")
	case "json":
		w.Header().Set("Content-Type", "application/json")
	case "yaml", "yml":
		w.Header().Set("Context-Type", "text/x-yaml")
	}
	w.WriteHeader(http.StatusOK)
	b, _ := io.ReadAll(reader)
	w.Write(b)
}

func (tm *taskMaster) htmlAlert(w http.ResponseWriter, r *http.Request) {

	dt, _ := time.Parse("2006-01-02", r.URL.Query().Get("date"))
	if dt.IsZero() {
		dt = time.Now()
	}
	alerts, err := tm.taskCache.GetAlertsByDate(dt)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "text/html")
	w.Write(alertHTML(alerts, dt))
}

// htmlFiles handles GET /web/files - displays file messages for a specific date
func (tm *taskMaster) htmlFiles(w http.ResponseWriter, r *http.Request) {
	dt, _ := time.Parse("2006-01-02", r.URL.Query().Get("date"))
	if dt.IsZero() {
		dt = time.Now()
	}

	files, err := tm.taskCache.GetFileMessagesByDate(dt)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "text/html")
	w.Write(filesHTML(files, dt))
}

// htmlTask handles GET /web/task - displays task summary and table for a specific date
func (tm *taskMaster) htmlTask(w http.ResponseWriter, r *http.Request) {
	dt, _ := time.Parse("2006-01-02", r.URL.Query().Get("date"))
	if dt.IsZero() {
		dt = time.Now()
	}

	// Get filter parameters
	taskType := r.URL.Query().Get("type")
	job := r.URL.Query().Get("job")
	result := r.URL.Query().Get("result")

	// Get tasks with filters
	tasks, err := tm.taskCache.GetTasksByDate(dt, taskType, job, result)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "text/html")
	w.Write(taskHTML(tasks, dt, taskType, job, result))
}

// htmlAbout handles GET /web/about - displays system information and cache statistics
func (tm *taskMaster) htmlAbout(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "text/html")
	w.Write(tm.aboutHTML())
}

// filesHTML renders the file messages HTML page
func filesHTML(files []cache.FileMessage, date time.Time) []byte {
	// Calculate statistics
	totalFiles := len(files)
	matchedFiles := 0
	totalTasks := 0

	for _, file := range files {
		if len(file.TaskNames) > 0 {
			matchedFiles++
			totalTasks += len(file.TaskNames)
		}
	}

	unmatchedFiles := totalFiles - matchedFiles

	// Calculate navigation dates
	prevDate := date.AddDate(0, 0, -1)
	nextDate := date.AddDate(0, 0, 1)

	data := map[string]interface{}{
		"Date":           date.Format("Monday, January 2, 2006"),
		"DateValue":      date.Format("2006-01-02"),
		"PrevDate":       prevDate.Format("2006-01-02"),
		"NextDate":       nextDate.Format("2006-01-02"),
		"Files":          files,
		"TotalFiles":     totalFiles,
		"MatchedFiles":   matchedFiles,
		"UnmatchedFiles": unmatchedFiles,
		"TotalTasks":     totalTasks,
		"CurrentPage":    "files",
		"PageTitle":      "File Messages",
	}

	// Template functions
	funcMap := template.FuncMap{
		"formatBytes": func(bytes int64) string {
			const unit = 1024
			if bytes < unit {
				return fmt.Sprintf("%d B", bytes)
			}
			div, exp := int64(unit), 0
			for n := bytes / unit; n >= unit; n /= unit {
				div *= unit
				exp++
			}
			return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
		},
		"formatReceivedTime": func(t time.Time) string {
			return t.Format(time.RFC3339)
		},
		"formatTaskTime": func(t time.Time) string {
			return t.Format("2006-01-02T15")
		},
	}

	// Parse and execute template using the same pattern as alertHTML
	tmpl, err := template.New("files").Funcs(funcMap).Parse(HeaderTemplate + FilesTemplate)
	if err != nil {
		return []byte(err.Error())
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return []byte(err.Error())
	}

	return buf.Bytes()
}

// generateSummaryFromTasks creates a summary of tasks grouped by type:job
func generateSummaryFromTasks(tasks []cache.TaskView) map[string]*cache.Stats {
	summary := make(map[string]*cache.Stats)

	for _, t := range tasks {
		// Get job from TaskView.Job or extract from Meta
		job := t.Job
		if job == "" {
			if meta, err := url.ParseQuery(t.Meta); err == nil {
				job = meta.Get("job")
			}
		}

		// Create key in format "type:job"
		key := strings.TrimRight(t.Type+":"+job, ":")

		// Get or create stats for this type:job combination
		stat, found := summary[key]
		if !found {
			stat = &cache.Stats{
				CompletedTimes: make([]time.Time, 0),
				ErrorTimes:     make([]time.Time, 0),
				ExecTimes:      &cache.DurationStats{},
			}
			summary[key] = stat
		}

		// Convert TaskView to task.Task for processing
		taskTime := tmpl.TaskTime(task.Task{
			ID:      t.ID,
			Type:    t.Type,
			Job:     t.Job,
			Info:    t.Info,
			Result:  task.Result(t.Result),
			Meta:    t.Meta,
			Msg:     t.Msg,
			Created: t.Created,
			Started: t.Started,
			Ended:   t.Ended,
		})

		// Process based on result type
		if t.Result == "error" {
			stat.ErrorCount++
			stat.ErrorTimes = append(stat.ErrorTimes, taskTime)
		} else if t.Result == "complete" {
			stat.CompletedCount++
			stat.CompletedTimes = append(stat.CompletedTimes, taskTime)

			// Add execution time for completed tasks
			if t.Started != "" && t.Ended != "" {
				startTime, err1 := time.Parse(time.RFC3339, t.Started)
				endTime, err2 := time.Parse(time.RFC3339, t.Ended)
				if err1 == nil && err2 == nil {
					stat.ExecTimes.Add(endTime.Sub(startTime))
				}
			}
		}
		// Note: warn and alert results don't contribute to execution time stats
	}

	return summary
}

// taskHTML renders the task summary and table HTML page
func taskHTML(tasks []cache.TaskView, date time.Time, taskType, job, result string) []byte {
	// Calculate navigation dates
	prevDate := date.AddDate(0, 0, -1)
	nextDate := date.AddDate(0, 0, 1)

	// Generate summary from tasks data
	summary := generateSummaryFromTasks(tasks)

	// Calculate statistics
	totalTasks := len(tasks)
	completedTasks := 0
	errorTasks := 0
	alertTasks := 0
	warnTasks := 0
	runningTasks := 0

	for _, t := range tasks {
		switch t.Result {
		case "complete":
			completedTasks++
		case "error":
			errorTasks++
		case "alert":
			alertTasks++
		case "warn":
			warnTasks++
		case "":
			runningTasks++
		}
	}

	data := map[string]interface{}{
		"Date":           date.Format("Monday, January 2, 2006"),
		"DateValue":      date.Format("2006-01-02"),
		"PrevDate":       prevDate.Format("2006-01-02"),
		"NextDate":       nextDate.Format("2006-01-02"),
		"Tasks":          tasks,
		"Summary":        summary,
		"TotalTasks":     totalTasks,
		"CompletedTasks": completedTasks,
		"ErrorTasks":     errorTasks,
		"AlertTasks":     alertTasks,
		"WarnTasks":      warnTasks,
		"RunningTasks":   runningTasks,
		"CurrentType":    taskType,
		"CurrentJob":     job,
		"CurrentResult":  result,
		"CurrentPage":    "task",
		"PageTitle":      "Task Dashboard",
	}

	// Template functions
	funcMap := template.FuncMap{
		"formatTime": func(t time.Time) string {
			return t.Format("2006-01-02T15:04:05Z")
		},
		"formatDuration": func(start, end string) string {
			if start == "" || end == "" {
				return "N/A"
			}
			startTime, err1 := time.Parse(time.RFC3339, start)
			endTime, err2 := time.Parse(time.RFC3339, end)
			if err1 != nil || err2 != nil {
				return "N/A"
			}
			duration := endTime.Sub(startTime)
			return duration.String()
		},
		"getJobFromMeta": func(meta string) string {
			if meta == "" {
				return ""
			}
			if v, err := url.ParseQuery(meta); err == nil {
				return v.Get("job")
			}
			return ""
		},
		"add": func(a, b int) int {
			return a + b
		},
		"slice": func(s string, start, end int) string {
			if start >= len(s) {
				return ""
			}
			if end > len(s) {
				end = len(s)
			}
			return s[start:end]
		},
	}

	// Parse and execute template
	tmpl, err := template.New("task").Funcs(funcMap).Parse(HeaderTemplate + TaskTemplate)
	if err != nil {
		return []byte(err.Error())
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return []byte(err.Error())
	}

	return buf.Bytes()
}

// aboutHTML renders the about page HTML
func (tm *taskMaster) aboutHTML() []byte {
	// Get basic system information
	sts := stats{
		AppName:    "flowlord",
		Version:    tools.Version,
		RunTime:    gtools.PrintDuration(time.Since(tm.initTime)),
		NextUpdate: tm.nextUpdate.Format("2006-01-02T15:04:05"),
		LastUpdate: tm.lastUpdate.Format("2006-01-02T15:04:05"),
	}

	// Get database size information
	dbSize, err := tm.taskCache.GetDBSize()
	if err != nil {
		return []byte("Error getting database size: " + err.Error())
	}

	// Get table statistics
	tableStats, err := tm.taskCache.GetTableStats()
	if err != nil {
		return []byte("Error getting table statistics: " + err.Error())
	}

	// Create data structure for template
	data := map[string]interface{}{
		"AppName":     sts.AppName,
		"Version":     sts.Version,
		"RunTime":     sts.RunTime,
		"LastUpdate":  sts.LastUpdate,
		"NextUpdate":  sts.NextUpdate,
		"TotalDBSize": dbSize.TotalSize,
		"PageCount":   dbSize.PageCount,
		"PageSize":    dbSize.PageSize,
		"DBPath":      dbSize.DBPath,
		"TableStats":  tableStats,
		"CurrentPage": "about",
		"DateValue":   "", // About page doesn't need date
		"PageTitle":   "System Information",
	}

	// Parse and execute template
	tmpl, err := template.New("about").Parse(HeaderTemplate + AboutTemplate)
	if err != nil {
		return []byte("Error parsing template: " + err.Error())
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return []byte("Error executing template: " + err.Error())
	}

	return buf.Bytes()
}

// AlertData holds both the alerts and summary data for the template
type AlertData struct {
	Alerts  []cache.AlertRecord
	Summary []cache.SummaryLine
}

// alertHTML will take a list of task and display a html webpage that is easily to digest what is going on.
func alertHTML(tasks []cache.AlertRecord, date time.Time) []byte {
	// Generate summary data using BuildCompactSummary
	summary := cache.BuildCompactSummary(tasks)

	// Create data structure for template
	data := map[string]interface{}{
		"Alerts":      tasks,
		"Summary":     summary,
		"CurrentPage": "alert",
		"DateValue":   date.Format("2006-01-02"),
		"Date":        date.Format("Monday, January 2, 2006"),
		"PageTitle":   "Task Alerts",
	}

	tmpl, err := template.New("alert").Parse(HeaderTemplate + AlertTemplate)
	if err != nil {
		return []byte(err.Error())
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return []byte(err.Error())
	}

	return buf.Bytes()
}

type request struct {
	From string // start
	To   string // end
	At   string // single time

	Batch

	Execute bool
}

func (tm *taskMaster) Backloader(w http.ResponseWriter, r *http.Request) {
	req := request{
		Batch: Batch{
			Meta: make(Meta),
		},
	}
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
	if req.At == "" && req.From == "" && req.To == "" {
		msg = append(msg, "no time provided using today")
		at = time.Now()
	}
	if start.IsZero() && end.IsZero() && at.IsZero() {
		msg = append(msg, "invalid time format (to|from|at), using today")
		at = time.Now()
	}
	if !at.IsZero() {
		start = at
		end = at
	}

	workflowPath, phase := tm.Cache.Search(req.Task, req.Job)
	if workflowPath != "" {
		msg = append(msg, "phase found in "+workflowPath)
		req.Template = phase.Template
		req.Workflow = workflowPath
	}
	if req.Template == "" {
		name := req.Task
		if req.Job != "" {
			name = req.Task + ":" + req.Job
		}
		return response{Status: "no template found for " + name, code: http.StatusBadRequest}
	}
	rules := struct {
		MetaFile string              `uri:"meta-file"`
		Meta     map[string][]string `uri:"meta"`
	}{}

	if err := uri.UnmarshalQuery(phase.Rule, &rules); err != nil {
		return response{Status: "invalid rule found for " + req.Task, code: http.StatusBadRequest}
	}

	// If no meta/meta-file is provided use phase defaults
	if len(req.Meta) == 0 && req.Metafile == "" {
		req.Meta = rules.Meta
		req.Metafile = rules.MetaFile
	}

	if len(req.Meta) > 0 && req.Metafile != "" {
		return response{Status: "Unsupported: meta and meta-file both used, use one only", code: http.StatusBadRequest}
	}

	// Set default by value if not provided
	if req.By == "" {
		req.By = "day"
		msg = append(msg, "using default day iterator")
	}

	// Create Batch struct and use ExpandTasks

	tasks, err := req.Batch.Range(start, end, tm.fOpts)
	if err != nil {
		return response{Status: err.Error(), code: http.StatusBadRequest}
	}

	return response{Tasks: tasks, Count: len(tasks), Status: strings.Join(msg, ", ")}
}

func parseTime(s string) time.Time {
	t, err := time.Parse("2006-01-02", s)
	if err == nil {
		return t
	}
	t, err = time.Parse("2006-01-02T15", s)
	if err == nil {
		return t
	}
	t, _ = time.Parse(time.RFC3339, s)
	return t
}

type Meta map[string][]string

// UnmarshalJSON with the format of map[string]string and map[string][]string
func (m Meta) UnmarshalJSON(d []byte) error {
	if m == nil {
		return errors.New("assignment to nil map")
	}
	v := make(map[string]string)
	if err := json.Unmarshal(d, &v); err == nil {
		for k, v := range v {
			m[k] = []string{v}
		}
		return nil
	}

	m2 := (map[string][]string)(m)
	return json.Unmarshal(d, &m2)
}
