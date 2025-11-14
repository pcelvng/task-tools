package main

import (
	"bytes"
	"embed"
	"encoding/json"
	"errors"
	"html/template"
	"io"
	"io/fs"
	"log"
	"net/http"
	"net/url"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	gtools "github.com/jbsmith7741/go-tools"
	"github.com/jbsmith7741/go-tools/appenderr"
	"github.com/jbsmith7741/uri"
	"github.com/pcelvng/task"

	tools "github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/apps/flowlord/cache"
	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task-tools/slack"
	"github.com/pcelvng/task-tools/tmpl"
)

//go:embed handler/alert.tmpl
var AlertTemplate string

//go:embed handler/files.tmpl
var FilesTemplate string

//go:embed handler/task.tmpl
var TaskTemplate string

//go:embed handler/workflow.tmpl
var WorkflowTemplate string

//go:embed handler/header.tmpl
var HeaderTemplate string

//go:embed handler/about.tmpl
var AboutTemplate string

//go:embed handler/static/*
var StaticFiles embed.FS

var isLocal = false

// getBaseFuncMap returns a template.FuncMap with all common template functions
func getBaseFuncMap() template.FuncMap {
	return template.FuncMap{
		// Time formatting functions
		"formatFullDate": func(t time.Time) string {
			return t.Format(time.RFC3339)
		},
		"formatTimeHour": func(t time.Time) string {
			return t.Format("2006-01-02T15")
		},
		// Duration formatting
		"formatDuration": gtools.PrintDuration,
		// Size formatting
		"formatBytes": func(bytes int64) string {
			if bytes < 0 {
				return "0 B"
			}
			return humanize.Bytes(uint64(bytes))
		}, 
		// String manipulation
		"slice": func(s string, start, end int) string {
			if start >= len(s) {
				return ""
			}
			if end > len(s) {
				end = len(s)
			}
			return s[start:end]
		},
		// Math functions
		"add": func(a, b int) int {
			return a + b
		},
	}
}

func (tm *taskMaster) StartHandler() {
	router := chi.NewRouter()

	// Enable gzip compression for all responses
	router.Use(middleware.Compress(5))

	// Static file serving - serve embedded static files
	// Create a sub-filesystem that strips the "handler/" prefix
	staticFS, err := fs.Sub(StaticFiles, "handler/static")
	if err != nil {
		log.Fatal("Failed to create static filesystem:", err)
	}
	router.Handle("/static/*", http.StripPrefix("/static/", http.FileServer(http.FS(staticFS))))

	router.Get("/", tm.htmlAbout)
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
	router.Get("/web/workflow", tm.htmlWorkflow)
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
	wCache := make(map[string]map[string]cache.Phase) // [file][task:job]Phase
	workflowFiles := tm.taskCache.GetWorkflowFiles()
	for _, filePath := range workflowFiles {
		phases, err := tm.taskCache.GetPhasesForWorkflow(filePath)
		if err != nil {
			continue
		}
		phaseMap := make(map[string]cache.Phase)
		for _, j := range phases {
			phaseMap[pName(j.Phase.Topic(), j.Phase.Job())] = j.Phase
		}
		wCache[filePath] = phaseMap
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
		for _, ph := range w {
			k := pName(ph.Topic(), ph.Job())
			// check for parents
			for ph.DependsOn != "" {
				if t, found := wCache[f][ph.DependsOn]; found {
					k = ph.DependsOn
					ph = t
				} else {
					break
				}

			}

			children := tm.getAllChildren(ph.Topic(), f, ph.Job())
			// todo: remove children from SQLite
			if _, found := sts.Workflow[f]; !found {
				sts.Workflow[f] = make(map[string]cEntry)
			}
			warning := ph.Validate()
			if ph.DependsOn != "" {
				warning += "parent task not found: " + ph.DependsOn
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
	v := tm.taskCache.GetTask(id)
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
	if tm.taskCache.IsDir() {
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

	// Get dates with alerts for calendar highlighting
	datesWithData, _ := tm.taskCache.DatesByType("alerts")

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "text/html")
	w.Write(alertHTML(alerts, dt, datesWithData))
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

	// Get dates with file messages for calendar highlighting
	datesWithData, _ := tm.taskCache.DatesByType("files")

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "text/html")
	w.Write(filesHTML(files, dt, datesWithData))
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
	
	// Get pagination parameters
	page := 1
	if pageStr := r.URL.Query().Get("page"); pageStr != "" {
		if p, err := strconv.Atoi(pageStr); err == nil && p > 0 {
			page = p
		}
	}


	// Get ALL tasks for the date (no filtering at all) to populate summary and dropdowns
	queryStart := time.Now() 
	allTasks, err := tm.taskCache.GetTasksByDate(dt, "", "", "")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	queryTime := time.Since(queryStart)
	
	// Get dates with tasks for calendar highlighting
	datesWithData, _ := tm.taskCache.DatesByType("tasks")

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "text/html")
	htmlBytes := taskHTML(allTasks, dt, taskType, job, result, datesWithData, page, queryTime)
	w.Write(htmlBytes)
}

// htmlWorkflow handles GET /web/workflow - displays workflow phases from database
func (tm *taskMaster) htmlWorkflow(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "text/html")
	w.Write(workflowHTML(tm.taskCache))
}

// htmlAbout handles GET /web/about - displays system information and cache statistics
func (tm *taskMaster) htmlAbout(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "text/html")
	w.Write(tm.aboutHTML())
}

// filesHTML renders the file messages HTML page
func filesHTML(files []cache.FileMessage, date time.Time, datesWithData []string) []byte {
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
		"isLocal":        isLocal,
		"DatesWithData":  datesWithData,
	}

	// Parse and execute template using the shared funcMap
	tmpl, err := template.New("files").Funcs(getBaseFuncMap()).Parse(HeaderTemplate + FilesTemplate)
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
const pageSize = 100 
// taskHTML renders the task summary and table HTML page
func taskHTML(allTasks []cache.TaskView, date time.Time, taskType, job, result string, datesWithData []string, page int, queryTime time.Duration) []byte {
	renderStart := time.Now()
	
	// Calculate navigation dates
	prevDate := date.AddDate(0, 0, -1)
	nextDate := date.AddDate(0, 0, 1)

	// Generate summary from ALL tasks (not filtered)
	summary := generateSummaryFromTasks(allTasks) //TODO: replace with taskCache.Recap() 

	// Calculate statistics and extract unique types/jobs from ALL tasks
	totalAllTasks := len(allTasks)
	completedTasks := 0
	errorTasks := 0
	alertTasks := 0
	warnTasks := 0
	runningTasks := 0
	
	// Extract unique types and jobs for filter dropdowns from ALL tasks
	uniqueTypes := make(map[string]struct{})
	uniqueJobs := make(map[string]map[string]struct{}) // type -> jobs

	for _, t := range allTasks {
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
		
		// Track unique types
		if t.Type != "" {
			uniqueTypes[t.Type] = struct{}{}
			
			// Track jobs per type
			if uniqueJobs[t.Type] == nil {
				uniqueJobs[t.Type] = make(map[string]struct{})
			}
			if t.Job != "" {
				uniqueJobs[t.Type][t.Job] = struct{}{}
			}
		}
	}
	
	// Convert to sorted slices for template
	types := make([]string, 0, len(uniqueTypes))
	for t := range uniqueTypes {
		types = append(types, t)
	}
	sort.Strings(types)
	
	// Convert jobs map to sorted slices
	jobsByType := make(map[string][]string)
	for typ, jobs := range uniqueJobs {
		jobList := make([]string, 0, len(jobs))
		for j := range jobs {
			jobList = append(jobList, j)
		}
		sort.Strings(jobList)
		jobsByType[typ] = jobList
	}
	
	// Filter tasks for display based on taskType, job, and result parameters
	filteredTasks := make([]cache.TaskView, 0, len(allTasks))
	for _, t := range allTasks {
		// Apply type filter if specified
		if taskType != "" && t.Type != taskType {
			continue
		}
		// Apply job filter if specified
		if job != "" && t.Job != job {
			continue
		}
		// Apply result filter if specified
		if result != "" {
			// Handle "running" as empty result
			if result == "running" {
				if t.Result != "" {
					continue
				}
			} else if t.Result != result {
				continue
			}
		}
		filteredTasks = append(filteredTasks, t)
	}
	
	// Calculate pagination based on filtered tasks
	totalFilteredTasks := len(filteredTasks)
	totalPages := (totalFilteredTasks + pageSize - 1) / pageSize
	if totalPages == 0 {
		totalPages = 1
	}
	if page > totalPages {
		page = totalPages
	}
	
	// Slice tasks for current page
	startIdx := (page - 1) * pageSize
	endIdx := startIdx + pageSize
	if endIdx > totalFilteredTasks {
		endIdx = totalFilteredTasks
	}
	if startIdx > totalFilteredTasks {
		startIdx = totalFilteredTasks
	}
	
	pagedTasks := filteredTasks[startIdx:endIdx]

	data := map[string]interface{}{
		"Date":           date.Format("Monday, January 2, 2006"),
		"DateValue":      date.Format("2006-01-02"),
		"PrevDate":       prevDate.Format("2006-01-02"),
		"NextDate":       nextDate.Format("2006-01-02"),
		"Tasks":          pagedTasks,        // Only show current page's tasks
		"Summary":        summary,
		"TotalTasks":     totalAllTasks,     // Total count of ALL tasks for the day (for summary)
		"CompletedTasks": completedTasks,    // Stats from ALL tasks
		"ErrorTasks":     errorTasks,
		"AlertTasks":     alertTasks,
		"WarnTasks":      warnTasks,
		"RunningTasks":   runningTasks,
		"CurrentType":    taskType,
		"CurrentJob":     job,
		"CurrentResult":  result,
		"CurrentPage":    "task",
		"PageTitle":      "Task Dashboard",
		"isLocal":        isLocal,
		"DatesWithData":  datesWithData,
		"UniqueTypes":    types,
		"JobsByType":     jobsByType,
		// Pagination info (based on filtered tasks)
		"Page":           page,
		"PageSize":       pageSize,
		"TotalPages":     totalPages,
		"StartIndex":     startIdx + 1,
		"EndIndex":       endIdx,
		"FilteredCount":  totalFilteredTasks,  // Number of tasks after filtering
	}

	// Parse and execute template using base funcMap
	tmpl, err := template.New("task").Funcs(getBaseFuncMap()).Parse(HeaderTemplate + TaskTemplate)
	if err != nil {
		return []byte(err.Error())
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return []byte(err.Error())
	}
	
	htmlSize := buf.Len()
	renderTime := time.Since(renderStart)
	
	// Single consolidated log with all metrics
	log.Printf("Task page: date=%s filters=[type=%q job=%q result=%q] tasks=%d filtered=%d page=%d/%d query=%v render=%v size=%.2fMB",
		date.Format("2006-01-02"), taskType, job, result,
		totalAllTasks, totalFilteredTasks, page, totalPages,
		queryTime, renderTime, float64(htmlSize)/(1024*1024))

	return buf.Bytes()
}

// workflowHTML renders the workflow phases HTML page
func workflowHTML(tCache *cache.SQLite) []byte {
	// Get all workflow files and their phases
	workflowFiles := tCache.GetWorkflowFiles()

	workflowFileSummary := make(map[string]int)
	allPhases := make([]cache.PhaseDB, 0)

	for _, filePath := range workflowFiles {
		phases, err := tCache.GetPhasesForWorkflow(filePath)
		if err != nil {
			continue
		}

		workflowFileSummary[filePath] = len(phases)
		allPhases = append(allPhases, phases...)
	}

	data := map[string]interface{}{
		"Phases":              allPhases,
		"WorkflowFileSummary": workflowFileSummary,
		"CurrentPage":         "workflow",
		"PageTitle":           "Workflow Dashboard",
		"isLocal":             isLocal,
		"DatesWithData":       []string{}, // Workflow page doesn't use date picker with highlights
	}

	// Parse and execute template using the shared funcMap
	tmpl, err := template.New("workflow").Funcs(getBaseFuncMap()).Parse(HeaderTemplate + WorkflowTemplate)
	if err != nil {
		return []byte("Error:"+ err.Error())
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return []byte("Error:" + err.Error())
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
		"AppName":           sts.AppName,
		"Version":           sts.Version,
		"RunTime":           sts.RunTime,
		"LastUpdate":        sts.LastUpdate,
		"NextUpdate":        sts.NextUpdate,
		"TotalDBSize":       dbSize.TotalSize,
		"PageCount":         dbSize.PageCount,
		"PageSize":          dbSize.PageSize,
		"DBPath":            dbSize.DBPath,
		"TableStats":        tableStats,
		"SchemaVersion":     tm.taskCache.GetSchemaVersion(),
		"Retention":         gtools.PrintDuration(tm.taskCache.Retention),
		"TaskTTL":           gtools.PrintDuration(tm.taskCache.TaskTTL),
		"MinFrequency":      gtools.PrintDuration(tm.slack.MinFrequency),
		"MaxFrequency":      gtools.PrintDuration(tm.slack.MaxFrequency),
		"CurrentFrequency":  gtools.PrintDuration(tm.slack.GetCurrentDuration()),
		"CurrentPage":       "about",
		"DateValue":         "", // About page doesn't need date
		"PageTitle":         "System Information",
		"isLocal":           isLocal,
		"DatesWithData":     []string{}, // About page doesn't use date picker with highlights
	}

	// Parse and execute template using the shared funcMap
	tmpl, err := template.New("about").Funcs(getBaseFuncMap()).Parse(HeaderTemplate + AboutTemplate)
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
func alertHTML(tasks []cache.AlertRecord, date time.Time, datesWithData []string) []byte {
	// Generate summary data using BuildCompactSummary
	summary := cache.BuildCompactSummary(tasks)

	// Create data structure for template
	data := map[string]interface{}{
		"Alerts":        tasks,
		"Summary":       summary,
		"CurrentPage":   "alert",
		"DateValue":     date.Format("2006-01-02"),
		"Date":          date.Format("Monday, January 2, 2006"),
		"PageTitle":     "Task Alerts",
		"isLocal":       isLocal,
		"DatesWithData": datesWithData,
	}

	// Parse and execute template using the shared funcMap
	tmpl, err := template.New("alert").Funcs(getBaseFuncMap()).Parse(HeaderTemplate + AlertTemplate)
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

	workflowPath, phase := tm.taskCache.Search(req.Task, req.Job)
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
