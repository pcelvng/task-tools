package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/pcelvng/task"
)

const maxImportUploadSize = 64 << 20 // 64 MB

type importResult struct {
	Status   string   `json:"status"`
	Lines    int      `json:"lines"`
	Imported int      `json:"imported"`
	Skipped  int      `json:"skipped"`
	Errors   []string `json:"errors,omitempty"`
}

func parseTasksJSONL(r io.Reader) ([]task.Task, []string) {
	var tasks []task.Task
	var errs []string

	scanner := bufio.NewScanner(r)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var t task.Task
		if err := json.Unmarshal([]byte(line), &t); err != nil {
			errs = append(errs, fmt.Sprintf("line %d: %v", lineNum, err))
			continue
		}
		tasks = append(tasks, t)
	}
	if err := scanner.Err(); err != nil {
		errs = append(errs, fmt.Sprintf("read error: %v", err))
	}

	return tasks, errs
}

func (tm *taskMaster) importTasks(tasks []task.Task) importResult {
	result := importResult{
		Status: "ok",
		Lines:  len(tasks),
	}

	for _, t := range tasks {
		if t.ID == "" {
			result.Skipped++
			continue
		}
		tm.taskCache.Add(t)
		result.Imported++
	}

	if err := tm.taskCache.Sync(); err != nil {
		result.Status = "sync failed: " + err.Error()
	}

	return result
}

func (tm *taskMaster) importHandler(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(maxImportUploadSize); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	file, _, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "missing file field", http.StatusBadRequest)
		return
	}
	defer file.Close()

	tasks, parseErrs := parseTasksJSONL(file)
	result := tm.importTasks(tasks)
	result.Errors = parseErrs

	switch {
	case len(parseErrs) > 0 && result.Imported > 0:
		result.Status = "partial"
	case len(parseErrs) > 0 && result.Imported == 0:
		result.Status = "failed"
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(result)
}
