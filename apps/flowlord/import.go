package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/pcelvng/task"

	"github.com/pcelvng/task-tools/file"
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

func validateImportPath(pth string) error {
	pth = strings.TrimSpace(pth)
	if pth == "" {
		return fmt.Errorf("empty path")
	}
	u, err := url.Parse(pth)
	if err != nil {
		return fmt.Errorf("invalid path: %w", err)
	}
	switch strings.ToLower(u.Scheme) {
	case "s3", "gs", "gcs", "mc", "mcs", "minio":
	default:
		return fmt.Errorf("import path must use s3://, gs://, mc://, or mcs://")
	}
	if u.Host == "" {
		return fmt.Errorf("import path missing bucket")
	}
	if strings.Contains(pth, `\`) {
		return fmt.Errorf("import path must not contain backslashes")
	}

	obj := strings.TrimPrefix(u.Path, "/")
	if obj == "" && !strings.ContainsAny(pth, "*?[]") {
		return fmt.Errorf("import path missing object key")
	}
	for _, seg := range strings.Split(obj, "/") {
		if seg == "." || seg == ".." {
			return fmt.Errorf("import path must not contain . or .. path segments")
		}
	}
	return nil
}

func (tm *taskMaster) importReader(path string) (io.ReadCloser, error) {
	if err := validateImportPath(path); err != nil {
		return nil, err
	}
	return file.NewGlobReader(strings.TrimSpace(path), tm.fOpts)
}

func (tm *taskMaster) importHandler(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(maxImportUploadSize); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var src io.ReadCloser
	if path := strings.TrimSpace(r.FormValue("path")); path != "" {
		var err error
		src, err = tm.importReader(path)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	} else {
		upload, _, err := r.FormFile("file")
		if err != nil {
			http.Error(w, "missing file or path", http.StatusBadRequest)
			return
		}
		src = upload
	}
	defer src.Close()

	tasks, parseErrs := parseTasksJSONL(src)
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
