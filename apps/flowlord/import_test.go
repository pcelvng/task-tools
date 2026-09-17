package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/hydronica/trial"
	"github.com/pcelvng/task"

	"github.com/pcelvng/task-tools/apps/flowlord/sqlite"
)

// GenerateSampleTasks returns task records suitable for JSONL import testing.
// Scenarios mirror test/seed_dev.sql: multi-phase pipeline, hourly reruns, mixed results.
func GenerateSampleTasks(now time.Time) []task.Task {
	now = now.UTC()
	day := now.Format("2006-01-02")
	ts := func(hour, min, sec int) string {
		return time.Date(now.Year(), now.Month(), now.Day(), hour, min, sec, 0, time.UTC).Format(time.RFC3339)
	}
	cron := func(hour int) string {
		return fmt.Sprintf("%sT%02d", day, hour)
	}

	return []task.Task{
		{
			ID: "seed-pipeline", Type: "task1", Job: "t2",
			Info: "?date=" + day + "&hour=" + day + "T11",
			Result: task.CompleteResult, Meta: "cron=" + cron(11), Msg: "ok",
			Created: ts(11, 0, 0), Started: ts(11, 0, 5), Ended: ts(11, 0, 30),
		},
		{
			ID: "seed-pipeline", Type: "task2",
			Info: "?date=" + day + "&hour=" + day + "T12",
			Result: task.AlertResult, Meta: "cron=" + cron(12), Msg: "downstream alert: missing file",
			Created: ts(12, 0, 0), Started: ts(12, 0, 2), Ended: ts(12, 0, 10),
		},
		{
			ID: "seed-pipeline", Type: "task3",
			Info: "?date=" + day + "&hour=" + day + "T12",
			Result: task.ErrResult, Meta: "cron=" + cron(12), Msg: "child failed after parent alert",
			Created: ts(12, 5, 0), Started: ts(12, 5, 1), Ended: ts(12, 5, 8),
		},
		{
			ID: "seed-hourly", Type: "task1", Job: "t4",
			Info: "?date=" + day + "&hour=" + day + "T10",
			Result: task.CompleteResult, Meta: "cron=" + cron(10), Msg: "hour 10 complete",
			Created: ts(10, 0, 0), Started: ts(10, 0, 1), Ended: ts(10, 0, 2),
		},
		{
			ID: "seed-hourly", Type: "task1", Job: "t4",
			Info: "?date=" + day + "&hour=" + day + "T13",
			Result: task.AlertResult, Meta: "cron=" + cron(13), Msg: "hour 13 quality check failed",
			Created: ts(13, 0, 0), Started: ts(13, 0, 1), Ended: ts(13, 0, 4),
		},
		{
			ID: "seed-long-id-abcdefghijklmnopqrstuvwxyz12345", Type: "task1", Job: "t2",
			Info: "?date=" + day + "&hour=" + day + "T14",
			Result: task.AlertResult, Meta: "cron=" + cron(14), Msg: "Validation failed: missing required field 'email'",
			Created: ts(14, 0, 0), Started: ts(14, 0, 1), Ended: ts(14, 0, 1),
		},
		{
			ID: "seed-running", Type: "task1", Job: "t2",
			Info: "?date=" + day,
			Meta: "cron=" + cron(15),
			Created: ts(15, 0, 0),
		},
	}
}

func tasksToJSONL(tasks []task.Task) string {
	var b strings.Builder
	for _, t := range tasks {
		line, err := json.Marshal(t)
		if err != nil {
			panic(err)
		}
		b.Write(line)
		b.WriteByte('\n')
	}
	return b.String()
}

func TestGenerateSampleTasksJSONL(t *testing.T) {
	tasks := GenerateSampleTasks(time.Now())
	content := tasksToJSONL(tasks)

	outPath := "test/sample_tasks.jsonl"
	if err := os.WriteFile(outPath, []byte(content), 0644); err != nil {
		t.Fatalf("write %s: %v", outPath, err)
	}
	t.Logf("Sample JSONL written to %s (%d tasks). Upload via About page to populate an empty database.", outPath, len(tasks))
}

func TestParseTasksJSONL(t *testing.T) {
	valid := task.Task{ID: "abc", Type: "sql", Created: "2024-01-15T10:00:00Z"}
	validLine, _ := json.Marshal(valid)

	input := string(validLine) + "\n\n{\"id\":\"def\",\"type\":\"x\"}\nnot json\n{\"type\":\"no-id\"}\n"
	tasks, errs := parseTasksJSONL(strings.NewReader(input))

	if len(tasks) != 3 {
		t.Fatalf("expected 3 parsed tasks, got %d", len(tasks))
	}
	if tasks[0].ID != "abc" || tasks[1].ID != "def" || tasks[2].Type != "no-id" {
		t.Fatalf("unexpected tasks: %+v", tasks)
	}
	if len(errs) != 1 {
		t.Fatalf("expected 1 parse error, got %d: %v", len(errs), errs)
	}
	if !strings.Contains(errs[0], "line 4") {
		t.Fatalf("expected line 4 error, got %q", errs[0])
	}
}

func TestImportTasks(t *testing.T) {
	db := &sqlite.SQLite{LocalPath: ":memory:"}
	if err := db.Open(testPath+"/workflow/", nil); err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	tm := &taskMaster{taskCache: db}

	type input struct {
		tasks []task.Task
		date  time.Time
	}
	type expect struct {
		result importResult
		count  int
	}

	fn := func(in input) (expect, error) {
		result := tm.importTasks(in.tasks)
		out := expect{result: result}
		if in.date.IsZero() {
			return out, nil
		}
		_, count, err := db.GetTasksByDate(in.date, &sqlite.TaskFilter{Page: 1, Limit: 100})
		if err != nil {
			return expect{}, err
		}
		out.count = count
		return out, nil
	}

	now := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)
	sampleTasks := GenerateSampleTasks(now)
	cases := trial.Cases[input, expect]{
		"sample tasks": {
			Input: input{tasks: sampleTasks, date: now},
			Expected: expect{
				result: importResult{
					Status:   "ok",
					Lines:    len(sampleTasks),
					Imported: len(sampleTasks),
				},
				count: len(sampleTasks),
			},
		},
		"skips missing id": {
			Input: input{tasks: []task.Task{
				{ID: "ok", Type: "t", Created: "2024-01-15T10:00:00Z"},
				{Type: "missing-id"},
			}},
			Expected: expect{
				result: importResult{
					Status:   "ok",
					Lines:    2,
					Imported: 1,
					Skipped:  1,
				},
			},
		},
	}
	trial.New(fn, cases).SubTest(t)
}

func TestImportHandler(t *testing.T) {
	db := &sqlite.SQLite{LocalPath: ":memory:"}
	if err := db.Open(testPath+"/workflow/", nil); err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	tm := &taskMaster{taskCache: db}

	type input struct {
		fileField bool
		content   string
	}
	type expect struct {
		status int
		result importResult
	}

	fn := func(in input) (expect, error) {
		body := &bytes.Buffer{}
		writer := multipart.NewWriter(body)
		if in.fileField {
			part, err := writer.CreateFormFile("file", "tasks.jsonl")
			if err != nil {
				return expect{}, err
			}
			if _, err := part.Write([]byte(in.content)); err != nil {
				return expect{}, err
			}
		} else if err := writer.WriteField("other", "value"); err != nil {
			return expect{}, err
		}
		if err := writer.Close(); err != nil {
			return expect{}, err
		}

		req := httptest.NewRequest(http.MethodPost, "/import", body)
		req.Header.Set("Content-Type", writer.FormDataContentType())
		rec := httptest.NewRecorder()
		tm.importHandler(rec, req)

		out := expect{status: rec.Code}
		if rec.Code != http.StatusOK {
			return out, nil
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &out.result); err != nil {
			return expect{}, err
		}
		return out, nil
	}

	uploadTasks := GenerateSampleTasks(time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC))
	cases := trial.Cases[input, expect]{
		"uploads jsonl": {
			Input: input{
				fileField: true,
				content:   tasksToJSONL(uploadTasks),
			},
			Expected: expect{
				status: http.StatusOK,
				result: importResult{
					Status:   "ok",
					Lines:    len(uploadTasks),
					Imported: len(uploadTasks),
				},
			},
		},
		"missing file field": {
			Input:  input{fileField: false},
			Expected: expect{status: http.StatusBadRequest},
		},
	}
	trial.New(fn, cases).SubTest(t)
}
