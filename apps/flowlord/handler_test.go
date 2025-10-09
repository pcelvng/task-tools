package main

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/hydronica/trial"
	"github.com/pcelvng/task"

	"github.com/pcelvng/task-tools/apps/flowlord/cache"
)

const testPath = "../../internal/test"

func TestMain(t *testing.M) {
		staticPath = "./static"
		t.Run() 
		os.Remove(":memory") 
}


// loadTaskViewData loads TaskView data from a JSON file
func loadTaskViewData(filename string) ([]cache.TaskView, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	var tasks []cache.TaskView
	err = json.Unmarshal(data, &tasks)
	if err != nil {
		return nil, err
	}

	return tasks, nil
}

func TestBackloader(t *testing.T) {
	sqlDB := &cache.SQLite{LocalPath: ":memory"}
	err := sqlDB.Open(testPath+"/workflow/f3.toml", nil )
	//cache, err := workflow.New(testPath+"/workflow/f3.toml", nil)
	today := time.Now().Format("2006-01-02")
	toHour := time.Now().Format(DateHour)
	if err != nil {
		t.Fatal(err)
	}
	tm := &taskMaster{
		taskCache: sqlDB,
	}
	fn := func(req request) (response, error) {

		resp := tm.backload(req)

		if resp.code >= 400 {
			return response{}, errors.New(resp.Status)
		}

		// only keep the first and last for long lists
		if resp.Count > 2 {
			resp.Tasks = []task.Task{resp.Tasks[0], resp.Tasks[resp.Count-1]}
		}
		return resp, nil
	}
	cases := trial.Cases[request, response]{
		"now": {
			Input: request{
				Batch: Batch{
					Task:     "sql",
					Job:      "load",
					Template: "./file.txt?ts={YYYY}-{MM}-{DD}",
				},
			},
			Expected: response{
				Tasks: []task.Task{
					{Type: "sql", Job: "load", Meta: "cron=" + toHour + "&job=load", Info: "./file.txt?ts=" + today},
				},
				Count: 1,
			},
		},
		"from_cache": {
			Input: request{
				Batch: Batch{
					Task: "task1",
				},
				At: "2022-06-12",
				// Template: "?date={yyyy}-{mm}-{dd}" // from f3.toml file
			},
			Expected: response{
				Count: 1,
				Tasks: []task.Task{{Type: "task1", Info: "?date=2022-06-12", Meta: "cron=2022-06-12T00&workflow=f3.toml"}},
			},
		},
		"hourly": {
			Input: request{
				Batch: Batch{
					Task:     "hourly",
					Template: "?day={YYYY}-{MM}-{DD}T{HH}",
					By:       "hour",
				},
				From: "2020-01-01T00",
				To:   "2020-01-02T23",
			},
			Expected: response{
				Tasks: []task.Task{
					{Type: "hourly", Info: "?day=2020-01-01T00", Meta: "cron=2020-01-01T00"}, // first
					{Type: "hourly", Info: "?day=2020-01-02T23", Meta: "cron=2020-01-02T23"}, // last
				},
				Count: 48,
			},
		},
		"daily": {
			Input: request{
				Batch: Batch{
					Task:     "daily",
					Template: "?date={YYYY}-{MM}-{DD}",
					By:       "day",
				},
				From: "2020-01-01",
				To:   "2020-02-01",
			},
			Expected: response{
				Tasks: []task.Task{
					{Type: "daily", Info: "?date=2020-01-01", Meta: "cron=2020-01-01T00"},
					{Type: "daily", Info: "?date=2020-02-01", Meta: "cron=2020-02-01T00"},
				},
				Count: 32,
			},
		},
		"monthly": {
			Input: request{
				Batch: Batch{
					Task:     "month",
					Template: "?table=exp.tbl_{YYYY}_{MM}",
					By:       "month",
				},
				From: "2020-01-01",
				To:   "2020-12-12",
			},
			Expected: response{
				Tasks: []task.Task{
					{Type: "month", Info: "?table=exp.tbl_2020_01", Meta: "cron=2020-01-01T00"},
					{Type: "month", Info: "?table=exp.tbl_2020_12", Meta: "cron=2020-12-01T00"},
				},
				Count: 12,
			},
		},
		"weekly": {
			Input: request{
				Batch: Batch{
					Task:     "week",
					Template: "?date={YYYY}-{MM}-{DD}",
					By:       "week",
				},
				From: "2020-01-01",
				To:   "2020-02-01",
			},
			Expected: response{
				Tasks: []task.Task{
					{Type: "week", Info: "?date=2020-01-01", Meta: "cron=2020-01-01T00"},
					{Type: "week", Info: "?date=2020-01-29", Meta: "cron=2020-01-29T00"},
				},
				Count: 5,
			},
		},
		"meta_template": {
			Input: request{
				Batch: Batch{
					Task:     "meta",
					Template: "{meta:file}?date={YYYY}-{mm}-{dd}&value={meta:value}",
					Meta:     Meta{"file": {"s3://task-bucket/data/f.txt"}, "value": {"apple"}},
				},
				At: "2020-02-20",
			},
			Expected: response{
				Tasks: []task.Task{
					{Type: "meta", Info: "s3://task-bucket/data/f.txt?date=2020-02-20&value=apple", Meta: "cron=2020-02-20T00&file=s3://task-bucket/data/f.txt&value=apple"},
				},
				Count: 1,
			},
		},
		"phase_not_found": {
			Input: request{
				Batch: Batch{
					Task: "unknown",
				},
			},
			ShouldErr: true,
		},
		"invalid_time": {
			Input: request{
				Batch: Batch{
					Task: "task1",
				},
				At: "2022-120-01",
			},
			Expected: response{
				Count: 1,
				Tasks: []task.Task{
					{Type: "task1", Info: time.Now().Format("?date=2006-01-02"), Meta: "cron=" + toHour + "&workflow=f3.toml"},
				}},
		},
		"to only": {
			Input: request{
				Batch: Batch{
					Task: "task1",
				},
				To: "2022-12-01",
			},
			Expected: response{
				Count: 1,
				Tasks: []task.Task{
					{Type: "task1", Info: "?date=2022-12-01", Meta: "cron=2022-12-01T00&workflow=f3.toml"},
				}},
		},
		"from only": {
			Input: request{
				Batch: Batch{
					Task: "task1",
				},
				From: "2022-12-01",
			},
			Expected: response{
				Count: 1,
				Tasks: []task.Task{
					{Type: "task1", Info: "?date=2022-12-01", Meta: "cron=2022-12-01T00&workflow=f3.toml"},
				},
			},
		},
		"backwards": {
			Input: request{
				Batch: Batch{
					Task:     "month",
					Template: "?table=exp.tbl_{YYYY}_{MM}",
					By:       "month",
				},
				From: "2021-01-01",
				To:   "2020-10-01",
			},
			Expected: response{
				Tasks: []task.Task{
					{Type: "month", Info: "?table=exp.tbl_2021_01", Meta: "cron=2021-01-01T00"},
					{Type: "month", Info: "?table=exp.tbl_2020_10", Meta: "cron=2020-10-01T00"},
				},
				Count: 4,
			},
		},
		"meta-file": {
			Input: request{
				Batch: Batch{
					Task:     "mfile",
					Template: "?president={meta:name}&start={meta:start}&end={meta:end}",
					Metafile: "./test/presidents.json",
				},
			},
			Expected: response{
				Tasks: []task.Task{
					{
						Type: "mfile", Info: "?president=george washington&start=1789&end=1797",
						Meta: "cron=" + toHour + "&end=1797&name=george washington&start=1789"},
					{
						Type: "mfile",
						Info: "?president=james madison&start=1809&end=1817",
						Meta: "cron=" + toHour + "&end=1817&name=james madison&start=1809",
					},
				},
				Count: 4,
			},
		},
		"meta-default": {
			Input: request{
				Batch: Batch{
					Task: "batch-president",
				},
			},
			Expected: response{
				Tasks: []task.Task{
					{
						Type: "batch-president", Info: "?president=george washington&start=1789&end=1797",
						Meta: "cron=" + toHour + "&end=1797&name=george washington&start=1789&workflow=f3.toml"},
					{
						Type: "batch-president",
						Info: "?president=james madison&start=1809&end=1817",
						Meta: "cron=" + toHour + "&end=1817&name=james madison&start=1809&workflow=f3.toml",
					},
				},
				Count: 4,
			},
		},
		"override-file": {
			Input: request{
				Batch: Batch{
					Task:     "b-meta",
					Metafile: "test/kv.json",
				},
			},
			Expected: response{
				Tasks: []task.Task{
					{Type: "b-meta", Info: "?key=fruit&val=apple", Meta: "cron=" + toHour + "&key=fruit&val=apple&workflow=f3.toml"},
					{Type: "b-meta", Info: "?key=animal&val=dog", Meta: "cron=" + toHour + "&key=animal&val=dog&workflow=f3.toml"},
				},
				Count: 2,
			},
		},
		"override-meta": {
			Input: request{
				Batch: Batch{
					Task: "batch-president",
					Meta: Meta{"name": {"bob", "albert"}, "start": {"1111", "1120"}, "end": {"1120", "1130"}},
				},
			},
			Expected: response{
				Tasks: []task.Task{
					{
						Type: "batch-president",
						Info: "?president=bob&start=1111&end=1120",
						Meta: "cron=" + toHour + "&end=1120&name=bob&start=1111&workflow=f3.toml",
					},
					{
						Type: "batch-president",
						Info: "?president=albert&start=1120&end=1130",
						Meta: "cron=" + toHour + "&end=1130&name=albert&start=1120&workflow=f3.toml",
					},
				},
				Count: 2,
			},
		},
	}
	trial.New(fn, cases).Comparer(trial.EqualOpt(
		trial.IgnoreAllUnexported,
		trial.IgnoreFields("Status"),
		ignoreTask,
	)).SubTest(t)
}

func TestMeta_UnmarshalJSON(t *testing.T) {
	fn := func(d string) (Meta, error) {
		m := make(Meta)
		err := m.UnmarshalJSON([]byte(d))
		return m, err
	}
	cases := trial.Cases[string, Meta]{
		"map_string": {
			Input:    `{"key":"value","k2":"v2"}`,
			Expected: Meta{"key": []string{"value"}, "k2": []string{"v2"}},
		},
		"map_slice": {
			Input:    `{"key":["v1","v2"],"k2":["v3","v4"]}`,
			Expected: Meta{"key": []string{"v1", "v2"}, "k2": []string{"v3", "v4"}},
		},
		"mixed": {
			Input:     `{"key":["1"], "k2":"v"}`,
			ShouldErr: true,
		},
	}
	trial.New(fn, cases).SubTest(t)
}

// TestWebAlertPreview generates an HTML preview of the alert template for visual inspection
// this provides an html file
func TestAlertHTML(t *testing.T) {

	// Create sample alert data to showcase the templating
	sampleAlerts := []cache.AlertRecord{
		{
			TaskID:    "task-001",
			TaskTime:  trial.TimeHour("2024-01-15T11"),
			Type:      "data-validation",
			Job:       "quality-check",
			Msg:       "Validation failed: missing required field 'email'",
			CreatedAt: trial.Time(time.RFC3339, "2024-01-15T11:15:00Z"),
		},
		{
			TaskID:    "task-002",
			TaskTime:  trial.TimeHour("2024-01-15T12"),
			Type:      "data-validation",
			Job:       "quality-check",
			Msg:       "Validation failed: missing required field 'email'",
			CreatedAt: trial.Time(time.RFC3339, "2024-01-15T12:15:00Z"),
		},
		{
			TaskID:    "task-003",
			TaskTime:  trial.TimeHour("2024-01-15T11"),
			Type:      "file-transfer",
			Job:       "backup",
			Msg:       "File transfer completed: 1.2GB transferred",
			CreatedAt: trial.Time(time.RFC3339, "2024-01-15T12:00:00Z"),
		},
		{
			TaskID:    "task-004",
			TaskTime:  trial.TimeHour("2024-01-15T13"),
			Type:      "database-sync",
			Job:       "replication",
			Msg:       "Database sync failed: connection timeout",
			CreatedAt: trial.Time(time.RFC3339, "2024-01-15T13:30:00Z"),
		},
		{
			TaskID:    "task-005",
			TaskTime:  trial.TimeHour("2024-01-15T13"),
			Type:      "notification",
			Job:       "email-alert",
			Msg:       "Email notification sent to 150 users",
			CreatedAt: trial.Time(time.RFC3339, "2024-01-15T14:00:00Z"),
		},
	}

	// Generate HTML using the alertHTML function
	htmlContent := alertHTML(sampleAlerts, trial.TimeDay("2024-01-15"))

	// Write HTML to a file for easy viewing
	outputFile := "handler/alert_preview.html"
	err := os.WriteFile(outputFile, htmlContent, 0644)
	if err != nil {
		t.Fatalf("Failed to write HTML file: %v", err)
	}

	t.Logf("Alert preview generated and saved to: ./%s", outputFile)

	// Basic validation that HTML was generated
	if len(htmlContent) == 0 {
		t.Error("Generated HTML content is empty")
	}

}


// TestFilesHTML generate a html file based on the files.tmpl it is used for vision examination of the files
func TestFilesHTML(t *testing.T) {
	// Create sample file messages
	files := []cache.FileMessage{
		{
			ID:           1,
			Path:         "gs://bucket/data/2024-01-15/file1.json",
			Size:         1024,
			LastModified: time.Now().Add(-1 * time.Hour),
			ReceivedAt:   time.Now().Add(-30 * time.Minute),
			TaskTime:     time.Now().Add(-1 * time.Hour),
			TaskIDs:      []string{"task-1", "task-2"},
			TaskNames:    []string{"data-load:import", "transform:clean"},
		},
		{
			ID:           2,
			Path:         "gs://bucket/data/2024-01-15/file2.csv",
			Size:         2048,
			LastModified: time.Now().Add(-2 * time.Hour),
			ReceivedAt:   time.Now().Add(-15 * time.Minute),
			TaskTime:     time.Now().Add(-2 * time.Hour),
			TaskIDs:      []string{},
			TaskNames:    []string{},
		},
	}

	date := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	html := filesHTML(files, date)

		// Write HTML to a file for easy viewing
		outputFile := "handler/files_preview.html"
		err := os.WriteFile(outputFile, html, 0644)
		if err != nil {
			t.Fatalf("Failed to write HTML file: %v", err)
		}
	
		t.Logf("Alert preview generated and saved to: ./%s", outputFile)
		
	// Basic checks
	if len(html) == 0 {
		t.Error("Expected HTML output, got empty")
	}

}


func TestTaskHTML(t *testing.T) {
	// Load TaskView data from JSON file
	testTasks, err := loadTaskViewData("test/tasks.json")
	if err != nil {
		t.Fatalf("Failed to load task data: %v", err)
	}

	// Set test date
	date := trial.TimeDay("2024-01-15")

	// Test with no filters - summary will be generated from tasks data
	html := taskHTML(testTasks, date, "", "", "")

	// Write HTML to a file for easy viewing
	outputFile := "handler/task_preview.html"
	err = os.WriteFile(outputFile, html, 0644)
	if err != nil {
		t.Fatalf("Failed to write HTML file: %v", err)
	}

	t.Logf("Task preview generated and saved to: ./%s", outputFile)

	// Basic checks
	if len(html) == 0 {
		t.Error("Expected HTML output, got empty")
	}

}

func TestWorkflowHTML(t *testing.T) {
	// Load workflow files 
	taskCache := &cache.SQLite{LocalPath: ":memory"}
	if err := taskCache.Open(testPath+"/workflow/", nil); err != nil  { 
		t.Fatalf("Failed to create test cache: %v", err)
	}

	// Test with no filters - summary will be generated from tasks data
	html := workflowHTML(taskCache)

	// Write HTML to a file for easy viewing
	outputFile := "handler/workflow_preview.html"
	err := os.WriteFile(outputFile, html, 0644)
	if err != nil {
		t.Fatalf("Failed to write HTML file: %v", err)
	}

	t.Logf("Task preview generated and saved to: ./%s", outputFile)

	// Basic checks
	if len(html) == 0 {
		t.Error("Expected HTML output, got empty")
	}

}


func TestAboutHTML(t *testing.T) {
	// Create a real SQLite cache for testing
	taskCache := &cache.SQLite{LocalPath: ":memory"}
	if err := taskCache.Open(testPath+"/workflow/", nil); err != nil  { 
		t.Fatalf("Failed to create test cache: %v", err)
	}

	// Create a mock taskMaster with test data
	tm := &taskMaster{
		initTime:   time.Now().Add(-2 * time.Hour), // 2 hours ago
		nextUpdate: time.Now().Add(30 * time.Minute), // 30 minutes from now
		lastUpdate: time.Now().Add(-15 * time.Minute), // 15 minutes ago
		taskCache:  taskCache,
	}

	// Generate HTML using the aboutHTML method
	html := tm.aboutHTML()

	// Write HTML to a file for easy viewing
	outputFile := "handler/about_preview.html"
	err := os.WriteFile(outputFile, html, 0644)
	if err != nil {
		t.Fatalf("Failed to write HTML file: %v", err)
	}

	t.Logf("About preview generated and saved to: ./%s", outputFile)

	// Basic checks
	if len(html) == 0 {
		t.Error("Expected HTML output, got empty")
	}

	// Check that key content is present
	htmlStr := string(html)
	if !strings.Contains(htmlStr, "flowlord") {
		t.Error("Expected 'flowlord' in HTML output")
	}
	if !strings.Contains(htmlStr, "System Information") {
		t.Error("Expected 'System Information' in HTML output")
	}
	if !strings.Contains(htmlStr, "Table Breakdown") {
		t.Error("Expected 'Table Breakdown' in HTML output")
	}
}
