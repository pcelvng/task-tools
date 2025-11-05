package cache

import (
	"testing"

	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/file/stat"
)

/*
func TestAdd(t *testing.T) {
	fn := func(tasks []task.Task) (map[string]TaskJob, error) {
		cache := &Memory{cache: make(map[string]TaskJob)}
		for _, t := range tasks {
			cache.Add(t)
		}
		for k, v := range cache.cache {
			v.count = len(v.Events)
			v.Events = nil
			cache.cache[k] = v
		}
		return cache.cache, nil
	}
	cases := trial.Cases[[]task.Task, map[string]TaskJob]{
		"no id": {
			Input: []task.Task{
				{Type: "test"},
			},
		},
		"created": {
			Input: []task.Task{
				{Type: "pull", ID: "id1", Info: "?date=2023-01-01", Created: "2023-01-01T00:00:00Z"},
			},
			Expected: map[string]TaskJob{
				"id1": {
					LastUpdate: trial.TimeDay("2023-01-01"),
					count:      1,
				},
			},
		},
		"completed": {
			Input: []task.Task{
				{Type: "pull", ID: "id1", Info: "?date=2023-01-01", Created: "2023-01-01T00:00:00Z"},
				{Type: "pull", ID: "id1", Info: "?date=2023-01-01", Created: "2023-01-01T00:00:00Z", Ended: "2023-01-01T00:00:01Z", Result: task.CompleteResult},
			},
			Expected: map[string]TaskJob{
				"id1": {
					LastUpdate: trial.Time(time.RFC3339, "2023-01-01T00:00:01Z"),
					Completed:  true,
					count:      2,
				},
			},
		},
		"failed": {
			Input: []task.Task{
				{Type: "pull", ID: "id1", Info: "?date=2023-01-01", Created: "2023-01-01T00:00:00Z"},
				{Type: "pull", ID: "id1", Info: "?date=2023-01-01", Created: "2023-01-01T00:00:00Z", Ended: "2023-01-01T00:00:01Z", Result: task.ErrResult, Msg: "Error with pull from X"},
			},
			Expected: map[string]TaskJob{
				"id1": {
					LastUpdate: trial.Time(time.RFC3339, "2023-01-01T00:00:01Z"),
					Completed:  true,
					count:      2,
				},
			},
		},
		"retry": {
			Input: []task.Task{
				{Type: "pull", ID: "id1", Info: "?date=2023-01-01", Created: "2023-01-01T00:00:00Z"},
				{Type: "pull", ID: "id1", Info: "?date=2023-01-01", Created: "2023-01-01T00:00:00Z", Ended: "2023-01-01T00:00:01Z", Result: task.ErrResult, Msg: "Error with pull from X"},
				{Type: "pull", ID: "id1", Info: "?date=2023-01-01", Created: "2023-01-01T00:01:00Z", Meta: "retry=1"},
			},
			Expected: map[string]TaskJob{
				"id1": {
					LastUpdate: trial.Time(time.RFC3339, "2023-01-01T00:01:00Z"),
					Completed:  false,
					count:      3,
				},
			},
		},
		"child": {
			Input: []task.Task{
				{Type: "pull", ID: "id1", Info: "?date=2023-01-01", Created: "2023-01-01T00:00:00Z"},
				{Type: "pull", ID: "id1", Info: "?date=2023-01-01", Created: "2023-01-01T00:00:00Z", Ended: "2023-01-01T00:00:01Z", Result: task.CompleteResult},
				{Type: "transform", ID: "id1", Info: "/product/2023-01-01/data.txt", Created: "2023-01-01T00:02:00Z"},
			},
			Expected: map[string]TaskJob{
				"id1": {
					LastUpdate: trial.Time(time.RFC3339, "2023-01-01T00:02:00Z"),
					Completed:  false,
					count:      3,
				},
			},
		},
		"multi-child": {
			Input: []task.Task{
				{Type: "pull", ID: "id1", Info: "?date=2023-01-01", Started: "2023-01-01T00:00:00Z"},
				{Type: "pull", ID: "id1", Info: "?date=2023-01-01", Started: "2023-01-01T00:00:00Z", Ended: "2023-01-01T00:00:01Z", Result: task.CompleteResult},
				{Type: "transform", ID: "id1", Info: "/product/2023-01-01/data.txt", Started: "2023-01-01T00:02:00Z"},
				{Type: "transform", ID: "id1", Info: "/product/2023-01-01/data.txt", Started: "2023-01-01T00:02:00Z", Ended: "2023-01-01T00:02:15Z", Result: task.CompleteResult},
				{Type: "load", ID: "id1", Info: "/product/2023-01-01/data.txt?table=schema.product", Started: "2023-01-01T00:04:00Z"},
				{Type: "load", ID: "id1", Info: "/product/2023-01-01/data.txt?table=schema.product", Started: "2023-01-01T00:04:00Z", Ended: "2023-01-01T00:05:12Z", Result: task.CompleteResult},
			},
			Expected: map[string]TaskJob{
				"id1": {
					LastUpdate: trial.Time(time.RFC3339, "2023-01-01T00:05:12Z"),
					Completed:  true,
					count:      6,
				},
			},
		},
	}
	trial.New(fn, cases).SubTest(t)
}

func TestRecycle(t *testing.T) {
	now := time.Now()
	cache := Memory{
		ttl: time.Hour,
		cache: map[string]TaskJob{
			"keep": {
				Completed:  false,
				LastUpdate: now.Add(-30 * time.Minute),
				Events:     []task.Task{{Type: "test1"}},
			},
			"expire": {
				Completed:  true,
				LastUpdate: now.Add(-90 * time.Minute),
			},
			"not-completed": {
				Completed:  false,
				LastUpdate: now.Add(-90 * time.Minute),
				Events: []task.Task{
					{Type: "test1", Created: now.String()},
					{Type: "test1", Created: now.String(), Result: task.CompleteResult},
					{Type: "test2", Created: now.String()},
				},
			},
		},
	}

	stat := cache.Recycle()
	stat.ProcessTime = 0
	expected := Stat{
		Count:   1,
		Removed: 2,
		Unfinished: []task.Task{
			{Type: "test2", Created: now.String()},
		}}
	if eq, diff := trial.Equal(stat, expected); !eq {
		t.Logf(diff)
	}
}

func TestRecap(t *testing.T) {
	fn := func(in []task.Task) (map[string]string, error) {
		c := &Memory{cache: map[string]TaskJob{}}
		for _, t := range in {
			c.Add(t)
		}
		result := map[string]string{}
		for k, v := range c.Recap() {
			result[k] = v.String()
		}
		return result, nil
	}
	cases := trial.Cases[[]task.Task, map[string]string]{
		"task no job": {
			Input: []task.Task{{ID: "abc", Type: "test1", Info: "?date=2020-01-02", Result: "complete", Started: "2023-01-01T00:00:00Z", Ended: "2023-01-01T00:00:10Z"}},
			Expected: map[string]string{
				"test1": "min: 10s max: 10s avg: 10s\n\tComplete: 1 2020/01/02\n",
			},
		},
		"task:job": {
			Input: []task.Task{
				{ID: "abc", Type: "test1", Job: "job1", Info: "?day=2020-01-01", Result: "complete", Started: "2023-01-01T00:00:00Z", Ended: "2023-01-01T00:00:10Z"},
				{ID: "abc", Type: "test1", Job: "job1", Info: "?day=2020-01-02", Result: "complete", Started: "2023-01-01T00:00:00Z", Ended: "2023-01-01T00:00:15Z"},
				{ID: "abc", Type: "test1", Job: "job1", Info: "?day=2020-01-03", Result: "complete", Started: "2023-01-01T00:00:00Z", Ended: "2023-01-01T00:00:05Z"},
			},
			Expected: map[string]string{
				"test1:job1": "min: 5s max: 15s avg: 10s\n\tComplete: 3 2020/01/01-2020/01/03\n",
			},
		},
		"with errors": {
			Input: []task.Task{
				{ID: "abc", Type: "test1", Job: "job1", Info: "?day=2020-01-01", Result: "complete", Started: "2023-01-01T00:00:00Z", Ended: "2023-01-01T00:00:10Z"},
				{ID: "abc", Type: "test1", Job: "job1", Info: "?day=2020-01-02", Result: "error", Started: "2023-01-01T00:00:00Z", Ended: "2023-01-01T00:00:15Z"},
				{ID: "abc", Type: "test1", Job: "job1", Info: "?day=2020-01-03", Result: "complete", Started: "2023-01-01T00:00:00Z", Ended: "2023-01-01T00:00:05Z"},
			},
			Expected: map[string]string{
				"test1:job1": "min: 5s max: 10s avg: 7.5s\n\tComplete: 2 2020/01/01,2020/01/03\n\tError: 1 2020/01/02\n",
			},
		},
		"hourly": {
			Input: []task.Task{
				{ID: "abc", Type: "proc", Job: "hour", Info: "?hour=2020-01-01T05", Result: "complete", Started: "2023-01-01T00:00:00Z", Ended: "2023-01-01T00:00:10Z"},
				{ID: "abc", Type: "proc", Job: "hour", Info: "?hour_utc=2020-01-01T06", Result: "complete", Started: "2023-01-01T00:00:00Z", Ended: "2023-01-01T00:00:15Z"},
				{ID: "abc", Type: "proc", Job: "hour", Info: "?hour=2020-01-01T07", Result: "complete", Started: "2023-01-01T00:00:00Z", Ended: "2023-01-01T00:00:05Z"},
				{ID: "abc", Type: "proc", Job: "hour", Info: "?hour=2020-01-01T08", Result: "complete", Started: "2023-01-01T00:00:00Z", Ended: "2023-01-01T00:00:47Z"},
				{ID: "abc", Type: "proc", Job: "hour", Info: "?hour=2020-01-01T09", Result: "complete", Started: "2023-01-01T00:00:00Z", Ended: "2023-01-01T00:01:33Z"},
			},
			Expected: map[string]string{
				"proc:hour": "min: 5s max: 1m33s avg: 34s\n\tComplete: 5 2020/01/01T05-2020/01/01T09\n",
			},
		},
		"monthly": {
			Input: []task.Task{
				{ID: "abc", Type: "month", Info: "?day=2020-01-01", Result: "complete", Started: "2023-01-01T00:00:00Z", Ended: "2023-01-01T00:00:10Z"},
				{ID: "abc", Type: "month", Info: "?day=2020-02-01", Result: "complete", Started: "2023-01-01T00:00:00Z", Ended: "2023-01-01T00:00:15Z"},
			},
			Expected: map[string]string{
				"month": "min: 10s max: 15s avg: 12.5s\n\tComplete: 2 2020/01/01,2020/02/01\n",
			},
		},
		"meta_job": {
			Input: []task.Task{
				{ID: "abc", Type: "test1", Info: "?day=2020-01-01", Result: "complete", Started: "2023-01-01T00:00:00Z", Ended: "2023-01-01T00:00:10Z", Meta: "job=job1"},
				{ID: "abc", Type: "test1", Info: "?day=2020-01-02", Result: "complete", Started: "2023-01-01T00:00:00Z", Ended: "2023-01-01T00:00:15Z", Meta: "job=job1"},
				{ID: "abc", Type: "test1", Info: "?day=2020-01-03", Result: "complete", Started: "2023-01-01T00:00:00Z", Ended: "2023-01-01T00:00:05Z", Meta: "job=job1"},
			},
			Expected: map[string]string{
				"test1:job1": "min: 5s max: 15s avg: 10s\n\tComplete: 3 2020/01/01-2020/01/03\n",
			},
		},
	}
	trial.New(fn, cases).SubTest(t)
}
*/

// TestDatesByType tests the unified date query method
func TestDatesByType(t *testing.T) {
	// Create in-memory database
	db := &SQLite{LocalPath: ":memory:"}
	if err := db.initDB(); err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Close()

	// Add sample task records
	db.Add(task.Task{
		ID:      "task1",
		Type:    "data-load",
		Job:     "import",
		Created: "2024-01-15T10:00:00Z",
		Result:  task.CompleteResult,
		Ended:   "2024-01-15T10:05:00Z",
	})
	db.Add(task.Task{
		ID:      "task2",
		Type:    "data-load",
		Job:     "import",
		Created: "2024-01-16T10:00:00Z",
		Result:  task.CompleteResult,
		Ended:   "2024-01-16T10:05:00Z",
	})

	// Add sample alert records with specific created_at times
	_, err := db.db.Exec(`
		INSERT INTO alert_records (task_id, task_time, task_type, job, msg, created_at)
		VALUES (?, ?, ?, ?, ?, ?),
		       (?, ?, ?, ?, ?, ?)
	`, "alert1", "2024-01-15T11:00:00Z", "data-validation", "check", "Validation error", "2024-01-15T11:00:00Z",
	   "alert2", "2024-01-17T11:00:00Z", "data-validation", "check", "Validation error", "2024-01-17T11:00:00Z")
	if err != nil {
		t.Fatalf("Failed to insert alerts: %v", err)
	}

	// Add sample file messages
	fileMsg1 := stat.Stats{
		Path: "gs://bucket/file1.json", 
		Size: 1024, 
	}
	db.AddFileMessage(fileMsg1, []string{}, []string{})

	// Test "tasks" type
	taskDates, err := db.DatesByType("tasks")
	if err != nil {
		t.Errorf("DatesByType('tasks') error: %v", err)
	}
	if len(taskDates) != 2 {
		t.Errorf("Expected 2 task dates, got %d", len(taskDates))
	}
	if len(taskDates) > 0 && taskDates[0] != "2024-01-16" {
		t.Errorf("Expected first task date '2024-01-16', got '%s'", taskDates[0])
	}

	// Test "alerts" type
	alertDates, err := db.DatesByType("alerts")
	if err != nil {
		t.Errorf("DatesByType('alerts') error: %v", err)
	}
	if len(alertDates) != 2 {
		t.Errorf("Expected 2 alert dates, got %v", alertDates)
	}

	// Test "files" type
	fileDates, err := db.DatesByType("files")
	if err != nil {
		t.Errorf("DatesByType('files') error: %v", err)
	}
	if len(fileDates) == 0 {
		t.Error("Expected at least 1 file date")
	}

	// Test invalid type
	_, err = db.DatesByType("invalid")
	if err == nil {
		t.Error("Expected error for invalid data type, got nil")
	}

	// Test backward compatibility methods
	taskDates2, _ := db.GetDatesWithTasks()
	if len(taskDates2) != len(taskDates) {
		t.Error("GetDatesWithTasks() should return same results as DatesByType('tasks')")
	}

	alertDates2, _ := db.GetDatesWithAlerts()
	if len(alertDates2) != len(alertDates) {
		t.Error("GetDatesWithAlerts() should return same results as DatesByType('alerts')")
	}

	fileDates2, _ := db.GetDatesWithFiles()
	if len(fileDates2) != len(fileDates) {
		t.Error("GetDatesWithFiles() should return same results as DatesByType('files')")
	}
}