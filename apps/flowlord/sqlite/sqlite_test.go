package sqlite

import (
	"testing"

	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/file/stat"
)

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

	// Rebuild the date index to capture the directly-inserted alerts
	// (In production, all inserts go through Add/AddAlert/AddFileMessage which maintain the index)
	if err := db.RebuildDateIndex(); err != nil {
		t.Fatalf("Failed to rebuild date index: %v", err)
	}

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