package sqlite

import (
	"testing"
	"time"

	"github.com/pcelvng/task"
)

func TestTaskFilterOrderByClause(t *testing.T) {
	tests := []struct {
		name      string
		sort      string
		direction string
		want      string
	}{
		{"default empty", "", "", "ORDER BY created DESC"},
		{"unknown column", "bogus", "asc", "ORDER BY created DESC"},
		{"type asc", "type", "asc", "ORDER BY type ASC"},
		{"type desc", "type", "desc", "ORDER BY type DESC"},
		{"msg", "msg", "asc", "ORDER BY msg ASC"},
		{"queue_seconds", "queue_seconds", "desc", "ORDER BY queue_seconds DESC"},
		{"task_seconds", "task_seconds", "asc", "ORDER BY task_seconds ASC"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &TaskFilter{Sort: tt.sort, Direction: tt.direction}
			f.NormalizeSort()
			if got := f.orderByClause(); got != tt.want {
				t.Errorf("orderByClause() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestGetTasksByDateSort(t *testing.T) {
	db := &SQLite{LocalPath: ":memory:"}
	if err := db.initDB(); err != nil {
		t.Fatalf("initDB: %v", err)
	}
	defer db.Close()

	day := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	db.Add(task.Task{ID: "b", Type: "zebra", Job: "j", Created: "2024-01-15T10:00:00Z", Result: task.CompleteResult})
	db.Add(task.Task{ID: "a", Type: "alpha", Job: "j", Created: "2024-01-15T11:00:00Z", Result: task.CompleteResult})
	db.Add(task.Task{ID: "c", Type: "middle", Job: "j", Created: "2024-01-15T12:00:00Z", Result: task.CompleteResult})

	tasks, _, err := db.GetTasksByDate(day, &TaskFilter{Sort: "type", Direction: "asc", Page: 1, Limit: 10})
	if err != nil {
		t.Fatalf("GetTasksByDate: %v", err)
	}
	if len(tasks) != 3 {
		t.Fatalf("expected 3 tasks, got %d", len(tasks))
	}
	if tasks[0].Type != "alpha" || tasks[1].Type != "middle" || tasks[2].Type != "zebra" {
		t.Errorf("unexpected type order: %s, %s, %s", tasks[0].Type, tasks[1].Type, tasks[2].Type)
	}

	// Default (no sort) is created DESC
	tasks, _, err = db.GetTasksByDate(day, &TaskFilter{Page: 1, Limit: 10})
	if err != nil {
		t.Fatalf("GetTasksByDate default: %v", err)
	}
	if tasks[0].ID != "c" || tasks[2].ID != "b" {
		t.Errorf("default created DESC order wrong: ids %s, %s, %s", tasks[0].ID, tasks[1].ID, tasks[2].ID)
	}
}
