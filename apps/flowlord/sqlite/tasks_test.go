package sqlite

import (
	"testing"
	"time"

	"github.com/pcelvng/task"
)

func TestGetTasksByDate(t *testing.T) {
	db := &SQLite{LocalPath: ":memory:"}
	if err := db.initDB(); err != nil {
		t.Fatalf("initDB: %v", err)
	}
	defer db.Close()

	day := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	db.Add(task.Task{ID: "1", Type: "alpha", Job: "load", Created: "2024-01-15T10:00:00Z", Result: task.CompleteResult})
	db.Add(task.Task{ID: "2", Type: "zebra", Job: "load", Created: "2024-01-15T11:00:00Z", Result: task.ErrResult})
	db.Add(task.Task{ID: "3", Type: "alpha", Job: "check", Created: "2024-01-15T12:00:00Z", Result: task.ErrResult})
	db.Add(task.Task{ID: "b", Type: "zebra", Job: "j", Created: "2024-01-15T10:00:00Z", Result: task.CompleteResult})
	db.Add(task.Task{ID: "a", Type: "alpha", Job: "j", Created: "2024-01-15T11:00:00Z", Result: task.CompleteResult})
	db.Add(task.Task{ID: "c", Type: "middle", Job: "j", Created: "2024-01-15T12:00:00Z", Result: task.CompleteResult})
	db.Add(task.Task{ID: "shared", Type: "alpha", Job: "load", Created: "2024-01-15T13:00:00Z", Result: task.CompleteResult})
	db.Add(task.Task{ID: "shared", Type: "zebra", Job: "load", Created: "2024-01-15T14:00:00Z", Result: task.ErrResult})

	t.Run("sort type asc", func(t *testing.T) {
		tasks, _, err := db.GetTasksByDate(day, &TaskFilter{Sort: "type", Direction: "asc", Page: 1, Limit: 10})
		if err != nil {
			t.Fatalf("GetTasksByDate: %v", err)
		}
		if len(tasks) < 3 {
			t.Fatalf("expected at least 3 tasks, got %d", len(tasks))
		}
		// First three by type among the seeded set when sorted asc should start with alpha...
		if tasks[0].Type != "alpha" {
			t.Errorf("first type = %q, want alpha", tasks[0].Type)
		}
		last := tasks[len(tasks)-1]
		if last.Type != "zebra" {
			t.Errorf("last type = %q, want zebra", last.Type)
		}
	})

	t.Run("default created desc", func(t *testing.T) {
		tasks, _, err := db.GetTasksByDate(day, &TaskFilter{Page: 1, Limit: 10})
		if err != nil {
			t.Fatalf("GetTasksByDate default: %v", err)
		}
		if len(tasks) == 0 {
			t.Fatal("expected tasks")
		}
		if tasks[0].Created < tasks[len(tasks)-1].Created {
			t.Errorf("expected created DESC, first=%s last=%s", tasks[0].Created, tasks[len(tasks)-1].Created)
		}
	})

	t.Run("multi type and job", func(t *testing.T) {
		tasks, count, err := db.GetTasksByDate(day, &TaskFilter{
			Type: []string{"alpha", "zebra"}, Job: []string{"load"}, Page: 1, Limit: 10,
		})
		if err != nil {
			t.Fatalf("multi type: %v", err)
		}
		if count != 4 {
			t.Fatalf("expected 4 load tasks of alpha|zebra, got count=%d len=%d", count, len(tasks))
		}
	})

	t.Run("multi result", func(t *testing.T) {
		_, count, err := db.GetTasksByDate(day, &TaskFilter{
			Result: []string{"error", "complete"}, Type: []string{"alpha"}, Page: 1, Limit: 10,
		})
		if err != nil {
			t.Fatalf("multi result: %v", err)
		}
		if count != 4 { // alpha+load complete, alpha+check error, alpha+j complete, shared alpha complete
			t.Fatalf("expected 4 alpha error|complete, got count=%d", count)
		}
	})

	t.Run("multi id", func(t *testing.T) {
		tasks, count, err := db.GetTasksByDate(day, &TaskFilter{
			ID: []string{"a", "c"}, Page: 1, Limit: 10,
		})
		if err != nil {
			t.Fatalf("multi id: %v", err)
		}
		if count != 2 || len(tasks) != 2 {
			t.Fatalf("expected 2 tasks for ids a|c, got count=%d len=%d", count, len(tasks))
		}
	})

	t.Run("id and type", func(t *testing.T) {
		tasks, count, err := db.GetTasksByDate(day, &TaskFilter{
			ID: []string{"shared"}, Type: []string{"zebra"}, Page: 1, Limit: 10,
		})
		if err != nil {
			t.Fatalf("id+type: %v", err)
		}
		if count != 1 || len(tasks) != 1 || tasks[0].Type != "zebra" {
			t.Fatalf("expected 1 zebra shared task, got count=%d tasks=%v", count, tasks)
		}
	})
}
