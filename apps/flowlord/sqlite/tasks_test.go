package sqlite

import (
	"testing"
	"time"

	"github.com/hydronica/trial"
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

func TestGetHourlyCountsByDate(t *testing.T) {
	db := &SQLite{LocalPath: ":memory:"}
	if err := db.initDB(); err != nil {
		t.Fatalf("initDB: %v", err)
	}
	defer db.Close()

	day := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	db.Add(task.Task{
		ID: "a", Type: "alpha", Job: "load",
		Created: "2024-01-15T10:00:00Z", Meta: "cron=2024-01-15T10",
		Result: task.CompleteResult,
	})
	db.Add(task.Task{
		ID: "b", Type: "alpha", Job: "load",
		Created: "2024-01-15T11:00:00Z", Meta: "cron=2024-01-15T11",
		Result: task.ErrResult,
	})
	db.Add(task.Task{
		ID: "c", Type: "beta", Job: "check",
		Created: "2024-01-15T12:00:00Z", Meta: "cron=2024-01-15T12",
		Result: task.CompleteResult,
	})

	type input struct {
		filter TaskFilter
	}
	type expect struct {
		total  TaskCounts
		hour10 TaskCounts
		hour11 TaskCounts
		hour12 TaskCounts
	}

	fn := func(in input) (expect, error) {
		total, hourly, err := db.GetHourlyCountsByDate(day, &in.filter)
		if err != nil {
			return expect{}, err
		}
		return expect{
			total:  total,
			hour10: hourly[10],
			hour11: hourly[11],
			hour12: hourly[12],
		}, nil
	}

	cases := trial.Cases[input, expect]{
		"id only": {
			Input: input{filter: TaskFilter{ID: []string{"a"}}},
			Expected: expect{
				total:  TaskCounts{Total: 1, Completed: 1},
				hour10: TaskCounts{Total: 1, Completed: 1},
			},
		},
		"id and result": {
			Input: input{filter: TaskFilter{
				ID: []string{"a", "b"}, Result: []string{"error"},
			}},
			Expected: expect{
				total:  TaskCounts{Total: 1, Error: 1},
				hour11: TaskCounts{Total: 1, Error: 1},
			},
		},
		"id and type": {
			Input: input{filter: TaskFilter{
				ID: []string{"c"}, Type: []string{"beta"},
			}},
			Expected: expect{
				total:  TaskCounts{Total: 1, Completed: 1},
				hour12: TaskCounts{Total: 1, Completed: 1},
			},
		},
		"id and type mismatch": {
			Input: input{filter: TaskFilter{
				ID: []string{"c"}, Type: []string{"alpha"},
			}},
			Expected: expect{},
		},
	}
	trial.New(fn, cases).SubTest(t)
}

func TestAddStoresRunningForEmptyResult(t *testing.T) {
	db := &SQLite{LocalPath: ":memory:"}
	if err := db.initDB(); err != nil {
		t.Fatalf("initDB: %v", err)
	}
	defer db.Close()

	created := "2024-01-15T10:00:00Z"
	db.Add(task.Task{ID: "1", Type: "alpha", Job: "load", Created: created})

	var result string
	err := db.db.QueryRow(`SELECT result FROM task_records WHERE id = ?`, "1").Scan(&result)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if result != ResultRunning {
		t.Errorf("result = %q, want %q", result, ResultRunning)
	}

	db.Add(task.Task{
		ID: "1", Type: "alpha", Job: "load", Created: created,
		Result: task.CompleteResult,
	})
	err = db.db.QueryRow(`SELECT result FROM task_records WHERE id = ?`, "1").Scan(&result)
	if err != nil {
		t.Fatalf("query after update: %v", err)
	}
	if result != string(task.CompleteResult) {
		t.Errorf("result after complete = %q, want %q", result, task.CompleteResult)
	}
}
