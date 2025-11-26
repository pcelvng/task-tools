package sqlite

import (
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"

	"github.com/pcelvng/task"
	"github.com/pcelvng/task/bus"

	"github.com/pcelvng/task-tools/tmpl"
)

// TaskJob describes info about completed tasks that are within the cache
type TaskJob struct {
	LastUpdate time.Time // time since the last event with id
	Completed  bool
	count      int
	Events     []task.Task
}

// TaskFilter contains options for filtering and paginating task queries.
// Empty string fields are ignored in the query.
type TaskFilter struct {
	ID     string // Filter by task ID (resets other filters)
	Type   string // Filter by task type
	Job    string // Filter by job name
	Result string // Filter by result status (complete, error, alert, warn, or "running" for empty)
	Page   int    // Page number (1-based, default: 1)
	Limit  int    // Number of results per page (default: 100)
}

// TaskView represents a task with calculated times from the tasks view
type TaskView struct {
	ID           string `json:"id"`
	Type         string `json:"type"`
	Job          string `json:"job"`
	Info         string `json:"info"`
	Result       string `json:"result"`
	Meta         string `json:"meta"`
	Msg          string `json:"msg"`
	TaskSeconds  int    `json:"task_seconds"`
	TaskTime     string `json:"task_time"`
	QueueSeconds int    `json:"queue_seconds"`
	QueueTime    string `json:"queue_time"`
	Created      string `json:"created"`
	Started      string `json:"started"`
	Ended        string `json:"ended"`
}

func (s *SQLite) Add(t task.Task) {
	if t.ID == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Use UPSERT to handle both new tasks and updates
	result, err := s.db.Exec(`
		INSERT INTO task_records (id, type, job, info, result, meta, msg, created, started, ended)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (TYPE, job, id, created) 
		DO UPDATE SET
			result = excluded.result,
			meta = excluded.meta,
			msg = excluded.msg,
			started = excluded.started,
			ended = excluded.ended
	`,
		t.ID, t.Type, t.Job, t.Info, t.Result, t.Meta, t.Msg,
		t.Created, t.Started, t.Ended,
	)

	if err != nil {
		log.Printf("ERROR: Failed to insert task record: %v", err)
		return
	}

	// Update date index for this task's date
	if t.Created != "" {
		s.updateDateIndex(t.Created, "tasks")
	}

	// Check if this was an update (conflict) rather than insert
	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		// This indicates a conflict occurred and the record was updated
		// Log this as it's unexpected for new task creation
		log.Printf("WARNING: Task creation conflict detected - task %s:%s:%s at %s was updated instead of inserted",
			t.Type, t.Job, t.ID, t.Created)
	}
}

func (s *SQLite) GetTask(id string) TaskJob {
	s.mu.Lock()
	defer s.mu.Unlock()

	var tj TaskJob

	// Get all task records for this ID, ordered by created time
	rows, err := s.db.Query(`
		SELECT id, type, job, info, result, meta, msg,
		       created, started, ended
		FROM task_records
		WHERE id = ?
		ORDER BY created
	`, id)
	if err != nil {
		return tj
	}
	defer rows.Close()

	var events []task.Task
	var lastUpdate time.Time
	var completed bool

	for rows.Next() {
		var t task.Task
		err := rows.Scan(
			&t.ID, &t.Type, &t.Job, &t.Info, &t.Result, &t.Meta, &t.Msg,
			&t.Created, &t.Started, &t.Ended,
		)
		if err != nil {
			continue
		}
		events = append(events, t)

		// Track completion status and last update time
		if t.Result != "" {
			completed = true
			if ended, err := time.Parse(time.RFC3339, t.Ended); err == nil {
				if ended.After(lastUpdate) {
					lastUpdate = ended
				}
			}
		} else {
			if created, err := time.Parse(time.RFC3339, t.Created); err == nil {
				if created.After(lastUpdate) {
					lastUpdate = created
				}
			}
		}
	}

	tj = TaskJob{
		LastUpdate: lastUpdate,
		Completed:  completed,
		Events:     events,
		count:      len(events),
	}

	return tj
}

// Recycle cleans up any records older than day in the DB tables: files, alerts and tasks.
func (s *SQLite) Recycle(t time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	day := t.Format("2006-01-02")
	totalDeleted := 0

	// Delete old task records
	result, err := s.db.Exec("DELETE FROM task_records WHERE created < ?", day)
	if err != nil {
		return totalDeleted, fmt.Errorf("error deleting old task records: %w", err)
	}
	rowsAffected, _ := result.RowsAffected()
	totalDeleted += int(rowsAffected)

	// Delete old alert records
	result, err = s.db.Exec("DELETE FROM alert_records WHERE created_at < ?", day)
	if err != nil {
		return totalDeleted, fmt.Errorf("error deleting old alert records: %w", err)
	}
	rowsAffected, _ = result.RowsAffected()
	totalDeleted += int(rowsAffected)

	// Delete old file messages
	result, err = s.db.Exec("DELETE FROM file_messages WHERE received_at < ?", day)
	if err != nil {
		return totalDeleted, fmt.Errorf("error deleting old file messages: %w", err)
	}
	rowsAffected, _ = result.RowsAffected()
	totalDeleted += int(rowsAffected)

	// Delete old date index entries
	result, err = s.db.Exec("DELETE FROM date_index WHERE date < ?", day)
	if err != nil {
		return totalDeleted, fmt.Errorf("error deleting old date index: %w", err)
	}
	rowsAffected, _ = result.RowsAffected()
	totalDeleted += int(rowsAffected)

	return totalDeleted, nil
}

// CheckIncompleteTasks checks for tasks that have not completed within the TTL period
// and adds them to the alerts table with deduplication. Returns count of incomplete tasks found.
// Uses a JOIN query to efficiently find incomplete tasks without existing alerts.
func (s *SQLite) CheckIncompleteTasks() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	tasks := make([]task.Task, 0)
	t := time.Now()

	// Use LEFT JOIN to find incomplete tasks that don't have existing alerts
	// This eliminates the need for separate deduplication queries
	rows, err := s.db.Query(`
		SELECT tr.id, tr.type, tr.job, tr.info, tr.result, tr.meta, tr.msg,
		       tr.created, tr.started, tr.ended
		FROM task_records tr
		LEFT JOIN alert_records ar ON (
			tr.id = ar.task_id AND 
			tr.type = ar.task_type AND 
			tr.job = ar.job AND 
			ar.msg LIKE 'INCOMPLETE:%'
		)
		WHERE tr.created < ? 
		AND tr.result = '' 
		AND ar.id IS NULL
	`, t.Add(-s.TaskTTL))
	if err != nil {
		return 0
	}
	defer rows.Close()

	// Process incomplete records that don't have alerts
	for rows.Next() {
		var task task.Task
		err := rows.Scan(
			&task.ID, &task.Type, &task.Job, &task.Info, &task.Result,
			&task.Meta, &task.Msg, &task.Created, &task.Started, &task.Ended,
		)
		if err != nil {
			continue
		}

		// Add incomplete task to alert list
		tasks = append(tasks, task)

		// Add alert directly (no need to check for duplicates since JOIN already filtered them)
		taskID := task.ID
		if taskID == "" {
			taskID = "unknown"
		}

		taskTime := tmpl.TaskTime(task)

		_, err = s.db.Exec(`
			INSERT INTO alert_records (task_id, task_time, task_type, job, msg)
			VALUES (?, ?, ?, ?, ?)
		`, taskID, taskTime, task.Type, task.Job, "INCOMPLETE: unfinished task detected")

		if err != nil {
			// Continue processing even if alert insertion fails
			continue
		}
	}

	return len(tasks)
}

// Recap returns a summary of task statistics for a given day
func (s *SQLite) Recap(day time.Time) TaskStats {
	s.mu.Lock()
	defer s.mu.Unlock()

	data := make(map[string]*Stats)
	rows, err := s.db.Query(`
		SELECT id, type, job, info, result, meta, msg,
		       created, started, ended
		FROM task_records 
		WHERE created >= ? AND created < ?
	`, day.Format("2006-01-02"), day.Add(24*time.Hour).Format("2006-01-02"))
	if err != nil {
		return data
	}
	defer rows.Close()

	for rows.Next() {
		var t task.Task
		err := rows.Scan(
			&t.ID, &t.Type, &t.Job, &t.Info, &t.Result, &t.Meta, &t.Msg,
			&t.Created, &t.Started, &t.Ended,
		)
		if err != nil {
			continue
		}
		key := strings.TrimRight(t.Type+":"+t.Job, ":")
		stat, found := data[key]
		if !found {
			stat = &Stats{
				CompletedTimes: make([]time.Time, 0),
				ErrorTimes:     make([]time.Time, 0),
				AlertTimes:     make([]time.Time, 0),
				WarnTimes:      make([]time.Time, 0),
				RunningTimes:   make([]time.Time, 0),
				ExecTimes:      &DurationStats{},
			}
			data[key] = stat
		}
		stat.Add(t)
	}

	return TaskStats(data)
}

func (s *SQLite) SendFunc(p bus.Producer) func(string, *task.Task) error {
	return func(topic string, tsk *task.Task) error {
		s.Add(*tsk)
		return p.Send(topic, tsk.JSONBytes())
	}
}

// GetTasksByDate retrieves tasks for a specific date with optional filtering and pagination
func (s *SQLite) GetTasksByDate(date time.Time, filter *TaskFilter) ([]TaskView, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Handle nil filter or set defaults
	if filter == nil {
		filter = &TaskFilter{}
	}
	if filter.Limit <= 0 {
		filter.Limit = DefaultPageSize
	}
	if filter.Page <= 0 {
		filter.Page = 1 // default to first page
	}

	dateStr := date.Format("2006-01-02")

	// Build WHERE clause with filters
	whereClause := "WHERE DATE(created) = ?"
	args := []interface{}{dateStr}

	// If ID is specified, only filter by ID (ignores other filters)
	if filter.ID != "" {
		whereClause += " AND id = ?"
		args = append(args, filter.ID)
	} else {
		// Apply other filters only when ID is not specified
		if filter.Type != "" {
			whereClause += " AND type = ?"
			args = append(args, filter.Type)
		}

		if filter.Job != "" {
			whereClause += " AND job = ?"
			args = append(args, filter.Job)
		}

		if filter.Result != "" {
			// Handle "running" as empty result
			if filter.Result == "running" {
				whereClause += " AND result = ''"
			} else {
				whereClause += " AND result = ?"
				args = append(args, filter.Result)
			}
		}
	}

	// Get total count of filtered results
	countQuery := "SELECT COUNT(*) FROM tasks " + whereClause
	var totalCount int
	err := s.db.QueryRow(countQuery, args...).Scan(&totalCount)
	if err != nil {
		return nil, 0, err
	}

	// Build main query with pagination
	query := `SELECT id, type, job, info, result, meta, msg, task_seconds, task_time, queue_seconds, queue_time, created, started, ended
		FROM tasks ` + whereClause + `
		ORDER BY created DESC
		LIMIT ? OFFSET ?`

	// Calculate offset from page number
	offset := (filter.Page - 1) * filter.Limit
	args = append(args, filter.Limit, offset)

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var tasks []TaskView
	for rows.Next() {
		var t TaskView
		err := rows.Scan(
			&t.ID, &t.Type, &t.Job, &t.Info, &t.Result, &t.Meta, &t.Msg,
			&t.TaskSeconds, &t.TaskTime, &t.QueueSeconds, &t.QueueTime,
			&t.Created, &t.Started, &t.Ended,
		)
		if err != nil {
			t.Result = string(task.ErrResult)
			t.Msg = err.Error()
		}
		tasks = append(tasks, t)
	}

	return tasks, totalCount, nil
}

// GetTaskRecapByDate creates a recap of tasks for a specific date
func (s *SQLite) GetTaskRecapByDate(date time.Time) (TaskStats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	dateStr := date.Format("2006-01-02")

	query := `SELECT id, type, job, info, result, meta, msg, created, started, ended
		FROM task_records
		WHERE DATE(created) = ?
		ORDER BY created`

	rows, err := s.db.Query(query, dateStr)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	data := make(map[string]*Stats)
	for rows.Next() {
		var t task.Task
		err := rows.Scan(
			&t.ID, &t.Type, &t.Job, &t.Info, &t.Result, &t.Meta, &t.Msg,
			&t.Created, &t.Started, &t.Ended,
		)
		if err != nil {
			continue
		}

		job := t.Job
		if job == "" {
			v, _ := url.ParseQuery(t.Meta)
			job = v.Get("job")
		}
		key := strings.TrimRight(t.Type+":"+job, ":")
		stat, found := data[key]
		if !found {
			stat = &Stats{
				CompletedTimes: make([]time.Time, 0),
				ErrorTimes:     make([]time.Time, 0),
				AlertTimes:     make([]time.Time, 0),
				WarnTimes:      make([]time.Time, 0),
				RunningTimes:   make([]time.Time, 0),
				ExecTimes:      &DurationStats{},
			}
			data[key] = stat
		}
		stat.Add(t)
	}

	return TaskStats(data), nil
}

// extractJobFromTask is a helper function to get job from task
func extractJobFromTask(t task.Task) string {
	job := t.Job
	if job == "" {
		if meta, err := url.ParseQuery(t.Meta); err == nil {
			job = meta.Get("job")
		}
	}
	return job
}
