package cache

import (
	"database/sql"
	_ "embed"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/tmpl"
	"github.com/pcelvng/task/bus"
	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schema string

type SQLite struct {
	db  *sql.DB
	ttl time.Duration
}

func NewSQLite(ttl time.Duration, dbPath string) (*SQLite, error) {
	if ttl < time.Hour {
		ttl = time.Hour
	}

	// Open the database
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, err
	}

	// Execute the schema
	_, err = db.Exec(schema)
	if err != nil {
		return nil, err
	}

	return &SQLite{
		db:  db,
		ttl: ttl,
	}, nil
}

func (s *SQLite) Add(t task.Task) {
	if t.ID == "" {
		return
	}

	// Start a transaction
	tx, err := s.db.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()

	// Check if event exists
	var eventExists bool
	err = tx.QueryRow("SELECT EXISTS(SELECT 1 FROM events WHERE id = ?)", t.ID).Scan(&eventExists)
	if err != nil {
		return
	}

	// Determine completion status and last update time
	completed := t.Result != ""
	var lastUpdate time.Time
	if completed {
		lastUpdate, _ = time.Parse(time.RFC3339, t.Ended)
	} else {
		lastUpdate, _ = time.Parse(time.RFC3339, t.Created)
	}

	// Insert or update event
	if !eventExists {
		_, err = tx.Exec(`
			INSERT INTO events (id, completed, last_update)
			VALUES (?, ?, ?)
		`, t.ID, completed, lastUpdate)
	} else {
		_, err = tx.Exec(`
			UPDATE events 
			SET completed = ?, last_update = ?
			WHERE id = ?
		`, completed, lastUpdate, t.ID)
	}
	if err != nil {
		return
	}

	// Insert task log
	_, err = tx.Exec(`
		INSERT INTO task_log (
			id, type, job, info, result, meta, msg,
			created, started, ended, event_id
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		t.ID, t.Type, t.Job, t.Info, t.Result, t.Meta, t.Msg,
		t.Created, t.Started, t.Ended, t.ID,
	)
	if err != nil {
		return
	}

	// Commit the transaction
	tx.Commit()
}

func (s *SQLite) Get(id string) TaskJob {
	var tj TaskJob
	var completed bool
	var lastUpdate time.Time

	// Get event info
	err := s.db.QueryRow(`
		SELECT completed, last_update
		FROM events
		WHERE id = ?
	`, id).Scan(&completed, &lastUpdate)

	if err != nil {
		return tj
	}

	// Get all task logs for this event
	rows, err := s.db.Query(`
		SELECT id, type, job, info, result, meta, msg,
		       created, started, ended
		FROM task_log
		WHERE event_id = ?
		ORDER BY created
	`, id)
	if err != nil {
		return tj
	}
	defer rows.Close()

	var events []task.Task
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
	}

	tj = TaskJob{
		LastUpdate: lastUpdate,
		Completed:  completed,
		Events:     events,
		count:      len(events),
	}

	return tj
}

func (s *SQLite) Recycle() Stat {
	tasks := make([]task.Task, 0)
	t := time.Now()

	// Start a transaction
	tx, err := s.db.Begin()
	if err != nil {
		return Stat{}
	}
	defer tx.Rollback()

	// Get total count before deletion
	var total int
	err = tx.QueryRow("SELECT COUNT(*) FROM events").Scan(&total)
	if err != nil {
		return Stat{}
	}

	// Get expired events and their last task log
	rows, err := tx.Query(`
		SELECT e.id, e.completed, tl.id, tl.type, tl.job, tl.info, tl.result,
		       tl.meta, tl.msg, tl.created, tl.started, tl.ended
		FROM events e
		JOIN task_log tl ON e.id = tl.event_id
		WHERE e.last_update < ?
		AND tl.created = (
			SELECT MAX(created)
			FROM task_log
			WHERE event_id = e.id
		)
	`, t.Add(-s.ttl))
	if err != nil {
		return Stat{}
	}
	defer rows.Close()

	// Process expired events
	for rows.Next() {
		var (
			eventID   string
			completed bool
			task      task.Task
		)
		err := rows.Scan(
			&eventID, &completed,
			&task.ID, &task.Type, &task.Job, &task.Info, &task.Result,
			&task.Meta, &task.Msg, &task.Created, &task.Started, &task.Ended,
		)
		if err != nil {
			continue
		}

		if !completed {
			tasks = append(tasks, task)
		}

		// Delete the event and its task logs
		_, err = tx.Exec("DELETE FROM task_log WHERE event_id = ?", eventID)
		if err != nil {
			continue
		}
		_, err = tx.Exec("DELETE FROM events WHERE id = ?", eventID)
		if err != nil {
			continue
		}
	}

	// Get remaining count
	var remaining int
	err = tx.QueryRow("SELECT COUNT(*) FROM events").Scan(&remaining)
	if err != nil {
		return Stat{}
	}

	// Commit the transaction
	tx.Commit()

	return Stat{
		Count:       remaining,
		Removed:     total - remaining,
		ProcessTime: time.Since(t),
		Unfinished:  tasks,
	}
}

func (s *SQLite) Recap() map[string]*Stats {
	data := make(map[string]*Stats)
	rows, err := s.db.Query(`
		SELECT id, type, job, info, result, meta, msg,
		       created, started, ended
		FROM task_log
	`)
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
				ExecTimes:      &DurationStats{},
			}
			data[key] = stat
		}
		stat.Add(t)
	}

	return data
}

func (s *SQLite) SendFunc(p bus.Producer) func(string, *task.Task) error {
	return func(topic string, tsk *task.Task) error {
		s.Add(*tsk)
		return p.Send(topic, tsk.JSONBytes())
	}
}

func (s *SQLite) Close() error {
	return s.db.Close()
}


// AddAlert stores an alert record in the database
func (s *SQLite) AddAlert(t task.Task, message string) error {
	// Allow empty task ID for job send failures - store as "unknown"
	taskID := t.ID
	if taskID == "" {
		taskID = "unknown"
	}

	// Store task created time as string (no need to parse to time.Time for insert)
	taskCreated := t.Created

	// Extract job using helper function
	job := extractJobFromTask(t)

	_, err := s.db.Exec(`
		INSERT INTO alert_records (task_id, task_type, job, msg, task_created)
		VALUES (?, ?, ?, ?, ?)
	`, taskID, t.Type, job, message, taskCreated)

	return err
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

// GetAlertsByDate retrieves all alerts for a specific date
func (s *SQLite) GetAlertsByDate(date time.Time) ([]AlertRecord, error) {
	dateStr := date.Format("2006-01-02")
	
	query := `SELECT id, task_id, task_type, job, msg, created_at, task_created
			FROM alert_records 
			WHERE date(created_at) = ?
			ORDER BY created_at DESC`

	rows, err := s.db.Query(query, dateStr)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var alerts []AlertRecord
	for rows.Next() {
		var alert AlertRecord
		err := rows.Scan(
			&alert.ID, &alert.TaskID, &alert.TaskType,
			&alert.Job, &alert.Msg, &alert.CreatedAt, &alert.TaskCreated,
		)
		if err != nil {
			continue
		}
		alerts = append(alerts, alert)
	}

	return alerts, nil
}


// BuildCompactSummary processes alerts in memory to create compact summary
// Groups by TaskType:Job and collects task times for proper date formatting
func BuildCompactSummary(alerts []AlertRecord) []SummaryLine {
	groups := make(map[string]*summaryGroup)
	
	for _, alert := range alerts {
		key := alert.TaskType
		if alert.Job != "" {
			key += ":" + alert.Job
		}
		
		// Extract TaskTime from alert meta (not TaskCreated)
		taskTime := extractTaskTimeFromAlert(alert)
		
		if summary, exists := groups[key]; exists {
			summary.Count++
			summary.TaskTimes = append(summary.TaskTimes, taskTime)
		} else {
			groups[key] = &summaryGroup{
				Key:       key,
				Count:     1,
				TaskTimes: []time.Time{taskTime},
			}
		}
	}
	
	// Convert map to slice and format time ranges using tmpl.PrintDates
	var result []SummaryLine
	for _, summary := range groups {
		// Use tmpl.PrintDates for consistent formatting with existing Slack notifications
		timeRange := tmpl.PrintDates(summary.TaskTimes)
		
		result = append(result, SummaryLine{
			Key:       summary.Key,
			Count:     summary.Count,
			FirstTime: time.Time{}, // Not used anymore
			LastTime:  time.Time{}, // Not used anymore
			TimeRange: timeRange,
		})
	}
	
	// Use proper sorting (can be replaced with slices.Sort in Go 1.21+)
	sort.Slice(result, func(i, j int) bool {
		if result[i].Count != result[j].Count {
			return result[i].Count > result[j].Count // Sort by count descending
		}
		return result[i].Key < result[j].Key // Then by key ascending
	})
	
	return result
}

// summaryGroup is used internally for building compact summaries
type summaryGroup struct {
	Key       string
	Count     int
	TaskTimes []time.Time
}

// extractTaskTimeFromAlert extracts the task time from alert meta
// This uses the same logic as tmpl.TaskTime to get the proper task execution time
func extractTaskTimeFromAlert(alert AlertRecord) time.Time {
	// For now, fallback to TaskCreated time since we don't store meta in alert_records
	// TODO: Consider adding meta field to alert_records if TaskTime is critical for proper grouping
	return alert.TaskCreated
}

/*
// GetTasks retrieves all tasks with parsed URL and meta information
func (s *SQLite) GetTasks() ([]TaskView, error) {
	rows, err := s.db.Query(`
		SELECT id, type, job, info, meta, msg, result,
		       task_seconds, task_time, queue_seconds, queue_time,
		       created, started, ended
		FROM tasks
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []TaskView
	for rows.Next() {
		var t TaskView
		var createdStr, startedStr, endedStr string

		err := rows.Scan(
			&t.ID, &t.Type, &t.Job, &t.Info, &t.Meta, &t.Msg, &t.Result,
			&t.TaskSeconds, &t.TaskTime, &t.QueueSeconds, &t.QueueTime,
			&createdStr, &startedStr, &endedStr,
		)
		if err != nil {
			continue
		}

		// Parse timestamps
		t.Created, _ = time.Parse(time.RFC3339, createdStr)
		t.Started, _ = time.Parse(time.RFC3339, startedStr)
		t.Ended, _ = time.Parse(time.RFC3339, endedStr)

		// Parse URL if present
		if t.Info != "" {
			t.ParsedURL, _ = url.Parse(t.Info)
		}

		// Parse meta parameters
		if t.Meta != "" {
			v, err := url.ParseQuery(t.Meta)
			if err == nil {
				for _, val := range v {
					if len(val) > 0 {
						t.ParsedParam = val[0]
						break
					}
				}
			}
		}

		tasks = append(tasks, t)
	}

	return tasks, nil
}

// GetTaskByID retrieves a single task by ID with parsed URL and meta information
func (s *SQLite) GetTaskByID(id string) (*TaskView, error) {
	row := s.db.QueryRow(`
		SELECT id, type, job, info, meta, msg, result,
		       task_seconds, task_time, queue_seconds, queue_time,
		       created, started, ended
		FROM tasks
		WHERE id = ?
	`, id)

	var t TaskView
	var createdStr, startedStr, endedStr string

	err := row.Scan(
		&t.ID, &t.Type, &t.Job, &t.Info, &t.Meta, &t.Msg, &t.Result,
		&t.TaskSeconds, &t.TaskTime, &t.QueueSeconds, &t.QueueTime,
		&createdStr, &startedStr, &endedStr,
	)
	if err != nil {
		return nil, err
	}

	// Parse timestamps
	t.Created, _ = time.Parse(time.RFC3339, createdStr)
	t.Started, _ = time.Parse(time.RFC3339, startedStr)
	t.Ended, _ = time.Parse(time.RFC3339, endedStr)

	// Parse URL if present
	if t.Info != "" {
		t.ParsedURL, _ = url.Parse(t.Info)
	}

	// Parse meta parameters
	if t.Meta != "" {
		v, err := url.ParseQuery(t.Meta)
		if err == nil {
			for _, val := range v {
				if len(val) > 0 {
					t.ParsedParam = val[0]
					break
				}
			}
		}
	}

	return &t, nil
} */
