package cache

import (
	"database/sql"
	_ "embed"
	"encoding/json"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pcelvng/task"
	"github.com/pcelvng/task/bus"
	_ "modernc.org/sqlite"

	"github.com/pcelvng/task-tools/file/stat"
	"github.com/pcelvng/task-tools/tmpl"
)

//go:embed schema.sql
var schema string

type SQLite struct {
	db  *sql.DB
	ttl time.Duration
	mu  sync.Mutex
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

	s.mu.Lock()
	defer s.mu.Unlock()

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
	s.mu.Lock()
	defer s.mu.Unlock()

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
	s.mu.Lock()
	defer s.mu.Unlock()

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
	s.mu.Lock()
	defer s.mu.Unlock()

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
	s.mu.Lock()
	defer s.mu.Unlock()

	// Allow empty task ID for job send failures - store as "unknown"
	taskID := t.ID
	if taskID == "" {
		taskID = "unknown"
	}

	// Extract job using helper function
	job := extractJobFromTask(t)

	// Get task time using tmpl.TaskTime function
	taskTime := tmpl.TaskTime(t)

	_, err := s.db.Exec(`
		INSERT INTO alert_records (task_id, task_time, task_type, job, msg)
		VALUES (?, ?, ?, ?, ?)
	`, taskID, taskTime, t.Type, job, message)

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
	s.mu.Lock()
	defer s.mu.Unlock()

	dateStr := date.Format("2006-01-02")

	query := `SELECT id, task_id, task_time, task_type, job, msg, created_at
			FROM alert_records 
			WHERE DATE(created_at) = ?
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
			&alert.ID, &alert.TaskID, &alert.TaskTime, &alert.Type,
			&alert.Job, &alert.Msg, &alert.CreatedAt,
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
		key := alert.Type
		if alert.Job != "" {
			key += ":" + alert.Job
		}

		// Extract TaskTime from alert meta (not TaskCreated)

		if summary, exists := groups[key]; exists {
			summary.Count++
			summary.TaskTimes = append(summary.TaskTimes, alert.TaskTime)
		} else {
			groups[key] = &summaryGroup{
				Key:       key,
				Count:     1,
				TaskTimes: []time.Time{alert.TaskTime},
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

// FileMessage represents a file message record
type FileMessage struct {
	ID           int       `json:"id"`
	Path         string    `json:"path"`
	Size         int64     `json:"size"`
	LastModified time.Time `json:"last_modified"`
	ReceivedAt   time.Time `json:"received_at"`
	TaskTime     time.Time `json:"task_time"`
	TaskIDs      []string  `json:"task_ids"`
	TaskNames    []string  `json:"task_names"`
}

// AddFileMessage stores a file message in the database
func (s *SQLite) AddFileMessage(sts stat.Stats, taskIDs []string, taskNames []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Parse last modified time from the created field
	var lastModified time.Time
	if sts.Created != "" {
		lastModified, _ = time.Parse(time.RFC3339, sts.Created)
	}

	// Extract task time from path
	taskTime := tmpl.PathTime(sts.Path)

	// Convert slices to JSON arrays
	var taskIDsJSON, taskNamesJSON sql.NullString
	if len(taskIDs) > 0 {
		if jsonBytes, err := json.Marshal(taskIDs); err == nil {
			taskIDsJSON = sql.NullString{String: string(jsonBytes), Valid: true}
		}
	}
	if len(taskNames) > 0 {
		if jsonBytes, err := json.Marshal(taskNames); err == nil {
			taskNamesJSON = sql.NullString{String: string(jsonBytes), Valid: true}
		}
	}

	_, err := s.db.Exec(`
		INSERT INTO file_messages (path, size, last_modified, task_time, task_ids, task_names)
		VALUES (?, ?, ?, ?, ?, ?)
	`, sts.Path, sts.Size, lastModified, taskTime, taskIDsJSON, taskNamesJSON)

	return err
}

// GetFileMessages retrieves file messages with optional filtering
func (s *SQLite) GetFileMessages(limit int, offset int) ([]FileMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	query := `
		SELECT id, path, size, last_modified, received_at, task_time, task_ids, task_names
		FROM file_messages
		ORDER BY received_at DESC
		LIMIT ? OFFSET ?
	`

	rows, err := s.db.Query(query, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []FileMessage
	for rows.Next() {
		var msg FileMessage
		var taskIDsJSON, taskNamesJSON sql.NullString

		err := rows.Scan(
			&msg.ID, &msg.Path, &msg.Size, &msg.LastModified, &msg.ReceivedAt,
			&msg.TaskTime, &taskIDsJSON, &taskNamesJSON,
		)
		if err != nil {
			continue
		}

		// Parse JSON arrays
		if taskIDsJSON.Valid {
			json.Unmarshal([]byte(taskIDsJSON.String), &msg.TaskIDs)
		}
		if taskNamesJSON.Valid {
			json.Unmarshal([]byte(taskNamesJSON.String), &msg.TaskNames)
		}

		messages = append(messages, msg)
	}

	return messages, nil
}

// GetFileMessagesByDate retrieves file messages for a specific date
func (s *SQLite) GetFileMessagesByDate(date time.Time) ([]FileMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	dateStr := date.Format("2006-01-02")
	query := `
		SELECT id, path, size, last_modified, received_at, task_time, task_ids, task_names
		FROM file_messages
		WHERE DATE(received_at) = ?
		ORDER BY received_at DESC
	`

	rows, err := s.db.Query(query, dateStr)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []FileMessage
	for rows.Next() {
		var msg FileMessage
		var taskIDsJSON, taskNamesJSON sql.NullString

		err := rows.Scan(
			&msg.ID, &msg.Path, &msg.Size, &msg.LastModified, &msg.ReceivedAt,
			&msg.TaskTime, &taskIDsJSON, &taskNamesJSON,
		)
		if err != nil {
			continue
		}

		// Parse JSON arrays
		if taskIDsJSON.Valid {
			json.Unmarshal([]byte(taskIDsJSON.String), &msg.TaskIDs)
		}
		if taskNamesJSON.Valid {
			json.Unmarshal([]byte(taskNamesJSON.String), &msg.TaskNames)
		}

		messages = append(messages, msg)
	}

	return messages, nil
}

// GetFileMessagesWithTasks retrieves file messages with their associated task details
func (s *SQLite) GetFileMessagesWithTasks(limit int, offset int) ([]FileMessageWithTasks, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	query := `
		SELECT 
			fm.id, fm.path, fm.task_time, fm.received_at,
			json_extract(t.value, '$') as task_id,
			tl.type as task_type,
			tl.job as task_job,
			tl.result as task_result,
			tl.created as task_created,
			tl.started as task_started,
			tl.ended as task_ended
		FROM file_messages fm,
			 json_each(fm.task_ids) as t
		JOIN task_log tl ON json_extract(t.value, '$') = tl.id
		WHERE fm.task_ids IS NOT NULL
		ORDER BY fm.received_at DESC, fm.id
		LIMIT ? OFFSET ?
	`

	rows, err := s.db.Query(query, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []FileMessageWithTasks
	for rows.Next() {
		var result FileMessageWithTasks
		var taskCreated, taskStarted, taskEnded sql.NullString

		err := rows.Scan(
			&result.FileID, &result.Path, &result.TaskTime, &result.ReceivedAt,
			&result.TaskID, &result.TaskType, &result.TaskJob, &result.TaskResult,
			&taskCreated, &taskStarted, &taskEnded,
		)
		if err != nil {
			continue
		}

		// Parse timestamps
		if taskCreated.Valid {
			result.TaskCreated, _ = time.Parse(time.RFC3339, taskCreated.String)
		}
		if taskStarted.Valid {
			result.TaskStarted, _ = time.Parse(time.RFC3339, taskStarted.String)
		}
		if taskEnded.Valid {
			result.TaskEnded, _ = time.Parse(time.RFC3339, taskEnded.String)
		}

		results = append(results, result)
	}

	return results, nil
}

// FileMessageWithTasks represents a file message with associated task details
type FileMessageWithTasks struct {
	FileID      int       `json:"file_id"`
	Path        string    `json:"path"`
	TaskTime    time.Time `json:"task_time"`
	ReceivedAt  time.Time `json:"received_at"`
	TaskID      string    `json:"task_id"`
	TaskType    string    `json:"task_type"`
	TaskJob     string    `json:"task_job"`
	TaskResult  string    `json:"task_result"`
	TaskCreated time.Time `json:"task_created"`
	TaskStarted time.Time `json:"task_started"`
	TaskEnded   time.Time `json:"task_ended"`
}
