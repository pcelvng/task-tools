package cache

import (
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jbsmith7741/go-tools/appenderr"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task/bus"
	_ "modernc.org/sqlite"

	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task-tools/file/stat"
	"github.com/pcelvng/task-tools/tmpl"
)

//go:embed schema.sql
var schema string

type SQLite struct {
	LocalPath  string
	BackupPath string

	TaskTTL   time.Duration `toml:"task-ttl" comment:"time that tasks are expected to have completed in. This values tells the cache how long to keep track of items and alerts if items haven't completed when the cache is cleared"`
	Retention time.Duration // 90 days

	db    *sql.DB
	fOpts file.Options
	//ttl time.Duration
	mu sync.Mutex
}

// Open the sqlite DB. If localPath doesn't exist then check if BackupPath exists and copy it to localPath
// ?: should this open the workflow file and load that into the database as well? 
func (o *SQLite) Open(workflowPath string, fOpts file.Options) error {
	o.fOpts = fOpts
	if o.TaskTTL < time.Hour {
		o.TaskTTL = time.Hour
	}

	backupSts, _ := file.Stat(o.BackupPath, &fOpts)
	localSts, _ := file.Stat(o.LocalPath, &fOpts)

	if localSts.Size == 0 && backupSts.Size > 0 {
		log.Printf("Restoring local DB from backup %s", o.BackupPath)
		// no local file but backup exists so copy it down
		if err := copyFiles(o.BackupPath, o.LocalPath, fOpts); err != nil {
			log.Println(err) // TODO: should this be fatal?
		}
	}

	// Open the database
	db, err := sql.Open("sqlite", o.LocalPath)
	if err != nil {
		return err
	}
	o.db = db

	// Execute the schema if the migration version is not the same as the current schema version
	//TODO: version the schema and migrate if needed
	_, err = db.Exec(schema)
	if err != nil {
		return err
	}

	//TODO: load workflow file into the database

	return nil
}

func copyFiles(src, dst string, fOpts file.Options) error {
	r, err := file.NewReader(src, &fOpts)
	if err != nil {
		return fmt.Errorf("init reader err: %w", err)
	}
	w, err := file.NewWriter(dst, &fOpts)
	if err != nil {
		return fmt.Errorf("init writer err: %w", err)
	}
	_, err = io.Copy(w, r)
	if err != nil {
		return fmt.Errorf("copy err: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("close writer err: %w", err)
	}
	return r.Close()
}

// Close the DB connection and copy the current file to the backup location
func (o *SQLite) Close() error {
	errs := appenderr.New()
	errs.Add(o.db.Close())
	if o.BackupPath != "" {
		log.Printf("Backing up DB to %s", o.BackupPath)
		errs.Add(o.Sync())
	}
	return errs.ErrOrNil()
}

// Sync the local DB to the backup location
func (o *SQLite) Sync() error {
	return copyFiles(o.LocalPath, o.BackupPath, o.fOpts)
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
	err = tx.QueryRow("SELECT COUNT(*) FROM task_records").Scan(&total)
	if err != nil {
		return Stat{}
	}

	// Get expired task records
	rows, err := tx.Query(`
		SELECT id, type, job, info, result, meta, msg,
		       created, started, ended
		FROM task_records
		WHERE created < ?
	`, t.Add(-s.TaskTTL))
	if err != nil {
		return Stat{}
	}
	defer rows.Close()

	// Process expired records
	for rows.Next() {
		var task task.Task
		err := rows.Scan(
			&task.ID, &task.Type, &task.Job, &task.Info, &task.Result,
			&task.Meta, &task.Msg, &task.Created, &task.Started, &task.Ended,
		)
		if err != nil {
			continue
		}

		// Check if task is incomplete
		if task.Result == "" {
			tasks = append(tasks, task)
		}

		// TODO: Deletion logic commented out for later implementation
		// Delete the expired record
		// _, err = tx.Exec(`
		// 	DELETE FROM task_records 
		// 	WHERE id = ? AND type = ? AND job = ? AND created = ?
		// `, task.ID, task.Type, task.Job, task.Created)
		// if err != nil {
		// 	continue
		// }
	}

	// Get remaining count
	var remaining int
	err = tx.QueryRow("SELECT COUNT(*) FROM task_records").Scan(&remaining)
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

// CheckIncompleteTasks checks for tasks that have not completed within the TTL period
// and adds them to the alerts table with deduplication. Returns count of alerts added.
// Uses a JOIN query to efficiently find incomplete tasks without existing alerts.
func (s *SQLite) CheckIncompleteTasks() Stat {
	s.mu.Lock()
	defer s.mu.Unlock()

	tasks := make([]task.Task, 0)
	alertsAdded := 0
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
			ar.msg LIKE 'INCOMPLETE:%' AND 
			ar.created_at > datetime('now', '-1 day')
		)
		WHERE tr.created < ? 
		AND tr.result = '' 
		AND ar.id IS NULL
	`, t.Add(-s.TaskTTL))
	if err != nil {
		return Stat{}
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

		if err == nil {
			alertsAdded++
		}
	}

	return Stat{
		Count:       alertsAdded,
		Removed:     0, // No deletion in this method
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
		FROM task_records
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
		SELECT id, path, SIZE, last_modified, received_at, task_time, task_ids, task_names
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
		SELECT id, path, SIZE, last_modified, received_at, task_time, task_ids, task_names
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
			json_extract(t.value, '$') AS task_id,
			tl.type AS task_type,
			tl.job AS task_job,
			tl.result AS task_result,
			tl.created AS task_created,
			tl.started AS task_started,
			tl.ended AS task_ended
		FROM file_messages fm,
			 json_each(fm.task_ids) AS t
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

// GetTasksByDate retrieves tasks for a specific date with optional filtering using the tasks view
func (s *SQLite) GetTasksByDate(date time.Time, taskType, job, result string) ([]TaskView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	dateStr := date.Format("2006-01-02")

	// Build query with optional filters using the tasks view
	query := `SELECT id, type, job, info, result, meta, msg, task_seconds, task_time, queue_seconds, queue_time, created, started, ended
		FROM tasks
		WHERE DATE(created) = ?`
	args := []interface{}{dateStr}

	if taskType != "" {
		query += " AND type = ?"
		args = append(args, taskType)
	}

	if job != "" {
		query += " AND job = ?"
		args = append(args, job)
	}

	if result != "" {
		query += " AND result = ?"
		args = append(args, result)
	}

	query += " ORDER BY created DESC"

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
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
			continue
		}
		tasks = append(tasks, t)
	}

	return tasks, nil
}

// GetTaskSummaryByDate creates a summary of tasks for a specific date
func (s *SQLite) GetTaskSummaryByDate(date time.Time) (map[string]*Stats, error) {
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
				ExecTimes:      &DurationStats{},
			}
			data[key] = stat
		}
		stat.Add(t)
	}

	return data, nil
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

// DBSizeInfo contains database size information
type DBSizeInfo struct {
	TotalSize string `json:"total_size"`
	PageCount int64  `json:"page_count"`
	PageSize  int64  `json:"page_size"`
	DBPath    string `json:"db_path"`
}

// TableStat contains information about a database table
type TableStat struct {
	Name       string  `json:"name"`
	RowCount   int64   `json:"row_count"`
	SizeBytes  int64   `json:"size_bytes"`
	SizeHuman  string  `json:"size_human"`
	Percentage float64 `json:"percentage"`
}

// GetDBSize returns database size information
func (s *SQLite) GetDBSize() (*DBSizeInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Get page count and page size
	var pageCount, pageSize int64
	err := s.db.QueryRow("PRAGMA page_count").Scan(&pageCount)
	if err != nil {
		return nil, err
	}

	err = s.db.QueryRow("PRAGMA page_size").Scan(&pageSize)
	if err != nil {
		return nil, err
	}

	// Get database file path
	var dbPath string
	err = s.db.QueryRow("PRAGMA database_list").Scan(&dbPath, nil, nil)
	if err != nil {
		// If we can't get the path, use a default
		dbPath = "unknown"
	}

	totalSize := pageCount * pageSize
	totalSizeStr := formatBytes(totalSize)

	return &DBSizeInfo{
		TotalSize: totalSizeStr,
		PageCount: pageCount,
		PageSize:  pageSize,
		DBPath:    dbPath,
	}, nil
}

// GetTableStats returns statistics for all tables in the database
func (s *SQLite) GetTableStats() ([]TableStat, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Get total database size first
	var totalSize int64
	err := s.db.QueryRow("SELECT page_count * page_size FROM pragma_page_count(), pragma_page_size()").Scan(&totalSize)
	if err != nil {
		return nil, err
	}

	// Get list of tables
	rows, err := s.db.Query(`
		SELECT name FROM sqlite_master 
		WHERE type='table' AND name NOT LIKE 'sqlite_%'
		ORDER BY name
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		tables = append(tables, tableName)
	}

	var stats []TableStat
	for _, tableName := range tables {
		// Get row count
		var rowCount int64
		err := s.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&rowCount)
		if err != nil {
			continue // Skip tables we can't read
		}

		// Get table size using pragma table_info and estimate
		// This is an approximation since SQLite doesn't provide exact table sizes
		var sizeBytes int64
		if rowCount > 0 {
			// Estimate size based on row count and average row size
			// This is a rough approximation
			avgRowSize := int64(200) // Estimated average row size in bytes
			sizeBytes = rowCount * avgRowSize
		}

		percentage := float64(0)
		if totalSize > 0 {
			percentage = float64(sizeBytes) / float64(totalSize) * 100
		}

		stats = append(stats, TableStat{
			Name:       tableName,
			RowCount:   rowCount,
			SizeBytes:  sizeBytes,
			SizeHuman:  formatBytes(sizeBytes),
			Percentage: percentage,
		})
	}

	return stats, nil
}

// formatBytes converts bytes to human readable format
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
