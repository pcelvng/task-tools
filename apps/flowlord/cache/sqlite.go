package cache

import (
	"database/sql"
	_ "embed"
	"net/url"
	"strings"
	"time"

	"github.com/pcelvng/task"
	"github.com/pcelvng/task/bus"
	"modernc.org/sqlite"
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

	// Register the SQLite driver
	sql.Register("sqlite", &sqlite.Driver{})

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
