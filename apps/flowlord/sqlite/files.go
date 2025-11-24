package sqlite

import (
	"database/sql"
	"encoding/json"
	"time"

	"github.com/pcelvng/task-tools/file/stat"
	"github.com/pcelvng/task-tools/tmpl"
)

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

	if err != nil {
		return err
	}

	// Update date index - use current time for received_at
	s.updateDateIndex(time.Now().Format(time.RFC3339), "files")

	return nil
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



