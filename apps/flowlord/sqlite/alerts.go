package sqlite

import (
	"sort"
	"time"

	"github.com/pcelvng/task"

	"github.com/pcelvng/task-tools/tmpl"
)

// AlertRecord represents an alert stored in the database
type AlertRecord struct {
	ID        int64     `json:"id"`
	TaskID    string    `json:"task_id"`
	TaskTime  time.Time `json:"task_time"`
	Type      string    `json:"type"`
	Job       string    `json:"job"`
	Msg       string    `json:"msg"`
	CreatedAt time.Time `json:"created_at"`
}

// SummaryLine represents a grouped alert summary for dashboard display
type SummaryLine struct {
	Key       string `json:"key"`        // "task.type:job"
	Count     int    `json:"count"`      // number of alerts
	TimeRange string `json:"time_range"` // formatted time range
}

// summaryGroup is used internally for building compact summaries
type summaryGroup struct {
	Key       string
	Count     int
	TaskTimes []time.Time
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

	if err != nil {
		return err
	}

	// Update date index - use current time for alert created_at
	s.updateDateIndex(time.Now().Format(time.RFC3339), "alerts")

	return nil
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

// GetAlertsAfterTime retrieves all alerts created after a specific time
func (s *SQLite) GetAlertsAfterTime(afterTime time.Time) ([]AlertRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	query := `SELECT id, task_id, task_time, task_type, job, msg, created_at
			FROM alert_records 
			WHERE created_at > ?
			ORDER BY created_at ASC`

	rows, err := s.db.Query(query, afterTime.Format("2006-01-02 15:04:05"))
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


