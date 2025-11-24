package sqlite

import (
	"fmt"
	"log"
	"time"
)

// updateDateIndex updates the date_index table for a given timestamp and data type
// This method should be called within an existing lock (s.mu.Lock)
func (s *SQLite) updateDateIndex(timestamp, dataType string) {
	// Parse timestamp to extract date
	t, err := time.Parse(time.RFC3339, timestamp)
	if err != nil {
		// Try other formats if RFC3339 fails
		t, err = time.Parse("2006-01-02 15:04:05", timestamp)
		if err != nil {
			return // Skip if we can't parse the timestamp
		}
	}

	dateStr := t.Format("2006-01-02")

	// Determine which column to update
	var column string
	switch dataType {
	case "tasks":
		column = "has_tasks"
	case "alerts":
		column = "has_alerts"
	case "files":
		column = "has_files"
	default:
		return
	}

	// First try to insert the date, if it already exists, update the column
	_, err = s.db.Exec("INSERT OR IGNORE INTO date_index (date) VALUES (?)", dateStr)
	if err != nil {
		log.Printf("WARNING: Failed to insert date into date_index for %s on %s: %v", dataType, dateStr, err)
		return
	}

	// Now update the specific column
	query := fmt.Sprintf("UPDATE date_index SET %s = 1 WHERE DATE = ?", column)
	_, err = s.db.Exec(query, dateStr)
	if err != nil {
		log.Printf("WARNING: Failed to update date_index for %s on %s: %v", dataType, dateStr, err)
	}
}

// GetDatesWithData returns a list of dates (YYYY-MM-DD format) that have any data
// for tasks, alerts, or files within the retention period
func (s *SQLite) GetDatesWithData() ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	query := `
		SELECT DISTINCT date_val FROM (
			SELECT DISTINCT DATE(created) AS date_val FROM task_records
			WHERE created >= datetime('now', '-' || ? || ' days')
			UNION
			SELECT DISTINCT DATE(created_at) AS date_val FROM alert_records
			WHERE created_at >= datetime('now', '-' || ? || ' days')
			UNION
			SELECT DISTINCT DATE(received_at) AS date_val FROM file_messages
			WHERE received_at >= datetime('now', '-' || ? || ' days')
		)
		ORDER BY date_val DESC
	`

	// Use retention period in days (default 90)
	retentionDays := int(s.Retention.Hours() / 24)
	if retentionDays == 0 {
		retentionDays = 90
	}

	rows, err := s.db.Query(query, retentionDays, retentionDays, retentionDays)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var dates []string
	for rows.Next() {
		var date string
		if err := rows.Scan(&date); err != nil {
			continue
		}
		dates = append(dates, date)
	}

	return dates, nil
}

// DatesByType returns a list of dates (YYYY-MM-DD format) that have data for the specified type
// dataType can be "tasks", "alerts", or "files"
// This uses the date_index table for instant lookups
func (s *SQLite) DatesByType(dataType string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var column string
	switch dataType {
	case "tasks":
		column = "has_tasks"
	case "alerts":
		column = "has_alerts"
	case "files":
		column = "has_files"
	default:
		return nil, fmt.Errorf("invalid data type: %s (must be 'tasks', 'alerts', or 'files')", dataType)
	}

	query := fmt.Sprintf(`
		SELECT DATE 
		FROM date_index
		WHERE %s = 1
		ORDER BY DATE DESC
	`, column)

	rows, err := s.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var dates []string
	for rows.Next() {
		var date string
		if err := rows.Scan(&date); err != nil {
			continue
		}
		dates = append(dates, date)
	}

	return dates, nil
}

// GetDatesWithTasks returns a list of dates (YYYY-MM-DD format) that have task records
func (s *SQLite) GetDatesWithTasks() ([]string, error) {
	return s.DatesByType("tasks")
}

// GetDatesWithAlerts returns a list of dates (YYYY-MM-DD format) that have alert records
func (s *SQLite) GetDatesWithAlerts() ([]string, error) {
	return s.DatesByType("alerts")
}

// GetDatesWithFiles returns a list of dates (YYYY-MM-DD format) that have file message records
func (s *SQLite) GetDatesWithFiles() ([]string, error) {
	return s.DatesByType("files")
}

// RebuildDateIndex scans all tables and rebuilds the date_index table
// This should be called once during migration or can be exposed as an admin endpoint
func (s *SQLite) RebuildDateIndex() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Println("Starting date_index rebuild...")

	// Clear existing index
	_, err := s.db.Exec("DELETE FROM date_index")
	if err != nil {
		return fmt.Errorf("failed to clear date_index: %w", err)
	}

	// Populate from task_records
	// First insert the dates, then update the has_tasks flag
	_, err = s.db.Exec(`
		INSERT OR IGNORE INTO date_index (DATE, has_tasks)
		SELECT DISTINCT DATE(created), 1
		FROM task_records
	`)
	if err != nil {
		return fmt.Errorf("failed to populate date_index from tasks: %w", err)
	}

	// Populate from alert_records
	// Insert new dates and update has_alerts for existing dates
	_, err = s.db.Exec(`
		INSERT OR IGNORE INTO date_index (DATE)
		SELECT DISTINCT DATE(created_at)
		FROM alert_records
		WHERE DATE(created_at) NOT IN (SELECT DATE FROM date_index)
	`)
	if err != nil {
		return fmt.Errorf("failed to insert new dates from alerts: %w", err)
	}

	_, err = s.db.Exec(`
		UPDATE date_index
		SET has_alerts = 1
		WHERE DATE IN (SELECT DISTINCT DATE(created_at) FROM alert_records)
	`)
	if err != nil {
		return fmt.Errorf("failed to update date_index from alerts: %w", err)
	}

	// Populate from file_messages
	// Insert new dates and update has_files for existing dates
	_, err = s.db.Exec(`
		INSERT OR IGNORE INTO date_index (DATE)
		SELECT DISTINCT DATE(received_at)
		FROM file_messages
		WHERE DATE(received_at) NOT IN (SELECT DATE FROM date_index)
	`)
	if err != nil {
		return fmt.Errorf("failed to insert new dates from files: %w", err)
	}

	_, err = s.db.Exec(`
		UPDATE date_index
		SET has_files = 1
		WHERE DATE IN (SELECT DISTINCT DATE(received_at) FROM file_messages)
	`)
	if err != nil {
		return fmt.Errorf("failed to update date_index from files: %w", err)
	}

	log.Println("Successfully rebuilt date_index")
	return nil
}



