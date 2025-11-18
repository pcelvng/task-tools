package sqlite

import (
	"fmt"
)

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
	TableBytes int64   `json:"table_bytes"`
	TableHuman string  `json:"table_human"`
	IndexBytes int64   `json:"index_bytes"`
	IndexHuman string  `json:"index_human"`
	TotalBytes int64   `json:"total_bytes"`
	TotalHuman string  `json:"total_human"`
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

	dbPath := s.LocalPath
	if s.BackupPath != "" {
		dbPath = s.BackupPath
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
	var totalTableSize int64

	for _, tableName := range tables {
		// Get row count
		var rowCount int64
		err := s.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&rowCount)
		if err != nil {
			continue // Skip tables we can't read
		}

		// Calculate more accurate table size
		var tableBytes int64
		if rowCount > 0 {
			// Try to get actual table size using dbstat if available
			var actualSize int64
			err := s.db.QueryRow(fmt.Sprintf(`
				SELECT SUM(pgsize) FROM dbstat WHERE name = '%s' AND aggregate = 1
			`, tableName)).Scan(&actualSize)

			if err == nil && actualSize > 0 {
				// Use actual size from dbstat
				tableBytes = actualSize
			}
		}

		// Get index sizes for this table
		indexBytes := s.getIndexSize(tableName)
		totalBytes := tableBytes + indexBytes
		totalTableSize += totalBytes

		percentage := float64(0)
		if totalSize > 0 {
			percentage = float64(totalBytes) / float64(totalSize) * 100
		}

		stats = append(stats, TableStat{
			Name:       tableName,
			RowCount:   rowCount,
			TableBytes: tableBytes,
			TableHuman: formatBytes(tableBytes),
			IndexBytes: indexBytes,
			IndexHuman: formatBytes(indexBytes),
			TotalBytes: totalBytes,
			TotalHuman: formatBytes(totalBytes),
			Percentage: percentage,
		})
	}

	return stats, nil
}

// getIndexSize calculates the total size of all indexes for a table
func (s *SQLite) getIndexSize(tableName string) int64 {
	// Get all indexes for this table
	rows, err := s.db.Query(fmt.Sprintf(`
		SELECT name FROM sqlite_master 
		WHERE type='index' AND tbl_name='%s' AND name NOT LIKE 'sqlite_%%'
	`, tableName))
	if err != nil {
		return 0
	}
	defer rows.Close()

	var totalIndexSize int64
	for rows.Next() {
		var indexName string
		if err := rows.Scan(&indexName); err != nil {
			continue
		}

		// Try to get actual index size using dbstat
		var indexSize int64
		err := s.db.QueryRow(fmt.Sprintf(`
			SELECT SUM(pgsize) FROM dbstat WHERE name = '%s' AND aggregate = 1
		`, indexName)).Scan(&indexSize)

		if err == nil && indexSize > 0 {
			totalIndexSize += indexSize
		}
	}

	return totalIndexSize
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


