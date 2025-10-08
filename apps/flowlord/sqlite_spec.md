# SQLite Technical Specification
Purpose: Technical specification for converting flowlord to fully utilize SQLite for troubleshooting, historical records and configuration management.

## Current Implementation Status

**✅ COMPLETED:**
- Task Records: Single table with composite primary key implemented
- Alert Records: Simplified schema implemented and integrated
- File Messages: File processing history tracking implemented
- Web Dashboard: Alert, Files, and Task dashboards implemented
- Cache Integration: SQLite cache fully integrated into taskmaster

**❌ NOT IMPLEMENTED:**
- Database Maintenance: Simple configuration-driven backup and retention system

## Database Schema Design

### Alert Records
Store individual alert records immediately when tasks are sent to the alert channel. Replace file-based reporting with database storage.

**CURRENT IMPLEMENTATION:**
```sql
CREATE TABLE IF NOT EXISTS alert_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT,             -- task ID (can be empty for job send failures)
    task_time TIMESTAMP,      -- task time (can be empty)
    task_type TEXT NOT NULL,  -- task type for quick filtering
    job TEXT,                 -- task job for quick filtering
    msg TEXT NOT NULL,        -- alert message (contains alert context)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_alert_records_created_at ON alert_records (created_at);
```

**ORIGINAL SPECIFICATION (NOT IMPLEMENTED):**
```sql
CREATE TABLE alert_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    alert_type TEXT NOT NULL, -- 'retry_failed', 'alerted', 'unfinished', 'job_send_failed'
    task_type TEXT NOT NULL,  -- task type for quick filtering
    job TEXT,                 -- task job for quick filtering
    msg TEXT,                 -- alert message (can be task_msg or custom alert message)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    task_created TIMESTAMP    -- keep for alert timeline context
);
```

### File Topic Message History
**✅ IMPLEMENTED** - File processing history tracking with pattern matching results.

```sql
CREATE TABLE IF NOT EXISTS file_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    path TEXT NOT NULL,                    -- File path (e.g., "gs://bucket/path/file.json")
    size INTEGER,                          -- File size in bytes
    last_modified TIMESTAMP,               -- When file was modified (from file system/GCS metadata)
    received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- When the record was received (time.Now())
    task_time TIMESTAMP,                   -- Time extracted from path using tmpl.PathTime(sts.Path)
    task_ids TEXT,                         -- JSON array of task IDs (null if no matches)
    task_names TEXT                        -- JSON array of task names (type:job format, null if no matches)
);

-- Indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_file_messages_path ON file_messages (path);
CREATE INDEX IF NOT EXISTS idx_file_messages_received ON file_messages (received_at);

-- Example queries for file message history

-- Get all files processed in the last 24 hours
SELECT path, size, last_modified, received_at, task_time, task_ids, task_names
FROM file_messages 
WHERE received_at >= datetime('now', '-1 day')
ORDER BY received_at DESC;

-- Get files that have matching tasks (with task names for quick reference)
SELECT path, size, task_time, task_ids, task_names
FROM file_messages 
WHERE task_ids IS NOT NULL
ORDER BY received_at DESC;

-- Get files that didn't match any patterns (for debugging)
SELECT path, size, received_at, task_time
FROM file_messages 
WHERE task_ids IS NULL
ORDER BY received_at DESC;

-- JOIN files with their matching tasks (returns multiple rows per file if multiple tasks)
-- This is the key query for getting file + task details
SELECT 
    fm.id as file_id,
    fm.path,
    fm.task_time,
    fm.received_at,
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
ORDER BY fm.received_at DESC, fm.id;

-- Get files that created specific task types
SELECT DISTINCT
    fm.path,
    fm.task_time,
    fm.received_at,
    COUNT(*) as task_count
FROM file_messages fm,
     json_each(fm.task_ids) as t
JOIN task_log tl ON json_extract(t.value, '$') = tl.id
WHERE tl.type = 'data-load'
  AND fm.received_at >= datetime('now', '-1 day')
GROUP BY fm.id, fm.path, fm.task_time, fm.received_at
ORDER BY fm.received_at DESC;

-- Get file processing statistics by time period
SELECT 
    date(received_at) as date,
    COUNT(*) as total_files,
    SUM(CASE WHEN task_ids IS NOT NULL THEN 1 ELSE 0 END) as matched_files,
    SUM(CASE WHEN task_ids IS NULL THEN 1 ELSE 0 END) as unmatched_files,
    SUM(CASE 
        WHEN task_ids IS NOT NULL 
        THEN json_array_length(task_ids) 
        ELSE 0 
    END) as total_tasks_created
FROM file_messages 
WHERE received_at >= datetime('now', '-7 days')
GROUP BY date(received_at)
ORDER BY date DESC;

-- Find files by specific task time range
SELECT path, size, task_time, task_ids, task_names
FROM file_messages 
WHERE task_time >= datetime('now', '-1 day')
  AND task_time < datetime('now')
ORDER BY task_time DESC;

-- Get files with their task names (without joining task_log table)
-- This is useful for quick overview without needing task details
SELECT 
    fm.path,
    fm.task_time,
    fm.received_at,
    json_extract(t.value, '$') as task_id,
    json_extract(n.value, '$') as task_name
FROM file_messages fm,
     json_each(fm.task_ids) as t,
     json_each(fm.task_names) as n
WHERE fm.task_ids IS NOT NULL
  AND json_each.key = json_each.key  -- Ensures same index for both arrays
ORDER BY fm.received_at DESC;

-- Find files that created specific task types by name pattern
SELECT DISTINCT
    fm.path,
    fm.task_time,
    fm.received_at,
    fm.task_names
FROM file_messages fm
WHERE fm.task_names IS NOT NULL
  AND fm.task_names LIKE '%data-load%'
  AND fm.received_at >= datetime('now', '-1 day')
ORDER BY fm.received_at DESC;

-- Count files by task type (using task names)
SELECT 
    json_extract(n.value, '$') as task_type,
    COUNT(DISTINCT fm.id) as file_count
FROM file_messages fm,
     json_each(fm.task_names) as n
WHERE fm.task_names IS NOT NULL
  AND fm.received_at >= datetime('now', '-7 days')
GROUP BY json_extract(n.value, '$')
ORDER BY file_count DESC;
```

### Enhanced Task Recording
**✅ IMPLEMENTED** - Single table system for simplified task tracking.

```sql
-- Single table for all task records
CREATE TABLE IF NOT EXISTS task_records (
    id TEXT,
    type TEXT,
    job TEXT,
    info TEXT,
    result TEXT,               -- NULL if not completed
    meta TEXT,
    msg TEXT,                  -- NULL if not completed
    created TIMESTAMP,
    started TIMESTAMP,         -- NULL if not started
    ended TIMESTAMP,           -- NULL if not completed
    PRIMARY KEY (type, job, id, created)
);

-- Indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_task_records_type ON task_records (type);
CREATE INDEX IF NOT EXISTS idx_task_records_job ON task_records (job);
CREATE INDEX IF NOT EXISTS idx_task_records_created ON task_records (created);
CREATE INDEX IF NOT EXISTS idx_task_records_type_job ON task_records (type, job);
CREATE INDEX IF NOT EXISTS idx_task_records_date_range ON task_records (created, ended);

-- Create a view that calculates task and queue times
CREATE VIEW IF NOT EXISTS tasks AS
SELECT 
    task_records.id,
    task_records.type,
    task_records.job,
    task_records.info,
    task_records.meta,
    task_records.msg,
    task_records.result,
    -- Calculate task duration in seconds
    CAST((julianday(task_records.ended) - julianday(task_records.started)) * 24 * 60 * 60 AS INTEGER) as task_seconds,
    -- Format task duration as HH:MM:SS
    strftime('%H:%M:%S', 
        CAST((julianday(task_records.ended) - julianday(task_records.started)) * 24 * 60 * 60 AS INTEGER) / 3600 || ':' ||
        CAST((julianday(task_records.ended) - julianday(task_records.started)) * 24 * 60 * 60 AS INTEGER) % 3600 / 60 || ':' ||
        CAST((julianday(task_records.ended) - julianday(task_records.started)) * 24 * 60 * 60 AS INTEGER) % 60
    ) as task_time,
    -- Calculate queue time in seconds
    CAST((julianday(task_records.started) - julianday(task_records.created)) * 24 * 60 * 60 AS INTEGER) as queue_seconds,
    -- Format queue duration as HH:MM:SS
    strftime('%H:%M:%S', 
        CAST((julianday(task_records.started) - julianday(task_records.created)) * 24 * 60 * 60 AS INTEGER) / 3600 || ':' ||
        CAST((julianday(task_records.started) - julianday(task_records.created)) * 24 * 60 * 60 AS INTEGER) % 3600 / 60 || ':' ||
        CAST((julianday(task_records.started) - julianday(task_records.created)) * 24 * 60 * 60 AS INTEGER) % 60
    ) as queue_time,
    task_records.created,
    task_records.started,
    task_records.ended
FROM task_records;

-- Common query patterns
-- All completed tasks
SELECT * FROM task_records WHERE result IS NOT NULL;

-- Tasks by type and job
SELECT * FROM task_records WHERE type = ? AND job = ?;

-- Incomplete tasks
SELECT * FROM task_records WHERE result IS NULL;

-- Tasks by date range
SELECT * FROM task_records 
WHERE created BETWEEN ? AND ?;
```

**Implementation Changes:**
- Replace `events` + `task_log` with single `task_records` table
- Use composite PRIMARY KEY (type, job, id, created) to handle retries naturally
- Each retry creates a new record with unique `created` timestamp
- Track task creation (when first submitted) and completion (when finished)
- NULL values for `started`, `ended`, `result`, `msg` until completion
- Log conflicts on task creation (unexpected duplicates) for monitoring
- Maintain existing Cache interface for backward compatibility


## Database Maintenance Plan

### Simple Configuration-Driven Approach

The database maintenance system will be kept as simple as possible with all configuration defined in the Options struct at startup. No configuration values will be stored in the database except for the active migration script version.

**Important Note**: Database maintenance is non-critical to the application's core functionality. The system is designed to handle complete database loss gracefully and can start fresh without any issues. The database serves as a convenience for troubleshooting and historical records, but the application will continue to function normally even if the entire database is deleted and recreated.

### Options Struct Configuration

```go
type Options struct {
	LocalPath  string
	BackupPath string
	Retention  time.Duration // 90 days
}
```

### Database Initialization Logic

The SQLite database will be initialized using `sqlite.Options` with an `Open() error` method instead of `NewSQLite`. The initialization process will:

1. **Check for existing backup**: Use `file.Stat()` to check if a backup file exists at `BackupPath`
2. **Compare file dates**: Compare the modification dates of the local database file vs the backup file using `stat.Stats.ModTime`
3. **Use latest version**: Initialize with the most recent database file (local or backup)
4. **Fallback to local**: If no backup exists or backup is older, use the local database file

```go
// Database initialization logic
func (opts *Options) Open() error {
    // Check if backup exists using file.Stat
    backupStats, backupErr := file.Stat(opts.BackupPath, nil)
    localStats, localErr := file.Stat(opts.LocalPath, nil)
    
    // If backup exists and local doesn't, copy backup to local
    if backupErr == nil && localErr != nil {
        return opts.copyBackupToLocal()
    }
    
    // If both exist, compare modification times
    if backupErr == nil && localErr == nil {
        if backupStats.ModTime.After(localStats.ModTime) {
            // Backup is newer, copy backup to local
            return opts.copyBackupToLocal()
        }
    }
    
    // Use local database (either no backup or local is newer)
    return opts.openLocal()
}

// copyBackupToLocal copies the backup file to the local path
func (opts *Options) copyBackupToLocal() error {
    // Open backup file for reading
    backupReader, err := file.NewReader(opts.BackupPath, nil)
    if err != nil {
        return fmt.Errorf("failed to open backup file: %w", err)
    }
    defer backupReader.Close()
    
    // Create local file for writing
    localWriter, err := file.NewWriter(opts.LocalPath, nil)
    if err != nil {
        return fmt.Errorf("failed to create local file: %w", err)
    }
    defer localWriter.Close()
    
    // Copy backup to local
    _, err = io.Copy(localWriter, backupReader)
    if err != nil {
        return fmt.Errorf("failed to copy backup to local: %w", err)
    }
    
    // Open the local database
    return opts.openLocal()
}
```

### Default Configuration

```toml
# Database Maintenance Configuration
local_path = "./tasks.db"
backup_path = "./backups/tasks.db"
retention = "2160h"  # 90 days
```

### Migration Version Tracking

```sql
-- Simple migration version tracking (only config value stored in DB)
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Implementation Features

**Backup System:**
- Local backup creation at application start and stop
- Automatic copying of backup to local if backup is newer than local database
- Backup verification and integrity checks
- Simple file-based backup management

**Data Retention:**
- Single retention period applies to all tables (90 days default)
- Automatic cleanup based on age (retention duration)
- Graceful handling of incomplete tasks during cleanup
- Startup maintenance checks and cleanup

**Startup Maintenance:**
- Check for existing backup and compare file dates
- Copy backup to local if it's newer than local database
- Migration version checking and application
- Database integrity verification
- Cleanup of expired data based on retention period

### Maintenance Operations

**Startup Operations:**
1. Check for backup file existence using file.Stat
2. Compare local vs backup file modification dates
3. Copy backup to local if backup is newer
4. Check and apply any pending schema migrations
5. Verify database integrity
6. Run cleanup based on retention period
7. Create initial backup

**Runtime Operations:**
1. Periodic cleanup based on retention period

**Shutdown Operations:**
1. Create final backup before shutdown
2. Verify backup integrity

### Benefits of This Approach

- **Simple Configuration**: All settings in Options struct, no database config storage
- **Minimal Database Schema**: Only migration version tracking in database
- **Flexible**: Easy to adjust policies without database changes
- **Non-Critical**: Database loss is not catastrophic - application continues normally
- **Self-Healing**: System can start fresh and rebuild database as needed
- **Maintainable**: Clear separation of configuration and data
- **Scalable**: Configurable limits prevent unbounded growth

## Current Web Dashboard Implementation

**✅ IMPLEMENTED** - Basic web dashboards for monitoring and troubleshooting.

### Available Endpoints
```
GET    /web/alert?date=YYYY-MM-DD    - Alert dashboard for specific date
GET    /web/files?date=YYYY-MM-DD    - File processing dashboard for specific date  
GET    /web/task?date=YYYY-MM-DD     - Task summary dashboard for specific date
GET    /web/about                     - About page with system information
GET    /info                          - JSON status information
GET    /refresh                       - Manual workflow refresh trigger
```

### Dashboard Features
- **Alert Dashboard**: Shows alerts grouped by task type with time ranges
- **Files Dashboard**: Displays file processing history with pattern matching results
- **Task Dashboard**: Shows task execution summary with filtering by type, job, result
- **Date Navigation**: All dashboards support date parameter for historical viewing
- **Responsive Design**: Basic HTML/CSS with embedded static files


## Current UI Implementation

**✅ IMPLEMENTED** - Basic web dashboards with HTML templates and embedded static files.

### Alert Management Interface

#### Current Alert Dashboard Implementation
**✅ IMPLEMENTED** - Web-based alert dashboard with compact summary view.

**Current Features:**
- **Alert Summary View**: Compact grouped display by task type with time ranges
- **Date Navigation**: URL parameter `?date=YYYY-MM-DD` for historical viewing
- **HTML Templates**: Server-side rendering using embedded Go templates
- **Static Assets**: Embedded CSS and JavaScript files

**Current Implementation:**
```go
// GET /web/alert?date=YYYY-MM-DD
func (tm *taskMaster) htmlAlert(w http.ResponseWriter, r *http.Request) {
    dt, _ := time.Parse("2006-01-02", r.URL.Query().Get("date"))
    if dt.IsZero() {
        dt = time.Now()
    }
    alerts, err := tm.taskCache.GetAlertsByDate(dt)
    // ... render HTML template
}
```

**Current Alert Summary Format:**
```
Flowlord Alerts - Today (2025/09/19)
════════════════════════════════════════════════════════════
task.file-check:                       3  2025/09/19T11-2025/09/19T13
task.salesforce:                       9  2025/09/19T08-2025/09/19T15
task.data-load:                        1  2025/09/19T14
════════════════════════════════════════════════════════════
Total: 13 alerts across 3 task types
```

### File Processing Dashboard

**✅ IMPLEMENTED** - File processing history with pattern matching results.

**Current Features:**
- **File History**: Shows all files processed with size, timestamps, and task matches
- **Pattern Matching**: Displays which tasks were created from each file
- **Date Filtering**: Historical file processing data by date
- **JSON Arrays**: Task IDs and names stored as JSON for efficient querying

### Task Summary Dashboard

**✅ IMPLEMENTED** - Task execution summary with filtering capabilities.

**Current Features:**
- **Task Filtering**: Filter by type, job, result status
- **Time Calculations**: Task duration and queue time calculations via SQL view
- **Date Range**: View tasks for specific dates
- **Execution History**: Complete task lifecycle tracking