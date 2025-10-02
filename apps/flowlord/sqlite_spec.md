# SQLite Technical Specification
Purpose: Technical specification for converting flowlord to fully utilize SQLite for troubleshooting, historical records and configuration management.

## Database Schema Design

### Alert Records
Store individual alert records immediately when tasks are sent to the alert channel. Replace file-based reporting with database storage.

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

CREATE INDEX idx_alert_records_created_at ON alert_records (created_at);

-- Example queries for alert management

-- Startup check and day-based dashboard - get today's alerts
SELECT * FROM alert_records 
WHERE date(created_at) = date('now')
ORDER BY created_at DESC;

-- Get alerts for specific date (for dashboard)
SELECT * FROM alert_records 
WHERE date(created_at) = ?
ORDER BY created_at DESC;

-- Get full task details for an alert (when needed)
SELECT ar.*, t.* FROM alert_records ar
JOIN task_log t ON ar.task_id = t.id
WHERE ar.id = ?;

-- Raw data for compact summary (grouping done in Go)
-- This provides the data for: "task.file-check: 3  2025/09/19T11-2025/09/19T13"
SELECT task_type, job, task_created, created_at 
FROM alert_records 
WHERE date(created_at) = date('now')
ORDER BY task_type, job, created_at;
```

### Workflow Phase Storage
Store loaded workflow files and phase configurations for dependency mapping and validation.

```sql
CREATE TABLE workflow_files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_path TEXT UNIQUE NOT NULL,
    file_hash TEXT NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_modified TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

CREATE TABLE workflow_phases (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_file_id INTEGER NOT NULL,
    task_name TEXT NOT NULL,
    job_name TEXT,
    depends_on TEXT,
    rule TEXT,
    template TEXT,
    retry_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (workflow_file_id) REFERENCES workflow_files(id),
    UNIQUE(workflow_file_id, task_name, job_name)
);

CREATE TABLE workflow_dependencies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_phase_id INTEGER NOT NULL,
    child_phase_id INTEGER NOT NULL,
    dependency_type TEXT DEFAULT 'direct', -- 'direct', 'conditional'
    FOREIGN KEY (parent_phase_id) REFERENCES workflow_phases(id),
    FOREIGN KEY (child_phase_id) REFERENCES workflow_phases(id),
    UNIQUE(parent_phase_id, child_phase_id)
);

CREATE INDEX idx_workflow_phases_task ON workflow_phases (task_name, job_name);
CREATE INDEX idx_workflow_dependencies_parent ON workflow_dependencies (parent_phase_id);
CREATE INDEX idx_workflow_dependencies_child ON workflow_dependencies (child_phase_id);
```

### File Topic Message History
Log every message that comes through the files topic with pattern matching results.

```sql
CREATE TABLE file_messages (
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
CREATE INDEX idx_file_messages_path ON file_messages (path);
CREATE INDEX idx_file_messages_received ON file_messages (received_at);

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
Replace dual-table system with single table for simplified task tracking.

```sql
-- Remove existing tables
DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS task_log;

-- Single table for all task records
CREATE TABLE task_records (
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
CREATE INDEX idx_task_records_type ON task_records (type);
CREATE INDEX idx_task_records_job ON task_records (job);
CREATE INDEX idx_task_records_created ON task_records (created);
CREATE INDEX idx_task_records_type_job ON task_records (type, job);
CREATE INDEX idx_task_records_date_range ON task_records (created, ended);

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

### Workflow and Phase
This replaces the in-memory workflow.Cache system with a persistent SQLite-based approach while maintaining the exact same interface and naming conventions.

#### Updated Schema Design

```sql
-- Workflow file tracking (replaces in-memory workflow file cache)
CREATE TABLE workflow_files (
    file_path TEXT PRIMARY KEY,
    file_hash TEXT NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_modified TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

-- Workflow phases (matches Phase struct exactly)
CREATE TABLE workflow_phases (
    workflow_file_path TEXT NOT NULL,
    task TEXT NOT NULL,           -- topic:job format (e.g., "data-load:hourly")
    depends_on TEXT,
    rule TEXT,                    -- URI query parameters (e.g., "cron=0 0 * * *&offset=1h")
    template TEXT,
    retry INTEGER DEFAULT 0,      -- threshold of times to retry
    status TEXT,                  -- phase status info (warnings, errors, validation messages)
    PRIMARY KEY (workflow_file_path, task),
);

-- Task relationships are generated dynamically from workflow_phases
-- No separate table needed - relationships are derived from depends_on field
-- This approach is simpler, more maintainable, and always up-to-date

-- Indexes for performance
CREATE INDEX idx_workflow_phases_task ON workflow_phases (task);
CREATE INDEX idx_workflow_phases_depends_on ON workflow_phases (depends_on);
CREATE INDEX idx_workflow_phases_status ON workflow_phases (status);
```

#### Benefits of Human-Readable Keys

**1. Direct File Path References**
- `workflow_files.file_path` is the primary key (e.g., "workflows/data-load.toml")
- No need to join tables to see which file a phase belongs to
- Easy to identify and debug workflow issues

**2. Composite Primary Keys for Phases**
- `(workflow_file_path, task_name, job_name)` uniquely identifies each phase
- Directly readable: `("workflows/data-load.toml", "data-load", "hourly")`
- No surrogate IDs to remember or map

**3. Dynamic Task Relationships**
- Task relationships are generated from `depends_on` field in workflow_phases
- No separate table to maintain or keep in sync
- Always up-to-date with current workflow configuration
- Simpler schema with fewer tables and foreign keys

**4. Phase Status Tracking**
- `status` field stores validation messages, warnings, and errors for each phase
- Replaces console logging with persistent database storage
- Enables querying and filtering phases by status
- Provides better debugging and monitoring capabilities

**Status Field Usage Examples:**
- `"invalid phase: rule and dependsOn are blank"`
- `"no valid rule found: cron=invalid"`
- `"parent task not found: data-load"`
- `"ignored rule: cron=0 0 * * *"`
- `"warning: retry count exceeds recommended limit"`
- `""` (empty string for phases with no issues)

**Example Queries (Much More Readable):**

```sql
-- Find all phases in a specific workflow file
SELECT task, depends_on, rule, status
FROM workflow_phases 
WHERE workflow_file_path = 'workflows/data-load.toml';

-- Find phases that depend on a specific task type
SELECT workflow_file_path, task, rule, status
FROM workflow_phases 
WHERE depends_on = 'data-load';

-- Find phases by topic (using LIKE for topic:job matching)
SELECT workflow_file_path, task, depends_on, rule, status
FROM workflow_phases 
WHERE task LIKE 'data-load:%';

-- Find phases with warnings or errors
SELECT workflow_file_path, task, status
FROM workflow_phases 
WHERE status IS NOT NULL AND status != '';

-- Find phases with specific status messages
SELECT workflow_file_path, task, status
FROM workflow_phases 
WHERE status LIKE '%warning%' OR status LIKE '%error%';

-- Generate task relationships dynamically (parent -> child)
SELECT 
    parent.depends_on as parent_task,
    parent.task as child_task,
    parent.workflow_file_path,
    parent.rule as child_rule,
    parent.status as child_status
FROM workflow_phases parent
WHERE parent.depends_on IS NOT NULL AND parent.depends_on != '';

-- Find all children of a specific task
SELECT 
    child.task as child_task,
    child.workflow_file_path,
    child.rule as child_rule,
    child.status as child_status
FROM workflow_phases child
WHERE child.depends_on = 'data-load';

-- Find all parents of a specific task
SELECT 
    parent.depends_on as parent_task,
    parent.workflow_file_path,
    parent.rule as parent_rule,
    parent.status as parent_status
FROM workflow_phases parent
WHERE parent.task = 'data-load:hourly';

-- Get workflow file info with phase count and status summary
SELECT 
    wf.file_path, 
    wf.file_hash, 
    wf.loaded_at, 
    COUNT(wp.task) as phase_count,
    COUNT(CASE WHEN wp.status IS NOT NULL AND wp.status != '' THEN 1 END) as phases_with_status
FROM workflow_files wf
LEFT JOIN workflow_phases wp ON wf.file_path = wp.workflow_file_path
GROUP BY wf.file_path;
```

#### Maintained Interface Design
The new SQLite-based implementation will maintain the exact same interface as the current `workflow.Cache`:

```go
// Keep existing workflow.Cache interface unchanged
type Cache interface {
    // Existing methods remain exactly the same
    Search(task, job string) (path string, ph Phase)
    Get(t task.Task) Phase
    Children(t task.Task) []Phase
    Refresh() (changedFiles []string, err error)
    IsDir() bool
    Close() error
}

// Keep existing Phase struct unchanged
type Phase struct {
    Task      string // Should use Topic() and Job() for access
    Rule      string
    DependsOn string // Task that the previous workflow depends on
    Retry     int
    Template  string // template used to create the task
}

// Keep existing Workflow struct unchanged
type Workflow struct {
    Checksum string  // md5 hash for the file to check for changes
    Phases   []Phase `toml:"phase"`
}
```

#### Implementation Strategy
- **Same Package**: Keep everything in `workflow` package
- **Same Structs**: Maintain `Phase` and `Workflow` structs exactly as they are
- **Same Methods**: All existing methods return the same types and behavior
- **SQLite Backend**: Replace in-memory storage with SQLite persistence
- **Zero Breaking Changes**: All existing unit tests continue to work unchanged

#### Key Benefits
1. **Persistence**: Workflow configurations survive restarts
2. **Historical Tracking**: Full audit trail of task relationships
3. **Performance**: Indexed queries for fast dependency resolution
4. **Scalability**: No memory limitations for large workflow sets
5. **Debugging**: Rich querying capabilities for troubleshooting
6. **Simplified Architecture**: Single SQLite instance replaces in-memory cache

#### Implementation Plan

**Phase 1: Update Workflow Package**
1. Modify existing `workflow.Cache` to use SQLite backend
2. Keep all existing interfaces, structs, and method signatures unchanged
3. Add SQLite persistence to workflow file loading
4. Implement task relationship tracking within existing structure

**Phase 2: Update Flowlord Integration**
1. No changes needed to `taskmaster.go` - same interface
2. Update workflow loading to use SQLite persistence
3. Add task relationship recording to existing task processing
4. Maintain all existing method calls and behavior

**Phase 3: Handler Updates**
1. Update handlers to query SQLite for workflow data
2. Add task relationship queries to existing endpoints
3. Enhance alert system with SQLite-based data
4. Maintain existing response formats

**Phase 4: Testing and Validation**
1. All existing unit tests continue to work unchanged
2. Add SQLite-specific integration tests
3. Performance testing for SQLite queries
4. Migration testing from existing workflow files

#### Migration Strategy

**Seamless Replacement Approach:**
- Keep existing `workflow.Cache` interface and structs
- Replace in-memory storage with SQLite persistence
- Zero breaking changes to existing code
- All unit tests continue to work without modification

**Key Implementation Details:**

```go
// Keep existing Cache struct, add SQLite backend
type Cache struct {
    db  *sql.DB
    path string
    isDir bool
    fOpts file.Options
    mutex sync.RWMutex
    // Remove: Workflows map[string]Workflow
}

// Keep existing methods with SQLite implementation using simplified schema
func (c *Cache) Search(task, job string) (path string, ph Phase) {
    // Query: SELECT workflow_file_path, task, depends_on, rule, template, retry, status
    //        FROM workflow_phases WHERE task = ? OR task LIKE ?
    //        (where ? is either exact match or topic:job format)
    // Return same results as before, with status info available
}

func (c *Cache) Get(t task.Task) Phase {
    // Query: SELECT workflow_file_path, task, depends_on, rule, template, retry, status
    //        FROM workflow_phases WHERE task = ? OR task LIKE ?
    //        (where ? is either exact match or topic:job format)
    // Return same Phase struct with status info
}

func (c *Cache) Children(t task.Task) []Phase {
    // Query: SELECT workflow_file_path, task, depends_on, rule, template, retry, status
    //        FROM workflow_phases WHERE depends_on = ? OR depends_on LIKE ?
    //        (where ? matches the task type or topic:job format)
    // Return same []Phase slice with status info
}

func (c *Cache) Refresh() (changedFiles []string, err error) {
    // Check file hashes against workflow_files table using file_path as key
    // Load changed files into SQLite using file_path as primary key
    // Return same changedFiles list
}

// Dynamic task relationship queries (no separate table needed)
func (c *Cache) GetTaskRelationships(parentTask string) ([]Phase, error) {
    // Query: SELECT workflow_file_path, task, depends_on, rule, template, retry
    //        FROM workflow_phases WHERE depends_on = ?
    //        Returns all phases that depend on the parent task
}

func (c *Cache) GetTaskDependencies(childTask string) ([]Phase, error) {
    // Query: SELECT workflow_file_path, task, depends_on, rule, template, retry
    //        FROM workflow_phases WHERE task = ?
    //        Returns the phase that defines the child task and its dependencies
}
```

**Database Schema Migration:**
- Add new tables alongside existing ones
- Migrate existing workflow data if needed
- Remove old tables after successful migration
- Maintain data integrity throughout process

#### Required Code Changes

**1. Package Imports**
```go
// Keep existing import - no changes needed
import "github.com/pcelvng/task-tools/workflow"
```

**2. TaskMaster Struct Update**
```go
// Keep existing struct - no changes needed
type taskMaster struct {
    // ... other fields
    *workflow.Cache  // Same as before
    // ... other fields
}
```

**3. Workflow Loading Changes**
```go
// Keep existing code - no changes needed
if tm.Cache, err = workflow.New(tm.path, tm.fOpts); err != nil {
    return fmt.Errorf("workflow setup %w", err)
}
```

**4. Dependency Resolution Updates**
```go
// Keep existing code - no changes needed
phase := tm.Cache.Get(task)
children := tm.Cache.Children(task)
```

**5. Handler Updates**
```go
// Keep existing code - no changes needed
// All existing method calls work the same
// Only internal implementation changes to use SQLite
```

## Database Maintenance

### Backup and Restoration
```sql
-- Create metadata table for backup tracking
CREATE TABLE backup_metadata (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    backup_type TEXT NOT NULL, -- 'scheduled', 'shutdown', 'manual'
    backup_path TEXT NOT NULL,
    backup_size INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    verified BOOLEAN DEFAULT FALSE,
    gcs_path TEXT
);
```

**Implementation Requirements**:
- Automated GCS backup during application shutdown
- Periodic backup scheduling (configurable intervals)
- Restoration logic comparing local vs GCS timestamps
- Database schema migration support
- Backup verification and integrity checks

### Retention and Size Management
```sql
-- Create retention policies table
CREATE TABLE retention_policies (
    table_name TEXT PRIMARY KEY,
    retention_days INTEGER NOT NULL,
    max_records INTEGER,
    cleanup_enabled BOOLEAN DEFAULT TRUE,
    last_cleanup TIMESTAMP
);

-- Default retention policies
INSERT INTO retention_policies (table_name, retention_days, max_records) VALUES
('alert_records', 90, 100000),
('file_messages', 30, 50000),
('file_pattern_matches', 30, 50000),
('task_execution_log', 7, 500000),
('task_relationships', 7, 100000);
```

## API Endpoints Specification

### Required REST Endpoints
```
GET    /api/metrics              - Database size and performance metrics
GET    /api/tasks                - Task search and filtering
POST   /api/tasks/search         - Advanced task search
GET    /api/alerts               - Alert history and management
GET    /api/files                - File processing history
GET    /api/workflows            - Workflow configuration and status
GET    /api/summary              - Dashboard summary data
DELETE /api/cleanup              - Manual cleanup operations
POST   /api/backup               - Manual backup trigger
```

### Response Formats
```json
// GET /api/summary response
{
  "time_period": "24h",
  "task_summary": {
    "total": 1500,
    "completed": 1200,
    "failed": 50,
    "in_progress": 250
  },
  "by_type": {
    "data-load": {"completed": 400, "failed": 10, "avg_duration": "2m30s"},
    "transform": {"completed": 800, "failed": 40, "avg_duration": "5m15s"}
  }
}

// GET /api/alerts response
{
  "alerts": [
    {
      "id": 123,
      "task_id": "abc-123",
      "alert_type": "retry_exhausted",
      "severity": "critical",
      "message": "Task failed after 3 retries",
      "created_at": "2023-12-01T10:30:00Z",
      "dashboard_link": "/dashboard/alerts/2023-12-01"
    }
  ],
  "pagination": {"page": 1, "total": 150}
}
```

## UI Component Specifications

### Summary Status Dashboard
- Time-based filtering (hour/day/week/month)
- Task breakdown by type and job with completion statistics
- Average execution time calculations and trends
- Error rate visualization and alerting thresholds

### Alert Management Interface

#### Alert Dashboard Design
Replace the current file-based alert reporting with a comprehensive web dashboard for alert management and analysis.

#### Dashboard Components

**1. Alert Summary View (Compact)**
```
Flowlord Alerts - Today (2025/09/19)
════════════════════════════════════════════════════════════
task.file-check:                       3  2025/09/19T11-2025/09/19T13
task.salesforce:                       9  2025/09/19T08-2025/09/19T15
task.data-load:                        1  2025/09/19T14
════════════════════════════════════════════════════════════
Total: 13 alerts across 3 task types
```

**Implementation:**
- Use simple SQL: `SELECT * FROM alert_records WHERE date(created_at) = date('now')`
- Process in Go to create compact summary (replaces current Slack formatting logic)
- Group by `task_type:job`, count occurrences, calculate time ranges
- Display both count and time span (first alert → last alert)

**2. Detailed Alert Timeline**
- Chronological list of all alerts for the selected day
- Expandable rows showing full task details (via JOIN with task_log)
- Click-through to individual task execution details
- Color coding by alert_type: retry_exhausted (red), alert_result (orange), unfinished (yellow)

**3. Alert Filtering and Navigation**
- Date picker for viewing historical alerts
- Filter by alert_type, task_type, job
- Quick links: Today, Yesterday, Last 7 days
- Search by task_id or message content

#### Dashboard Implementation

**Simple HTML Rendering Approach:**
- Go queries SQLite directly and renders HTML via templates
- No JSON APIs needed for dashboard UI
- Vanilla JavaScript only for basic interactions (date picker, auto-refresh)
- Leverage existing `handler/alert.tmpl` pattern

**Page Structure:**
```html
/alerts                    - Today's alert dashboard (default)
/alerts/2025-09-19        - Specific date dashboard
/alerts/summary           - Compact summary view only
```

**Implementation Pattern:**
```go
func (tm *taskMaster) alertDashboard(w http.ResponseWriter, r *http.Request) {
    date := chi.URLParam(r, "date") // or default to today
    alerts := tm.taskCache.GetAlertsByDate(date) // single query
    summary := buildCompactSummary(alerts)       // process in memory, same logic as current Slack formatting
    
    data := struct {
        Date    string
        Alerts  []AlertRecord
        Summary []SummaryLine  // processed summary data
        Total   int
    }{date, alerts, summary, len(alerts)}
    
    tmpl.Execute(w, data) // render HTML directly
}

// Process alerts in memory to create compact summary
func buildCompactSummary(alerts []AlertRecord) []SummaryLine {
    groups := make(map[string]*SummaryLine)
    for _, alert := range alerts {
        key := alert.TaskType + ":" + alert.Job
        if summary, exists := groups[key]; exists {
            summary.Count++
            summary.updateTimeRange(alert.TaskCreated)
        } else {
            groups[key] = &SummaryLine{
                Key: key, Count: 1, 
                FirstTime: alert.TaskCreated, 
                LastTime: alert.TaskCreated,
            }
        }
    }
    return mapToSlice(groups) // convert to sorted slice
}
```

**Optional API Endpoint (for troubleshooting only):**
```
GET /api/alerts?date=YYYY-MM-DD&format=json  - JSON output for debugging/scripts
```

**Features:**
- Server-side rendering for fast page loads
- Simple date navigation via URL parameters
- Auto-refresh via basic JavaScript setInterval
- Minimal client-side complexity

#### Startup Alert Integration

**On Application Start:**
1. Query today's alerts: `SELECT * FROM alert_records WHERE date(created_at) = date('now')`
2. If alerts found, generate compact summary in existing Slack format
3. Send single startup notification: "Found X alerts from today" + summary + dashboard link
4. Dashboard link: `/alerts` (always points to today)

**Benefits:**
- Replaces file-based alert reports entirely
- Real-time alert visibility via web dashboard  
- Maintains familiar compact summary format for Slack
- Historical alert analysis and trending
- Better debugging with full task context
- Mobile-friendly alert monitoring

### File Processing Dashboard
- Searchable file processing history with pattern match results
- File processing timeline and status indicators
- Pattern rule debugging and performance metrics
- File processing success/failure analytics

### Workflow Visualization
- Interactive dependency graphs showing phase relationships
- Phase configuration validation and issue highlighting
- Next scheduled execution times and cron schedules
- Workflow performance analytics and bottleneck identification

### Task Search and Management
- Advanced search by type, job, result, uniqueID, time range
- Task lifecycle tracking and execution history
- Retry and failure analysis with root cause identification
- Bulk operations for task management

## Technical Architecture

### Database Design Principles
- Single SQLite file for simplicity and performance
- Optimized indexes for common query patterns
- Prepared statements for security and performance
- Batch operations for high-throughput scenarios
- Foreign key constraints for data integrity

### Workflow Architecture Strategy
- Complete replacement of in-memory storage with SQLite persistence
- Maintain exact same workflow.Cache interface and behavior
- Simplified architecture with single SQLite instance
- Enhanced functionality with persistent task relationship tracking
- Zero breaking changes - all existing code continues to work

### Performance Considerations
- Connection pooling and prepared statement caching
- Asynchronous operations for non-critical writes
- Query optimization with proper indexing strategy
- WAL mode for better concurrent access
- Vacuum and analyze operations for maintenance