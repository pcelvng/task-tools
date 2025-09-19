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
    file_path TEXT NOT NULL,
    file_size INTEGER,
    file_modified_at TIMESTAMP,
    received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    bucket_name TEXT,
    etag TEXT,
    md5_hash TEXT,
    match_found BOOLEAN DEFAULT FALSE,
    processing_time_ms INTEGER
);

CREATE TABLE file_pattern_matches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_message_id INTEGER NOT NULL,
    workflow_phase_id INTEGER NOT NULL,
    pattern TEXT NOT NULL,
    task_sent BOOLEAN DEFAULT FALSE,
    task_id TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (file_message_id) REFERENCES file_messages(id),
    FOREIGN KEY (workflow_phase_id) REFERENCES workflow_phases(id)
);

CREATE INDEX idx_file_messages_path ON file_messages (file_path);
CREATE INDEX idx_file_messages_received ON file_messages (received_at);
CREATE INDEX idx_file_pattern_matches_file ON file_pattern_matches (file_message_id);
```

### Enhanced Task Recording
Redesign task storage for optimal querying, deduplication, and system tracking.

```sql
-- Modify existing events table to include deduplication
ALTER TABLE events ADD COLUMN task_hash TEXT;
ALTER TABLE events ADD COLUMN first_seen TIMESTAMP;
ALTER TABLE events ADD COLUMN last_seen TIMESTAMP;

-- Enhanced task_log with better indexing and deduplication support
CREATE TABLE task_execution_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL,
    execution_sequence INTEGER NOT NULL, -- For tracking task progression
    type TEXT NOT NULL,
    job TEXT,
    info TEXT,
    result TEXT,
    meta TEXT,
    msg TEXT,
    created TIMESTAMP,
    started TIMESTAMP,
    ended TIMESTAMP,
    workflow_file TEXT,
    phase_matched BOOLEAN DEFAULT FALSE,
    children_triggered INTEGER DEFAULT 0,
    retry_count INTEGER DEFAULT 0,
    is_duplicate BOOLEAN DEFAULT FALSE,
    FOREIGN KEY (event_id) REFERENCES events(id)
);

CREATE INDEX idx_task_execution_type_job ON task_execution_log (type, job);
CREATE INDEX idx_task_execution_created ON task_execution_log (created);
CREATE INDEX idx_task_execution_result ON task_execution_log (result);
CREATE INDEX idx_task_execution_event_id ON task_execution_log (event_id);
CREATE INDEX idx_task_execution_workflow ON task_execution_log (workflow_file);
```

### Task Relationships and Dependencies
Track task.Done message processing and child task triggering.

```sql
CREATE TABLE task_relationships (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_task_id TEXT NOT NULL,
    child_task_id TEXT NOT NULL,
    relationship_type TEXT DEFAULT 'triggered', -- 'triggered', 'retry', 'failed_retry'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    workflow_phase_id INTEGER,
    FOREIGN KEY (parent_task_id) REFERENCES events(id),
    FOREIGN KEY (child_task_id) REFERENCES events(id),
    FOREIGN KEY (workflow_phase_id) REFERENCES workflow_phases(id)
);

CREATE INDEX idx_task_relationships_parent ON task_relationships (parent_task_id);
CREATE INDEX idx_task_relationships_child ON task_relationships (child_task_id);
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

### Backward Compatibility Strategy
- Maintain existing Cache interface during migration
- Gradual replacement of memory storage with SQLite
- Preserve current API contracts and behavior
- Seamless transition without downtime

### Performance Considerations
- Connection pooling and prepared statement caching
- Asynchronous operations for non-critical writes
- Query optimization with proper indexing strategy
- WAL mode for better concurrent access
- Vacuum and analyze operations for maintenance