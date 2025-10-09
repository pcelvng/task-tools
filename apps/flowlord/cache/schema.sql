-- SQL schema for the task cache
-- Single table for all task records with composite primary key
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
    -- SQLite doesn't have parse_url function, we'll need to handle this in Go
    task_records.meta,
    -- SQLite doesn't have parse_param function, we'll need to handle this in Go
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

-- Alert records table for storing alert events
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

-- File message history table for tracking file processing
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

-- Workflow file tracking (replaces in-memory workflow file cache)
CREATE TABLE IF NOT EXISTS workflow_files (
    file_path TEXT PRIMARY KEY,
    file_hash TEXT NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_modified TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

-- Workflow phases (matches Phase struct exactly)
CREATE TABLE IF NOT EXISTS workflow_phases (
    workflow_file_path TEXT NOT NULL,
    task TEXT NOT NULL,           -- topic:job format (e.g., "data-load:hourly")
    depends_on TEXT,
    rule TEXT,                    -- URI query parameters (e.g., "cron=0 0 * * *&offset=1h")
    template TEXT,
    retry INTEGER DEFAULT 0,      -- threshold of times to retry
    status TEXT                  -- phase status info (warnings, errors, validation messages)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_workflow_phases_task ON workflow_phases (task);
CREATE INDEX IF NOT EXISTS idx_workflow_phases_depends_on ON workflow_phases (depends_on);
CREATE INDEX IF NOT EXISTS idx_workflow_phases_status ON workflow_phases (status);