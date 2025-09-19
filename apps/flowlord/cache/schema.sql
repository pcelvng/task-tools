-- SQL schema for the task cache
CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    completed BOOLEAN,
    last_update TIMESTAMP
);

CREATE TABLE IF NOT EXISTS task_log (
    id TEXT,
    type TEXT,
    job TEXT,
    info TEXT,
    result TEXT,
    meta TEXT,
    msg TEXT,
    created TIMESTAMP,
    started TIMESTAMP,
    ended TIMESTAMP,
    event_id TEXT,
    FOREIGN KEY (event_id) REFERENCES events(id)
);

CREATE INDEX IF NOT EXISTS task_log_created ON task_log (created);
CREATE INDEX IF NOT EXISTS task_log_started ON task_log (started);
CREATE INDEX IF NOT EXISTS task_log_type ON task_log (type);
CREATE INDEX IF NOT EXISTS task_log_job ON task_log (job);
CREATE INDEX IF NOT EXISTS task_log_event_id ON task_log (event_id);

-- Create a view that calculates task and queue times
CREATE VIEW IF NOT EXISTS tasks AS
SELECT 
    task_log.id,
    task_log.type,
    task_log.job,
    task_log.info,
    -- SQLite doesn't have parse_url function, we'll need to handle this in Go
    task_log.meta,
    -- SQLite doesn't have parse_param function, we'll need to handle this in Go
    task_log.msg,
    task_log.result,
    -- Calculate task duration in seconds
    CAST((julianday(task_log.ended) - julianday(task_log.started)) * 24 * 60 * 60 AS INTEGER) as task_seconds,
    -- Format task duration as HH:MM:SS
    strftime('%H:%M:%S', 
        CAST((julianday(task_log.ended) - julianday(task_log.started)) * 24 * 60 * 60 AS INTEGER) / 3600 || ':' ||
        CAST((julianday(task_log.ended) - julianday(task_log.started)) * 24 * 60 * 60 AS INTEGER) % 3600 / 60 || ':' ||
        CAST((julianday(task_log.ended) - julianday(task_log.started)) * 24 * 60 * 60 AS INTEGER) % 60
    ) as task_time,
    -- Calculate queue time in seconds
    CAST((julianday(task_log.started) - julianday(task_log.created)) * 24 * 60 * 60 AS INTEGER) as queue_seconds,
    -- Format queue duration as HH:MM:SS
    strftime('%H:%M:%S', 
        CAST((julianday(task_log.started) - julianday(task_log.created)) * 24 * 60 * 60 AS INTEGER) / 3600 || ':' ||
        CAST((julianday(task_log.started) - julianday(task_log.created)) * 24 * 60 * 60 AS INTEGER) % 3600 / 60 || ':' ||
        CAST((julianday(task_log.started) - julianday(task_log.created)) * 24 * 60 * 60 AS INTEGER) % 60
    ) as queue_time,
    task_log.created,
    task_log.started,
    task_log.ended
FROM task_log;

-- Alert records table for storing alert events
CREATE TABLE IF NOT EXISTS alert_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT,             -- task ID (can be empty for job send failures)
    task_type TEXT NOT NULL,  -- task type for quick filtering
    job TEXT,                 -- task job for quick filtering
    msg TEXT NOT NULL,        -- alert message (contains alert context)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    task_created TIMESTAMP    -- keep for alert timeline context
);

CREATE INDEX IF NOT EXISTS idx_alert_records_created_at ON alert_records (created_at);