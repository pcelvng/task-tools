-- Dev seed: same-day tasks + alerts for local UI testing (Alerts ID → Tasks).
--
-- DataGrip: open tasks.db (repo root), then run this file against it.
-- Safe to re-run: only rows with id/task_id like 'seed-%' (and tagged unknown) are replaced.
-- Dates use UTC today so the dashboard date picker default (today) shows the data.

BEGIN;

DELETE FROM task_records WHERE id LIKE 'seed-%';
DELETE FROM alert_records WHERE task_id LIKE 'seed-%'
   OR (task_id = 'unknown' AND msg LIKE '[seed]%');

INSERT INTO task_records (id, type, job, info, result, meta, msg, created, started, ended) VALUES
-- One pipeline ID, three phases (what the Alerts ID click should list)
(
    'seed-pipeline', 'task1', 't2',
    '?date=' || date('now') || '&hour=' || date('now') || 'T11',
    'complete', 'cron=' || date('now') || 'T11', 'ok',
    date('now') || 'T11:00:00Z', date('now') || 'T11:00:05Z', date('now') || 'T11:00:30Z'
),
(
    'seed-pipeline', 'task2', '',
    '?date=' || date('now') || '&hour=' || date('now') || 'T12',
    'alert', 'cron=' || date('now') || 'T12', 'downstream alert: missing file',
    date('now') || 'T12:00:00Z', date('now') || 'T12:00:02Z', date('now') || 'T12:00:10Z'
),
(
    'seed-pipeline', 'task3', '',
    '?date=' || date('now') || '&hour=' || date('now') || 'T12',
    'error', 'cron=' || date('now') || 'T12', 'child failed after parent alert',
    date('now') || 'T12:05:00Z', date('now') || 'T12:05:01Z', date('now') || 'T12:05:08Z'
),
-- Same type:job:id, two created hours (second instance of the ID that day)
(
    'seed-hourly', 'task1', 't4',
    '?date=' || date('now') || '&hour=' || date('now') || 'T10',
    'complete', 'cron=' || date('now') || 'T10', 'hour 10 complete',
    date('now') || 'T10:00:00Z', date('now') || 'T10:00:01Z', date('now') || 'T10:00:02Z'
),
(
    'seed-hourly', 'task1', 't4',
    '?date=' || date('now') || '&hour=' || date('now') || 'T13',
    'alert', 'cron=' || date('now') || 'T13', 'hour 13 quality check failed',
    date('now') || 'T13:00:00Z', date('now') || 'T13:00:01Z', date('now') || 'T13:00:04Z'
),
(
    'seed-long-id-abcdefghijklmnopqrstuvwxyz12345', 'task1', 't2',
    '?date=' || date('now') || '&hour=' || date('now') || 'T14',
    'alert', 'cron=' || date('now') || 'T14', 'Validation failed: missing required field ''email''',
    date('now') || 'T14:00:00Z', date('now') || 'T14:00:01Z', date('now') || 'T14:00:01Z'
);

INSERT INTO alert_records (task_id, task_time, task_type, job, msg, created_at) VALUES
(
    'seed-pipeline', date('now') || 'T12:00:00Z', 'task2', '',
    'downstream alert: missing file', date('now') || 'T12:00:10Z'
),
(
    'seed-hourly', date('now') || 'T13:00:00Z', 'task1', 't4',
    'hour 13 quality check failed', date('now') || 'T13:00:04Z'
),
(
    'seed-long-id-abcdefghijklmnopqrstuvwxyz12345', date('now') || 'T14:00:00Z', 'task1', 't2',
    'Validation failed: missing required field ''email''', date('now') || 'T14:00:01Z'
),
-- No matching task; Alerts page should not link this ID
(
    'unknown', date('now') || 'T15:00:00Z', 'task1', 't2',
    '[seed] send failure with empty task id', date('now') || 'T15:00:00Z'
);

INSERT OR IGNORE INTO date_index ("date") VALUES (date('now'));
UPDATE date_index
SET has_tasks = 1, has_alerts = 1
WHERE "date" = date('now');

COMMIT;
