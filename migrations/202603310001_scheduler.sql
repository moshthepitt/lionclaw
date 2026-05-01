CREATE TABLE IF NOT EXISTS scheduler_jobs (
    job_id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    runtime_id TEXT NOT NULL,
    schedule_kind TEXT NOT NULL CHECK (schedule_kind IN ('once', 'interval', 'cron')),
    schedule_json TEXT NOT NULL,
    prompt_text TEXT NOT NULL,
    delivery_json TEXT,
    retry_attempts INTEGER NOT NULL DEFAULT 1,
    next_run_at_ms INTEGER,
    running_run_id TEXT,
    last_run_at_ms INTEGER,
    last_status TEXT CHECK (last_status IS NULL OR last_status IN ('running', 'completed', 'failed', 'dead_letter', 'interrupted')),
    last_error TEXT,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_scheduler_jobs_due
    ON scheduler_jobs (enabled, next_run_at_ms);

CREATE INDEX IF NOT EXISTS idx_scheduler_jobs_running
    ON scheduler_jobs (running_run_id);

CREATE TABLE IF NOT EXISTS scheduler_job_runs (
    run_id TEXT PRIMARY KEY NOT NULL,
    job_id TEXT NOT NULL,
    attempt_no INTEGER NOT NULL,
    trigger_kind TEXT NOT NULL CHECK (trigger_kind IN ('schedule', 'manual', 'retry', 'recovery')),
    scheduled_for_ms INTEGER,
    started_at_ms INTEGER NOT NULL,
    finished_at_ms INTEGER,
    status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed', 'dead_letter', 'interrupted')),
    session_id TEXT,
    turn_id TEXT,
    delivery_status TEXT CHECK (delivery_status IN ('pending', 'delivered', 'failed', 'not_requested')),
    error_text TEXT,
    FOREIGN KEY (job_id) REFERENCES scheduler_jobs(job_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_scheduler_job_runs_job_started
    ON scheduler_job_runs (job_id, started_at_ms DESC);

CREATE INDEX IF NOT EXISTS idx_scheduler_job_runs_status
    ON scheduler_job_runs (status, started_at_ms DESC);

CREATE TABLE IF NOT EXISTS scheduler_state (
    state_id INTEGER PRIMARY KEY NOT NULL CHECK (state_id = 1),
    lease_owner TEXT,
    lease_expires_at_ms INTEGER,
    last_tick_started_at_ms INTEGER,
    last_tick_finished_at_ms INTEGER
);

INSERT OR IGNORE INTO scheduler_state
    (state_id, lease_owner, lease_expires_at_ms, last_tick_started_at_ms, last_tick_finished_at_ms)
VALUES
    (1, NULL, NULL, NULL, NULL);
