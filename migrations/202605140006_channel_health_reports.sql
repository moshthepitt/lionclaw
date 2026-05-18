CREATE TABLE IF NOT EXISTS channel_health_reports (
    report_id TEXT PRIMARY KEY NOT NULL,
    channel_id TEXT NOT NULL,
    reporter_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('ok', 'warning', 'error')),
    checks_json TEXT NOT NULL,
    observed_at_ms INTEGER NOT NULL,
    created_at_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_channel_health_reports_latest
    ON channel_health_reports(channel_id, observed_at_ms DESC);
