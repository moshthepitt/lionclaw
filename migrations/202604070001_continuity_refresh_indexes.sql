CREATE INDEX IF NOT EXISTS idx_session_turns_recent_failures
    ON session_turns (started_at_ms DESC, turn_id DESC)
    WHERE status IN ('failed', 'timed_out', 'cancelled', 'interrupted');

CREATE INDEX IF NOT EXISTS idx_channel_peers_pending_recent
    ON channel_peers (updated_at_ms DESC, channel_id, peer_id)
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_scheduler_jobs_attention_recent
    ON scheduler_jobs (updated_at_ms DESC, job_id DESC)
    WHERE last_status IN ('failed', 'dead_letter', 'interrupted');
