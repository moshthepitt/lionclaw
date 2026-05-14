CREATE TABLE session_turns_new (
    turn_id TEXT PRIMARY KEY NOT NULL,
    session_id TEXT NOT NULL,
    sequence_no INTEGER NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('normal', 'retry', 'continue', 'runtime_control')),
    status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed', 'timed_out', 'cancelled', 'interrupted')),
    display_user_text TEXT NOT NULL,
    prompt_user_text TEXT NOT NULL,
    assistant_text TEXT NOT NULL,
    error_code TEXT,
    error_text TEXT,
    runtime_id TEXT NOT NULL,
    started_at_ms INTEGER NOT NULL,
    finished_at_ms INTEGER,
    FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE,
    UNIQUE (session_id, sequence_no)
);

INSERT INTO session_turns_new (
    turn_id,
    session_id,
    sequence_no,
    kind,
    status,
    display_user_text,
    prompt_user_text,
    assistant_text,
    error_code,
    error_text,
    runtime_id,
    started_at_ms,
    finished_at_ms
)
SELECT
    turn_id,
    session_id,
    sequence_no,
    kind,
    status,
    display_user_text,
    prompt_user_text,
    assistant_text,
    error_code,
    error_text,
    runtime_id,
    started_at_ms,
    finished_at_ms
FROM session_turns;

DROP TABLE session_turns;
ALTER TABLE session_turns_new RENAME TO session_turns;

CREATE INDEX IF NOT EXISTS idx_session_turns_session_sequence
    ON session_turns (session_id, sequence_no);

CREATE INDEX IF NOT EXISTS idx_session_turns_session_started
    ON session_turns (session_id, started_at_ms);

CREATE INDEX IF NOT EXISTS idx_session_turns_recent_failures
    ON session_turns (started_at_ms DESC, turn_id DESC)
    WHERE status IN ('failed', 'timed_out', 'cancelled', 'interrupted');
