-- Mission store: append-only event log (source of truth) + derived effect
-- ledger (rebuildable cursor). Patterns proven in the LionClaw kernel:
-- gap-free per-stream sequence with a structural optimistic-concurrency PK
-- (session_turns), and an idempotent leased effect driver (channel_outbox).

CREATE TABLE missions (
    mission_id     TEXT PRIMARY KEY NOT NULL,
    workspace_dir  TEXT NOT NULL,
    objective      TEXT NOT NULL,
    created_at_ms  INTEGER NOT NULL
) STRICT;

CREATE TABLE mission_events (
    mission_id      TEXT    NOT NULL REFERENCES missions (mission_id),
    sequence_no     INTEGER NOT NULL CHECK (sequence_no > 0),
    recorded_at_ms  INTEGER NOT NULL,
    schema_version  INTEGER NOT NULL,
    payload_json    TEXT    NOT NULL,
    -- Set for two-event (Requested/outcome) pairs; class distinguishes the
    -- request from its single recorded outcome.
    idempotency_key TEXT,
    idem_class      TEXT CHECK (idem_class IN ('request', 'outcome')),
    CHECK ((idempotency_key IS NULL) = (idem_class IS NULL)),
    PRIMARY KEY (mission_id, sequence_no)
) STRICT;

-- At most one request and one outcome per idempotency key: replays surface
-- as constraint violations, never as duplicated history.
CREATE UNIQUE INDEX idx_mission_events_idem
    ON mission_events (mission_id, idem_class, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- Derived work queue: rebuildable from the log (fold's inflight set).
-- Lease fields follow the channel_outbox CAS-lease discipline.
CREATE TABLE mission_effects (
    effect_id           TEXT PRIMARY KEY NOT NULL, -- idempotency key of the request
    mission_id          TEXT NOT NULL REFERENCES missions (mission_id),
    source_seq          INTEGER NOT NULL,
    kind                TEXT NOT NULL CHECK (kind IN ('role_run', 'oracle_run')),
    request_json        TEXT NOT NULL,
    status              TEXT NOT NULL CHECK (status IN ('queued', 'leased', 'done', 'failed')),
    lease_owner         TEXT,
    lease_expires_at_ms INTEGER,
    created_at_ms       INTEGER NOT NULL,
    updated_at_ms       INTEGER NOT NULL,
    CHECK (
        status != 'leased'
        OR (lease_owner IS NOT NULL AND lease_expires_at_ms IS NOT NULL)
    )
) STRICT;

CREATE UNIQUE INDEX idx_mission_effects_source ON mission_effects (mission_id, source_seq);
CREATE INDEX idx_mission_effects_due ON mission_effects (mission_id, status);

-- Persisted fold snapshot: a discard-and-rebuildable cursor (exactly one live
-- row per mission), version-stamped so a reducer change forces a full refold.
CREATE TABLE mission_snapshots (
    mission_id       TEXT PRIMARY KEY NOT NULL REFERENCES missions (mission_id),
    upto_sequence_no INTEGER NOT NULL,
    reducer_version  INTEGER NOT NULL,
    state_json       TEXT NOT NULL,
    created_at_ms    INTEGER NOT NULL
) STRICT;
