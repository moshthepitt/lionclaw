-- Mission store: append-only event log (source of truth). Patterns proven in
-- the LionClaw kernel:
-- gap-free per-stream sequence with a structural optimistic-concurrency PK
-- (session_turns) and request/outcome uniqueness enforced in the log.

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
    effect_id       TEXT,
    effect_class    TEXT CHECK (effect_class IN ('request', 'outcome')),
    CHECK ((effect_id IS NULL) = (effect_class IS NULL)),
    PRIMARY KEY (mission_id, sequence_no)
) STRICT;

-- At most one request and one outcome per effect identity: replays surface
-- as constraint violations, never as duplicated history.
CREATE UNIQUE INDEX idx_mission_events_effect
    ON mission_events (mission_id, effect_class, effect_id)
    WHERE effect_id IS NOT NULL;

-- Persisted fold snapshot: a discard-and-rebuildable cursor (exactly one live
-- row per mission), version-stamped so a reducer change forces a full refold.
CREATE TABLE mission_snapshots (
    mission_id       TEXT PRIMARY KEY NOT NULL REFERENCES missions (mission_id),
    upto_sequence_no INTEGER NOT NULL,
    reducer_version  INTEGER NOT NULL,
    state_json       TEXT NOT NULL,
    created_at_ms    INTEGER NOT NULL
) STRICT;
