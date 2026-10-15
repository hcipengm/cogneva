-- Migration: 046_backup_runs
-- See the Postgres migration of the same name for what this table is and why
-- the record cannot live inside the package it records.
--
-- Dialect differences, and only these:
--   * `TIMESTAMPTZ` becomes `DATETIME`: MySQL's `TIMESTAMP` is bounded at 2038
--     and converts through the session's time zone, and a run's start instant is
--     both long-lived and read against instants written by other processes.
--   * The partial index over the open runs has no MySQL form -- MySQL has no
--     partial indexes -- so it covers `outcome` in full. The reader's query is
--     unchanged; it just reads a wider index than it needs.
--   * The closed set on `outcome` is a `CHECK` here rather than an enum column,
--     so both dialects accept the same statements and neither becomes the only
--     place the set is written down.
CREATE TABLE IF NOT EXISTS backup_runs (
    run_id                VARCHAR(255) NOT NULL,
    started_at            DATETIME NOT NULL,
    finished_at           DATETIME NULL,
    outcome               VARCHAR(16) NOT NULL,
    package               VARCHAR(255) NULL,
    bytes                 BIGINT NULL,
    keep_target           INTEGER NOT NULL,
    run_deadline_seconds  INTEGER NOT NULL,
    kept_after            INTEGER NULL,
    oldest_kept_at        DATETIME NULL,
    error                 TEXT NULL,
    PRIMARY KEY (run_id),
    CONSTRAINT backup_runs_outcome_is_known
        CHECK (outcome IN ('running', 'ok', 'failed', 'abandoned'))
);

CREATE INDEX backup_runs_started_at_idx ON backup_runs (started_at);

CREATE INDEX backup_runs_open_idx ON backup_runs (outcome, started_at);
