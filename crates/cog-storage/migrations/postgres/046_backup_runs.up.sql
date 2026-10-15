-- Migration: 046_backup_runs
-- The backup account: one row per backup run, and the only record of a run that
-- is not carried inside the thing it records.
--
-- Every run already writes down what it did. It writes it into `manifest.json`,
-- inside the package the run produced, and that placement fails three separate
-- ways:
--
--   * The record shares the retention knob with the thing it records.
--     `enforce_retention` runs only on the success path, so a package pruned at
--     the keep bound takes its own manifest with it. The account of a run is
--     deleted by the same act that deletes the run's output.
--   * The count of packages is the knob, not a fact. A week of successes and a
--     week of failures look identical from the directory listing -- a failing
--     run prunes nothing, so the old packages sit exactly where they were -- and
--     the one number that tells them apart, the age of the newest package, had
--     no reader anywhere in the tree.
--   * The record is unreadable from outside the volume. It cannot be queried,
--     so no rule can be written over it, and the only reader is whoever opens
--     the package by hand.
--
-- This table is that record, moved out of the package and into the database the
-- backup is already connected to. The writing process needs nothing new to reach
-- it: the backup runs the platform's own binary, holds a PostgreSQL client and
-- the platform's own credentials.
--
-- `run_id` is the package stem -- the same clock read that names the package,
-- which is what makes `started_at` and the package name two views of one
-- instant rather than two measurements taken a moment apart.
--
-- `outcome` is a closed set of three, and `running` is one of them because the
-- row is written before the run does any work. It is the only account of a run
-- that was killed -- an OOM kill, an eviction, a node drain -- and a table that
-- only recorded terminal outcomes would be silent in exactly the case where the
-- record is the only surviving witness. The transition out of `running` happens
-- at most once: a terminal row is never rewritten, so a run that reports twice
-- cannot overwrite what it first said.
--
-- `bytes` is the package file's size and nothing else; `keep_target` is the keep
-- bound this run was actually run under, recorded per row so that a changed knob
-- does not make the old history read as if it had always been the new one;
-- `kept_after` and `oldest_kept_at` are what this run's own retention pass left
-- behind, and they are the two halves of one claim -- "N packages are kept" is
-- not falsifiable from a count, because a healthy week and a week of failures
-- produce the same count. The pair is written together and read together.
--
-- `run_deadline_seconds` is the other bound this run was run under, recorded per
-- row for the same reason: it is the maximum the run's own scheduler allows it to
-- take, and it is what makes an open row conclusive. `running` is written before
-- the run does any work, and a process that is killed cannot come back to correct
-- its own row -- so without a declared bound an open row is a claim that can
-- never be settled by anyone, and it stays open forever. With the bound on the
-- row, the row itself carries the answer: past `started_at + run_deadline_seconds`
-- the run is not late, it is gone, and whoever next writes to this table can say
-- so. A bound read from configuration at reconcile time would instead judge
-- yesterday's run by today's knob.
--
-- `outcome` therefore holds four values, not three. `abandoned` is the one no run
-- writes about itself: it is what the account concludes about a run that stopped
-- reporting, so `finished_at` stays NULL on such a row -- there is no instant at
-- which that run finished, and writing the reconcile time there would put a time
-- in a column that means "when this run ended" while the run's end is exactly
-- what nobody saw. A run that reported is never recorded as abandoned, and one
-- that was abandoned can never report afterwards: its row has left `running`,
-- which is the only state a report can move.
--
-- Rows are never deleted. One row per run per day is under a thousand rows a
-- decade, and the account of past runs is the whole point of the table.
CREATE TABLE IF NOT EXISTS backup_runs (
    run_id                TEXT PRIMARY KEY,
    started_at            TIMESTAMPTZ NOT NULL,
    finished_at           TIMESTAMPTZ,
    outcome               TEXT NOT NULL,
    package               TEXT,
    bytes                 BIGINT,
    keep_target           INTEGER NOT NULL,
    run_deadline_seconds  INTEGER NOT NULL,
    kept_after            INTEGER,
    oldest_kept_at        TIMESTAMPTZ,
    error                 TEXT,
    CONSTRAINT backup_runs_outcome_is_known
        CHECK (outcome IN ('running', 'ok', 'failed', 'abandoned'))
);

-- The reader walks this table newest-first, and the two questions asked of it
-- most often -- when did a run last finish, and is any run still open -- are
-- both answered by a scan from this end.
CREATE INDEX IF NOT EXISTS backup_runs_started_at_idx
    ON backup_runs (started_at DESC);

-- Finding the open runs is the one query a reader has to run on a wakeup to
-- notice a run that never came back, and it is asked against a table that is
-- nearly all terminal rows.
CREATE INDEX IF NOT EXISTS backup_runs_open_idx
    ON backup_runs (started_at) WHERE outcome = 'running';
