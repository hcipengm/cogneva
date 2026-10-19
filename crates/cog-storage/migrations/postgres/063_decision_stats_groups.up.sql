-- Migration: 063_decision_stats_groups
-- The meta-learning engine's evidence moves out of one file and into a table.
--
-- The file it replaces was rewritten whole on every recorded decision -- read
-- the map, add one to one counter, serialize every group back out, rename over
-- the old one. That shape has no transaction (a crash mid-write leaves the
-- previous file, which is at least the right answer, or a truncated one, which
-- is not), no concurrency control (two processes each read the same map and
-- each write back the other's increment away), and no way to ask a question of
-- it without loading everything. Counts are the one thing a database does
-- better than a file.
--
-- The primary key is the group identity, decision included. The decision is
-- part of what the count is *about* -- "pipeline won 7 of 10" and "roundtable
-- won 7 of 10" are two facts, and a row that kept only the group would make
-- them one. A deployment is one database, so there is no tenant column: the
-- install's boundary is the database's, and a column holding a constant for
-- every row would be one no reader could discriminate on.
--
-- `successes <= attempts` is a check rather than a convention because both
-- counters are incremented by a delta: each delta preserves the invariant, so
-- the sum does too, and a row that breaks it is a bug in a writer rather than a
-- reading anyone should have to interpret.
CREATE TABLE IF NOT EXISTS decision_stats_groups (
    category          VARCHAR(32)  NOT NULL,
    group_key         VARCHAR(191) NOT NULL,
    decision          VARCHAR(191) NOT NULL,
    attempts          BIGINT       NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    successes         BIGINT       NOT NULL DEFAULT 0 CHECK (successes >= 0),
    first_recorded_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    last_recorded_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (category, group_key, decision),
    CONSTRAINT decision_stats_successes_within_attempts
        CHECK (successes <= attempts)
);

-- The reader that matters takes every row (the engine hydrates its whole map at
-- start), so the primary key's index is already the one that serves it. The
-- second index is for the question the table exists to make askable: which
-- groups are accumulating evidence slowly enough that the sample floor will
-- never be reached.
CREATE INDEX IF NOT EXISTS idx_decision_stats_group_totals
    ON decision_stats_groups(category, group_key, attempts DESC);
