-- Migration: 061_producer_heartbeat_trigger
-- What started the run that wrote a producer's heartbeat.
--
-- The heartbeat table keeps one row per producer, and that row is what the
-- stopped rule subtracts from `time()`. A manual run of the same producer
-- therefore overwrote the timer's evidence: after one person runs it by hand,
-- the rule reads a fresh moment and reports a timer that has been dead for
-- hours as healthy. The proof of a stall could be supplied by the very act that
-- is not a scheduled delivery.
--
-- The key becomes (source, trigger) so both runs keep their own row, and the
-- rules select `trigger = 'timer'`. A manual row stays on the exposition -- a
-- person should be able to see their own delivery -- but it no longer stands in
-- for a scheduled one. Existing rows are the timer's: they predate anyone being
-- able to say otherwise, and the default reproduces the old reading rather than
-- inventing a history.
--
-- `ALTER TABLE IF EXISTS` because a fresh database has not created this table
-- yet -- the observability store creates it at startup, after migrations run --
-- and on that path the table is born with the new key. A database that already
-- holds the table gets it here. Neither path drops a row.
ALTER TABLE IF EXISTS principle_producer_pushes
    ADD COLUMN IF NOT EXISTS trigger TEXT NOT NULL DEFAULT 'timer';

ALTER TABLE IF EXISTS principle_producer_pushes
    DROP CONSTRAINT IF EXISTS principle_producer_pushes_pkey;

ALTER TABLE IF EXISTS principle_producer_pushes
    ADD PRIMARY KEY (source, trigger);

ALTER TABLE IF EXISTS principle_producer_pushes
    DROP CONSTRAINT IF EXISTS principle_producer_pushes_trigger_check;

ALTER TABLE IF EXISTS principle_producer_pushes
    ADD CONSTRAINT principle_producer_pushes_trigger_check
    CHECK (trigger IN ('timer', 'manual'));
