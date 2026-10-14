-- Migration: 042_change_reopen
-- A row the account had as done, whose check a later pass found no longer holds.
--
-- Every reading of `change_ledger` answers "what is true of the tree now".
-- `landed_at` is the exception: it is the one column that remembers a claim the
-- account has since stopped making, and the write that moves a row out of
-- `landed` clears it along with the state (the columns agree with the state by
-- construction). So the moment the account learns it was wrong is the moment it
-- stops being able to say it had ever said anything -- "the ledger called this
-- done and the tree disagreed" and "nothing ever measured this" become the same
-- row, and only the first is a fact about the tree having moved.
--
-- Measured before this table existed: of 1369 landed rows, the oldest had been
-- measured a day and fifteen hours earlier, and none of them was due to be read
-- again ever -- `outstanding` excludes the state by definition. Eight rows whose
-- checks had stopped holding were still being reported as done, and nothing in
-- the account distinguished them from the ones that were.
--
-- `was_landed_at` is the time the next write takes away, and it is nullable on
-- purpose: a row whose state said `landed` with no `landed_at` behind it is a row
-- disagreeing with itself, and the record is the one place that disagreement is
-- visible. Refusing to write it would hide it at the only moment it shows.
--
-- Not keyed on the identity. A row reopened three times was three facts about
-- the tree, and a key would keep only the last of them; the index on the identity
-- is there for the question a reader actually asks -- how often did this one come
-- back -- without making it the key.
CREATE TABLE IF NOT EXISTS change_reopen (
    id            BIGSERIAL   PRIMARY KEY,
    observed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    kind          VARCHAR(32) NOT NULL,
    scope         VARCHAR(191) NOT NULL,
    literal       VARCHAR(512) NOT NULL,
    was_landed_at TIMESTAMPTZ,
    measured_rev  VARCHAR(64) NOT NULL DEFAULT ''
);

-- The series, read in time order: how many came back this week, and whether the
-- rate is moving.
CREATE INDEX IF NOT EXISTS idx_change_reopen_observed_at
    ON change_reopen (observed_at);

-- One identity's history, oldest first: the other question a reader asks.
CREATE INDEX IF NOT EXISTS idx_change_reopen_identity
    ON change_reopen (kind, scope, literal, observed_at);
