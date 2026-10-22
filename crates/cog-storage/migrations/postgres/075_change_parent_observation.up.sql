-- Migration: 075_change_parent_observation
--
-- What a landed row means needs one more column: "the artifact is in this tree"
-- and "the artifact got there now" are the same reading on every existing column.
-- A row is landed when its check is satisfied, and a check written after the
-- artifact already existed is satisfied by a tree that did not change. The
-- difference between a change that put something there and a demand recorded
-- after the fact is only visible by asking the same check one revision earlier.
--
-- The column keeps that answer's own words (satisfied / absent /
-- unsettled:<reason> / unreadable), not a verdict: which of the two stories a
-- row tells is a comparison the reader makes, and the storage does not depend on
-- this module's vocabulary for it.
--
-- NULL is "this row was never asked", which is every row measured before this
-- column existed and every outstanding row (its check is absent by definition,
-- so there is nothing to compare it with).

ALTER TABLE change_ledger
    ADD COLUMN IF NOT EXISTS parent_observation TEXT;
