-- Migration: 072_change_reopen_scope_present
-- Whether the surface a reopened claim was measured on was still in the tree the
-- pass read.
--
-- A reopen records one fact today -- the row had landed and its check no longer
-- holds -- and two very different stories arrive under it. A claim can stop
-- holding because the file it lived in was removed on purpose, or because the
-- thing it asked for stopped being true inside a surface that is still here.
-- The first is a decision that took a demand off the tree; the second is a
-- regression. Read together, an account whose six rows left with a deleted
-- directory reads as six outstanding debts, and the reader has no row to check
-- that against.
--
-- The pass already asks the tree this question: it is what decides whether an
-- absent scope retires the row as `surface_moved`. What it did not do was keep
-- the answer. Kept beside the reopen, the two stories separate without asking
-- the history again.
--
-- Three readings, not two. `true` is a criterion that stopped holding inside a
-- surface that is here; `false` is a surface that is gone; NULL is "the tree
-- could not be asked" -- the look failed for a reason that says nothing about
-- the scope, and rows written before this column existed have the same absence.
-- A NOT NULL with a default would file "could not ask" and "predates the
-- column" as one of the two answers, which is how a reading starts reporting a
-- fact nobody measured.
ALTER TABLE change_reopen
    ADD COLUMN IF NOT EXISTS scope_present boolean;

COMMENT ON COLUMN change_reopen.scope_present IS
    'Whether the row''s scope was present in the revision this reopen was read at: '
    'true = the check stopped holding inside a surface that is still there, '
    'false = the surface had left, NULL = the tree could not be asked (or the row '
    'predates the column)';
