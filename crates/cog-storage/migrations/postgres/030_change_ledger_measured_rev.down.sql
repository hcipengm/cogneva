-- Migration: 030_change_ledger_measured_rev (down)
-- Dropping the column keeps every state and loses the version each was measured
-- against. What is lost is the ability to tell a state that has been re-checked
-- against the current mainline from one measured against a tree that has since
-- moved -- after this, every row reads as a claim with no revision, which is the
-- state the column was added to end.

ALTER TABLE change_ledger
    DROP COLUMN IF EXISTS measured_rev;
