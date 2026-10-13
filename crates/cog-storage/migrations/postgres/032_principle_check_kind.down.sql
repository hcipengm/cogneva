-- Migration: 032_principle_check_kind (down)
-- Dropping the column keeps every state and loses which ones were a person's to
-- decide. After this, a principle nobody machine-checks and one no machine can
-- check are the same row again, so "how much is ungated" counts the human-only
-- rules as debt a second time -- which is the reading the column was added to end.

ALTER TABLE principle_ledger_declared
    DROP COLUMN IF EXISTS check_kind;

ALTER TABLE principle_ledger
    DROP COLUMN IF EXISTS check_kind;
