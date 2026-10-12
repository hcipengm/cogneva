-- Migration: 030_change_ledger_measured_rev (down)
-- Dropping the column keeps every state and loses the version each was measured
-- against. See the Postgres migration of the same name.

ALTER TABLE change_ledger
    DROP COLUMN measured_rev;
