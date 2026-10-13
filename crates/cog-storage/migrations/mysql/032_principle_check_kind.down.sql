-- Migration: 032_principle_check_kind (down)
-- Dropping the column keeps every state and loses which ones were a person's to
-- decide. See the Postgres migration of the same name.

ALTER TABLE principle_ledger_declared
    DROP COLUMN check_kind;

ALTER TABLE principle_ledger
    DROP COLUMN check_kind;
