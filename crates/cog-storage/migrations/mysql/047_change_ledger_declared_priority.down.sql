-- Migration: 047_change_ledger_declared_priority (down)
-- Dropping the column keeps every declaration, every identity and every state,
-- and loses only the band the documents asked for. See the Postgres migration of
-- the same name.

ALTER TABLE change_ledger_declared
    DROP COLUMN priority;
