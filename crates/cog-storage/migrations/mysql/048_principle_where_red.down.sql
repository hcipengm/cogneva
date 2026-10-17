-- Migration: 048_principle_where_red (down)
-- Drops the answer, not the gates; see the Postgres migration of the same name.

ALTER TABLE principle_ledger_declared
    DROP COLUMN where_red;

ALTER TABLE principle_ledger
    DROP COLUMN where_red;
