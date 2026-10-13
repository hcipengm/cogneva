-- Migration: 040_change_ledger_first_seen_rev
-- The tree revision the account was reading when a row first entered it. See the
-- Postgres migration of the same name for why the ledger needs it, why the
-- verdict is derived from `first_seen_rev^` rather than stored beside it, and why
-- it is written once and never moved.
ALTER TABLE change_ledger
    ADD COLUMN first_seen_rev VARCHAR(64) NULL;
