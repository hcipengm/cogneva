-- Migration: 076_change_ledger_blocked (down)
--
-- Dropping the columns changes no row's state and no identity; what is lost is
-- which outstanding rows were waiting and on what.

DROP INDEX change_ledger_blocked_idx ON change_ledger;

ALTER TABLE change_ledger
    DROP COLUMN blocked_since,
    DROP COLUMN unblock_condition,
    DROP COLUMN blocked_reason;
