-- Migration: 076_change_ledger_blocked (down)
--
-- Dropping the columns changes no row's state, no landed_at and no identity:
-- what is lost is which outstanding rows were waiting and on what, which is
-- exactly what the schema before this migration could not say. The wait itself
-- is still where it was before -- in whoever is waiting for it.

DROP INDEX IF EXISTS change_ledger_blocked_idx;

ALTER TABLE change_ledger
    DROP CONSTRAINT IF EXISTS change_ledger_blocked_condition_says_something;
ALTER TABLE change_ledger
    DROP CONSTRAINT IF EXISTS change_ledger_blocked_shape;
ALTER TABLE change_ledger
    DROP CONSTRAINT IF EXISTS change_ledger_blocked_closed;

ALTER TABLE change_ledger
    DROP COLUMN IF EXISTS blocked_since,
    DROP COLUMN IF EXISTS unblock_condition,
    DROP COLUMN IF EXISTS blocked_reason;
