-- Migration: 073_principle_priority (down)
-- Dropping the column loses the bands the memories stated, not the principles.
-- The documents still carry them, and re-reading the corpus restores every one,
-- because a band is transcribed rather than measured.

ALTER TABLE principle_ledger_declared
    DROP COLUMN IF EXISTS priority;

ALTER TABLE principle_ledger
    DROP COLUMN IF EXISTS priority;
