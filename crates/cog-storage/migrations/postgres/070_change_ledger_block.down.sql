-- Migration: 070_change_ledger_block (down)
-- Dropping the column costs no fact the account holds: the face is a function
-- of the citations, and the citations stay.
DROP INDEX IF EXISTS change_ledger_by_block_idx;

-- Put the band's own order back: the reads fall back to the key they had
-- before the blocking term existed.
DROP INDEX IF EXISTS change_ledger_outstanding_order_idx;
CREATE INDEX IF NOT EXISTS change_ledger_outstanding_order_idx
    ON change_ledger (priority_rank DESC, measured_at ASC NULLS FIRST, first_seen_at ASC)
    WHERE state NOT IN ('landed', 'retired');

ALTER TABLE change_ledger DROP CONSTRAINT IF EXISTS change_ledger_block_closed;
ALTER TABLE change_ledger DROP COLUMN IF EXISTS block;

ALTER TABLE change_ledger_declared
    DROP CONSTRAINT IF EXISTS change_ledger_declared_block_closed;
ALTER TABLE change_ledger_declared DROP COLUMN IF EXISTS block;

DROP FUNCTION IF EXISTS change_ledger_block(JSONB);
