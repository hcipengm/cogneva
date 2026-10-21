-- Migration: 069_change_ledger_priority_rank (down)
-- Dropping the rank costs no fact the account holds: it is a function of the
-- citations, and the citations stay. The reads fall back to staleness alone,
-- which is what they ordered by before this column existed.
DROP INDEX IF EXISTS change_ledger_outstanding_order_idx;

ALTER TABLE change_ledger
    DROP COLUMN IF EXISTS priority_rank;

DROP FUNCTION IF EXISTS change_ledger_priority_rank(JSONB);
