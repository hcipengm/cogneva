-- Migration: 048_principle_where_red (down)
-- Dropping the column loses the answer, not the gates: `host_gate` and
-- `platform_gate` still say whether a principle is checked. What goes away is the
-- platform's ability to say *where* -- the sentence returns to living only in the
-- carrier's own body, with no reader on this side.

ALTER TABLE principle_ledger_declared
    DROP COLUMN IF EXISTS where_red;

ALTER TABLE principle_ledger
    DROP COLUMN IF EXISTS where_red;
