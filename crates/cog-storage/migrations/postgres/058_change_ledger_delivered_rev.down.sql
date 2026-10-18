-- Migration: 058_change_ledger_delivered_rev (down)
-- Drops the delivery revision. No state was moved by this column's writer, so
-- dropping it loses the delivery reading and nothing else.
ALTER TABLE change_ledger DROP COLUMN IF EXISTS delivered_rev;
