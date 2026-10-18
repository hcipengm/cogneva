-- Migration: 058_change_ledger_delivered_rev (down)
ALTER TABLE change_ledger DROP COLUMN delivered_rev;
