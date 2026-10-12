-- Migration: 031_principle_ledger (down)
-- The intakes first, the account last: the same order the Postgres down
-- migration uses, so a queued reading is never left naming a table that is
-- already gone.

DROP TABLE IF EXISTS principle_refusals;
DROP TABLE IF EXISTS principle_ledger_declared;
DROP TABLE IF EXISTS principle_ledger;
