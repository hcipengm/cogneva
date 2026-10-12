-- Migration: 031_principle_ledger (down)
-- The intakes go first: they name the account's slug, and dropping the account
-- out from under a queued reading would leave rows that no fold can ever place.
-- The account itself last, so the down migration is the exact reverse of the up
-- one rather than an order that happens to work once.

DROP TABLE IF EXISTS principle_refusals;
DROP TABLE IF EXISTS principle_ledger_declared;
DROP TABLE IF EXISTS principle_ledger;
