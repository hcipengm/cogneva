-- Migration: 058_change_ledger_delivered_rev
-- Which commit delivered a change. See the Postgres file for why delivery is a
-- different fact from the state, and why the column is never cleared.
--
-- No `IF NOT EXISTS`, as on the other MySQL files.
ALTER TABLE change_ledger
    ADD COLUMN delivered_rev VARCHAR(64) NULL;
