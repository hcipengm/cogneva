-- Migration: 040_change_ledger_first_seen_rev (down)
-- Dropping the column keeps every row and every state, and loses only the tree
-- revision the row entered at. See the Postgres migration of the same name.

ALTER TABLE change_ledger
    DROP COLUMN first_seen_rev;
