-- Migration: 075_change_parent_observation (down)
--
-- The column is a reading taken at measurement time and never a state: dropping
-- it changes no row's state, no landed_at and no identity. Rows measured while it
-- existed keep their state and lose the comparison, which is what the schema
-- before this migration could say.

ALTER TABLE change_ledger
    DROP COLUMN IF EXISTS parent_observation;
