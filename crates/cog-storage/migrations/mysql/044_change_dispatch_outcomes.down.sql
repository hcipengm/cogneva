-- Migration: 044_change_dispatch_outcomes (down)
-- See the Postgres migration of the same name: the passes stay recorded, and
-- what is lost is where each pass's share of the owed set went. The values
-- written while the columns existed are dropped with them and cannot be
-- reconstructed from the columns that remain.
ALTER TABLE change_dispatch_state DROP COLUMN dead_environment;
ALTER TABLE change_dispatch_state DROP COLUMN held_back;
ALTER TABLE change_dispatch_state DROP COLUMN rearmed;
