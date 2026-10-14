-- Migration: 044_change_dispatch_outcomes
-- See the Postgres migration of the same name for why these three counts exist:
-- `dead_environment` (a task held the row, the environment refused every
-- handover the budget allowed, and it is given up on), `rearmed` (handovers
-- re-filed at a later generation, an annotation over an outcome rather than one
-- of them), and `held_back` (work owed at the instant a pass decided to hand
-- nothing over) -- which together make the per-pass counts add up to `pending`
-- on a paused pass as well as on a dispatching one.
--
-- Unlike the Postgres file this one has no `IF NOT EXISTS` on the added columns:
-- the dialect does not support it, so a second run of this file fails on the
-- duplicate column and names it, rather than silently passing for a migration
-- that did nothing.
ALTER TABLE change_dispatch_state
    ADD COLUMN dead_environment INT NOT NULL DEFAULT 0;

ALTER TABLE change_dispatch_state
    ADD COLUMN held_back INT NOT NULL DEFAULT 0;

ALTER TABLE change_dispatch_state
    ADD COLUMN rearmed INT NOT NULL DEFAULT 0;
