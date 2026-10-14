-- Migration: 044_change_dispatch_outcomes (down)
-- Drops the three counts. What is lost is not the record of the passes
-- themselves -- `pending`, the outcomes, and the decision stay -- but the
-- account's ability to say where a pass's share of the owed set went. The rows
-- already written keep nothing of these values to reconstruct: a pass that ran
-- while the columns existed recorded them, and dropping the column drops what it
-- recorded. Rows written after the drop cannot carry them either, so the loss is
-- permanent for the span of the downgrade rather than only for its past.
ALTER TABLE change_dispatch_state DROP COLUMN IF EXISTS dead_environment;
ALTER TABLE change_dispatch_state DROP COLUMN IF EXISTS held_back;
ALTER TABLE change_dispatch_state DROP COLUMN IF EXISTS rearmed;
