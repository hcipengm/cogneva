-- Migration: 044_change_dispatch_outcomes
-- The three per-pass counts that were computed and then dropped.
--
-- A dispatch pass classifies every row it read into exactly one outcome before
-- it does anything else, and the row it wrote down kept five of the counts. Two
-- of them never reached the table at all, and the third had no column because
-- nothing produced it:
--
-- 1. `dead_environment`. A task held the row, the environment refused the
--    handover, the pass re-filed it at the next generation, and the budget for
--    doing that ran out. The row is owed and it is not in flight and nothing is
--    going to be tried again -- and it was in `pending` and in no other column,
--    so the rows the environment killed were exactly the rows the account could
--    not name. Measured 2026-10-14: 187 tasks in the graph from this loop, none
--    completed, 205 of 215 failures reading `terminal_env_failure: environment
--    error: LLM upstream refused` -- the heaviest class of the day, and the one
--    this table could not show.
--
-- 2. `rearmed`, the same re-filings counted where they were decided rather than
--    where they ended. It is an annotation over an outcome and not an outcome
--    itself: a row re-armed and then submitted is in both numbers. What it
--    answers is how much of a pass's work went into getting back to where it
--    started, which no single count can say.
--
-- 3. `held_back`, work owed at the instant the pass decided to hand nothing
--    over. It is the column the arithmetic was missing: the pause short-circuits
--    before any row is classified, so a paused pass wrote `pending` beside four
--    zeros, and `pending` could not be reconciled with anything. Those rows are
--    not `failed` -- nothing was attempted -- and they are not `already` -- no
--    task holds them -- so a pass that hands nothing over needs a word for them
--    that is not one of the words for what happened when it did.
--
-- With it the per-pass counts add up to the set they were computed from on both
-- paths: submitted + already + failed + dead_environment + held_back == pending,
-- every tick, whichever branch ran. That equality is the reading -- it is what
-- makes a pass that lost track of a row visible as a number that does not add
-- up, rather than as a row nobody could see was missing.
--
-- Defaults of 0 rather than NOT NULL without one: every row already written was
-- produced by a pass that either had no such count or (for a paused pass) had
-- nothing to put in it, and inventing a value for those rows is not this
-- migration's business. A defaulted column makes every historical row read as
-- "zero of these", which is true of what it recorded; a row written before the
-- column existed is not a measurement of the column.
ALTER TABLE change_dispatch_state
    ADD COLUMN IF NOT EXISTS dead_environment INTEGER NOT NULL DEFAULT 0;

ALTER TABLE change_dispatch_state
    ADD COLUMN IF NOT EXISTS held_back INTEGER NOT NULL DEFAULT 0;

ALTER TABLE change_dispatch_state
    ADD COLUMN IF NOT EXISTS rearmed INTEGER NOT NULL DEFAULT 0;
