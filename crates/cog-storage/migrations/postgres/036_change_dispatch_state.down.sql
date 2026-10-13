-- Migration: 036_change_dispatch_state (down)
-- Drops the dispatch pass's own series.
--
-- The rows are gone for good: every one is an observation of a pool at a moment
-- and nothing else recorded it, so a re-run of the loop cannot rebuild the past
-- -- it starts a new series from the tick after this runs. Dropping is safe for
-- the same reason it is lossy: no other account derives from this one, and a
-- reader that finds the table absent has lost a history, not a premise.

DROP TABLE IF EXISTS change_dispatch_state;
