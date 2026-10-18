-- Migration: 055_change_dispatch_window_start
-- Records when the pool's quota window began, on the same row that records how
-- much of it was left.
--
-- 052 put allowance / spent / reserve on the dispatch series, and a batch can
-- now be compared with the quota it was drawn from. What it cannot answer is how
-- old that quota is: upstreams that reset their weekly window hand the allowance
-- back along a time ramp, so "spent 4M of 8M" on a window six minutes old and on
-- a window five days old are different facts about how much a pass may hand
-- over, and the three columns alone read the same. The start is in the snapshot
-- the pass already copied (`LlmWindowPace::window_start_unix`); this is where
-- the copy lives past the snapshot's next overwrite.
--
-- Nullable for the same reason as the other three: zero would be a real instant
-- (the epoch) and a row written before the column existed is not a measurement
-- of the window at all. NULL is the only value that says "nothing was recorded".

ALTER TABLE change_dispatch_state
    ADD COLUMN IF NOT EXISTS window_start_unix BIGINT;
