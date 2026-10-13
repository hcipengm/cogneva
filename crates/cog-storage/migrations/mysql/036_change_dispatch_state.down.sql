-- Migration: 036_change_dispatch_state (down)
-- Drops the dispatch pass's own series. See the Postgres migration of the same
-- name: the rows are observations of a pool at moments, nothing else recorded
-- them, and dropping them loses a history rather than a premise.

DROP TABLE IF EXISTS change_dispatch_state;
