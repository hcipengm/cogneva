-- Migration: 055_change_dispatch_window_start (down)
-- Drops the window start; the three quota columns added by 052 stay.
ALTER TABLE change_dispatch_state DROP COLUMN window_start_unix;
