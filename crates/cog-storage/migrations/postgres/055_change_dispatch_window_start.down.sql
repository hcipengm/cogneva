-- Migration: 055_change_dispatch_window_start (down)
-- Drops the window start. The allowance / spent / reserve columns added by 052
-- stay: they answer a question this column only dates, and a downgrade that
-- removed them too would be rolling back two migrations from one down file.
ALTER TABLE change_dispatch_state DROP COLUMN IF EXISTS window_start_unix;
