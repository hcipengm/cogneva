-- Migration: 057_change_dispatch_delivered (down)
-- Drops the delivery count only. Nothing else was added by this migration, and
-- the state columns it annotates stay: delivery is an annotation over them, not
-- a replacement for any one of them.
ALTER TABLE change_dispatch_state DROP COLUMN IF EXISTS delivered;
