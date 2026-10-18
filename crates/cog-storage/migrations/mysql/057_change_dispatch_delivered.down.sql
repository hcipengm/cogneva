-- Migration: 057_change_dispatch_delivered (down)
ALTER TABLE change_dispatch_state DROP COLUMN delivered;
