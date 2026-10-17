-- Migration: 049_change_dispatch_unread_sources
-- See the Postgres migration of the same name: the pass records which of its
-- intakes it could not read, empty when all of them answered. A failed read
-- contributes no rows, so without this column a pass that lost its only source
-- and a pass with no debt to record are the same row.
--
-- NULL means the row predates the column and says nothing about sources; a row
-- written since always carries a value, so the two stay distinguishable. Like
-- the other MySQL migrations here, the added column has no IF NOT EXISTS -- the
-- dialect does not support it, and a second run failing on the duplicate name
-- is better than a migration that silently did nothing.
ALTER TABLE change_dispatch_state
    ADD COLUMN sources_unread TEXT NULL;
