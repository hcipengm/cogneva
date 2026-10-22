-- Migration: 074_retire_reason_answered_elsewhere (down)
--
-- See the Postgres down migration: in the account the rows return to `pending`,
-- which is where they stood before they were retired; in the intake they are
-- deleted because a request is not the account's history.
--
-- MySQL rewrites the ENUM in place, so a stored value the narrowed list no longer
-- holds becomes the empty string rather than an error -- the rows have to move
-- before the ENUM narrows, not after.
UPDATE change_ledger
   SET state = 'pending',
       retire_reason = NULL,
       retired_at = NULL
 WHERE retire_reason = 'answered_elsewhere';

ALTER TABLE change_ledger
    MODIFY COLUMN retire_reason
        ENUM('superseded', 'fixture', 'out_of_scope', 'surface_moved') NULL;

DELETE FROM change_ledger_retired WHERE reason = 'answered_elsewhere';

ALTER TABLE change_ledger_retired
    MODIFY COLUMN reason
        ENUM('superseded', 'fixture', 'out_of_scope', 'surface_moved') NOT NULL;
