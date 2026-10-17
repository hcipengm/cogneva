-- Migration: 051_retire_reason_surface_moved (down)
--
-- See the Postgres down migration: in the account the rows return to
-- `undecided`, in the intake they are deleted because a request is not the
-- account's history.
--
-- MySQL rewrites the ENUM in place, so a stored value the narrowed list no longer
-- holds becomes the empty string rather than an error -- the rows have to move
-- before the ENUM narrows, not after.
UPDATE change_ledger
   SET state = 'undecided',
       retire_reason = NULL,
       retired_at = NULL
 WHERE retire_reason = 'surface_moved';

ALTER TABLE change_ledger
    MODIFY COLUMN retire_reason
        ENUM('superseded', 'fixture', 'out_of_scope') NULL;

DELETE FROM change_ledger_retired WHERE reason = 'surface_moved';

ALTER TABLE change_ledger_retired
    MODIFY COLUMN reason
        ENUM('superseded', 'fixture', 'out_of_scope') NOT NULL;
