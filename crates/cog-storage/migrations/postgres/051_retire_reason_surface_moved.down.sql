-- Migration: 051_retire_reason_surface_moved (down)
--
-- The old schema has no word for "the surface this row measured left the tree on
-- purpose", so the rows carrying it have to go somewhere before the CHECKs
-- narrow.
--
-- In the account they move to `undecided` with the retirement cleared, which is
-- where a row nothing can settle belongs in the old vocabulary -- and they are
-- re-examinable from there: a later measurement of a tree that still has no such
-- scope leaves them undecided, and one that finds the scope back moves them to
-- whatever it finds. Deleting them would lose the identity and its sources, more
-- than a downgrade should cost.
--
-- In the intake they are deleted outright: a request is not the account's
-- history, and the rows it named are kept in the account above. Leaving them
-- would make the narrowing fail on exactly the values this migration introduced.
--
-- Order matters: the CHECKs are dropped before the rows move, and the
-- biconditional between `state` and `retire_reason` does not need touching here
-- because `undecided` with no reason satisfies it on both sides.

ALTER TABLE change_ledger
    DROP CONSTRAINT IF EXISTS change_ledger_retire_reason_check;

UPDATE change_ledger
   SET state = 'undecided',
       retire_reason = NULL,
       retired_at = NULL
 WHERE retire_reason = 'surface_moved';

ALTER TABLE change_ledger
    ADD CONSTRAINT change_ledger_retire_reason_check
    CHECK (retire_reason IS NULL
           OR retire_reason IN ('superseded', 'fixture', 'out_of_scope'));

ALTER TABLE change_ledger_retired
    DROP CONSTRAINT IF EXISTS change_ledger_retired_intake_reason_check;

DELETE FROM change_ledger_retired WHERE reason = 'surface_moved';

ALTER TABLE change_ledger_retired
    ADD CONSTRAINT change_ledger_retired_intake_reason_check
    CHECK (reason IN ('superseded', 'fixture', 'out_of_scope'));
