-- Migration: 038_change_ledger_retired
-- A second terminal state, beside `landed`, for a row whose identity turned out
-- not to be one the account should hold.
--
-- The account is append-only: a row that was folded in cannot be withdrawn once
-- it is in `change_ledger`, so a statement that named the wrong artifact -- a
-- fixture, a path outside the tree a criterion can read, or an identity a later
-- declaration renamed -- has no way out. Deleting it would be worse than leaving
-- it: the ledger would lose "this was asked for" and, the next time the same
-- identity appeared, would mint it again as if nothing had happened. The exit is
-- therefore a state, not a delete, and `retired` is that state.
--
-- `retired` is re-examinable, which is why it is a state rather than a tombstone
-- and why `retire_reason` and `retired_at` are cleared the moment a later
-- measurement finds any other state: a state here is a property of the
-- (identity, criterion revision) pair, not of the artifact, and a corpus that
-- changes back must be able to move the row back out. `retired_at` is kept from
-- the first measurement that found it retired and cleared when a later one does
-- not, mirroring `landed_at` exactly.
--
-- `state`'s closed set is a CHECK that the original CREATE TABLE named inline, so
-- it is widened by dropping and re-adding it under its generated name. This is
-- re-runnable by hand: the DROP always precedes the ADD, so a second run drops
-- what the first added and adds it again. Postgres has no `ADD CONSTRAINT IF NOT
-- EXISTS`.
ALTER TABLE change_ledger
    DROP CONSTRAINT IF EXISTS change_ledger_state_check;

ALTER TABLE change_ledger
    ADD CONSTRAINT change_ledger_state_check
    CHECK (state IN ('conflict', 'pending', 'partial', 'undecided', 'landed', 'retired'));

-- Why the row was retired. NULL in every other state: the reason is part of the
-- retirement, and a reason left on a row that is not retired would read as one.
-- The closed set is held here rather than left to convention, so a value outside
-- it is refused rather than stored -- the same idiom the state column uses.
ALTER TABLE change_ledger
    ADD COLUMN IF NOT EXISTS retire_reason VARCHAR(32)
        CHECK (retire_reason IS NULL
               OR retire_reason IN ('superseded', 'fixture', 'out_of_scope'));

-- When a measurement last found it retired. Set and cleared with `retire_reason`,
-- the same way `landed_at` is set and cleared with `landed`.
ALTER TABLE change_ledger
    ADD COLUMN IF NOT EXISTS retired_at TIMESTAMPTZ;

-- A row is retired if and only if it carries a reason. This is the half that
-- makes the state non-optional: a retirement with no reason would be a deletion
-- wearing a state's clothes, and the next reader would have nothing to argue
-- with. The other half -- a reason with no retirement -- is refused by the same
-- biconditional. Held in the schema as well as in the store because MySQL 5.7 has
-- no CHECK and cannot hold it, so the store cannot be the only place it lives.
ALTER TABLE change_ledger
    DROP CONSTRAINT IF EXISTS change_ledger_retired_reason_check;

ALTER TABLE change_ledger
    ADD CONSTRAINT change_ledger_retired_reason_check
    CHECK ((state = 'retired') = (retire_reason IS NOT NULL));
