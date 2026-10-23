-- Migration: 076_change_ledger_blocked
--
-- "Why is this not being worked on" is a fact about a change, and until now the
-- account could not hold it. A row's state answers whether the tree satisfies
-- the demand; it says nothing about a row waiting on a pool that is down, on a
-- reading only a run can produce, or on a decision nobody has made yet. Those
-- rows sat in a hand-written to-do table beside the ledger, which put the same
-- work in two books with no key between them.
--
-- Blocked is an orthogonal flag, not a fifth state. A row can be outstanding
-- and blocked at the same time, and the two are answered by different things:
-- state moves only when a measurement of the tree says so, while a block is
-- declared and cleared by whoever is waiting. Folding it into `state` would
-- make every reader that selects by state read "waiting for an owner" as "not
-- done yet", which is what it already reads, and lose the one distinction the
-- flag exists for.
--
-- The three columns are all-or-nothing: a block is a reason, a condition that
-- ends it, and the moment it started. A row carrying one without the others
-- cannot be re-read -- a reason with no condition can never be lifted by a
-- later pass, and a condition with no reason is a wait nobody can count.
--
-- `blocked_reason` is a closed set for the same reason `block` is: it is
-- grouped and counted, and a free-text reason would produce as many categories
-- as rows. A reason outside the set is refused at write time rather than
-- reading as an uncounted sixth kind.
ALTER TABLE change_ledger
    ADD COLUMN IF NOT EXISTS blocked_reason VARCHAR(32),
    ADD COLUMN IF NOT EXISTS unblock_condition TEXT,
    ADD COLUMN IF NOT EXISTS blocked_since TIMESTAMPTZ;

ALTER TABLE change_ledger
    DROP CONSTRAINT IF EXISTS change_ledger_blocked_closed;
ALTER TABLE change_ledger
    ADD CONSTRAINT change_ledger_blocked_closed
        CHECK (blocked_reason IS NULL
               OR blocked_reason IN ('pool_unavailable', 'upstream_reading',
                                     'owner_decision', 'external_input',
                                     'dependency'));

ALTER TABLE change_ledger
    DROP CONSTRAINT IF EXISTS change_ledger_blocked_shape;
ALTER TABLE change_ledger
    ADD CONSTRAINT change_ledger_blocked_shape
        CHECK ((blocked_reason IS NULL) = (unblock_condition IS NULL)
               AND (blocked_reason IS NULL) = (blocked_since IS NULL));

-- A block is a claim about work still owed, so it must never be empty where it
-- is set: the condition is what lets a later pass end the wait without asking
-- the person who declared it.
ALTER TABLE change_ledger
    DROP CONSTRAINT IF EXISTS change_ledger_blocked_condition_says_something;
ALTER TABLE change_ledger
    ADD CONSTRAINT change_ledger_blocked_condition_says_something
        CHECK (unblock_condition IS NULL OR length(btrim(unblock_condition)) >= 8);

-- The reads that hand out work ask "which outstanding rows are free to move",
-- so the index carries the block ahead of the ordering terms it already has.
CREATE INDEX IF NOT EXISTS change_ledger_blocked_idx
    ON change_ledger (blocked_reason, blocked_since)
    WHERE blocked_reason IS NOT NULL;
