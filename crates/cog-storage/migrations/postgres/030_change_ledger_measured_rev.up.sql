-- Migration: 030_change_ledger_measured_rev
-- Which revision a row's state was measured against.
--
-- A ledger state is a measurement of a tree, and a tree moves. `measured_at`
-- records *when* a state was reached but not *what* it was reached against, so a
-- `landed` row is a claim with no version attached: read a month later it says the
-- artifact is in the project, and nothing distinguishes a mainline that still
-- carries it from one that has since dropped it. "Is this in the project?" and
-- "was this ever in the project?" are different questions, and the revision is the
-- only thing that separates them.
--
-- That is also what makes the third answer sayable. A check that could not be
-- settled leaves the row's state alone, so a reader has to be able to tell a state
-- that no look has reached from one a look reached against an older tree; without
-- the revision, both read as the same bare state.
--
-- Nullable, and NULL is a value rather than a gap: a row no look has settled keeps
-- NULL, and so does a row measured by a process that had no revision to name.
-- Defaulting it would invent a version for a measurement that never had one, which
-- is the same error as reporting an unmeasurable check as a miss.
--
-- Kept as its own column rather than folded into `assertion` or into the store's
-- observation field: those answer "what was asked for" and "why", and this answers
-- "against what". It is a small, comparable value read on its own.

ALTER TABLE change_ledger
    ADD COLUMN IF NOT EXISTS measured_rev VARCHAR(64);
