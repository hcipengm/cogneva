-- Migration: 073_principle_priority
-- How urgent the memory that states a principle said landing it is.
--
-- The change ledger already keeps a band per declaration, transcribed from the
-- document that asks for the change rather than measured, because no tree says
-- one change matters more than another. A principle's declaring document is the
-- memory that states the rule, and until now that sentence had nowhere to land:
-- the principle ledger had no column for it, so every row the account filed from
-- this producer could only ever read `unstated` -- not because nobody had an
-- opinion, but because the account had no way to be told one.
--
-- `unstated` is a member of the closed set rather than NULL, for the reason the
-- declaration column gives: a reader must be able to tell "the memory said P0"
-- from "no memory said", and a nullable column invites both to be read as the
-- same thing. A row written before this column existed had no answer because
-- nothing asked the question, and `unstated` is exactly that fact.
--
-- On the intake the column is nullable, and that is not a second vocabulary: it
-- is the difference between a reading and a row. A reading that does not carry
-- the field at all -- a pusher older than this column -- says nothing, and a
-- merge must not let silence erase a band. A reading that looked and found no
-- document stating one pushes `unstated`, which is a value and does overwrite,
-- and that is what makes a declared band withdrawable. Both halves are the same
-- rule the other reading columns follow, with `check_kind` as the precedent.
--
-- Not part of any key: re-stating a band is the same row asked for at a
-- different urgency, not a second principle.

ALTER TABLE principle_ledger
    ADD COLUMN IF NOT EXISTS priority VARCHAR(16) NOT NULL DEFAULT 'unstated'
        CHECK (priority IN ('unstated', 'p0', 'p1', 'p2', 'p3'));

ALTER TABLE principle_ledger_declared
    ADD COLUMN IF NOT EXISTS priority VARCHAR(16) NULL
        CHECK (priority IN ('unstated', 'p0', 'p1', 'p2', 'p3'));
