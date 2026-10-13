-- Migration: 032_principle_check_kind
-- Which of the ways a principle can be checked this one is meant to be checked
-- by, as a value from a closed set of four.
--
-- The ledgers can say whether a gate exists, but not whether one *should*. A
-- principle nobody machine-checks and a principle that cannot be machine-checked
-- read identically in `state = unenforced`, and they are not the same finding:
-- the first is work, the second is a fact about the rule. Without this column the
-- reader has to choose between counting every human-only rule as a debt -- which
-- makes "how much is ungated" a number that can never fall, no matter how much
-- work is done -- or quietly leaving those rows out, which is the same hiding the
-- state column was built to avoid.
--
-- The four names:
--   shape       the sentence or command is recognisable as it is written, so the
--               check runs before the act (a session-side hook)
--   tree        the answer needs one walk of the tree, so it is read after the
--               fact, at a revision
--   live        the answer needs a reading off the running system, read after the
--               fact, against the same revision
--   judgement   no machine decides it; a person does, and the row stands as the
--               record that it is a person's to decide
--
-- Four names in one column, and the three "tiers" the design speaks of are a
-- grouping of them (`shape` | `tree`+`live` | `judgement`), not a second set of
-- values: the ledger stores the name, because the grouping is a presentation and
-- a presentation that lived in the schema would have to be migrated every time it
-- changed its mind.
--
-- Nullable, and NULL is a value rather than a gap: a principle whose tier nobody
-- has declared has none, and defaulting it to any of the four would be a claim
-- about how the principle is checked that no reading made. `judgement` is
-- declared, never defaulted -- it is the one value that must be said out loud,
-- since it is the record that this rule is a person's to hold.
--
-- `check_kind` lives on the account and on the intake both, for the same reason
-- `host_gate` does: the tier arrives with a reading, and the fold has to carry it
-- across. On the account it is read by whoever asks "which rows are mine to
-- measure" -- the tree and live tiers are a resident loop's work, and the
-- judgement tier is nobody's.

ALTER TABLE principle_ledger
    ADD COLUMN IF NOT EXISTS check_kind VARCHAR(16)
        CHECK (check_kind IN ('shape', 'tree', 'live', 'judgement'));

ALTER TABLE principle_ledger_declared
    ADD COLUMN IF NOT EXISTS check_kind VARCHAR(16)
        CHECK (check_kind IN ('shape', 'tree', 'live', 'judgement'));
