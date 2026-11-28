-- Migration: 100_runtime_principle
-- The rules the platform's employees work by, as rows rather than code.
--
-- This is not the principle ledger. That one asks whether each of the repo's own
-- engineering principles has a gate that goes red when it is broken; this one
-- asks which rules are in force for the next run and hands them to whoever runs
-- it. Same word, different question, so a separate table rather than a flag on
-- the other one -- merged, "how much of the corpus has a gate" and "which rules
-- are live" would be two aggregates over one table with a column deciding which
-- rows count for which, and every reader would have to remember it.
--
-- A row is a triple, because a rule has to be judgeable: what situation it
-- applies to (`trigger_condition`), what to look at (`criterion`), what to do
-- (`action`). A row missing any of the three is a persona note rather than a
-- rule, and the writer refuses it rather than storing an empty half that every
-- run reads as "this rule says nothing today".
--
-- `state` is the closed set and the pair of checks below is the whole lifecycle:
-- a proposal carries no decision time, a decided row always does (proposed ->
-- approved/rejected, approved -> retired, rejected -> proposed again by a new
-- proposal). There is no in-place edit of a live rule: its replacement is a new
-- row, and the old one stays in force until the replacement is approved and the
-- old one retired, so no sentence becomes binding without someone approving
-- exactly that sentence.
--
-- `trigger` is a reserved word in PostgreSQL; the column is named
-- `trigger_condition` so the statement needs no quoting, and every writer sees
-- the same spelling.
--
-- `used` counts the runs this rule was handed to. It is a reading of whether the
-- rule is doing anything: a rule in force that no run ever received and one that
-- every run receives ask different questions about whether the account is
-- serving its purpose. It is incremented, never recomputed -- the runs it counts
-- are not stored anywhere else, so a recomputation would have nothing to
-- recompute from and would silently reset to zero.

CREATE TABLE IF NOT EXISTS runtime_principle (
    slug              VARCHAR(191) PRIMARY KEY,
    trigger_condition TEXT         NOT NULL,
    criterion         TEXT         NOT NULL,
    action            TEXT         NOT NULL,
    state             VARCHAR(16)  NOT NULL
                      CHECK (state IN ('proposed', 'approved', 'rejected', 'retired')),
    proposed_by       TEXT         NOT NULL,
    proposed_at       TIMESTAMPTZ  NOT NULL,
    decided_at        TIMESTAMPTZ,
    used              BIGINT       NOT NULL DEFAULT 0,
    CONSTRAINT runtime_principle_decision_matches_state
        CHECK ((state = 'proposed') = (decided_at IS NULL))
);

-- The one query every run makes: the rows in force, oldest decision first.
CREATE INDEX IF NOT EXISTS runtime_principle_in_force
    ON runtime_principle (state, decided_at);
