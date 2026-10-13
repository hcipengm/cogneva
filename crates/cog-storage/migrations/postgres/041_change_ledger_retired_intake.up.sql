-- Migration: 041_change_ledger_retired_intake
-- The retirement declarations' intake: an assertion that a row's identity is one
-- the account should not hold, waiting for the ledger's own writer to fold it.
--
-- Why a second intake rather than a column on `change_ledger_declared`. That
-- intake's contract is that a declaration carries what a document asks for and
-- never a state -- it is written through the reading path, which cannot touch
-- `state`. A retirement is a state, so it cannot ride that door without breaking
-- the property that makes the declaration intake safe. The producer of a
-- retirement is also not a document asking for a change: it is a classifier
-- asserting that an identity is stale -- a fixture, an identity a later
-- declaration renamed, a path no criterion can read. Two intakes, each with its
-- own fold path, keep "a change was asked for" and "a row was retired" from being
-- the same act.
--
-- The key is the identity alone, not the identity plus a declarer. A row has one
-- retirement at a time -- a second classification of the same identity is the
-- same retirement, not a second one -- so keying on the identity makes a re-run
-- idempotent and refuses to let two classifiers hang two reasons on one row. The
-- `source` names who asserted it, for the trace a reader needs, without being
-- part of what makes the assertion unique.
--
-- `folded_at` is what makes the intake readable as a backlog. It is stamped by
-- the fold *after* the retirement is written, the same order the declaration fold
-- uses: a process that dies mid-fold leaves the retirement unfolded and the next
-- pass redoes it, which is idempotent because applying a retirement twice is
-- applying it once. Stamping it before the write would drop the retirement
-- instead, and nothing would say so.
--
-- `reason` is held to its closed set here as on `change_ledger`, so a value
-- outside it is refused rather than stored. The three names are the ones
-- `retire_reason` uses; the fold parses them back through the same enum.
CREATE TABLE IF NOT EXISTS change_ledger_retired (
    kind        VARCHAR(32)  NOT NULL,
    scope       VARCHAR(191) NOT NULL,
    literal     VARCHAR(512) NOT NULL,
    reason      VARCHAR(32)  NOT NULL,
    source      VARCHAR(512) NOT NULL,
    declared_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    folded_at   TIMESTAMPTZ,
    PRIMARY KEY (kind, scope, literal),
    CONSTRAINT change_ledger_retired_intake_reason_check
        CHECK (reason IN ('superseded', 'fixture', 'out_of_scope'))
);

-- The fold scans for what has not been folded, which is the only query the
-- intake is read by.
CREATE INDEX IF NOT EXISTS idx_change_ledger_retired_folded_at
    ON change_ledger_retired(folded_at);
