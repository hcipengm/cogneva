-- Migration: 028_change_ledger
-- The change ledger's source of truth: one row per outstanding change, keyed by
-- the identity of the artifact the change names -- kind:scope:literal, byte for
-- byte, the same string its check evaluates.
--
-- It lives in a table rather than in a file because its state is read as a
-- query, not rewritten as a document. "What is still outstanding" is an index
-- scan here, where the file form has to recompute and rewrite a few hundred
-- kilobytes of YAML every time and recover "what changed today" by diffing
-- against an opening snapshot. A file is also a surface anyone can write: the
-- same account then has two versions and no way to say which one is the ledger.
-- The write surface here is a single resident loop, elected by lease.
--
-- The three timestamps are the point of the table, and none of them is
-- interchangeable with another:
--   first_seen_at  when the row entered the account
--   measured_at    when it was last re-measured
--   landed_at      when a re-measurement found the check satisfied
-- Only the measuring side writes them. A change has no entry point here at all:
-- there is no call that sets a state or stamps landed_at on its own behalf, so
-- a change cannot declare itself done. That is the difference between a ledger
-- and a status field the judged party fills in.
--
-- measured_at is NULL until something actually measures the row. Seeding it
-- from first_seen_at would claim the document's assertion was a measurement of
-- the tree, and the reader has no way to tell those apart afterwards.
--
-- title / assertion / sources carry what a reader needs to act on the row --
-- what to do, and which documents asked for it -- so the repository views can
-- be rendered from this table alone rather than from a second copy of the rows.
--
-- Column widths: (kind, scope, literal) is the primary key because it is the
-- identity, not a surrogate. MySQL cannot key an unbounded TEXT column, so the
-- parts are bounded here rather than there, and the writer rejects an
-- over-long part instead of truncating it -- a truncated identity is a
-- different change that would silently share a row.

CREATE TABLE IF NOT EXISTS change_ledger (
    kind          VARCHAR(32)  NOT NULL,
    scope         VARCHAR(191) NOT NULL,
    literal       VARCHAR(512) NOT NULL,
    state         VARCHAR(16)  NOT NULL
        CHECK (state IN ('conflict', 'pending', 'partial', 'undecided', 'landed')),
    title         TEXT,
    assertion     TEXT,
    sources       JSONB NOT NULL DEFAULT '[]'::jsonb,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    measured_at   TIMESTAMPTZ,
    landed_at     TIMESTAMPTZ,
    PRIMARY KEY (kind, scope, literal)
);

-- The outstanding set is the query every writer and every driver runs.
CREATE INDEX IF NOT EXISTS idx_change_ledger_state ON change_ledger(state);

-- Re-measurement picks the stalest rows first, so it needs the age, not the
-- insertion order.
CREATE INDEX IF NOT EXISTS idx_change_ledger_measured_at ON change_ledger(measured_at);
