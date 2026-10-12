-- Migration: 029_change_ledger_declared
-- The change ledger's intake: one row per declaration that a document asks for a
-- change.
--
-- Why this is a table in front of `change_ledger` rather than a message channel.
-- The ledger has exactly one writer, and it stays that way: the resident loop
-- folds this intake into the ledger and is the only thing that ever writes a
-- `change_ledger` row. Everything else -- a session, a script, an extraction
-- batch -- writes here instead, which means "tell the ledger about a change" and
-- "the ledger learns about a change" are the same act. Without this the two
-- facts "a change was handed over" and "the ledger has a row for it" live in
-- different places, and the ledger is the one that can be missing the news.
--
-- A declaration is not a verdict, and cannot become one. It carries what a
-- document asks for, never a state: the fold writes through the reading path,
-- which by contract never touches `state`, `measured_at` or `landed_at`. So the
-- party that declares a change has no way to declare it done, and the intake
-- cannot shortcut the measurement that decides.
--
-- The key is (kind, scope, literal, doc): the identity of the change, plus the
-- document declaring it, not the identity alone. Two documents asking for the
-- same change are two declarations and both are kept -- the ledger cites every
-- document that asked, and keying on the identity would let the second
-- declaration overwrite the first document's citation. Keying on the document
-- too also makes a re-run of the declaring script idempotent, which matters
-- because re-declaring is how a corrected declaration is made: the row is
-- already there, so nothing double-counts.
--
-- `folded_at` is what makes the intake readable as a backlog. It is stamped by
-- the fold, after the reading is written, so a process that dies mid-fold leaves
-- the declaration unfolded and the next pass redoes it -- the reading path is an
-- idempotent upsert, so folding twice is the same as folding once. Stamping it
-- before the write would instead lose the reading, which is the direction that
-- silently drops a change.
--
-- The `verdict` column holds what the declaring document said about the change,
-- when it said anything. It is kept apart from the ledger's `state` for the same
-- reason `change_ledger.sources` keeps it apart: a document's claim and a
-- measurement of the tree disagreeing is the reading the ledger exists to
-- produce, and merging them loses it.
--
-- Column widths bound the identity parts, as in `change_ledger`: MySQL cannot
-- key an unbounded TEXT column, and the writer rejects an over-long part instead
-- of truncating it -- a truncated identity is a different change sharing a row.

CREATE TABLE IF NOT EXISTS change_ledger_declared (
    kind        VARCHAR(32)  NOT NULL,
    scope       VARCHAR(191) NOT NULL,
    literal     VARCHAR(512) NOT NULL,
    doc         VARCHAR(512) NOT NULL,
    title       TEXT,
    assertion   TEXT,
    verdict     TEXT,
    declared_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    folded_at   TIMESTAMPTZ,
    PRIMARY KEY (kind, scope, literal, doc)
);

-- The fold scans for what has not been folded, which is the only query the
-- intake is read by.
CREATE INDEX IF NOT EXISTS idx_change_ledger_declared_folded_at
    ON change_ledger_declared(folded_at);
