-- Migration: 031_principle_ledger
-- The principle ledger's source of truth: one row per principle, keyed by the
-- principle's own slug -- the memory's filename without its extension, which is
-- the same string the gate's registry and the memory's own links use.
--
-- It lives in a table for the same reason the change ledger does: what it
-- answers is read as a query, not rewritten as a document. "Which principles
-- have no gate" is an index scan here, and "how much of the corpus is covered"
-- is an aggregate, where a file form has to be recomputed, rewritten and
-- diffed. A file is also a surface anyone can write, and then the account has
-- two versions and no way to say which one is the ledger.
--
-- Three columns hold the two answers a principle's row is about, and the
-- difference between them is the point of the table:
--   host_gate      the shape of the check the host's own registry runs, or NULL
--   platform_gate  the contract test, in the platform's own tree, that declares
--                  it enforces this principle, or NULL
-- A principle with neither is an outstanding debt; a principle whose registry
-- could not be read is neither, and that third answer is `state IS NULL` (see
-- below) rather than a zero in either column.
--
-- `state` is NULL for a row no look has settled, and NULL is a value rather than
-- a gap. The closed set is two-valued on purpose: `enforced` and `unenforced`
-- are the only things a look is allowed to conclude, and a look that could not
-- conclude -- a registry that could not be read, a carrier file that is not in
-- the tree, a registry that could not be parsed -- writes neither `state` nor
-- `measured_at` and reports itself by reason instead. Defaulting it, or writing
-- `unenforced` for a look that failed, would report a missing gate where the
-- truth is a missing reading, and the two call for opposite work.
--
-- `measured_at` and `measured_rev` are therefore both nullable for the same
-- reason the change ledger's are: a state is a claim about a tree, and the tree
-- moves. The revision says which tree the claim was made against, so a
-- `unenforced` row a month old can be told from one that is still true.
--
-- `first_seen_at` is stamped once, on the intake side, and never moves: it is
-- what makes "this principle is new" sayable without a diff of yesterday's
-- list.
--
-- `refusals` is the count of times a gate refused work because of this
-- principle, and `last_refused_at` is when the most recent one was. They are
-- written by the fold of the refusal intake and by nothing else, so a principle
-- cannot report itself violated and a reader of the count is reading what the
-- gates actually refused. The count is accumulated rather than recomputed: the
-- events it sums are kept (see `principle_refusals`) so the sum can be
-- re-derived, and a count that disagreed with its own events would be the
-- reading that shows the fold is broken.
--
-- Column widths: the slug is the primary key because it is the identity, not a
-- surrogate. MySQL cannot key an unbounded TEXT column, so it is bounded here
-- rather than there, and the writer rejects an over-long slug instead of
-- truncating it -- a truncated identity is a different principle that would
-- silently share a row.

CREATE TABLE IF NOT EXISTS principle_ledger (
    slug            VARCHAR(191) NOT NULL,
    lead            TEXT,
    carrier_path    TEXT,
    carrier_sha256  VARCHAR(64),
    host_gate       TEXT,
    platform_gate   TEXT,
    state           VARCHAR(16)
        CHECK (state IN ('enforced', 'unenforced')),
    refusals        BIGINT NOT NULL DEFAULT 0,
    last_refused_at TIMESTAMPTZ,
    first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    measured_at     TIMESTAMPTZ,
    measured_rev    VARCHAR(64),
    PRIMARY KEY (slug)
);

-- The outstanding set is the query the driver runs: which principles have no
-- gate, stalest look first.
CREATE INDEX IF NOT EXISTS idx_principle_ledger_state
    ON principle_ledger(state, measured_at);

-- Coverage and the refuse-count reading are aggregates over the table.
CREATE INDEX IF NOT EXISTS idx_principle_ledger_refusals
    ON principle_ledger(refusals);

-- Migration: 031_principle_ledger_declared
-- The principle ledger's intake: one row per principle a host-side reading
-- pushed, with what that reading said about it.
--
-- Why this is a table in front of `principle_ledger` rather than the account
-- itself being written by the reader. The account has exactly one writer -- the
-- resident loop -- and it stays that way. A host-side script, a session, an
-- extraction batch: all of them write here, and the loop folds what they wrote
-- through the same reading path a direct write would use. Without this, "the
-- host told the platform about a principle" and "the platform has a row for it"
-- live in different places, and the platform is the one that can be missing the
-- news.
--
-- A reading is not a measurement, and cannot become one. It carries the
-- principle's identity, its sentence, the path and byte hash of the memory that
-- carries it, and the shape of the host's own gate -- never a state. The fold
-- writes through the reading path, which by contract never touches `state`,
-- `measured_at` or `measured_rev`, so the party that lists a principle has no
-- way to declare it enforced.
--
-- The key is (slug, source): the identity of the principle plus the reading
-- that named it. Re-running the same list is therefore idempotent, which
-- matters because re-pushing is how a corrected reading is made -- and a
-- principle named by two readings keeps both, so the citation is not
-- overwritten by the second.
--
-- `carrier_sha256` is what the reading saw the memory's bytes hash to, and it is
-- stored rather than compared and dropped: a hash that differs from the one on
-- the row is the drift reading -- the principle's text changed and the gate did
-- not follow -- and that is only sayable if the previous hash is still there.
--
-- `folded_at` is stamped by the fold, after the reading is written, so a
-- process that dies mid-fold leaves the row unfolded and the next pass repeats
-- it: the reading path is an idempotent upsert, so folding twice is the same as
-- folding once. Stamping first would instead lose the reading, which is the
-- direction that silently drops a principle out of the account.

CREATE TABLE IF NOT EXISTS principle_ledger_declared (
    slug           VARCHAR(191) NOT NULL,
    source         VARCHAR(512) NOT NULL,
    lead           TEXT,
    carrier_path   TEXT,
    carrier_sha256 VARCHAR(64),
    host_gate      TEXT,
    declared_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    folded_at      TIMESTAMPTZ,
    PRIMARY KEY (slug, source)
);

-- The fold scans for what has not been folded, which is the only query the
-- intake is read by.
CREATE INDEX IF NOT EXISTS idx_principle_ledger_declared_folded_at
    ON principle_ledger_declared(folded_at);

-- Migration: 031_principle_refusals
-- The refusal intake: one row per (principle, instant) a gate refused work,
-- waiting for the ledger's own writer to fold it into `principle_ledger`.
--
-- This is the first reader the question "how many times has this principle been
-- violated" has ever had. Before it, a gate that refused work left a line in a
-- terminal and nothing else: the refusals were counted by nobody, so a
-- principle that was being broken every day and one that had never come up read
-- exactly the same.
--
-- Rows rather than an increment, and the reason is the same one the intake
-- above exists for: the account has one writer. A hook that refused work cannot
-- add to `refusals` itself without becoming a second writer of the account, so
-- it reports the event and the loop counts it. Keeping the events also makes
-- the count checkable -- the sum over a principle's unfolded-and-folded rows is
-- what the counter must equal -- and makes "when" answerable, which an
-- increment cannot.
--
-- The key is (slug, refused_at, source) and `count` carries how many refusals
-- in that second came from that source. An event keyed by its instant alone
-- would collapse two refusals inside the same second into one, and a counter
-- that under-counts silently is worse than no counter; carrying the count keeps
-- the row idempotent -- re-reporting the same batch is the same fact -- without
-- losing one.
--
-- `folded_at` is stamped after the counter moves, and in the same transaction
-- as it. The ordering rule this platform uses elsewhere -- stamp after, so an
-- interruption repeats work the key makes free -- assumes the work is an
-- assignment, and adding to a counter is not one: a fold that committed the
-- additions without the marks would add the same events again on the next pass
-- and report more refusals than were queued. One commit is what makes the
-- ordering safe here.

CREATE TABLE IF NOT EXISTS principle_refusals (
    slug       VARCHAR(191) NOT NULL,
    refused_at TIMESTAMPTZ NOT NULL,
    source     VARCHAR(256) NOT NULL,
    count      BIGINT NOT NULL DEFAULT 1 CHECK (count > 0),
    reported_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    folded_at  TIMESTAMPTZ,
    PRIMARY KEY (slug, refused_at, source)
);

CREATE INDEX IF NOT EXISTS idx_principle_refusals_folded_at
    ON principle_refusals(folded_at);
