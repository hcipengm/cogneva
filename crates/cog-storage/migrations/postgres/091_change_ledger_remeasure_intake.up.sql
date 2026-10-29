-- Migration: 091_change_ledger_remeasure_intake
-- The re-measurement intake: an assertion that a row's state was measured by a
-- rule that has since been corrected, waiting for the ledger's own writer to
-- measure it again.
--
-- Why a third intake rather than a write. `change_ledger.state` is written
-- through one door, the resident writer's `apply_measurement`, so that a row's
-- state and its measured revision cannot disagree. A producer outside the
-- account cannot take a measurement -- judging a row means reading the tree at a
-- revision, which is the writer's job -- and a producer that wrote a state
-- directly would be a second door with a second idea of what the state means.
--
-- Why it is needed at all. Three of the five retire reasons already come back on
-- their own: `surface_moved` through `retired_surface_moved`, `answered_elsewhere`
-- through `retired_answered_elsewhere`, `out_of_scope` when a tree is mounted for
-- it. The other two -- `fixture` and `superseded` -- are classifications made by
-- a rule outside this account. When that rule narrows, the rows it retired keep a
-- state that describes the rule as it was, and no read in this account revisits
-- them: `outstanding` excludes `retired` on the ground that the question is
-- answered, and that ground is false. Measured on this platform: a fixture
-- recogniser that matched `probe_` anywhere in a name retired 19 real principles,
-- the recogniser was corrected, and all 19 rows stayed retired while their
-- principle rows said `unenforced` -- two accounts disagreeing for as long as
-- anyone cared to look.
--
-- The key is the identity alone, for the reason the retirement intake gives: a
-- row deserves one re-measurement at a time, a second request for the same
-- identity is the same request, and keying on the identity makes a re-run
-- idempotent. `source` names who asked, for the trace a reader needs, without
-- being part of what makes the request unique.
--
-- `folded_at` is stamped after the measurement is written, not before: a writer
-- that dies mid-fold leaves the request unfolded and the next pass measures
-- again, which is idempotent, while the other order would drop the request and
-- nothing would say so.
CREATE TABLE IF NOT EXISTS change_ledger_remeasure (
    kind        VARCHAR(32)  NOT NULL,
    scope       VARCHAR(191) NOT NULL,
    literal     VARCHAR(512) NOT NULL,
    source      VARCHAR(512) NOT NULL,
    declared_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    folded_at   TIMESTAMPTZ,
    PRIMARY KEY (kind, scope, literal)
);

-- The fold scans for what has not been folded; that is the only query this
-- intake is read by.
CREATE INDEX IF NOT EXISTS idx_change_ledger_remeasure_folded_at
    ON change_ledger_remeasure(folded_at);
