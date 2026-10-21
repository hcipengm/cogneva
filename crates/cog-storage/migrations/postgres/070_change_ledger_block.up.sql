-- Migration: 070_change_ledger_block
-- The face of the programme a change belongs to, as a grouping the account can
-- count.
--
-- The contents of a programme's faces were already in the ledger -- documents
-- that named a face became rows -- but the grouping never did: "how far along is
-- this face" was a substring search over provenance text, and two people asking
-- it typed two searches and got two answers. The heading is a property of the
-- document asking for a change, so it is stated by the declaration and folded
-- onto the row the same way the band is.
--
-- Folded, not chosen: when the documents asking for one change name more than
-- one face the row belongs to none of them. Picking one would file the work
-- under a heading half the people asking for it never chose, and the count of
-- rows in that state is published beside the per-face census, so a disagreement
-- never reads as silence.
--
-- The value is ASCII rather than the programme's own face names because it
-- travels through SQL, JSONB and metric labels, and a value that needs quoting
-- differs among the three. Declarations may still spell the face the way the
-- programme does; the intake folds those to these literals.
CREATE OR REPLACE FUNCTION change_ledger_block(sources JSONB)
    RETURNS TEXT
    LANGUAGE SQL
    IMMUTABLE
    PARALLEL SAFE
AS $$
    WITH named AS (
        SELECT DISTINCT entry->>'block' AS block
          FROM jsonb_array_elements(COALESCE(sources, '[]'::jsonb)) AS entry
         WHERE entry->>'block' IN ('token', 'resources', 'principles',
                                   'change', 'persistence', 'communication')
    )
    SELECT CASE WHEN count(*) = 1 THEN min(block) ELSE 'none' END FROM named
$$;

ALTER TABLE change_ledger
    ADD COLUMN IF NOT EXISTS block TEXT
        GENERATED ALWAYS AS (change_ledger_block(sources)) STORED;

-- The folded column is derived from the citations, so it cannot drift from
-- them, and a value the function does not recognise reads as `none` -- the same
-- direction the band's unknown values take: a face nobody defined must not
-- silently become a seventh heading.
ALTER TABLE change_ledger
    DROP CONSTRAINT IF EXISTS change_ledger_block_closed;
ALTER TABLE change_ledger
    ADD CONSTRAINT change_ledger_block_closed
        CHECK (block IN ('token', 'resources', 'principles', 'change',
                         'persistence', 'communication', 'none'));

ALTER TABLE change_ledger_declared
    ADD COLUMN IF NOT EXISTS block TEXT NOT NULL DEFAULT 'none';

ALTER TABLE change_ledger_declared
    DROP CONSTRAINT IF EXISTS change_ledger_declared_block_closed;
ALTER TABLE change_ledger_declared
    ADD CONSTRAINT change_ledger_declared_block_closed
        CHECK (block IN ('token', 'resources', 'principles', 'change',
                         'persistence', 'communication', 'none'));

-- The census groups every row by face -- landed, owed and retired -- so the
-- index covers the whole table rather than the outstanding slice: without it
-- each pass sorts the account, which is the same cost the band's index avoids
-- on the reads that hand rows out.
CREATE INDEX IF NOT EXISTS change_ledger_by_block_idx
    ON change_ledger (block);

-- The ordering key gains its first term: a change a commit delivered while the
-- account still owes it is holding the line, and it goes ahead of every band.
-- The index from the band's own migration led with `priority_rank` and no
-- longer matches the read's order, so it is replaced rather than left to be
-- ignored -- an index the planner cannot use is a sort on every pass wearing a
-- name that says otherwise.
DROP INDEX IF EXISTS change_ledger_outstanding_order_idx;
CREATE INDEX IF NOT EXISTS change_ledger_outstanding_order_idx
    ON change_ledger ((delivered_rev IS NOT NULL) DESC, priority_rank DESC,
                      measured_at ASC NULLS FIRST, first_seen_at ASC)
    WHERE state NOT IN ('landed', 'retired');
