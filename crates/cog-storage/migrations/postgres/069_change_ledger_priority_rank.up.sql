-- Migration: 069_change_ledger_priority_rank
-- The band a row is owed at, as a number the account can be ordered by.
--
-- A row's band is already answered by its sources: every citation carries the
-- band its document asked for, and the row's answer is the highest of them.
-- That answer is a computation over a JSONB array, so a reader cannot order by
-- it until after it holds the rows -- and the two reads that hand rows out take
-- a bounded slice of a set that is thousands deep, so the order has to be
-- decided before the slice is cut. Sorting in the caller would mean reading the
-- whole outstanding set every pass, which is exactly the bound the slice is
-- there to impose.
--
-- So the ledger keeps the rank beside the row, derived by the database from the
-- same `sources` bytes every reader sees. This is not the second answer the
-- contract refuses: that refusal is about a copy somebody has to remember to
-- update, which goes stale the first time a document corrects its band.
-- Re-prioritising a change is a corrected citation, which rewrites `sources`,
-- which recomputes this column in the same statement -- there is nothing to
-- keep in step, and a row whose citations say `unstated` cannot hold a rank
-- that says otherwise.
--
-- The map from band to rank is written once, in the function, and the numbers
-- are the contract's own: P0 4, P1 3, P2 2, P3 1, unstated 0. Unstated is zero
-- on purpose -- it means "no document asked for this band", and a row nothing
-- has prioritised must sort behind every row something did, without being
-- excluded: it is still work, it is just not urgent. A value outside the closed
-- set also reads as zero, which is the safe direction: an unknown band that
-- claimed to be urgent would push work into the head of the batch on a name
-- nobody defined.
CREATE OR REPLACE FUNCTION change_ledger_priority_rank(sources JSONB)
    RETURNS SMALLINT
    LANGUAGE SQL
    IMMUTABLE
    PARALLEL SAFE
AS $$
    SELECT COALESCE(max(CASE lower(entry->>'priority')
                             WHEN 'p0' THEN 4
                             WHEN 'p1' THEN 3
                             WHEN 'p2' THEN 2
                             WHEN 'p3' THEN 1
                             ELSE 0
                         END), 0)::SMALLINT
      FROM jsonb_array_elements(COALESCE(sources, '[]'::jsonb)) AS entry
$$;

-- Existing rows need no backfill: the expression is evaluated for every row as
-- the column is added, from the citations already stored. Rows folded before
-- the band existed have no `priority` key in their entries, which the function
-- reads as unstated -- the same answer the contract gives them, so the column
-- and the row's own `priority()` agree from the first read.
ALTER TABLE change_ledger
    ADD COLUMN IF NOT EXISTS priority_rank SMALLINT
        GENERATED ALWAYS AS (change_ledger_priority_rank(sources)) STORED;

-- The two bounded reads order by this column before their staleness keys, so
-- the index carries it first. Without one, every pass sorts the outstanding set
-- to take its slice.
CREATE INDEX IF NOT EXISTS change_ledger_outstanding_order_idx
    ON change_ledger (priority_rank DESC, measured_at ASC NULLS FIRST, first_seen_at ASC)
    WHERE state NOT IN ('landed', 'retired');
