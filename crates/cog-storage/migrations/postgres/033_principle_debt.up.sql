-- Migration: 033_principle_debt
-- One row per pass of the debt dispatcher: how many debt rows the tree owed,
-- what the pass did with them, and the ceiling it measured itself against.
--
-- Why this is a table rather than a log line. The dispatcher's own account of
-- itself is the only thing that answers "is the debt moving", and the answer is
-- a comparison -- this pass's submitted and over_bound against the last one's --
-- not a single number. A log line is not durable here: the evolution pod's
-- stdout reaches no collector on this host, so a count written only to the log
-- is a count that exists for as long as nobody looks, which is the same as
-- never having been written. The line stays (see the loop), as the second-hand
-- reading; this table is the durable one.
--
-- Append-only, and that is the whole schema decision. The row has no natural
-- key: two passes a minute apart are two readings, and the identity of a
-- reading is its position in the series rather than any of its values -- a pass
-- that measured the same numbers as the one before it is still a second
-- observation, and keying on the values would collapse it and hide that the
-- dispatcher had run at all. So the key is a surrogate and the store never
-- updates a row it wrote.
--
-- The counts are separate columns rather than one total, because the three move
-- for different reasons and only one of them is progress. `submitted` is work
-- handed to the pipeline; `already` is work that was in flight or done, which a
-- pass leaves alone; `failed` is work the pass could not hand over; and
-- `over_bound` is work the pass saw and did not propose because the in-flight
-- ceiling was already reached. Summing them would make a pass that re-proposed
-- everything and a pass that moved everything read the same, and the number a
-- reader would then tune the dispatcher against would reward proposing over
-- doing.
--
-- `bound` and `bound_source` are a pair, and neither is readable alone. The
-- bound is the ceiling the pass held itself to, and it is discovered from the
-- host rather than fixed: a reading that recorded only the number would leave a
-- fall back to the built-in default indistinguishable from the discovered
-- limit, and the whole point of the over_bound column is to say whether the
-- ceiling is what is holding the debt still. The source column names where the
-- number came from, so that question can be asked of the row.
--
-- `observed_at` is stamped by the store's clock, not carried by the caller: a
-- dispatcher that could name its own time could backdate a pass to look like it
-- ran before the work it moved, and the ordering of the series is what every
-- comparison on this table is drawn from.
--
-- `rev` names the tree the counts above are about. Without it a row says how much
-- debt there was without saying when, and only the process that wrote both rows
-- could compare them.
--
-- `delta` is the trend, and it is the one column here that is not a count of this
-- pass: it is how many debt rows the tree lost (positive) or gained (negative)
-- since the revision the previous row names. It is *read off the tree* rather
-- than off this table -- the count at the older revision is recovered from that
-- revision's own carrier files, which are in git -- so any reader holding the
-- two revisions can recompute the number and check it. A delta a process
-- remembered about itself is the one number the dispatcher must not grade itself
-- on, and this is the column a reader would otherwise have to take on trust.
--
-- NULL in `delta` is the absence of a comparison, not a comparison that came out
-- zero: the first pass of a process has no earlier revision to read, and a
-- revision that can no longer be resolved gives none either. Both are stored as
-- NULL rather than 0, because a week of no progress and a week nobody compared
-- are opposite readings of the same table.
--
-- No column is keyed and no row is deduplicated against another: the table is a
-- series, and its only reader question -- "the debt count now against the debt
-- count then" -- is answered by ordering, which `idx_principle_debt_observed_at`
-- exists for.

CREATE TABLE IF NOT EXISTS principle_debt (
    id           BIGSERIAL PRIMARY KEY,
    observed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    by_kind      TEXT NOT NULL,
    submitted    BIGINT NOT NULL DEFAULT 0,
    already      BIGINT NOT NULL DEFAULT 0,
    failed       BIGINT NOT NULL DEFAULT 0,
    over_bound   BIGINT NOT NULL DEFAULT 0,
    bound        BIGINT NOT NULL,
    bound_source VARCHAR(64) NOT NULL,
    rev          VARCHAR(64),
    delta        BIGINT
);

CREATE INDEX IF NOT EXISTS idx_principle_debt_observed_at
    ON principle_debt(observed_at);
