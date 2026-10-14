-- Migration: 043_principle_refusal_session
-- Which session refused, and how many offending spots the refusal carried.
--
-- Both are facts the refusing side already had in hand and threw away, and both
-- were found by reading the numbers this intake already holds (2026-10-14).
-- Neither is about the refusal itself: a gate refuses correctly either way.
--
-- 1. `session_id`. A reader gets "this principle was refused 27 times today",
--    and that number cannot be read as how well the rule is kept, because two
--    variables that have nothing to do with it move it. One is how many sessions
--    were running -- the same behaviour across five sessions is two and a half
--    times the absolute number. The other is retrying: a refusal that is reworded
--    and sent again is refused again, and lands as a second row. Measured
--    2026-10-14: three sessions, 27 refusals, and inside one of them three pairs
--    11s / 17s / 33s apart against a nearest-neighbour gap of 17m39s -- each pair
--    is one thing, recorded as two. The session separates both variables, and the
--    gate already reads it (it looks the briefing marker up by session); it simply
--    never recorded it.
--
--    Which refusals make up one incident is *not* stored. A stored incident would
--    bake today's retry window into every future row, where it could not be
--    revised; the reader derives it from (session, slug, instant) instead.
--
--    `''` rather than NULL for a path with no session -- the platform's own gate
--    has none -- for the reason 037 gives one column over: the honest answer is a
--    value, and a reader selecting by value would silently skip a NULL.
--
-- 2. `hits`. The gate counts how many offending spots a refused call carried, says
--    so in the message it hands back, and then drops it: the record takes the slug
--    and nothing else. So a write carrying seven circled numerals and a write
--    carrying one have been the same row. Measured 2026-10-14: every one of that
--    day's 27 rows has `count = 1`, so `count` is a degenerate column -- it earns
--    its name only when one session is refused twice inside one second, which has
--    not happened. `hits` is the magnitude that was being computed and discarded.
--
-- The primary key grows to include `session_id`. Without it two sessions refused
-- in the same second under one slug and source collapse into a single row, and
-- that row states something that did not happen: it names whichever session
-- arrived first while counting both. The new column has a default, so the old
-- columns stay a prefix of the new key and every existing row already satisfies
-- it; rebuilding the index is the whole of the cost.
--
-- Merging keeps the larger report, exactly as `count` does and for the same reason
-- (see 031): the intake cannot tell a re-report from a later report about the same
-- second, and dropping the larger one silently under-counts.
--
-- Written inline and guarded so it can be run twice by hand, like 037: `ADD
-- PRIMARY KEY` has no `IF NOT EXISTS`, so the widening sits inside a check on the
-- key that is actually in place.
ALTER TABLE principle_refusals
    ADD COLUMN IF NOT EXISTS session_id VARCHAR(64) NOT NULL DEFAULT '';

ALTER TABLE principle_refusals
    ADD COLUMN IF NOT EXISTS hits BIGINT NOT NULL DEFAULT 1 CHECK (hits > 0);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'principle_refusals'::regclass
          AND contype = 'p'
          AND pg_get_constraintdef(oid) LIKE '%session_id%'
    ) THEN
        ALTER TABLE principle_refusals DROP CONSTRAINT IF EXISTS principle_refusals_pkey;
        ALTER TABLE principle_refusals
            ADD PRIMARY KEY (slug, refused_at, source, session_id);
    END IF;
END $$;
