-- Migration: 043_principle_refusal_session (down)
-- Puts the key back the way it was. The refusals keep counting exactly as before,
-- so nothing about the account's arithmetic changes; what is lost is the ability
-- to tell whose refusal a row was and how many spots it carried, and with it the
-- per-session reading. The rows are gone for good: no other column derives either
-- value, and re-adding the columns cannot reconstruct a fact the gate held at the
-- instant of the refusal and did not write down.
--
-- The collapse is not optional. The wider key is what let one session's refusal
-- and another's sit in the same second under one slug and source; the narrower key
-- cannot hold both, and adding it without merging would simply fail on the
-- duplicate. The merge keeps what each column is for -- counts add, magnitudes
-- take the larger, the same rule the intake applies when it cannot tell a
-- re-report from a later report -- and the rows it merged are dropped after it,
-- so the survivor carries the whole of what the group held.
UPDATE principle_refusals r
SET count = m.count, hits = m.hits
FROM (
    SELECT slug, refused_at, source, sum(count) AS count, max(hits) AS hits
    FROM principle_refusals
    GROUP BY slug, refused_at, source
    HAVING count(*) > 1
) m
WHERE r.slug = m.slug AND r.refused_at = m.refused_at AND r.source = m.source;

-- All but one row of each collapsed group. A group of one has no smaller sibling
-- to match, so nothing unique is lost here.
DELETE FROM principle_refusals r
USING principle_refusals k
WHERE r.slug = k.slug
  AND r.refused_at = k.refused_at
  AND r.source = k.source
  AND r.session_id < k.session_id;

-- Narrowing is guarded the same way the widening is: a second run has nothing to
-- do, and `ADD PRIMARY KEY` would fail on the key it just restored.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'principle_refusals'::regclass
          AND contype = 'p'
          AND pg_get_constraintdef(oid) LIKE '%session_id%'
    ) THEN
        ALTER TABLE principle_refusals DROP CONSTRAINT IF EXISTS principle_refusals_pkey;
        ALTER TABLE principle_refusals
            ADD PRIMARY KEY (slug, refused_at, source);
    END IF;
END $$;

ALTER TABLE principle_refusals DROP COLUMN IF EXISTS session_id;
ALTER TABLE principle_refusals DROP COLUMN IF EXISTS hits;
