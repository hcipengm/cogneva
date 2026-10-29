-- Migration: 092_principle_refusal_shape (down)
-- Dropping the two columns loses the attribution and the text, not the count:
-- `principle_ledger.refusals` has already been folded from these rows and is
-- untouched. Rows written while the columns existed lose which shape they were
-- and join the same "shape unknown" population as the rows that predate them --
-- which is the honest answer, since after this the account cannot tell them
-- apart either.
--
-- The collapse is not optional, exactly as in 043's down: the wider key is what
-- let two shapes of one principle sit in the same second, and the narrower key
-- cannot hold both. Counts add, which is the rule the intake uses when it cannot
-- tell a re-report from a later report; the rows that were merged are dropped
-- after, so the survivor carries the whole of what the group held.
UPDATE principle_refusals r
SET count = m.count
FROM (
    SELECT slug, refused_at, source, session_id, sum(count) AS count
    FROM principle_refusals
    GROUP BY slug, refused_at, source, session_id
    HAVING count(*) > 1
) m
WHERE r.slug = m.slug
  AND r.refused_at = m.refused_at
  AND r.source = m.source
  AND r.session_id = m.session_id;

-- All but one row of each collapsed group: the criterion with the highest byte
-- order survives, since nothing else distinguishes them. A group of one has no
-- smaller sibling, so nothing unique is lost here.
DELETE FROM principle_refusals r
USING principle_refusals k
WHERE r.slug = k.slug
  AND r.refused_at = k.refused_at
  AND r.source = k.source
  AND r.session_id = k.session_id
  AND r.criterion < k.criterion;

-- Narrowing is guarded the same way the widening is: a second run has nothing to
-- do, and `ADD PRIMARY KEY` would fail on the key it just restored.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'principle_refusals'::regclass
          AND contype = 'p'
          AND pg_get_constraintdef(oid) LIKE '%criterion%'
    ) THEN
        ALTER TABLE principle_refusals DROP CONSTRAINT IF EXISTS principle_refusals_pkey;
        ALTER TABLE principle_refusals
            ADD PRIMARY KEY (slug, refused_at, source, session_id);
    END IF;
END $$;

ALTER TABLE principle_refusals DROP COLUMN IF EXISTS span;
ALTER TABLE principle_refusals DROP COLUMN IF EXISTS criterion;
