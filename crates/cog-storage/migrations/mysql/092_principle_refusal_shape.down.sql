-- Migration: 092_principle_refusal_shape (down)
-- Drops the attribution, keeps the counts, and merges the rows the wider key
-- allowed before narrowing it. See the Postgres migration of the same name for
-- why the merge is not optional.

UPDATE principle_refusals r
    JOIN (
        SELECT slug, refused_at, source, session_id, SUM(count) AS count
        FROM principle_refusals
        GROUP BY slug, refused_at, source, session_id
        HAVING COUNT(*) > 1
    ) m ON r.slug = m.slug AND r.refused_at = m.refused_at
       AND r.source = m.source AND r.session_id = m.session_id
SET r.count = m.count;

DELETE r FROM principle_refusals r
    JOIN principle_refusals k
      ON r.slug = k.slug AND r.refused_at = k.refused_at
     AND r.source = k.source AND r.session_id = k.session_id
     AND r.criterion < k.criterion;

ALTER TABLE principle_refusals
    DROP PRIMARY KEY;

ALTER TABLE principle_refusals
    ADD PRIMARY KEY (slug, refused_at, source, session_id);

ALTER TABLE principle_refusals DROP COLUMN span;

ALTER TABLE principle_refusals DROP COLUMN criterion;
