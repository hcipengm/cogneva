-- Migration: 092_principle_refusal_shape
-- Which shape a refusal matched, and the text that matched it. See the Postgres
-- migration of the same name for why the account needs either, and why the key
-- grows to hold the criterion.
--
-- The same two dialect differences 043 records apply here: the runner splits on
-- `;`, so the guarded block the Postgres file uses is not available and the key
-- change is unguarded -- a hand re-run fails loudly on the second `DROP PRIMARY
-- KEY` rather than silently. Postgres is the dialect this platform runs.

ALTER TABLE principle_refusals
    ADD COLUMN criterion VARCHAR(64) NOT NULL DEFAULT '';

ALTER TABLE principle_refusals
    ADD COLUMN span VARCHAR(512) NOT NULL DEFAULT '';

ALTER TABLE principle_refusals
    DROP PRIMARY KEY;

ALTER TABLE principle_refusals
    ADD PRIMARY KEY (slug, refused_at, source, session_id, criterion);
