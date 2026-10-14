-- Migration: 043_principle_refusal_session
-- Which session refused, and how many offending spots the refusal carried. See
-- the Postgres migration of the same name for why the intake needs either.
--
-- Two differences from the Postgres file, both forced by this dialect and its
-- runner rather than chosen. The runner splits a migration on `;` and sends each
-- piece as its own statement, so the anonymous block the Postgres file uses to
-- guard the key is not available here, and MySQL has no conditional DDL in a
-- plain statement. The key widening below is therefore unguarded: a hand re-run
-- fails on the second `ADD PRIMARY KEY`, which is a loud failure at a stated
-- place rather than a silent one. Postgres is the dialect this platform runs;
-- this file exists to keep the two describing the same table.
--
-- `hits` is BIGINT rather than an unsigned type with the bound folded in: 037
-- declined a CHECK for the same reason, and an unsigned column would accept a
-- zero where the meaning is "at least one offending spot".

ALTER TABLE principle_refusals
    ADD COLUMN session_id VARCHAR(64) NOT NULL DEFAULT '';

ALTER TABLE principle_refusals
    ADD COLUMN hits BIGINT NOT NULL DEFAULT 1;

ALTER TABLE principle_refusals
    DROP PRIMARY KEY;

ALTER TABLE principle_refusals
    ADD PRIMARY KEY (slug, refused_at, source, session_id);
