-- Migration: 043_principle_refusal_session (down)
-- Puts the key back the way it was. See the Postgres migration of the same name:
-- the counts survive, and what is lost is whose refusal a row was and how many
-- offending spots it carried.
--
-- Unlike the Postgres file this one does not collapse the rows the wider key
-- allowed to coexist. MySQL has no `UPDATE ... FROM`, and its self-referencing
-- derived tables are materialised, so folding the counts in one statement is not
-- expressible here. A table holding two rows in one second under one slug and
-- source with two different sessions therefore fails the `ADD PRIMARY KEY` below
-- on the duplicate, which names the pair rather than dropping one of them. No
-- such row exists in any deployment this dialect has ever had.

ALTER TABLE principle_refusals
    DROP PRIMARY KEY;

ALTER TABLE principle_refusals
    ADD PRIMARY KEY (slug, refused_at, source);

ALTER TABLE principle_refusals
    DROP COLUMN session_id;

ALTER TABLE principle_refusals
    DROP COLUMN hits;
