-- Migration: 045_principle_refusal_session_unknown
-- The session column's third state: the row's session was never recorded. See
-- the Postgres migration of the same name for the measurement this rests on and
-- for why the marker is `-`.
--
-- The statement is the same as the Postgres file's. No dialect difference here:
-- the only subquery reads `schema_migrations`, which is a different table, so
-- MySQL's rule against selecting from the table being updated in an `UPDATE`
-- does not apply. The runner splits this file on `;`, and there is one statement
-- to send, so nothing is lost in the split.

UPDATE principle_refusals
SET session_id = '-'
WHERE session_id = ''
  AND (
        briefed <> 'unknown'
     OR refused_at < (SELECT applied_at FROM schema_migrations WHERE version = 37)
  );
