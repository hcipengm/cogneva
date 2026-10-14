-- Migration: 045_principle_refusal_session_unknown (down)
-- Back to the two-value column. See the Postgres down migration of the same name
-- for what this does and does not restore.

UPDATE principle_refusals
SET session_id = ''
WHERE session_id = '-';
