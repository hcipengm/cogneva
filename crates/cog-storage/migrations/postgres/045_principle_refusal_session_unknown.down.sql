-- Migration: 045_principle_refusal_session_unknown (down)
-- Back to the two-value column, and back to reading two meanings as one.
--
-- `-` is written by the up migration and by nothing else, so everything holding
-- it is a row this migration classified. What comes back is the value the column
-- held before -- not the sessions those rows refused in. Those were never
-- recorded and this cannot recover them; reversing the marker restores the
-- ambiguity deliberately, which is what a down migration for a classification is.

UPDATE principle_refusals
SET session_id = ''
WHERE session_id = '-';
