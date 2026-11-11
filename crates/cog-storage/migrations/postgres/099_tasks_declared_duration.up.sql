-- The task row's number is a declaration, not a deadline.
--
-- The column was called `timeout_seconds`, and in this tree "timeout" means the
-- point at which something is killed: `run_command` still has one, the SSE
-- reader still has one. The task row's number is not that -- reclaim reads the
-- holder's lease and the measured inter-step window, and this number is only
-- what a reader sizes its own patience from. A schema reader who greps for
-- "timeout" finds a knife the platform removed, so the column is named for what
-- the number is.
ALTER TABLE tasks RENAME COLUMN timeout_seconds TO declared_duration_seconds;

ALTER TABLE task_defined RENAME COLUMN timeout_seconds TO declared_duration_seconds;
