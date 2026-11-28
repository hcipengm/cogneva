-- The task row's number is a declaration, not a deadline: same rename as
-- 099_tasks_declared_duration on the PostgreSQL side, whose comment carries the
-- reason. MySQL has no `task_defined` projection, so only `tasks` moves.
ALTER TABLE tasks CHANGE timeout_seconds declared_duration_seconds INT UNSIGNED NOT NULL DEFAULT 300;
