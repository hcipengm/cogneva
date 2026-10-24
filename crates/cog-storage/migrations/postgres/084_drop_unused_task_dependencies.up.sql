-- Migration: 084_drop_unused_task_dependencies
-- 010_task_dependencies created task_dependencies, but nothing has ever written
-- it and nothing reads it. Dependency edges live on the task itself: Task
-- carries them in blocked_by, and the storage contract states outright that
-- implementations must not maintain a separate reverse index for them. In the
-- store-authoritative mode the DAG executor derives get_dependencies and
-- get_graph from blocked_by on the task rows, so a second pod opening the same
-- workspace sees the same edges without any other carrier -- which is exactly
-- what the two-pod test in the executor asserts.
--
-- Keeping it is not inert: it is a second, permanently empty copy of the edge
-- set, so a row count taken on it reads "no dependencies were recorded" while
-- the graph is fully populated. It also invites the double write the contract
-- forbids. Drop it so the edges have the one carrier the code uses.

DROP TABLE IF EXISTS task_dependencies;
