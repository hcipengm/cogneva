-- Migration: 095_task_ledger (down)
-- Put the document table back, rebuilt from the ledger rather than from a
-- backup: the ledger is the truth, so a rollback that could not reconstruct
-- the old shape from it would mean the ledger never held everything the
-- document did.
--
-- Two facts cannot be recovered and are written as absent rather than as a
-- plausible zero: `error_cause` (the old document kept an enum the attempt
-- rows only stringified) and per-task `blocks` (derived from other rows'
-- `blocked_by`, so it is rebuilt by the same read rather than stored).
DO $$
BEGIN
    IF to_regclass('task_defined') IS NULL THEN
        RETURN;
    END IF;

    CREATE TABLE IF NOT EXISTS cog_dag_tasks (
        workspace_id TEXT NOT NULL,
        task_id TEXT NOT NULL,
        task JSONB NOT NULL,
        status TEXT NOT NULL,
        retry_history JSONB NOT NULL DEFAULT '[]'::jsonb,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (workspace_id, task_id)
    );

    INSERT INTO cog_dag_tasks (workspace_id, task_id, task, status, retry_history, updated_at)
    SELECT d.workspace_id,
           d.task_id,
           jsonb_strip_nulls(jsonb_build_object(
               'id', d.task_id,
               'task_type', CASE
                   WHEN d.kind IN ('planner', 'generator', 'evaluator', 'reviewer',
                                   'llm_call', 'tool_call', 'file_op',
                                   'db_transaction', 'network_request',
                                   'wasm_skill', 'skill', 'dag_node')
                   THEN to_jsonb(d.kind)
                   ELSE jsonb_build_object('custom', d.kind) END,
               'status', t.to_state,
               'input', d.input::jsonb,
               'result', a.result::jsonb,
               'error', a.error_text,
               'blocked_by', to_jsonb(d.blocked_by),
               'priority', d.priority,
               'created_at', d.created_at,
               'updated_at', t.at,
               'agent_id', COALESCE(a.agent_id, d.agent_id),
               'workspace_id', d.workspace_id,
               'retry_count', COALESCE(c.retry_count, (
                   SELECT count(*) FROM task_attempt x
                    WHERE x.task_id = d.task_id AND x.ended_at IS NOT NULL
                      AND x.outcome IS NOT NULL
                      AND x.outcome <> 'completed')),
               'max_retries', d.max_retries,
               'retry_not_before', t.not_before,
               'started_at', c.started_at,
               'timeout_seconds', d.timeout_seconds,
               'lease_owner', c.lease_owner,
               'lease_expires_at', c.lease_expires_at,
               'goal_id', d.goal_id,
               'parent_task_id', d.parent_task_id,
               'sink_task_id', d.sink_task_id,
               'is_executable', d.is_executable,
               'face_id', d.face_id,
               'venue', d.venue,
               'owner_unit', d.owner_unit,
               'bound_by_handover_id', d.bound_by_handover_id,
               'submitted_by', d.submitted_by,
               'acceptance', to_jsonb(d.acceptance),
               'action_planner_meta', CASE
                   WHEN d.planner_verified IS NULL THEN NULL
                   ELSE jsonb_strip_nulls(jsonb_build_object(
                       'verified', d.planner_verified,
                       'version', d.planner_version,
                       'note', d.planner_note,
                       'source', d.planner_source,
                       'confidence', d.planner_confidence,
                       'timestamp', d.planner_at)) END)),
           t.to_state,
           COALESCE((
               SELECT jsonb_agg(jsonb_build_object(
                          'attempt', x.attempt_no,
                          'error', COALESCE(x.error_text, ''),
                          'timestamp', COALESCE(x.ended_at, x.started_at))
                        ORDER BY x.attempt_no)
                 FROM task_attempt x
                WHERE x.task_id = d.task_id AND x.ended_at IS NOT NULL), '[]'::jsonb),
           t.at
      FROM (SELECT DISTINCT ON (task_id) *
              FROM task_defined
             ORDER BY task_id, rev DESC) d
      JOIN (SELECT DISTINCT ON (task_id) *
              FROM task_transition
             ORDER BY task_id, seq DESC) t ON t.task_id = d.task_id
      LEFT JOIN task_current c ON c.task_id = d.task_id
      LEFT JOIN (SELECT DISTINCT ON (task_id) *
                   FROM task_attempt
                  ORDER BY task_id, attempt_no DESC) a ON a.task_id = d.task_id
    ON CONFLICT (workspace_id, task_id) DO NOTHING;
END
$$;

DROP TABLE IF EXISTS task_current;
DROP TABLE IF EXISTS task_attempt;
DROP TABLE IF EXISTS task_transition;
DROP TABLE IF EXISTS task_defined;

-- The guard function outlives the tables it fires from: dropping a table drops
-- its triggers, not the function they call. Left behind, it is a residue of a
-- schema that no longer exists -- and one that a later reader has to classify
-- as either "someone meant this here" or "an uninstall did not finish".
DROP FUNCTION IF EXISTS task_ledger_history_is_append_only();
