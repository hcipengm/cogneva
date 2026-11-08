-- Migration: 095_task_ledger
-- The task graph stops being a document and becomes a ledger.
--
-- `cog_dag_tasks` held one jsonb per task and every writer rewrote the whole
-- document under a compare-and-set on its status column. Three failures came
-- out of that shape, all measured on this cluster:
--
--   * A lost update is one interleaving away: two writers that read the same
--     document and each change a different field overwrite each other, and no
--     constraint can tell which of the two the second one meant to keep.
--   * A value read once is frozen forever. `timeout_seconds` is computed from
--     measured samples at planning time and written into the document; nothing
--     recomputes it, so 388 tasks planned on 2026-11-08 still carried 3600
--     when the export of that hour said 3463.
--   * A key that is not there answers NULL instead of failing. The first query
--     of that day used `task->>'goal_id'`, returned zero rows without an error,
--     and was read as "nothing was completed today".
--
-- What replaces it: every state change is a row that is never updated, the
-- definition is versioned rather than overwritten, each execution is its own
-- row, and what the dispatcher reads is a projection that only holds rows that
-- can still move. A task's current state is the last transition, not a field
-- somebody remembered to copy forward.

CREATE TABLE IF NOT EXISTS task_defined (
    task_id              TEXT        NOT NULL,
    rev                  BIGINT      NOT NULL,
    at                   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    workspace_id         TEXT        NOT NULL,
    kind                 TEXT        NOT NULL,
    -- The task's own payload. It is opaque by design: its shape belongs to
    -- whoever produced it, so it is stored as text and is not queryable. What
    -- the platform judges on has a column below.
    input                TEXT        NOT NULL,
    created_at           TIMESTAMPTZ NOT NULL,
    priority             INTEGER     NOT NULL DEFAULT 0,
    max_retries          INTEGER     NOT NULL DEFAULT 0,
    timeout_seconds      BIGINT      NOT NULL DEFAULT 0,
    is_executable        BOOLEAN     NOT NULL DEFAULT TRUE,
    acceptance           TEXT[]      NOT NULL DEFAULT '{}',
    blocked_by           TEXT[]      NOT NULL DEFAULT '{}',
    agent_id             TEXT,
    goal_id              TEXT,
    parent_task_id       TEXT,
    sink_task_id         TEXT,
    face_id              TEXT,
    venue                TEXT,
    owner_unit           TEXT,
    bound_by_handover_id TEXT,
    submitted_by         TEXT,
    planner_verified     BOOLEAN,
    planner_version      TEXT,
    planner_note         TEXT,
    planner_source       TEXT,
    planner_confidence   DOUBLE PRECISION,
    planner_at           TIMESTAMPTZ,
    PRIMARY KEY (task_id, rev)
);

-- "The definition of this task" is the newest revision, and this is that read.
CREATE INDEX IF NOT EXISTS idx_task_defined_latest
    ON task_defined(task_id, rev DESC)
;
-- The dependency question is asked in both directions: what blocks this task
-- is a column, what this task blocks is the same column read backwards.
CREATE INDEX IF NOT EXISTS idx_task_defined_blocked_by
    ON task_defined USING GIN (blocked_by)
;
CREATE INDEX IF NOT EXISTS idx_task_defined_workspace
    ON task_defined(workspace_id, kind)
;
CREATE INDEX IF NOT EXISTS idx_task_defined_face
    ON task_defined(workspace_id, face_id)
;
CREATE INDEX IF NOT EXISTS idx_task_defined_venue
    ON task_defined(workspace_id, venue)
;
-- Which person's daily allowance a task's spend lands on is read once per
-- proxied call, so it is an index and not a scan.
CREATE INDEX IF NOT EXISTS idx_task_defined_submitted_by
    ON task_defined(submitted_by)
;

CREATE TABLE IF NOT EXISTS task_transition (
    task_id    TEXT        NOT NULL,
    seq        BIGINT      NOT NULL,
    at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    from_state TEXT,
    to_state   TEXT        NOT NULL,
    actor      TEXT,
    cause      TEXT,
    detail     TEXT,
    -- A transition may say "not before then". The retry backoff is a decision
    -- taken by the attempt that failed, so it is part of the record rather
    -- than a field on the projection that a rebuild would silently drop.
    not_before TIMESTAMPTZ,
    PRIMARY KEY (task_id, seq),
    CONSTRAINT task_transition_to_state_named
        CHECK (to_state IN ('pending', 'scheduled', 'running',
                            'completed', 'failed', 'cancelled'))
);
CREATE INDEX IF NOT EXISTS idx_task_transition_at
    ON task_transition(at)
;
-- Counting a window of outcomes is the question the boards ask most often.
CREATE INDEX IF NOT EXISTS idx_task_transition_state_at
    ON task_transition(to_state, at)
;

CREATE TABLE IF NOT EXISTS task_attempt (
    task_id    TEXT        NOT NULL,
    attempt_no INTEGER     NOT NULL,
    started_at TIMESTAMPTZ,
    ended_at   TIMESTAMPTZ,
    owner      TEXT,
    agent_id   TEXT,
    outcome    TEXT,
    error_code TEXT,
    error_text TEXT,
    -- What the attempt produced. Opaque for the same reason `input` is.
    result     TEXT,
    PRIMARY KEY (task_id, attempt_no)
);
-- "Which attempt is still open" is asked by the stall detector and by every
-- write that closes one.
CREATE INDEX IF NOT EXISTS idx_task_attempt_open
    ON task_attempt(task_id) WHERE ended_at IS NULL
;

-- The projection the dispatcher reads: one row per task that can still move.
-- A terminal row is not here -- its outcome is the last transition and the
-- attempt row that closed it. `as_of_seq` and `def_rev` are the watermarks:
-- "is this reading current" is a comparison, not an assumption.
CREATE TABLE IF NOT EXISTS task_current (
    workspace_id     TEXT        NOT NULL,
    task_id          TEXT        NOT NULL,
    state            TEXT        NOT NULL,
    as_of_seq        BIGINT      NOT NULL,
    def_rev          BIGINT      NOT NULL,
    updated_at       TIMESTAMPTZ NOT NULL,
    attempt_no       INTEGER     NOT NULL DEFAULT 0,
    started_at       TIMESTAMPTZ,
    lease_owner      TEXT,
    lease_expires_at TIMESTAMPTZ,
    retry_not_before TIMESTAMPTZ,
    retry_count      INTEGER     NOT NULL DEFAULT 0,
    error_code       TEXT,
    error_text       TEXT,
    PRIMARY KEY (workspace_id, task_id),
    CONSTRAINT task_current_is_live
        CHECK (state IN ('pending', 'scheduled', 'running'))
);
CREATE INDEX IF NOT EXISTS idx_task_current_state
    ON task_current(workspace_id, state)
;
CREATE INDEX IF NOT EXISTS idx_task_current_task_id
    ON task_current(task_id)
;
CREATE INDEX IF NOT EXISTS idx_task_current_lease
    ON task_current(lease_expires_at) WHERE lease_expires_at IS NOT NULL
;

-- ─── The history is append-only ───
--
-- A definition revision and a state transition are records of what happened.
-- Rewriting one has exactly one use: keeping a second copy of the truth
-- somewhere else and patching both. The database is the only place that can
-- refuse it for every writer, including a person at a psql prompt, so the
-- refusal lives here. Deleting is left open on purpose: removing a task and
-- clearing a workspace are explicit administrative acts, and both are
-- spelled out at their call sites.
--
-- One `DO` block rather than three statements: every pod runs the startup
-- batch, and two sessions running `CREATE OR REPLACE FUNCTION` (or TRIGGER)
-- at once get `tuple concurrently updated` -- measured, and every concurrent
-- test on a fresh schema died on that line. Lock first, look, create only
-- what is missing; on an installed database this is one catalog read.
-- 先上锁，单独一条语句：整批是一次隐式事务，事务级的咨询锁活到批尾；而
-- `pg_advisory_xact_lock` 等待不会刷新语句快照——锁和「查一下缺不缺」写在同一
-- 条语句里时，等到锁的那个会话仍拿着等之前取的照片，看见的永远是「还没有」，
-- 于是第二个会话也去建，拿到 `tuple concurrently updated`。拆成两条，锁到手之后
-- 那条 `DO` 取的是新快照，看见前一个会话建好的东西。
SELECT pg_advisory_xact_lock(hashtext('cogneva:task-ledger:append-only'))
;
DO $$
BEGIN
    IF to_regclass('task_defined') IS NULL THEN
        RETURN;
    END IF;
    IF to_regprocedure('task_ledger_history_is_append_only()') IS NULL THEN
        EXECUTE $create$
            CREATE FUNCTION task_ledger_history_is_append_only() RETURNS TRIGGER AS $body$
            BEGIN
                RAISE EXCEPTION '% is append-only: a history row may not be rewritten',
                    TG_TABLE_NAME;
            END
            $body$ LANGUAGE plpgsql
        $create$;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid
                    WHERE c.relname = 'task_defined'
                      AND t.tgname = 'task_defined_is_append_only' AND NOT t.tgisinternal)
    THEN
        EXECUTE $create$ CREATE TRIGGER task_defined_is_append_only
                         BEFORE UPDATE ON task_defined
                         FOR EACH ROW EXECUTE FUNCTION task_ledger_history_is_append_only()
                 $create$;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid
                    WHERE c.relname = 'task_transition'
                      AND t.tgname = 'task_transition_is_append_only' AND NOT t.tgisinternal)
    THEN
        EXECUTE $create$ CREATE TRIGGER task_transition_is_append_only
                         BEFORE UPDATE ON task_transition
                         FOR EACH ROW EXECUTE FUNCTION task_ledger_history_is_append_only()
                 $create$;
    END IF;
END
$$;

-- ─── Backfill from the document table ───
--
-- One definition row per task at rev 1, one genesis transition carrying the
-- state the document had, and a projection row for the tasks that can still
-- move. The document's own fields become the definition; `status` becomes the
-- transition; `retry_history` becomes attempt rows named by the attempt number
-- they already carry.
DO $$
BEGIN
    IF to_regclass('cog_dag_tasks') IS NULL THEN
        RETURN;
    END IF;

    INSERT INTO task_defined (
        task_id, rev, at, workspace_id, kind, input, created_at, priority,
        max_retries, timeout_seconds, is_executable, acceptance, blocked_by,
        agent_id, goal_id, parent_task_id, sink_task_id, face_id, venue,
        owner_unit, bound_by_handover_id, submitted_by,
        planner_verified, planner_version, planner_note, planner_source,
        planner_confidence, planner_at
    )
    SELECT
        task_id,
        1,
        updated_at,
        workspace_id,
        CASE WHEN jsonb_typeof(task -> 'task_type') = 'object'
             THEN COALESCE(task -> 'task_type' ->> 'custom', 'dag_node')
             ELSE COALESCE(task ->> 'task_type', 'dag_node')
        END,
        COALESCE(task -> 'input', 'null'::jsonb)::text,
        COALESCE((task ->> 'created_at')::timestamptz, updated_at),
        COALESCE((task ->> 'priority')::integer, 0),
        COALESCE((task ->> 'max_retries')::integer, 0),
        COALESCE((task ->> 'timeout_seconds')::bigint, 0),
        COALESCE((task ->> 'is_executable')::boolean, TRUE),
        CASE WHEN jsonb_typeof(task -> 'acceptance') = 'array'
             THEN ARRAY(SELECT jsonb_array_elements_text(task -> 'acceptance'))
             ELSE '{}'::text[] END,
        CASE WHEN jsonb_typeof(task -> 'blocked_by') = 'array'
             THEN ARRAY(SELECT jsonb_array_elements_text(task -> 'blocked_by'))
             ELSE '{}'::text[] END,
        NULLIF(task ->> 'agent_id', ''),
        NULLIF(task ->> 'goal_id', ''),
        NULLIF(task ->> 'parent_task_id', ''),
        NULLIF(task ->> 'sink_task_id', ''),
        NULLIF(task ->> 'face_id', ''),
        NULLIF(task ->> 'venue', ''),
        NULLIF(task ->> 'owner_unit', ''),
        NULLIF(task ->> 'bound_by_handover_id', ''),
        NULLIF(task ->> 'submitted_by', ''),
        (task -> 'action_planner_meta' ->> 'verified')::boolean,
        NULLIF(task -> 'action_planner_meta' ->> 'version', ''),
        NULLIF(task -> 'action_planner_meta' ->> 'note', ''),
        NULLIF(task -> 'action_planner_meta' ->> 'source', ''),
        (task -> 'action_planner_meta' ->> 'confidence')::double precision,
        (task -> 'action_planner_meta' ->> 'timestamp')::timestamptz
    FROM cog_dag_tasks
    ON CONFLICT (task_id, rev) DO NOTHING;

    -- The state the document was in becomes the first transition. `from_state`
    -- is NULL: this is a genesis, not a move from a state the document named.
    --
    -- A status this vocabulary does not name is filed as `failed`, and the
    -- spelling the document used is kept in `detail`. The alternatives are both
    -- worse: widening the check would let every future writer park a task in a
    -- state nothing can dispatch, and leaving the row out would drop a task
    -- from the ledger while this migration claims to hold everything the
    -- document held. It is filed as terminal rather than live because nothing
    -- in this build knows how to move a task out of a state it cannot name.
    INSERT INTO task_transition (task_id, seq, at, from_state, to_state, actor,
                                 cause, detail, not_before)
    SELECT task_id,
           1,
           updated_at,
           NULL,
           CASE WHEN status IN ('pending', 'scheduled', 'running',
                                'completed', 'failed', 'cancelled')
                THEN status ELSE 'failed' END,
           'migration',
           'backfill',
           CASE WHEN status IN ('pending', 'scheduled', 'running',
                                'completed', 'failed', 'cancelled')
                THEN NULL ELSE 'unknown_status:' || status END,
           NULLIF(task ->> 'retry_not_before', '')::timestamptz
    FROM cog_dag_tasks
    ON CONFLICT (task_id, seq) DO NOTHING;

    -- Only rows that can still move go into the projection. The whole point of
    -- the split is that this table stays small enough to lock.
    INSERT INTO task_current (
        workspace_id, task_id, state, as_of_seq, def_rev, updated_at,
        attempt_no, started_at, lease_owner, lease_expires_at,
        retry_not_before, retry_count, error_code, error_text
    )
    SELECT
        workspace_id,
        task_id,
        status,
        1,
        1,
        updated_at,
        CASE WHEN status = 'running'
             THEN COALESCE(jsonb_array_length(retry_history), 0) + 1
             ELSE COALESCE(jsonb_array_length(retry_history), 0) END,
        NULLIF(task ->> 'started_at', '')::timestamptz,
        NULLIF(task ->> 'lease_owner', ''),
        NULLIF(task ->> 'lease_expires_at', '')::timestamptz,
        (SELECT t.not_before FROM task_transition t
          WHERE t.task_id = cog_dag_tasks.task_id ORDER BY t.seq DESC LIMIT 1),
        COALESCE(jsonb_array_length(retry_history), 0),
        NULLIF(task -> 'error_cause' ->> 'kind', ''),
        NULLIF(task ->> 'error', '')
    FROM cog_dag_tasks
    WHERE status IN ('pending', 'scheduled', 'running')
    ON CONFLICT (workspace_id, task_id) DO NOTHING;

    -- One attempt row per entry the document already carried in
    -- `retry_history`. The entry's own attempt number is the key: numbering
    -- them by arrival order here would renumber history the failed attempt
    -- already refers to.
    INSERT INTO task_attempt (task_id, attempt_no, started_at, ended_at,
                              error_text)
    SELECT d.task_id,
           (e ->> 'attempt')::integer,
           NULL,
           (e ->> 'timestamp')::timestamptz,
           NULLIF(e ->> 'error', '')
    FROM cog_dag_tasks d
    CROSS JOIN LATERAL jsonb_array_elements(d.retry_history) AS e
    WHERE (e ->> 'attempt') ~ '^[0-9]+$'
    ON CONFLICT (task_id, attempt_no) DO NOTHING;

    -- A row that is running has an attempt that has not ended. Writing its
    -- number into the projection without the attempt row would point the
    -- stall detector at a number nothing owns.
    INSERT INTO task_attempt (task_id, attempt_no, started_at, owner)
    SELECT c.task_id,
           c.attempt_no,
           COALESCE(c.started_at, c.updated_at),
           c.lease_owner
    FROM task_current c
    WHERE c.state = 'running' AND c.attempt_no > 0
    ON CONFLICT (task_id, attempt_no) DO NOTHING;

    -- A finished task's last attempt is the one that carried its result. The
    -- document held that result inline and kept no attempt row for it, so
    -- without this the outcome of every task that has already completed would
    -- be dropped by a migration whose whole claim is that the ledger holds
    -- everything the document did.
    INSERT INTO task_attempt (task_id, attempt_no, ended_at, agent_id, outcome,
                              error_code, error_text, result)
    SELECT d.task_id,
           COALESCE(jsonb_array_length(d.retry_history), 0) + 1,
           d.updated_at,
           NULLIF(d.task ->> 'agent_id', ''),
           d.status,
           NULLIF(d.task -> 'error_cause' ->> 'kind', ''),
           NULLIF(d.task ->> 'error', ''),
           CASE WHEN d.task ? 'result' THEN (d.task -> 'result')::text END
    FROM cog_dag_tasks d
    WHERE d.status IN ('completed', 'failed', 'cancelled')
    ON CONFLICT (task_id, attempt_no) DO NOTHING;

    -- The document table goes in the same change rather than in a later one.
    -- Leaving it would leave two answers to "what state is this task in", and
    -- the first writer that reaches the old one reintroduces the lost update
    -- this migration removes. Its rows are all above.
    DROP TABLE cog_dag_tasks;
END
$$;
