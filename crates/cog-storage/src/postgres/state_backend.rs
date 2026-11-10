use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json;
use sqlx::PgPool;
use std::collections::HashMap;

use cog_core::contract::outcome::FailureCause;
use cog_core::{
    AgentState, ContextBoard, Event, SFError, SFResult, StateBackend, TaskCheckpoint,
    UpstreamFailure,
};

use super::state_reads::{
    agent_state_from_json, checkpoint_from_row, AgentStateRow, CheckpointRow, AGENT_STATE_SQL,
    CHECKPOINT_SQL,
};
use super::task_ledger::{self, ledger_sql, LedgerRow};

/// The read of one task's event sequence from a position onward.
///
/// Named as a constant because the statement is the thing that has to hold for
/// the read to be a page rather than a sort of the task's whole history: the
/// `(task_id, offset_num)` index `init_schema` creates answers both conditions
/// and already delivers the rows in the order asked for, so the database does
/// no sort. A statement written inline is one nobody measures; this one is
/// explained by a test.
pub const EVENTS_PAGE_SQL: &str = r#"
    SELECT event_type, payload, offset_num, created_at
    FROM cog_events
    WHERE task_id = $1 AND offset_num >= $2
    ORDER BY offset_num ASC
    LIMIT $3
"#;

/// The rows the venue board folds, read in one statement.
///
/// The statement is the thing that has to hold for the board's read to be a
/// lookup: `idx_task_defined_venue` answers its predicate, and the label is few
/// rows in a large ledger, so the read must reach them through that index
/// rather than by folding every task's revisions. A test explains this
/// function against a table shaped like the live one and goes red when the plan
/// stops being an index scan.
///
/// The `btrim` clause is a filter after the index, not part of it -- an empty
/// label and a label of spaces are the same thing (`btrim` of either is empty),
/// and neither is a venue.
///
/// The venue read, as the store runs it.
///
/// A function rather than a constant because the statement is built from the
/// ledger's join, and a copy written out here would go on passing after the
/// store's stopped being indexable. The test that reads its plan calls this,
/// so it explains the same text production runs.
pub fn venue_tasks_sql() -> String {
    // 内层是索引能回答的那一问（任何一个修订带过标签），外层才是真正要的那一问
    // （**最新一版**带标签）。只在最内层筛，一件任务曾经挂过标签、后来被清掉的
    // 任务会以空标签回来；只在最外层筛，索引就用不上，读的代价跟着账本长度走。
    ledger_sql(
        "workspace_id = $1 AND venue IS NOT NULL AND btrim(venue) <> ''",
        "venue IS NOT NULL AND btrim(venue) <> ''",
    )
}

/// 完成 / 失败 / 取消一条任务前，先锁住它所在 workspace 的全部任务行。
///
/// 这条语句同时被多个执行进程跑（同一个 workspace 下不止一个执行器在结算任务），
/// 而它锁的是一个集合：不带 `ORDER BY` 时两个事务可以按相反的顺序取到同一批元组，
/// 各自握着一行、等对方那一行，Postgres 检测到环就杀掉其中一个。被杀掉的那个事务
/// 正是一次**已经算完但结果没落库**的完成——下一位持锁者只看得到「租约过期」，
/// 于是把整条任务重新派发、重新花一遍钱。2026-11-05 一天在生产日志里抓到 13 次，
/// 上下文全是这条语句。
///
/// `ORDER BY task_id` 给这批行一个全序，两个事务就只能是「同一顺序排队」而不是
/// 「互相等」。
pub const DAG_LOCK_WORKSPACE_SQL: &str =
    "SELECT task_id FROM task_current WHERE workspace_id = $1 ORDER BY task_id FOR UPDATE";

/// This store's schema, and the one a restore replays.
pub const DDL_TEMPLATE: &str = r#"
CREATE TABLE IF NOT EXISTS cog_agent_states (
                agent_id TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
;
CREATE TABLE IF NOT EXISTS cog_checkpoints (
                task_id TEXT PRIMARY KEY,
                snapshot_id TEXT NOT NULL,
                event_offset BIGINT NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
;
CREATE TABLE IF NOT EXISTS cog_events (
                id SERIAL PRIMARY KEY,
                task_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                payload JSONB NOT NULL,
                offset_num BIGINT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
;
CREATE INDEX IF NOT EXISTS idx_cog_events_task_id ON cog_events(task_id)
;
-- The offset is the backend's own assignment -- the sequence position a reader
-- resumes from -- so two events of one task may not share one. The count and
-- the insert are two round trips, and a task's owner is reclaimed on a lease,
-- which presumes the previous holder stopped rather than stopping it, so the
-- two writers only meet here: without the constraint the sequence can hand two
-- events the same position and a reader resuming from it loses one for good.
-- The sibling Redis backend gets this for free (its append returns the new
-- length), so dropping it leaves Postgres alone in being able to break that.
--
-- The same index answers `EVENTS_PAGE_SQL`: `task_id` and `offset_num` in one
-- btree, in the order the page asks for, so the read is an index scan instead
-- of a filter-then-sort. `idx_cog_events_task_id` is its prefix and therefore
-- redundant; it stays because removing it buys nothing measurable at one event
-- per iteration.
CREATE UNIQUE INDEX IF NOT EXISTS idx_cog_events_task_offset
    ON cog_events(task_id, offset_num)
;
-- The retention asks which task streams hold a row older than a cutoff and no
-- row newer than it, so both halves of that question are a range over the
-- timestamps -- and the answer is a set of task ids. `(created_at, task_id)`
-- answers both from the index alone, instead of reading addresses out of an
-- index built on the timestamp and then fetching every row to learn its task.
-- Without it the sweep's candidate selection is a sequential scan of the whole
-- archive once every two minutes.
CREATE INDEX IF NOT EXISTS idx_cog_events_created_at_task
    ON cog_events(created_at, task_id)
;
CREATE TABLE IF NOT EXISTS cog_context_boards (
                task_id TEXT NOT NULL,
                field_name TEXT NOT NULL,
                field_value TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (task_id, field_name)
            )
;
-- The checkpoint sweep asks, for each candidate row, whether any board field
-- names it; without this index that question is a sequential scan of every
-- board per candidate. A field value is an opaque string, so the index is on
-- the value alone -- and it has to be a hash index rather than a btree: a
-- btree keeps the value itself in the entry and refuses anything past a third
-- of a page, while boards do hold long values (one row of 11,456 bytes was
-- measured 2026-10-27). The question asked of it is equality only, which is
-- what a hash index answers.
CREATE INDEX IF NOT EXISTS idx_cog_context_boards_field_value
    ON cog_context_boards USING hash (field_value)
;
CREATE TABLE IF NOT EXISTS cog_dag_state (
                workspace_id TEXT PRIMARY KEY,
                state JSONB NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
;
-- The task graph is a ledger, not a document: every state change is a row that
-- is never updated, the definition is versioned rather than overwritten, and
-- what the dispatcher reads is a projection holding only rows that can still
-- move. The three failures the document shape produced -- a lost update one
-- interleaving away, a value read once and frozen forever, and a missing key
-- that answers NULL instead of failing -- are each a property of a mutable
-- document, not of the code that wrote it.
CREATE TABLE IF NOT EXISTS task_defined (
                task_id TEXT NOT NULL,
                rev BIGINT NOT NULL,
                at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                workspace_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                input TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                priority INTEGER NOT NULL DEFAULT 0,
                max_retries INTEGER NOT NULL DEFAULT 0,
                timeout_seconds BIGINT NOT NULL DEFAULT 0,
                is_executable BOOLEAN NOT NULL DEFAULT TRUE,
                acceptance TEXT[] NOT NULL DEFAULT '{}',
                blocked_by TEXT[] NOT NULL DEFAULT '{}',
                agent_id TEXT,
                goal_id TEXT,
                parent_task_id TEXT,
                sink_task_id TEXT,
                face_id TEXT,
                venue TEXT,
                owner_unit TEXT,
                bound_by_handover_id TEXT,
                submitted_by TEXT,
                planner_verified BOOLEAN,
                planner_version TEXT,
                planner_note TEXT,
                planner_source TEXT,
                planner_confidence DOUBLE PRECISION,
                planner_at TIMESTAMPTZ,
                PRIMARY KEY (task_id, rev)
            )
;
CREATE INDEX IF NOT EXISTS idx_task_defined_latest
            ON task_defined(task_id, rev DESC)
;
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
-- Which person's daily allowance a task's spend lands on is asked once per
-- proxied call, so it is an index and not a scan.
CREATE INDEX IF NOT EXISTS idx_task_defined_submitted_by
            ON task_defined(submitted_by)
;
CREATE TABLE IF NOT EXISTS task_transition (
                task_id TEXT NOT NULL,
                seq BIGINT NOT NULL,
                at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                from_state TEXT,
                to_state TEXT NOT NULL,
                actor TEXT,
                cause TEXT,
                detail TEXT,
                -- A transition may say "not before then". The retry backoff is
                -- a decision taken by the attempt that failed, so it is part of
                -- the record rather than a field on the projection that a
                -- rebuild would silently drop.
                not_before TIMESTAMPTZ,
                PRIMARY KEY (task_id, seq),
                CONSTRAINT task_transition_to_state_named
                    CHECK (to_state IN ('{task_status:pending}', '{task_status:scheduled}',
                                        '{task_status:running}', '{task_status:completed}',
                                        '{task_status:failed}', '{task_status:cancelled}'))
            )
;
CREATE INDEX IF NOT EXISTS idx_task_transition_at
            ON task_transition(at)
;
CREATE INDEX IF NOT EXISTS idx_task_transition_state_at
            ON task_transition(to_state, at)
;
CREATE TABLE IF NOT EXISTS task_attempt (
                task_id TEXT NOT NULL,
                attempt_no INTEGER NOT NULL,
                started_at TIMESTAMPTZ,
                ended_at TIMESTAMPTZ,
                owner TEXT,
                agent_id TEXT,
                outcome TEXT,
                error_code TEXT,
                error_text TEXT,
                result TEXT,
                PRIMARY KEY (task_id, attempt_no)
            )
;
CREATE INDEX IF NOT EXISTS idx_task_attempt_open
            ON task_attempt(task_id) WHERE ended_at IS NULL
;
-- The projection the dispatcher reads: one row per task that can still move.
-- `as_of_seq` and `def_rev` are watermarks -- "is this reading current" is a
-- comparison, not an assumption.
CREATE TABLE IF NOT EXISTS task_current (
                workspace_id TEXT NOT NULL,
                task_id TEXT NOT NULL,
                state TEXT NOT NULL,
                as_of_seq BIGINT NOT NULL,
                def_rev BIGINT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                attempt_no INTEGER NOT NULL DEFAULT 0,
                started_at TIMESTAMPTZ,
                lease_owner TEXT,
                lease_expires_at TIMESTAMPTZ,
                retry_not_before TIMESTAMPTZ,
                retry_count INTEGER NOT NULL DEFAULT 0,
                error_code TEXT,
                error_text TEXT,
                PRIMARY KEY (workspace_id, task_id),
                CONSTRAINT task_current_is_live
                    CHECK (state IN ('{task_status:pending}', '{task_status:scheduled}',
                                     '{task_status:running}'))
            )
;
CREATE INDEX IF NOT EXISTS idx_task_current_state
            ON task_current(workspace_id, state)
;
CREATE INDEX IF NOT EXISTS idx_task_current_task_id
            ON task_current(task_id)
;
CREATE INDEX IF NOT EXISTS idx_task_current_lease
            ON task_current(lease_expires_at) WHERE lease_expires_at IS NOT NULL
;
-- 定义修订与状态迁移是账本的**历史**，只许追加。改写它们只有一种来路：有人在
-- 别处又存了一份可以就地改的真值，然后两头开始互相矛盾。这条不变式交给数据库
-- 判，不交给每个写者自己记得——删（撤掉一件任务、清一个工作区）仍然是显式的
-- 管理动作，改不是。
--
-- 这段是 `DO` 而不是三条 `CREATE OR REPLACE`：每个 pod 启动都会跑一遍这个批，
-- 而 `CREATE OR REPLACE FUNCTION` / `TRIGGER` 在两个会话同时执行时拿到的是
-- `tuple concurrently updated`（实测：同一个库上并行起的用例全倒在这一句上）。
-- 「先上锁、再查、缺才建」在并发下是同一件事，在已经建好的库上是一次目录查询。
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
-- 一条任务的计划基线：它「打算怎么做」。
--
-- 与定义表并排而不是加列进去：基线在分解那一刻就定了、之后不改，
-- 混在一张每次重新定义都要追加一行的表里，甘特面就再也画不出偏差。
--
-- 这里**没有状态列**，这是刻意的：状态的唯一正本在转换表里，
-- 两边各存一份就是两份真值，迟早互相矛盾。
CREATE TABLE IF NOT EXISTS task_plan (
                workspace_id TEXT NOT NULL,
                task_id TEXT NOT NULL,
                planned_start TIMESTAMPTZ,
                planned_end TIMESTAMPTZ,
                budget_tokens BIGINT NOT NULL,
                baseline_rev TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (workspace_id, task_id)
            )
;
CREATE TABLE IF NOT EXISTS cog_evolution_promotions (
                id TEXT PRIMARY KEY,
                change_id TEXT NOT NULL,
                level TEXT NOT NULL,
                gate_kind TEXT,
                decision_reason TEXT NOT NULL,
                cluster TEXT NOT NULL,
                status TEXT NOT NULL,
                outcome TEXT NOT NULL,
                eval_summary TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
;
ALTER TABLE cog_evolution_promotions
            ADD COLUMN IF NOT EXISTS gate_kind TEXT
;
CREATE INDEX IF NOT EXISTS idx_cog_evolution_promotions_updated
            ON cog_evolution_promotions(updated_at DESC)
;
DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'cog_evolution_promotions' AND column_name = 'patch_id'
                ) AND NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'cog_evolution_promotions' AND column_name = 'change_id'
                ) THEN
                    ALTER TABLE cog_evolution_promotions RENAME COLUMN patch_id TO change_id;
                END IF;
            END $$;
"#;

/// The startup schema with every closed set filled from the enum that declares
/// it.
///
/// The task tables' `CHECK` constraints name the same words `TaskStatus::as_str`
/// writes. Left as literals they would be a second declaration: a renamed
/// variant would keep being refused by the constraint, and the first write
/// after the rename would fail on a schema that reads as current. Filled from
/// [`TaskStatus::ALL`], the constraint cannot disagree with the writer.
pub fn ddl() -> String {
    let mut slots: Vec<(&str, &str)> = Vec::new();
    for status in cog_core::TaskStatus::ALL {
        slots.push(("task_status", status.as_str()));
    }
    crate::postgres::fill_slots(DDL_TEMPLATE, &slots)
}

pub struct PostgresStateBackend {
    pool: PgPool,
}

/// What a state transition must still be true of the row at the moment it lands.
///
/// The status check every transition makes is one of these; the two below are the
/// conditions particular callers add. They are a struct rather than boolean
/// parameters because a call site reading `Some(unit), true` says nothing about
/// what the `true` guards, and the two answers here are never both in play.
#[derive(Default, Clone, Copy)]
struct TransitionGuard<'a> {
    /// The row's `owner_unit` must be this unit, and `updated` must still name it.
    owner: Option<&'a str>,
    /// The row must be unclaimed: 接活只能成功一次。
    claim: bool,
}

impl PostgresStateBackend {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Retry a database operation with exponential backoff.
    async fn retry<T, F, Fut>(&self, mut op: F) -> SFResult<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, sqlx::Error>>,
    {
        let mut last_err = None;
        for attempt in 0..3 {
            match op().await {
                Ok(v) => return Ok(v),
                Err(e) => {
                    last_err = Some(e);
                    if attempt < 2 {
                        tokio::time::sleep(std::time::Duration::from_millis(
                            200 * (attempt + 1) as u64,
                        ))
                        .await;
                    }
                }
            }
        }
        Err(SFError::Database(
            last_err
                .map(|e| e.to_string())
                .unwrap_or_else(|| "unknown database error".into()),
        ))
    }

    /// Auto-create the required tables and indexes if they do not exist.
    pub async fn init_schema(&self) -> SFResult<()> {
        crate::schema_ddl::apply(&self.pool, &ddl()).await
    }

    /// Rebuild `task_current` from the ledger, and say how many live rows
    /// resulted.
    ///
    /// This is the command the projection's claim of being rebuildable rests
    /// on: it runs the write path's own statements, so what a rebuild produces
    /// cannot differ from what the writers would have produced. `None`
    /// rebuilds every task; `Some(id)` rebuilds one, which is what a reader
    /// that suspects a stale row wants.
    ///
    /// What a rebuild cannot restore is the lease (`lease_owner`,
    /// `lease_expires_at`): presence is not history, so those come back NULL,
    /// which every reader already handles as "no evidence this is running".
    pub async fn rebuild_task_projection(&self, task_id: Option<&str>) -> SFResult<u64> {
        task_ledger::rebuild(&self.pool, task_id).await
    }

    /// The tasks of a workspace that can still move, read under the lock the
    /// caller already holds.
    ///
    /// This is the set every bulk decision works on: a terminal task cannot be
    /// completed, failed, cancelled or made ready, so a read that carried them
    /// would spend its time on rows that can only be skipped.
    async fn live_tasks(
        &self,
        conn: &mut sqlx::PgConnection,
        workspace_id: &str,
    ) -> SFResult<std::collections::HashMap<String, cog_core::Task>> {
        let sql = ledger_sql(
            "workspace_id = $1",
            &crate::postgres::fill_slots(
                "t.to_state IN ('{task_status:pending}', '{task_status:scheduled}', \
                 '{task_status:running}')",
                &task_ledger::live_status_slots(),
            ),
        );
        let rows: Vec<LedgerRow> = sqlx::query_as(&sql)
            .bind(workspace_id)
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| SFError::Database(e.to_string()))?;
        rows.into_iter()
            .map(|row| {
                let id = row.task_id.clone();
                row.into_task().map(|t| (id, t))
            })
            .collect()
    }

    /// The state every one of `ids` is in, read from the transition that put it
    /// there.
    ///
    /// A dependency that finished is not in the live projection, so readiness
    /// cannot be answered from that table: "not in the live set" covers both
    /// "completed" and "gone".
    async fn states_of(
        &self,
        conn: &mut sqlx::PgConnection,
        ids: &[String],
    ) -> SFResult<std::collections::HashMap<String, cog_core::TaskStatus>> {
        if ids.is_empty() {
            return Ok(std::collections::HashMap::new());
        }
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT DISTINCT ON (task_id) task_id, to_state FROM task_transition \
             WHERE task_id = ANY($1) ORDER BY task_id, seq DESC",
        )
        .bind(ids)
        .fetch_all(&mut *conn)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;
        Ok(rows
            .into_iter()
            .filter_map(|(id, state)| cog_core::TaskStatus::parse(&state).map(|s| (id, s)))
            .collect())
    }

    /// Move a task from one of `expected` to the state `updated` carries.
    ///
    /// The compare-and-set the document shape did with `UPDATE ... WHERE
    /// status = ANY(...)` is here a read of the last transition under the
    /// workspace lock, so the check and the write are one decision rather than
    /// two statements a second writer can slip between.
    ///
    /// `guard` carries the extra conditions a particular caller's write has to
    /// meet at that same instant; see [`TransitionGuard`].
    async fn transition_task(
        &self,
        workspace_id: &str,
        task_id: &str,
        expected: &[cog_core::TaskStatus],
        updated: &cog_core::Task,
        guard: TransitionGuard<'_>,
    ) -> SFResult<()> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| SFError::Database(e.to_string()))?;
        task_ledger::lock_workspace(&mut tx, workspace_id).await?;

        let current = task_ledger::current_state(&mut tx, task_id).await?;
        let Some(current) = current else {
            return Err(SFError::TaskFailed {
                task_id: task_id.into(),
                reason: "Task not found".into(),
            });
        };

        if let Some(owner) = guard.owner {
            let row: Option<(Option<String>,)> = sqlx::query_as(
                "SELECT owner_unit FROM task_defined WHERE task_id = $1 \
                 ORDER BY rev DESC LIMIT 1",
            )
            .bind(task_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| SFError::Database(e.to_string()))?;
            let stored = row.and_then(|(o,)| o);
            if stored.as_deref() != Some(owner) {
                return Err(SFError::Conflict(format!(
                    "task {task_id} is owned by {}, not {owner}",
                    stored.as_deref().unwrap_or("nobody")
                )));
            }
            // Moving the owner is a handover's decision, not a transition's:
            // a payload that names somebody else would write a second answer
            // to "whose is this" beside the one the handover wrote.
            if updated.owner_unit.as_deref() != Some(owner) {
                return Err(SFError::Conflict(format!(
                    "task {task_id} cannot be moved to owner {} by a state transition",
                    updated.owner_unit.as_deref().unwrap_or("nobody")
                )));
            }
        }

        if guard.claim {
            // 接活只能成功一次。守卫读的是库里这一行自己的认领，不是调用方读到
            // 的快照：两个消费者同时读到「待接」时，第二个的写必须落不下来，否则
            // 同一条活会被两个人同时接走，而两边都会认为自己有责任去干。
            //
            // 持有 = 有个名字、且那个人的租约还没到期，与 `Task::claim_held_now`
            // 同一个口径。只读到期的名字不行：一条回到队里、名字还留着的行会永远
            // 「已经被 X 接了」，而 X 早就不在了——那条活从此没人接得走。
            // `Pending` 行上的名字同样不算认领（那是上一轮交付的人）。
            let row: Option<(Option<String>, Option<chrono::DateTime<chrono::Utc>>)> =
                sqlx::query_as(
                    "SELECT d.agent_id, c.lease_expires_at \
                       FROM (SELECT agent_id FROM task_defined \
                              WHERE task_id = $1 AND workspace_id = $2 \
                              ORDER BY rev DESC LIMIT 1) d \
                       LEFT JOIN task_current c \
                         ON c.task_id = $1 AND c.workspace_id = $2",
                )
                .bind(task_id)
                .bind(workspace_id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| SFError::Database(e.to_string()))?;
            let held = row.and_then(|(agent, expires)| {
                agent.filter(|_| expires.is_some_and(|expires| expires > chrono::Utc::now()))
            });
            if current != cog_core::TaskStatus::Pending {
                if let Some(held) = held {
                    return Err(SFError::Conflict(format!(
                        "task {task_id} is already claimed by {held}"
                    )));
                }
            }
        }

        if !expected.contains(&current) {
            // 两条路对「状态不对」的回答不同，因为这个错在这个调用方那里含义不同：
            // 带 owner 的那条是「我读到的快照过期了，重读一次」，不带 owner 的那条
            // 是「这件任务不该被我推进」。把它报成一个，前者的重试逻辑会退化成把
            // 一次过期读当成本次任务的失败。
            //
            // 接活走的是前一种：一次没接上（别人先接了）不是这条活坏了，是这次
            // 读取过期了。
            return Err(match (guard.owner, guard.claim) {
                (Some(_), _) | (_, true) => SFError::Conflict(format!(
                    "task {task_id} is in {:?} state, not one of the states this transition accepts",
                    current
                )),
                (None, false) => SFError::TaskFailed {
                    task_id: task_id.into(),
                    reason: format!("Cannot transition task in {current:?} state"),
                },
            });
        }

        task_ledger::save_definition(&mut tx, workspace_id, updated).await?;
        if updated.status != current {
            match updated.status {
                cog_core::TaskStatus::Running => {
                    task_ledger::open_attempt(&mut tx, task_id, updated.lease_owner.as_deref())
                        .await?;
                }
                landed => {
                    if current == cog_core::TaskStatus::Running {
                        task_ledger::close_attempt(
                            &mut tx,
                            task_id,
                            landed,
                            None,
                            updated.error.as_deref(),
                            updated.result.as_ref(),
                            updated.agent_id.as_deref(),
                        )
                        .await?;
                    }
                }
            }
            task_ledger::append_transition(
                &mut tx,
                &task_ledger::Transition {
                    task_id,
                    from: Some(current),
                    to: updated.status,
                    actor: "store",
                    cause: "transition",
                    detail: None,
                    not_before: updated.retry_not_before,
                },
            )
            .await?;
        }

        task_ledger::project(&mut tx, Some(task_id)).await?;
        sqlx::query(
            "UPDATE task_current SET lease_owner = $2, lease_expires_at = $3 WHERE task_id = $1",
        )
        .bind(task_id)
        .bind(&updated.lease_owner)
        .bind(updated.lease_expires_at)
        .execute(&mut *tx)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        tx.commit()
            .await
            .map_err(|e| SFError::Database(e.to_string()))?;
        Ok(())
    }
}

/// 派发还要看的那几列，按 `TaskStatus::ALL` 的枚举收集，不从 SQL 里抄名字。
///
/// 单拎出来是因为这条查询换成「在这些状态里」之后，判据的完整性不再由 SQL 的
/// `<>` 顺手兜住：漏掉一个状态，那一列的行就悄悄读不回来了，而读不回来的行
/// 停在原地，看起来和「还没轮到它」一模一样。所以谁都可以读这个集合，也可以
/// 对它断言（见本文件末尾那条测试），但没人能在这里手写第二个名单。
fn live_status_filter() -> Vec<String> {
    cog_core::TaskStatus::ALL
        .iter()
        .filter(|status| status.affects_dispatch())
        .map(|status| status.as_str().to_string())
        .collect()
}

/// 一行负预算不是「零预算」，也不是可以顺手取绝对值的怪值：写入侧从不产生它，
/// 读到了就说明这一行是别人写坏的，报出来比折算成 0 更接近事实。
fn negative_budget(workspace_id: &str, task_id: &str, budget: i64) -> SFError {
    SFError::Database(format!(
        "task_plan row {workspace_id}/{task_id} holds a negative budget ({budget}); \
         a plan that never spent anything is 0, not below it"
    ))
}

#[async_trait]
impl StateBackend for PostgresStateBackend {
    async fn get_agent_state(&self, agent_id: &str) -> SFResult<Option<AgentState>> {
        let row: Option<AgentStateRow> = self
            .retry(|| async {
                sqlx::query_as(AGENT_STATE_SQL)
                    .bind(agent_id)
                    .fetch_optional(&self.pool)
                    .await
            })
            .await?;

        match row {
            Some((document,)) => Ok(Some(agent_state_from_json(&document)?)),
            None => Ok(None),
        }
    }

    async fn set_agent_state(&self, agent_id: &str, state: &AgentState) -> SFResult<()> {
        let value = serde_json::to_string(state)?;
        self.retry(|| async {
            sqlx::query(
                r#"
                INSERT INTO cog_agent_states (agent_id, state, updated_at)
                VALUES ($1, $2, NOW())
                ON CONFLICT (agent_id) DO UPDATE SET state = EXCLUDED.state, updated_at = NOW()
                "#,
            )
            .bind(agent_id)
            .bind(value.clone())
            .execute(&self.pool)
            .await
        })
        .await?;
        Ok(())
    }

    async fn cas_agent_state(
        &self,
        agent_id: &str,
        expected: &AgentState,
        new: &AgentState,
    ) -> SFResult<bool> {
        let expected_json = serde_json::to_string(expected)?;
        let new_json = serde_json::to_string(new)?;

        let result = self
            .retry(|| async {
                sqlx::query(
                    r#"
                    UPDATE cog_agent_states
                    SET state = $1, updated_at = NOW()
                    WHERE agent_id = $2 AND state = $3
                    "#,
                )
                .bind(new_json.clone())
                .bind(agent_id)
                .bind(expected_json.clone())
                .execute(&self.pool)
                .await
            })
            .await?;

        Ok(result.rows_affected() == 1)
    }

    async fn get_checkpoint(&self, task_id: &str) -> SFResult<Option<TaskCheckpoint>> {
        let row: Option<CheckpointRow> = self
            .retry(|| async {
                sqlx::query_as(CHECKPOINT_SQL)
                    .bind(task_id)
                    .fetch_optional(&self.pool)
                    .await
            })
            .await?;

        row.map(|row| checkpoint_from_row(task_id, row)).transpose()
    }

    async fn save_checkpoint(&self, checkpoint: &TaskCheckpoint) -> SFResult<()> {
        self.retry(|| async {
            sqlx::query(
                r#"
                INSERT INTO cog_checkpoints (task_id, snapshot_id, event_offset, timestamp)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (task_id) DO UPDATE SET
                    snapshot_id = EXCLUDED.snapshot_id,
                    event_offset = EXCLUDED.event_offset,
                    timestamp = EXCLUDED.timestamp
                "#,
            )
            .bind(&checkpoint.task_id)
            .bind(&checkpoint.snapshot_id)
            .bind(checkpoint.event_offset as i64)
            .bind(checkpoint.timestamp)
            .execute(&self.pool)
            .await
        })
        .await?;
        Ok(())
    }

    async fn delete_checkpoint(&self, task_id: &str) -> SFResult<()> {
        self.retry(|| async {
            sqlx::query("DELETE FROM cog_checkpoints WHERE task_id = $1")
                .bind(task_id)
                .execute(&self.pool)
                .await
        })
        .await?;
        Ok(())
    }

    async fn append_event(&self, task_id: &str, event: &Event) -> SFResult<u64> {
        let payload = serde_json::to_value(event)?;
        let event_type = event.event_type.clone();

        // The next position is one past the last the task holds, and a task's
        // writers take turns reading it: `MAX` reads a snapshot that a
        // concurrent writer's uncommitted row is not part of, so two writers
        // would compute one position for two events -- and the sequence's
        // constraint would refuse the second, dropping the event. Retrying is
        // not a substitute: the appends keep pace with each other, and what
        // comes out is an append that failed rather than an event that landed.
        // The lock is per task (its key is the task id, so appends to different
        // tasks do not queue) and is released by the transaction that took it.
        //
        // The position is read back from the inserted row rather than echoed
        // from what was computed: the number a caller stores as its resume
        // point has to be the one the sequence holds.
        let offset: i64 = self
            .retry(|| async {
                let mut tx = self.pool.begin().await?;
                sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1))")
                    .bind(task_id)
                    .execute(&mut *tx)
                    .await?;

                let offset: i64 = sqlx::query_scalar(
                    r#"
                    INSERT INTO cog_events (task_id, event_type, payload, offset_num, created_at)
                    SELECT $1, $2, $3, COALESCE(MAX(offset_num), 0) + 1, NOW()
                    FROM cog_events
                    WHERE task_id = $1
                    RETURNING offset_num
                    "#,
                )
                .bind(task_id)
                .bind(event_type.clone())
                .bind(payload.clone())
                .fetch_one(&mut *tx)
                .await?;

                tx.commit().await?;
                Ok(offset)
            })
            .await?;

        Ok(offset as u64)
    }

    async fn get_events(&self, task_id: &str, offset: u64, limit: usize) -> SFResult<Vec<Event>> {
        let rows: Vec<(String, serde_json::Value, i64, chrono::DateTime<Utc>)> = self
            .retry(|| async {
                sqlx::query_as(EVENTS_PAGE_SQL)
                    .bind(task_id)
                    .bind(offset as i64)
                    .bind(limit as i64)
                    .fetch_all(&self.pool)
                    .await
            })
            .await?;

        let mut events = Vec::with_capacity(rows.len());
        for (event_type, payload, offset_num, created_at) in rows {
            // The column holds the serialized event, not its payload: the writer
            // above stores `serde_json::to_value(event)`, so an event's own
            // payload sits under the envelope's `payload` key. Returning the
            // column directly put a level of wrapping in front of readers that
            // the sibling backends do not add -- Redis round-trips the event
            // through `serde_json` and the in-memory one keeps the `Event`
            // itself -- so the same append read back meant two different things
            // depending on which backend the process was wired with, and the one
            // that diverged is the one production runs. A reader that scans a
            // payload for a field found nothing here and everything under the
            // backend its own tests run against.
            //
            // Only the payload is taken from the envelope: the row's position is
            // the offset. A row that is not an envelope comes back as itself
            // rather than as NULL, because appending is the only way this column
            // is ever written -- such a row is a defect elsewhere, not a shape
            // for this read to invent.
            let payload = match payload.get("payload") {
                Some(inner) => inner.clone(),
                None => payload,
            };
            events.push(Event {
                offset: offset_num as u64,
                task_id: task_id.into(),
                event_type,
                payload,
                timestamp: created_at,
            });
        }
        Ok(events)
    }

    async fn get_board(&self, task_id: &str) -> SFResult<Option<ContextBoard>> {
        let rows: Vec<(String, String, chrono::DateTime<Utc>)> = self
            .retry(|| async {
                sqlx::query_as(
                    r#"
                    SELECT field_name, field_value, updated_at
                    FROM cog_context_boards
                    WHERE task_id = $1
                    "#,
                )
                .bind(task_id)
                .fetch_all(&self.pool)
                .await
            })
            .await?;

        if rows.is_empty() {
            return Ok(None);
        }

        let mut fields = HashMap::with_capacity(rows.len());
        let mut updated_at = Utc::now();
        for (field_name, field_value, row_updated_at) in rows {
            fields.insert(field_name, field_value);
            if row_updated_at > updated_at {
                updated_at = row_updated_at;
            }
        }

        Ok(Some(ContextBoard {
            task_id: task_id.into(),
            fields,
            updated_at,
        }))
    }

    async fn set_board_field(&self, task_id: &str, field: &str, value: &str) -> SFResult<()> {
        self.retry(|| async {
            sqlx::query(
                r#"
                INSERT INTO cog_context_boards (task_id, field_name, field_value, updated_at)
                VALUES ($1, $2, $3, NOW())
                ON CONFLICT (task_id, field_name) DO UPDATE SET
                    field_value = EXCLUDED.field_value,
                    updated_at = NOW()
                "#,
            )
            .bind(task_id)
            .bind(field)
            .bind(value)
            .execute(&self.pool)
            .await
        })
        .await?;
        Ok(())
    }

    async fn delete_board(&self, task_id: &str) -> SFResult<()> {
        self.retry(|| async {
            sqlx::query("DELETE FROM cog_context_boards WHERE task_id = $1")
                .bind(task_id)
                .execute(&self.pool)
                .await
        })
        .await?;
        Ok(())
    }

    async fn remove_board_field(&self, task_id: &str, field: &str) -> SFResult<()> {
        self.retry(|| async {
            sqlx::query("DELETE FROM cog_context_boards WHERE task_id = $1 AND field_name = $2")
                .bind(task_id)
                .bind(field)
                .execute(&self.pool)
                .await
        })
        .await?;
        Ok(())
    }

    async fn save_dag_state(&self, workspace_id: &str, state: &serde_json::Value) -> SFResult<()> {
        self.retry(|| async {
            sqlx::query(
                r#"
                INSERT INTO cog_dag_state (workspace_id, state, updated_at)
                VALUES ($1, $2, NOW())
                ON CONFLICT (workspace_id) DO UPDATE SET
                    state = EXCLUDED.state,
                    updated_at = NOW()
                "#,
            )
            .bind(workspace_id)
            .bind(state.clone())
            .execute(&self.pool)
            .await
        })
        .await?;
        Ok(())
    }

    async fn load_dag_state(&self, workspace_id: &str) -> SFResult<Option<serde_json::Value>> {
        let row: Option<(serde_json::Value,)> = self
            .retry(|| async {
                sqlx::query_as("SELECT state FROM cog_dag_state WHERE workspace_id = $1")
                    .bind(workspace_id)
                    .fetch_optional(&self.pool)
                    .await
            })
            .await?;

        Ok(row.map(|r| r.0))
    }

    fn dag_supports_fine_grained(&self) -> bool {
        true
    }

    async fn dag_get_task(
        &self,
        workspace_id: &str,
        task_id: &str,
    ) -> SFResult<Option<cog_core::Task>> {
        let sql = ledger_sql("workspace_id = $1 AND task_id = $2", "");
        let row: Option<LedgerRow> = self
            .retry(|| async {
                sqlx::query_as(&sql)
                    .bind(workspace_id)
                    .bind(task_id)
                    .fetch_optional(&self.pool)
                    .await
            })
            .await?;
        row.map(LedgerRow::into_task).transpose()
    }

    /// Write the task as the caller has it.
    ///
    /// Three different things are written here and they are kept apart on
    /// purpose: the definition goes in as a revision (only if it moved), the
    /// state goes in as a transition (only if it moved), and the presence --
    /// the lease and the attempt -- goes straight into the projection, because
    /// neither is history. A caller that writes the same task twice appends
    /// nothing the second time.
    async fn dag_set_task(
        &self,
        workspace_id: &str,
        task_id: &str,
        task: &cog_core::Task,
    ) -> SFResult<()> {
        if task.id != task_id {
            return Err(SFError::Validation(format!(
                "writing task {task_id} with a body that names {}",
                task.id
            )));
        }
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| SFError::Database(e.to_string()))?;
        task_ledger::write_task(&mut tx, workspace_id, task).await?;
        tx.commit()
            .await
            .map_err(|e| SFError::Database(e.to_string()))?;
        Ok(())
    }

    async fn dag_transition_task(
        &self,
        workspace_id: &str,
        task_id: &str,
        expected: &[cog_core::TaskStatus],
        updated: &cog_core::Task,
    ) -> SFResult<()> {
        self.transition_task(
            workspace_id,
            task_id,
            expected,
            updated,
            TransitionGuard::default(),
        )
        .await
    }

    async fn dag_claim_task(
        &self,
        workspace_id: &str,
        task_id: &str,
        expected: &[cog_core::TaskStatus],
        updated: &cog_core::Task,
    ) -> SFResult<()> {
        self.transition_task(
            workspace_id,
            task_id,
            expected,
            updated,
            TransitionGuard {
                claim: true,
                ..TransitionGuard::default()
            },
        )
        .await
    }

    async fn dag_transition_owned_task(
        &self,
        workspace_id: &str,
        task_id: &str,
        expected: &[cog_core::TaskStatus],
        updated: &cog_core::Task,
        owner: &str,
    ) -> SFResult<()> {
        self.transition_task(
            workspace_id,
            task_id,
            expected,
            updated,
            TransitionGuard {
                owner: Some(owner),
                ..TransitionGuard::default()
            },
        )
        .await
    }

    /// Remove every row a task owns.
    ///
    /// A purge rather than a state change: nothing about this task is history
    /// worth keeping, which is exactly the case the append-only rule does not
    /// cover. It still goes through one transaction so a half-removed task --
    /// a definition with no transition, which reads as "defined and never
    /// moved" -- cannot be observed.
    async fn dag_remove_task(&self, workspace_id: &str, task_id: &str) -> SFResult<()> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| SFError::Database(e.to_string()))?;
        for statement in [
            "DELETE FROM task_current WHERE task_id = $1",
            "DELETE FROM task_attempt WHERE task_id = $1",
            "DELETE FROM task_transition WHERE task_id = $1",
            "DELETE FROM task_defined WHERE task_id = $1 AND workspace_id = $2",
        ] {
            let mut query = sqlx::query(statement).bind(task_id);
            if statement.contains("$2") {
                query = query.bind(workspace_id);
            }
            query
                .execute(&mut *tx)
                .await
                .map_err(|e| SFError::Database(e.to_string()))?;
        }
        tx.commit()
            .await
            .map_err(|e| SFError::Database(e.to_string()))?;
        Ok(())
    }

    async fn dag_list_tasks(&self, workspace_id: &str) -> SFResult<Vec<String>> {
        let rows: Vec<(String,)> = self
            .retry(|| async {
                sqlx::query_as(
                    "SELECT DISTINCT task_id FROM task_defined WHERE workspace_id = $1 \
                     ORDER BY task_id",
                )
                .bind(workspace_id)
                .fetch_all(&self.pool)
                .await
            })
            .await?;
        Ok(rows.into_iter().map(|r| r.0).collect())
    }

    async fn dag_get_all_tasks(&self, workspace_id: &str) -> SFResult<Vec<cog_core::Task>> {
        let sql = ledger_sql("workspace_id = $1", "");
        let rows: Vec<LedgerRow> = self
            .retry(|| async {
                sqlx::query_as(&sql)
                    .bind(workspace_id)
                    .fetch_all(&self.pool)
                    .await
            })
            .await?;
        rows.into_iter().map(LedgerRow::into_task).collect()
    }

    /// 只有带场地标签的那些行；没有标签的整行不进结果。
    ///
    /// 标签是定义表上的一列，按 `(workspace_id, venue)` 的索引定位；整张图绝
    /// 大多数行没有标签（实测 2026-11-06：9,991 行里 9,987 行没有），所以那
    /// 个条件把读的量级压在这几行上，而不是压在一次整表扫描之后。
    ///
    /// 终态行必须留下：看板的「最近一条判据」和「写回量」两列读的就是已经
    /// 做完的行，排掉它们会让两列变空，而空的读数和「今天没交活」长得一样。
    async fn dag_get_venue_tasks(&self, workspace_id: &str) -> SFResult<Vec<cog_core::Task>> {
        let sql = ledger_sql(
            "workspace_id = $1 AND venue IS NOT NULL AND btrim(venue) <> ''",
            "",
        );
        let rows: Vec<LedgerRow> = self
            .retry(|| async {
                sqlx::query_as(&sql)
                    .bind(workspace_id)
                    .fetch_all(&self.pool)
                    .await
            })
            .await?;
        rows.into_iter().map(LedgerRow::into_task).collect()
    }

    /// 只有派发还要看的那些行；`failed` / `cancelled` 在查询里就被排掉。
    ///
    /// 收哪些状态由 [`cog_core::TaskStatus::affects_dispatch`] 一个地方决定，
    /// 这里只把它的答案拼成参数——SQL 里再抄一遍字符串，两处就会各自漂移，
    /// 而漂移的那一天表现为「某些行忽然不再被派发」。
    async fn dag_get_live_tasks(&self, workspace_id: &str) -> SFResult<Vec<cog_core::Task>> {
        let live = live_status_filter();
        let sql = ledger_sql("workspace_id = $1", "t.to_state = ANY($2::text[])");
        let rows: Vec<LedgerRow> = self
            .retry(|| async {
                sqlx::query_as(&sql)
                    .bind(workspace_id)
                    .bind(live.as_slice())
                    .fetch_all(&self.pool)
                    .await
            })
            .await?;
        rows.into_iter().map(LedgerRow::into_task).collect()
    }

    /// 只读某一状态的行。
    ///
    /// 状态是转换表最后一行，按 `(task_id, seq)` 的索引定位；读的范围跟着判据
    /// 走，而不是先把整张图读一遍再筛。
    async fn dag_get_tasks_in_status(
        &self,
        workspace_id: &str,
        status: cog_core::TaskStatus,
    ) -> SFResult<Vec<cog_core::Task>> {
        let sql = ledger_sql("workspace_id = $1", "t.to_state = $2");
        let rows: Vec<LedgerRow> = self
            .retry(|| async {
                sqlx::query_as(&sql)
                    .bind(workspace_id)
                    .bind(status.as_str())
                    .fetch_all(&self.pool)
                    .await
            })
            .await?;
        rows.into_iter().map(LedgerRow::into_task).collect()
    }

    /// 只数，不把行搬回来。
    ///
    /// 任务图读数每拍要八个状态的条数，而图只增不减：同一台库上整表读
    /// 3,598 行 / 1.0–1.8 秒 / 每小时 1,628 次（本机实测，2026-11-09）。数一条
    /// `GROUP BY` 与取回全部行是两件事，前者不随工作区的历史变贵。
    async fn dag_task_status_counts(&self, workspace_id: &str) -> SFResult<Vec<(String, u64)>> {
        // 与 `ledger_sql` 同两条索引：每个任务取最新一版定义定工作区，取最后一条
        // transition 定状态，再分组数。这不是「另一种读法」而是同一张账的另一种问法——
        // 把整行搬回 Rust 里数，代价就跟着工作区的历史走。
        let rows: Vec<(String, i64)> = self
            .retry(|| async {
                sqlx::query_as(
                    r#"
SELECT t.to_state, COUNT(*)::bigint
  FROM (SELECT DISTINCT ON (task_id) task_id FROM task_defined
         WHERE workspace_id = $1
         ORDER BY task_id, rev DESC) d
  JOIN LATERAL (
        SELECT to_state FROM task_transition
         WHERE task_id = d.task_id
         ORDER BY seq DESC LIMIT 1
       ) t ON TRUE
 GROUP BY t.to_state
"#,
                )
                .bind(workspace_id)
                .fetch_all(&self.pool)
                .await
            })
            .await?;
        Ok(rows
            .into_iter()
            .map(|(status, n)| (status, n.max(0) as u64))
            .collect())
    }

    /// 只读还能动的那几个状态的行。
    ///
    /// 与 [`Self::dag_get_tasks_in_status`] 同一条账：内层按工作区取任务，外层按状态
    /// 筛最新一版——读数要的是「Pending / Scheduled / Running 的行长什么样」，终态行
    /// 在这一问里一行都不需要。
    async fn dag_get_tasks_in_states(
        &self,
        workspace_id: &str,
        states: &[cog_core::TaskStatus],
    ) -> SFResult<Vec<cog_core::Task>> {
        let wanted: Vec<String> = states.iter().map(|s| s.as_str().to_string()).collect();
        let sql = ledger_sql("workspace_id = $1", "to_state = ANY($2::text[])");
        let rows: Vec<LedgerRow> = self
            .retry(|| async {
                sqlx::query_as(&sql)
                    .bind(workspace_id)
                    .bind(&wanted)
                    .fetch_all(&self.pool)
                    .await
            })
            .await?;
        rows.into_iter().map(LedgerRow::into_task).collect()
    }

    // ─── 计划基线（task_plan）───

    async fn save_task_plan(&self, plan: &cog_core::TaskPlan) -> SFResult<()> {
        // Postgres 没有无符号整数，而预算在类型上是 u64。超界的数绑进去会被
        // 静默折成一个错误的值，所以在这里拒绝而不是让它绕过去：一份读回来
        // 是负数的预算是比没有预算更坏的事。
        let budget = i64::try_from(plan.budget_tokens).map_err(|_| {
            SFError::Validation(format!(
                "task plan budget {} does not fit the BIGINT column that holds it",
                plan.budget_tokens
            ))
        })?;
        let workspace_id = plan.workspace_id.clone();
        let task_id = plan.task_id.clone();
        let baseline_rev = plan.baseline_rev.clone();
        let planned_start = plan.planned_start;
        let planned_end = plan.planned_end;
        self.retry(|| async {
            sqlx::query(
                r#"
                INSERT INTO task_plan
                    (workspace_id, task_id, planned_start, planned_end,
                     budget_tokens, baseline_rev, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, NOW())
                ON CONFLICT (workspace_id, task_id) DO UPDATE SET
                    planned_start = EXCLUDED.planned_start,
                    planned_end = EXCLUDED.planned_end,
                    budget_tokens = EXCLUDED.budget_tokens,
                    baseline_rev = EXCLUDED.baseline_rev,
                    updated_at = NOW()
                "#,
            )
            .bind(&workspace_id)
            .bind(&task_id)
            .bind(planned_start)
            .bind(planned_end)
            .bind(budget)
            .bind(&baseline_rev)
            .execute(&self.pool)
            .await
        })
        .await?;
        Ok(())
    }

    async fn task_plan(
        &self,
        workspace_id: &str,
        task_id: &str,
    ) -> SFResult<Option<cog_core::TaskPlan>> {
        type Row = (
            Option<chrono::DateTime<Utc>>,
            Option<chrono::DateTime<Utc>>,
            i64,
            String,
            chrono::DateTime<Utc>,
        );
        let row: Option<Row> = self
            .retry(|| async {
                sqlx::query_as(
                    r#"
                    SELECT planned_start, planned_end, budget_tokens, baseline_rev,
                           updated_at
                    FROM task_plan
                    WHERE workspace_id = $1 AND task_id = $2
                    "#,
                )
                .bind(workspace_id)
                .bind(task_id)
                .fetch_optional(&self.pool)
                .await
            })
            .await?;
        let budget = match row {
            None => return Ok(None),
            Some((_, _, budget, _, _)) if budget < 0 => {
                return Err(negative_budget(workspace_id, task_id, budget))
            }
            Some(row) => row,
        };
        let (planned_start, planned_end, budget, baseline_rev, updated_at) = budget;
        Ok(Some(cog_core::TaskPlan {
            workspace_id: workspace_id.to_string(),
            task_id: task_id.to_string(),
            planned_start,
            planned_end,
            budget_tokens: budget as u64,
            baseline_rev,
            updated_at,
        }))
    }

    async fn task_plans(&self, workspace_id: &str) -> SFResult<Vec<cog_core::TaskPlan>> {
        let rows: Vec<(
            String,
            Option<chrono::DateTime<Utc>>,
            Option<chrono::DateTime<Utc>>,
            i64,
            String,
            chrono::DateTime<Utc>,
        )> = self
            .retry(|| async {
                sqlx::query_as(
                    r#"
                    SELECT task_id, planned_start, planned_end, budget_tokens,
                           baseline_rev, updated_at
                    FROM task_plan
                    WHERE workspace_id = $1
                    "#,
                )
                .bind(workspace_id)
                .fetch_all(&self.pool)
                .await
            })
            .await?;
        rows.into_iter()
            .map(
                |(task_id, planned_start, planned_end, budget, baseline_rev, updated_at)| {
                    if budget < 0 {
                        return Err(negative_budget(workspace_id, &task_id, budget));
                    }
                    Ok(cog_core::TaskPlan {
                        workspace_id: workspace_id.to_string(),
                        task_id,
                        planned_start,
                        planned_end,
                        budget_tokens: budget as u64,
                        baseline_rev,
                        updated_at,
                    })
                },
            )
            .collect()
    }

    async fn task_kind_records(
        &self,
        workspace_id: &str,
        min_samples: u64,
    ) -> SFResult<Vec<cog_core::TaskKindRecord>> {
        // 三件事一起定：哪些行算样本、按什么分组、中位怎么取。
        //
        // 样本只有「跑完的」：跑了一半的任务时长没有意义，失败被重排的那几次会
        // 把同一条活算成好几个样本。种类拼写与 [`cog_core::TaskType::kind_key`]
        // 是同一把尺子（自定义取 `custom`，其余取变体名），分叉的那天这里读成
        // 空，表现是「没有样本」而不是报错——所以两处必须一起改。
        //
        // 工时的两端是「最后一次开工」（`task_attempt.started_at`）与「完成那条
        // 迁移」（`task_transition.at`），不是创建与最新更新：创建到开工之间的
        // 那段是**排队时间**，不是工期。这条差别不是小数点上的：现网
        // 五条 change_ledger 样本创建于 10-18、10-25 才被拾取，按创建量是 7.7 天，
        // 按开工量是 7 分钟——前者排出来的甘特条，会让 2354 条待办每一行都画成
        // 一周长，而它们实际各跑几分钟。没有开始时刻的行不贡献时长样本（它的
        // 工时从没被量过），所以 `samples` 数的是「量过工时的那些」，不是「跑完的
        // 那些」：两处都算的话，一个一条都没量过工时的种类也会拿到一行时长中位 0
        // 的记录，读起来像「量过了，是 0」。
        //
        // 花销取清点表里那件任务自己的累计数，中位而不是均值：一件跑了三小时的
        // 活能把均值拉走一个量级，而排期要的是一个典型的量。
        //
        // 两条轴各自数自己的样本，因为「跑完了」与「这件活花了多少」是两件事。
        // 时长是从任务行自己量出来的，几乎每条跑完的行都有；花销要清点表里
        // 有这件任务的一行，而那一行取决于调用方有没有把任务身份贴到请求上。
        // 2026-10-26 之前的调用贴的是运行 id，于是 10.4 万行清点里只有 767 行
        // 对得上任务图。如果只按「跑完了」的条数放行，一条带花销的样本就足以
        // 给整类任务定预算——那是拿一件活的账去当一整类的计划，而预算闸会照
        // 这个数把同类任务拦下。所以花销中位数要自己凑够 `$2` 条带账的样本，
        // 凑不够时给 0，调用方按 0 读作「没有成本计划」。
        //
        // 窗口是最近 30 天：平台上的任务大小随用途变化，五个月前的中位数拿来给
        // 今天排期，是拿旧世界的地图走新路。窗口内样本不够就不给这一行——没有
        // 估算比一个有旧估算的更好，前者读作「还没样本」，后者读作「量过了」。
        //
        // 缺花销不让整行消失：清点表里没有这一类的行时，中位花销是 0，而时长
        // 仍然是从任务行自己量出来的——那足以排一条甘特条。把「有没有花销样本」
        // 也塞进 HAVING，会让整行连同工时一起消失，于是账本按运行 id 记的那些
        // 天里，一件活也排不上期。
        let rows: Vec<(String, i64, i64, f64, f64)> = self
            .retry(|| async {
                sqlx::query_as(
                    r#"
                    WITH latest_def AS (
                        -- 「这件任务的定义」是最新一版修订；种类只在定义里。
                        SELECT DISTINCT ON (task_id) task_id, workspace_id, kind
                          FROM task_defined
                         ORDER BY task_id, rev DESC
                    ),
                    done AS (
                        -- 最近 30 天里跑完、并且完成之后再没有动过的任务。
                        --
                        -- 「跑完」是状态账本里的一条迁移，不是某个可被改写回
                        -- 去的字段：拿 `to_state = $3` 去取、再排掉它后面还有
                        -- 迁移的行，取到的就是「最后一句话是完成」。这条判断
                        -- 用得上 `idx_task_transition_state_at`，所以这条读的
                        -- 代价跟着最近 30 天的完成量走，不跟着账本长度走。
                        SELECT ld.kind,
                               t.task_id,
                               EXTRACT(EPOCH FROM (t.at - a.started_at)) AS dur
                          FROM task_transition t
                          JOIN latest_def ld ON ld.task_id = t.task_id
                          LEFT JOIN LATERAL (
                                SELECT x.started_at
                                  FROM task_attempt x
                                 WHERE x.task_id = t.task_id
                                   AND x.started_at IS NOT NULL
                                 ORDER BY x.attempt_no DESC
                                 LIMIT 1
                          ) a ON TRUE
                         WHERE ld.workspace_id = $1
                           AND t.to_state = $3
                           AND t.at > NOW() - INTERVAL '30 days'
                           AND NOT EXISTS (
                                SELECT 1 FROM task_transition n
                                 WHERE n.task_id = t.task_id AND n.seq > t.seq
                           )
                    ),
                    joined AS (
                        SELECT d.kind,
                               d.task_id,
                               d.dur,
                               m.total_tokens,
                               (m.task_id IS NOT NULL) AS costed
                          FROM done d
                          LEFT JOIN cog_observability_metrics m ON m.task_id = d.task_id
                    ),
                    tally AS (
                        SELECT kind,
                               COUNT(DISTINCT task_id) FILTER (WHERE dur IS NOT NULL)::BIGINT AS samples,
                               COUNT(DISTINCT task_id) FILTER (WHERE costed)::BIGINT AS cost_samples
                          FROM joined
                         GROUP BY kind
                    ),
                    -- 时长分位从 `done` 取，不从 `joined` 取：一件活有几行清点账，
                    -- 在 `joined` 里就有几行，按它算分位等于让调用次数多的活把
                    -- 分位拉向自己——那是「谁问得多」，不是「同类活跑多久」。
                    durations AS (
                        SELECT kind,
                               percentile_cont(0.5) WITHIN GROUP (ORDER BY dur) AS med_secs,
                               percentile_cont(0.99) WITHIN GROUP (ORDER BY dur) AS p99_secs
                          FROM done
                         WHERE dur IS NOT NULL
                         GROUP BY kind
                    )
                    SELECT t.kind,
                           t.samples,
                           COALESCE((percentile_cont(0.5) WITHIN GROUP (
                                         ORDER BY j.total_tokens)
                                     FILTER (WHERE j.costed AND t.cost_samples >= $2))::BIGINT, 0)
                               AS med_tokens,
                           COALESCE(d.med_secs, 0) AS med_secs,
                           COALESCE(d.p99_secs, 0) AS p99_secs
                      FROM tally t
                      JOIN joined j ON j.kind = t.kind
                      JOIN durations d ON d.kind = t.kind
                     GROUP BY t.kind, t.samples, t.cost_samples, d.med_secs, d.p99_secs
                    HAVING t.samples >= $2
                    "#,
                )
                .bind(workspace_id)
                .bind(min_samples as i64)
                // 状态词由 `TaskStatus::as_str` 决定：写进 SQL 的副本改了名不会
                // 报错，只会让这条查询一条也匹配不到，读起来像「还没有跑完的活」。
                .bind(cog_core::TaskStatus::Completed.as_str())
                .fetch_all(&self.pool)
                .await
            })
            .await?;
        Ok(rows
            .into_iter()
            .map(
                |(kind, samples, med_tokens, med_secs, p99_secs)| cog_core::TaskKindRecord {
                    kind,
                    samples: samples.max(0) as u64,
                    median_tokens: med_tokens.max(0) as u64,
                    median_duration_secs: med_secs.max(0.0).round() as u64,
                    p99_duration_secs: p99_secs.max(0.0).round() as u64,
                },
            )
            .collect())
    }

    /// Count the stored tasks by state across every workspace.
    ///
    /// Grouped in SQL rather than listed and counted here: the overview asks
    /// for a handful of numbers and the row count is unbounded, so shipping
    /// every task over the wire to produce four integers would make the
    /// overview's cost follow the cluster's history.
    async fn task_status_counts(&self) -> SFResult<Option<cog_core::TaskStatusCounts>> {
        let rows: Vec<(String, i64)> = self
            .retry(|| async {
                // 一件任务的状态是它最后一条迁移。分组前先把每件任务收敛到那
                // 一条（主键 (task_id, seq) 反着扫就够），所以这里数的是任务
                // 数，不是迁移条数——一个重试过三次的任务仍然是一件。
                sqlx::query_as(
                    r#"
                    SELECT last.to_state AS status, COUNT(*)::bigint
                      FROM (SELECT DISTINCT ON (task_id) task_id, to_state
                              FROM task_transition
                             ORDER BY task_id, seq DESC) last
                     GROUP BY last.to_state
                    "#,
                )
                .fetch_all(&self.pool)
                .await
            })
            .await?;

        let mut counts = cog_core::TaskStatusCounts::default();
        for (status, n) in rows {
            let n = n.max(0) as usize;
            match cog_core::TaskStatus::parse(&status) {
                Some(status) => counts.add(&status, n),
                // A status this build cannot name is still a stored task: it
                // stays in the total (the row exists) and is left out of every
                // bucket, so the mismatch is visible as a total that exceeds
                // the buckets instead of being quietly filed somewhere.
                None => {
                    tracing::warn!(
                        status = %status,
                        count = n,
                        "task row carries a status this build does not know; counted in the total only"
                    );
                    counts.total = counts.total.saturating_add(n);
                }
            }
        }
        Ok(Some(counts))
    }

    async fn dag_get_dependencies(
        &self,
        workspace_id: &str,
        task_id: &str,
    ) -> SFResult<Vec<String>> {
        Ok(self
            .dag_get_task(workspace_id, task_id)
            .await?
            .map(|t| t.blocked_by)
            .unwrap_or_default())
    }

    async fn dag_get_dependents(&self, workspace_id: &str, task_id: &str) -> SFResult<Vec<String>> {
        // 反向读：`blocked_by` 是数组列，GIN 索引让「谁等着这件任务」是一次
        // 索引取行，而不是把整张定义表读出来再判包含。
        let rows: Vec<(String,)> = self
            .retry(|| async {
                sqlx::query_as(task_ledger::DEPENDENTS_SQL)
                    .bind(workspace_id)
                    .bind(task_id)
                    .fetch_all(&self.pool)
                    .await
            })
            .await?;
        Ok(rows.into_iter().map(|r| r.0).collect())
    }

    async fn dag_complete_task(
        &self,
        workspace_id: &str,
        task_id: &str,
        result: serde_json::Value,
    ) -> SFResult<Vec<String>> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| SFError::Database(e.to_string()))?;
        task_ledger::lock_workspace(&mut tx, workspace_id).await?;

        // Only the rows that can still move, which is what the projection is.
        // Dependencies that already finished are not in it, so their states
        // are read from the transition that finished them rather than assumed
        // absent -- "not in the live set" is not "not completed".
        let mut tasks = self.live_tasks(&mut tx, workspace_id).await?;
        let states = self
            .states_of(
                &mut tx,
                tasks
                    .values()
                    .flat_map(|t| t.blocked_by.iter().cloned())
                    .collect::<Vec<_>>()
                    .as_slice(),
            )
            .await?;

        let task = tasks.get_mut(task_id).ok_or_else(|| SFError::TaskFailed {
            task_id: task_id.into(),
            reason: "Task not found".into(),
        })?;
        if task.status != cog_core::TaskStatus::Running {
            return Err(SFError::TaskFailed {
                task_id: task_id.into(),
                reason: format!(
                    "Cannot complete task in {:?} state — it may have been handled by timeout or retry",
                    task.status
                ),
            });
        }
        task.status = cog_core::TaskStatus::Completed;
        task.result = Some(result);
        task.updated_at = Utc::now();

        let dependent_ids: Vec<String> = tasks
            .values()
            .filter(|t| t.blocked_by.iter().any(|b| b == task_id))
            .map(|t| t.id.clone())
            .collect();

        let mut ready = Vec::new();
        for dep_id in dependent_ids {
            let all_ready = tasks
                .get(&dep_id)
                .map(|d| {
                    d.status == cog_core::TaskStatus::Pending
                        && d.blocked_by.iter().all(|b| {
                            if b.as_str() == task_id {
                                true
                            } else {
                                states.get(b) == Some(&cog_core::TaskStatus::Completed)
                            }
                        })
                })
                .unwrap_or(false);
            if all_ready {
                // 只报告就绪，不翻转状态（Scheduled = 已发布到 ready 流，
                // 由发布方 schedule_task 翻转），同事务串行保证判定一致
                ready.push(dep_id);
            }
        }

        let t = tasks.remove(task_id).expect("just read");
        task_ledger::close_attempt(
            &mut tx,
            task_id,
            cog_core::TaskStatus::Completed,
            None,
            None,
            t.result.as_ref(),
            t.agent_id.as_deref(),
        )
        .await?;
        task_ledger::append_transition(
            &mut tx,
            &task_ledger::Transition {
                task_id,
                from: Some(cog_core::TaskStatus::Running),
                to: cog_core::TaskStatus::Completed,
                actor: "executor",
                cause: "complete",
                detail: None,
                not_before: None,
            },
        )
        .await?;
        task_ledger::project(&mut tx, Some(task_id)).await?;

        tx.commit()
            .await
            .map_err(|e| SFError::Database(e.to_string()))?;
        Ok(ready)
    }

    async fn dag_fail_task(
        &self,
        workspace_id: &str,
        task_id: &str,
        error: String,
        cause: Option<UpstreamFailure>,
        payer: Option<FailureCause>,
        max_retries: u32,
        retry_delay: std::time::Duration,
    ) -> SFResult<(bool, Vec<String>)> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| SFError::Database(e.to_string()))?;
        task_ledger::lock_workspace(&mut tx, workspace_id).await?;

        // 终态的任务不在投影里（投影只放还动得了的），所以「找不到」这一句必须
        // 先分清是「没有这件任务」还是「它已经结束了」：end 之后到达的结局不是
        // 一次新的判定，把后者报成 `Task not found` 会让一条有意取消看起来像一次
        // 写错 id 的调用。
        if let Some(state) = task_ledger::current_state(&mut tx, task_id).await? {
            if state.is_terminal() {
                tracing::info!(
                    task_id = %task_id,
                    state = state.as_str(),
                    reported = %error,
                    "a failure arrived for a finished task; keeping the outcome it already had"
                );
                tx.rollback().await.ok();
                return Ok((false, Vec::new()));
            }
        }

        let mut tasks = self.live_tasks(&mut tx, workspace_id).await?;
        let task = tasks.get_mut(task_id).ok_or_else(|| SFError::TaskFailed {
            task_id: task_id.into(),
            reason: "Task not found".into(),
        })?;

        // 已经被取消的一行不再判一次：取消是别人（部署器 / 上游级联）对这一行做出的
        // 决定，而这次失败只是那之后执行侧交回来的结局。
        //
        // 2026-11-06 实测：13 轮 `ci-red-*-fix` 全部以 `terminal_env_failure: … no
        // handle …` 结束，而它们其实是被部署器按「版本已经走过」主动取消的——取消先
        // 写 Cancelled 与理由，执行侧随后用「没留下 handle」把状态和理由一起覆盖掉。
        // 于是每一次「有意取消」都被记成一次环境故障，还被抄进下一轮的题面当证据。
        // 结局不能改写别人的决定：先取消的赢。
        if task.status == cog_core::TaskStatus::Cancelled {
            tracing::info!(
                task_id = %task_id,
                reported = %error,
                kept = ?task.error,
                "a failure arrived for a cancelled task; keeping the cancellation"
            );
            return Ok((false, Vec::new()));
        }

        let was_running = task.status == cog_core::TaskStatus::Running;
        task.error = Some(error.clone());
        task.error_cause = cause;
        task.failure_payer = payer;
        task.updated_at = Utc::now();

        let should_retry = task.retry_count < max_retries;
        let not_before = if should_retry {
            task.retry_count += 1;
            task.status = cog_core::TaskStatus::Pending;
            task.retry_not_before = chrono::Duration::from_std(retry_delay)
                .ok()
                .map(|d| Utc::now() + d);
            task.retry_not_before
        } else {
            task.status = cog_core::TaskStatus::Failed;
            task.retry_not_before = None;
            None
        };

        // The attempt that failed ends here either way; a retry opens a new one
        // when the task goes back to running.
        task_ledger::record_failed_attempt(
            &mut tx,
            task_id,
            task_ledger::cause_code(cause),
            &error,
        )
        .await?;

        let from = if was_running {
            cog_core::TaskStatus::Running
        } else {
            task.status
        };
        task_ledger::append_transition(
            &mut tx,
            &task_ledger::Transition {
                task_id,
                from: Some(from),
                to: task.status,
                actor: "executor",
                cause: "fail",
                detail: Some(&error),
                not_before,
            },
        )
        .await?;
        task_ledger::project(&mut tx, Some(task_id)).await?;

        let mut cancelled = Vec::new();
        if !should_retry {
            // 递归级联取消全部非终态下游
            let mut stack = vec![task_id.to_string()];
            let mut visited = std::collections::HashSet::new();
            visited.insert(task_id.to_string());
            while let Some(current) = stack.pop() {
                let children: Vec<String> = tasks
                    .values()
                    .filter(|t| t.blocked_by.iter().any(|b| b == &current))
                    .map(|t| t.id.clone())
                    .collect();
                for child in children {
                    if !visited.insert(child.clone()) {
                        continue;
                    }
                    if let Some(t) = tasks.get_mut(&child) {
                        if !t.status.is_terminal() {
                            let previous = t.status;
                            t.status = cog_core::TaskStatus::Cancelled;
                            t.error = Some(format!(
                                "Cascade cancelled: upstream task '{}' permanently failed with error: {}",
                                task_id, error
                            ));
                            t.error_cause = None;
                            t.updated_at = Utc::now();
                            if previous == cog_core::TaskStatus::Running {
                                task_ledger::close_attempt(
                                    &mut tx,
                                    &child,
                                    cog_core::TaskStatus::Cancelled,
                                    None,
                                    t.error.as_deref(),
                                    None,
                                    None,
                                )
                                .await?;
                            }
                            task_ledger::append_transition(
                                &mut tx,
                                &task_ledger::Transition {
                                    task_id: &child,
                                    from: Some(previous),
                                    to: cog_core::TaskStatus::Cancelled,
                                    actor: "executor",
                                    cause: "cascade_cancel",
                                    detail: t.error.as_deref(),
                                    not_before: None,
                                },
                            )
                            .await?;
                            task_ledger::project(&mut tx, Some(&child)).await?;
                            cancelled.push(child.clone());
                        }
                    }
                    stack.push(child);
                }
            }
        }

        tx.commit()
            .await
            .map_err(|e| SFError::Database(e.to_string()))?;
        Ok((should_retry, cancelled))
    }

    async fn dag_cancel_task(
        &self,
        workspace_id: &str,
        task_id: &str,
        reason: String,
    ) -> SFResult<Vec<String>> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| SFError::Database(e.to_string()))?;
        task_ledger::lock_workspace(&mut tx, workspace_id).await?;

        let mut tasks = self.live_tasks(&mut tx, workspace_id).await?;
        let task = tasks.get_mut(task_id).ok_or_else(|| SFError::TaskFailed {
            task_id: task_id.into(),
            reason: "Task not found".into(),
        })?;
        if task.status.is_terminal() {
            return Err(SFError::TaskFailed {
                task_id: task_id.into(),
                reason: format!("Cannot cancel task in {:?} state", task.status),
            });
        }
        let previous = task.status;
        task.status = cog_core::TaskStatus::Cancelled;
        task.error = Some(reason.clone());
        task.error_cause = None;
        task.updated_at = Utc::now();
        if previous == cog_core::TaskStatus::Running {
            task_ledger::close_attempt(
                &mut tx,
                task_id,
                cog_core::TaskStatus::Cancelled,
                None,
                Some(&reason),
                None,
                None,
            )
            .await?;
        }
        task_ledger::append_transition(
            &mut tx,
            &task_ledger::Transition {
                task_id,
                from: Some(previous),
                to: cog_core::TaskStatus::Cancelled,
                actor: "executor",
                cause: "cancel",
                detail: Some(&reason),
                not_before: None,
            },
        )
        .await?;
        task_ledger::project(&mut tx, Some(task_id)).await?;

        let mut cancelled = Vec::new();
        let mut stack = vec![task_id.to_string()];
        let mut visited = std::collections::HashSet::new();
        visited.insert(task_id.to_string());
        while let Some(current) = stack.pop() {
            let children: Vec<String> = tasks
                .values()
                .filter(|t| t.blocked_by.iter().any(|b| b == &current))
                .map(|t| t.id.clone())
                .collect();
            for child in children {
                if !visited.insert(child.clone()) {
                    continue;
                }
                if let Some(t) = tasks.get_mut(&child) {
                    if !t.status.is_terminal() {
                        let previous = t.status;
                        t.status = cog_core::TaskStatus::Cancelled;
                        t.error = Some(format!(
                            "Cascade cancelled: upstream task '{}' was cancelled",
                            task_id
                        ));
                        t.updated_at = Utc::now();
                        if previous == cog_core::TaskStatus::Running {
                            task_ledger::close_attempt(
                                &mut tx,
                                &child,
                                cog_core::TaskStatus::Cancelled,
                                None,
                                t.error.as_deref(),
                                None,
                                None,
                            )
                            .await?;
                        }
                        task_ledger::append_transition(
                            &mut tx,
                            &task_ledger::Transition {
                                task_id: &child,
                                from: Some(previous),
                                to: cog_core::TaskStatus::Cancelled,
                                actor: "executor",
                                cause: "cascade_cancel",
                                detail: t.error.as_deref(),
                                not_before: None,
                            },
                        )
                        .await?;
                        task_ledger::project(&mut tx, Some(&child)).await?;
                        cancelled.push(child.clone());
                    }
                }
                stack.push(child);
            }
        }

        tx.commit()
            .await
            .map_err(|e| SFError::Database(e.to_string()))?;
        Ok(cancelled)
    }

    async fn dag_append_retry(
        &self,
        workspace_id: &str,
        task_id: &str,
        attempt: &cog_core::RetryAttempt,
    ) -> SFResult<()> {
        // 一次失败的重试就是一次尝试自己的结局，所以它落成一行尝试账，而不是
        // 往某个数组末尾追加一个对象。同一个 attempt 号重写一次是幂等的：那
        // 是同一个尝试被记了第二遍，不是第二次尝试。
        let attempt_no = attempt.attempt as i32;
        self.retry(|| async {
            sqlx::query(
                r#"
                INSERT INTO task_attempt (task_id, attempt_no, ended_at, error_text, outcome)
                SELECT d.task_id, $2, $3, $4, 'failed'
                  FROM (SELECT DISTINCT ON (task_id) task_id
                          FROM task_defined
                         WHERE task_id = $5 AND workspace_id = $1
                         ORDER BY task_id, rev DESC) d
                ON CONFLICT (task_id, attempt_no) DO UPDATE
                   SET ended_at = EXCLUDED.ended_at,
                       error_text = EXCLUDED.error_text,
                       outcome = EXCLUDED.outcome
                "#,
            )
            .bind(workspace_id)
            .bind(attempt_no)
            .bind(attempt.timestamp)
            .bind(&attempt.error)
            .bind(task_id)
            .execute(&self.pool)
            .await
        })
        .await?;
        Ok(())
    }

    async fn dag_get_retry_history(
        &self,
        workspace_id: &str,
        task_id: &str,
    ) -> SFResult<Vec<cog_core::RetryAttempt>> {
        // 失败过的尝试就是重试史。按尝试号排，因为调用方读的是「第几次失败、
        // 为什么」的次序，而账本的行序不是它。
        let rows: Vec<(i32, Option<String>, Option<DateTime<Utc>>)> = self
            .retry(|| async {
                sqlx::query_as(
                    r#"
                    SELECT a.attempt_no, a.error_text, a.ended_at
                      FROM task_attempt a
                     WHERE a.task_id = $3
                       AND a.outcome = $2
                       AND EXISTS (SELECT 1 FROM task_defined d
                                    WHERE d.task_id = $3 AND d.workspace_id = $1)
                     ORDER BY a.attempt_no
                    "#,
                )
                .bind(workspace_id)
                .bind(cog_core::TaskStatus::Failed.as_str())
                .bind(task_id)
                .fetch_all(&self.pool)
                .await
            })
            .await?;
        Ok(rows
            .into_iter()
            .map(|(attempt, error, ended_at)| cog_core::RetryAttempt {
                attempt: attempt.max(0) as u32,
                error: error.unwrap_or_default(),
                timestamp: ended_at.unwrap_or_else(Utc::now),
            })
            .collect())
    }

    async fn dag_clear_workspace(&self, workspace_id: &str) -> SFResult<()> {
        // 清一个工作区要把账本的四面一起清，否则留下的行读起来是半个任务：
        // 迁移还在、定义没了，下一次读这件任务会拿到一个没有种类的影子。
        //
        // 「这工作区的任务」按最新一版定义算，与读路径同一个口径：一件任务换过
        // 工作区时，旧工作区的清除不该把它带走，因为最新一版说了它现在在哪。
        const IDS: &str = r#"
            SELECT DISTINCT ON (task_id) task_id FROM task_defined
             WHERE workspace_id = $1 ORDER BY task_id, rev DESC
        "#;
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| SFError::Database(e.to_string()))?;
        for sql in [
            format!("WITH ids AS ({IDS}) DELETE FROM task_attempt WHERE task_id IN (SELECT task_id FROM ids)"),
            format!("WITH ids AS ({IDS}) DELETE FROM task_transition WHERE task_id IN (SELECT task_id FROM ids)"),
            format!("WITH ids AS ({IDS}) DELETE FROM task_defined WHERE task_id IN (SELECT task_id FROM ids)"),
            "DELETE FROM task_current WHERE workspace_id = $1".to_string(),
        ] {
            sqlx::query(&sql)
                .bind(workspace_id)
                .execute(&mut *tx)
                .await
                .map_err(|e| SFError::Database(e.to_string()))?;
        }
        tx.commit()
            .await
            .map_err(|e| SFError::Database(e.to_string()))?;
        Ok(())
    }
}

// ─── PromotionLedger (Postgres) ───

/// A promotion row as it comes back from `cog_evolution_promotions`, in the
/// column order both queries select. One alias rather than a third copy of the
/// tuple: `recent` and `awaiting` differ in their predicate, not their shape.
type PromotionRow = (
    String,
    String,
    String,
    Option<String>,
    String,
    String,
    String,
    String,
    Option<String>,
    chrono::DateTime<Utc>,
    chrono::DateTime<Utc>,
);

fn promotion_record(
    (
        id,
        change_id,
        level,
        gate_kind,
        decision_reason,
        cluster,
        status,
        outcome,
        eval_summary,
        created_at,
        updated_at,
    ): PromotionRow,
) -> cog_core::PromotionRecord {
    cog_core::PromotionRecord {
        id,
        change_id,
        level,
        gate_kind: gate_kind
            .as_deref()
            .and_then(cog_core::PromotionGateKind::parse),
        decision_reason,
        cluster,
        status: cog_core::PromotionStatus::parse(&status)
            .unwrap_or(cog_core::PromotionStatus::Failed),
        outcome,
        eval_summary,
        created_at,
        updated_at,
    }
}

#[async_trait]
impl cog_core::PromotionLedger for PostgresStateBackend {
    async fn record(&self, rec: cog_core::PromotionRecord) -> SFResult<()> {
        sqlx::query(
            r#"
            INSERT INTO cog_evolution_promotions
                (id, change_id, level, gate_kind, decision_reason, cluster, status, outcome, eval_summary, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (id) DO NOTHING
            "#,
        )
        .bind(&rec.id)
        .bind(&rec.change_id)
        .bind(&rec.level)
        .bind(rec.gate_kind.map(|k| k.as_str()))
        .bind(&rec.decision_reason)
        .bind(&rec.cluster)
        .bind(rec.status.as_str())
        .bind(&rec.outcome)
        .bind(&rec.eval_summary)
        .bind(rec.created_at)
        .bind(rec.updated_at)
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;
        Ok(())
    }

    async fn update_status(
        &self,
        id: &str,
        status: cog_core::PromotionStatus,
        outcome: &str,
    ) -> SFResult<()> {
        let result = sqlx::query(
            r#"
            UPDATE cog_evolution_promotions
            SET status = $1, outcome = $2, updated_at = NOW()
            WHERE id = $3
            "#,
        )
        .bind(status.as_str())
        .bind(outcome)
        .bind(id)
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;
        if result.rows_affected() == 0 {
            return Err(SFError::Validation(format!(
                "promotion record {id} not found"
            )));
        }
        Ok(())
    }

    async fn count_promoted_since(&self, since: chrono::DateTime<Utc>) -> SFResult<u64> {
        let row: (i64,) = sqlx::query_as(
            r#"
            SELECT COUNT(*) FROM cog_evolution_promotions
            WHERE status = $1 AND updated_at >= $2
            "#,
        )
        .bind(cog_core::PromotionStatus::Promoted.as_str())
        .bind(since)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;
        Ok(row.0 as u64)
    }

    async fn recent(&self, limit: usize) -> SFResult<Vec<cog_core::PromotionRecord>> {
        let rows: Vec<PromotionRow> = sqlx::query_as(
            r#"
            SELECT id, change_id, level, gate_kind, decision_reason, cluster, status,
                   outcome, eval_summary, created_at, updated_at
            FROM cog_evolution_promotions
            ORDER BY updated_at DESC
            LIMIT $1
            "#,
        )
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        Ok(rows.into_iter().map(promotion_record).collect())
    }

    async fn for_change(
        &self,
        cluster: &str,
        change_id: &str,
        statuses: &[cog_core::PromotionStatus],
        limit: usize,
    ) -> SFResult<Vec<cog_core::PromotionRecord>> {
        let wanted: Vec<String> = statuses.iter().map(|s| s.as_str().to_string()).collect();
        let rows: Vec<PromotionRow> = sqlx::query_as(
            r#"
            SELECT id, change_id, level, gate_kind, decision_reason, cluster, status,
                   outcome, eval_summary, created_at, updated_at
            FROM cog_evolution_promotions
            WHERE cluster = $1 AND change_id = $2 AND status = ANY($3)
            ORDER BY updated_at DESC
            LIMIT $4
            "#,
        )
        .bind(cluster)
        .bind(change_id)
        .bind(&wanted)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        Ok(rows.into_iter().map(promotion_record).collect())
    }

    async fn awaiting(
        &self,
        cluster: &str,
        limit: usize,
    ) -> SFResult<Vec<cog_core::PromotionRecord>> {
        let rows: Vec<PromotionRow> = sqlx::query_as(
            r#"
            SELECT id, change_id, level, gate_kind, decision_reason, cluster, status,
                   outcome, eval_summary, created_at, updated_at
            FROM cog_evolution_promotions
            WHERE cluster = $1 AND status = $2
            ORDER BY updated_at ASC
            LIMIT $3
            "#,
        )
        .bind(cluster)
        .bind(cog_core::PromotionStatus::AwaitingApproval.as_str())
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        Ok(rows.into_iter().map(promotion_record).collect())
    }
}

#[cfg(test)]
mod live_status_filter_tests {
    use super::live_status_filter;
    use cog_core::TaskStatus;

    /// 名单与判据是同一件事：收进来的每一个都得是派发还要看的，漏掉一个就会
    /// 让那一列的行永远读不回来。两个方向都断言，因为两种错法都安静——多收
    /// 一列只是多读几行（贵），少收一列是把一排活藏起来（看不见）。
    #[test]
    fn the_filter_names_exactly_the_statuses_dispatch_still_reads() {
        let listed = live_status_filter();
        let expected: Vec<String> = TaskStatus::ALL
            .iter()
            .filter(|status| status.affects_dispatch())
            .map(|status| status.as_str().to_string())
            .collect();
        assert_eq!(listed, expected);

        for status in TaskStatus::ALL {
            let named = listed.iter().any(|s| s == status.as_str());
            assert_eq!(
                named,
                status.affects_dispatch(),
                "{} 在这次读法里的去留与 affects_dispatch 不一致",
                status.as_str()
            );
        }
    }

    /// 终态的那两列不在这份名单里：把它们读回来，每五秒一拍就是几千行没人用的
    /// 正文，而这条读法跑在发布那一拍上。
    #[test]
    fn the_two_settled_columns_stay_out_of_the_read() {
        let listed = live_status_filter();
        for settled in [TaskStatus::Failed, TaskStatus::Cancelled] {
            assert!(
                !listed.iter().any(|s| s == settled.as_str()),
                "{} 是终态，不该出现在派发要读的名单里",
                settled.as_str()
            );
        }
    }
}

#[cfg(test)]
mod plan_schema_tests {
    use super::ddl;

    /// `task_plan` 建表语句里第一层括号之间的那一截。
    fn task_plan_columns(ddl: &str) -> Vec<String> {
        let start = ddl
            .find("CREATE TABLE IF NOT EXISTS task_plan (")
            .expect("task_plan 的建表语句在 DDL 里");
        let body = &ddl[start..];
        let open = body.find('(').expect("建表语句有列清单");
        let rest = &body[open + 1..];
        // 列清单以单独一行 `)` 收口。
        let close = rest
            .lines()
            .scan(0usize, |offset, line| {
                let at = *offset;
                *offset += line.len() + 1;
                Some((at, line))
            })
            .find(|(_, line)| line.trim() == ")")
            .map(|(at, _)| at)
            .expect("列清单有收口");
        rest[..close]
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty())
            // 列清单最后那条约束不是列。
            .filter(|l| !l.starts_with("PRIMARY KEY"))
            .map(|l| l.split_whitespace().next().unwrap().to_string())
            .collect()
    }

    /// 状态列的唯一正本在任务图上。计划表再存一份状态，两份就会各自演化，
    /// 而读的人永远不知道自己看的是哪一份——所以这条必须是红的。
    #[test]
    fn the_plan_table_holds_no_status_column() {
        let columns = task_plan_columns(&ddl());
        for forbidden in ["status", "state", "task_status"] {
            assert!(
                !columns.iter().any(|c| c == forbidden),
                "task_plan 不许有状态列（found {forbidden}）: {columns:?}"
            );
        }
    }

    /// 列集合钉死，免得有人顺手加一列「当前进度」之类的派生值：派生值一旦落列，
    /// 它就有自己的更新时间，读面再也分不清它是算出来的还是喂进来的。
    #[test]
    fn the_plan_table_column_set_is_exactly_the_plan() {
        assert_eq!(
            task_plan_columns(&ddl()),
            vec![
                "workspace_id",
                "task_id",
                "planned_start",
                "planned_end",
                "budget_tokens",
                "baseline_rev",
                "updated_at",
            ]
        );
    }
}
