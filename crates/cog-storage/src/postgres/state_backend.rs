use async_trait::async_trait;
use chrono::Utc;
use serde_json;
use sqlx::PgPool;
use std::collections::HashMap;

use cog_core::{
    AgentState, ContextBoard, Event, SFError, SFResult, StateBackend, TaskCheckpoint,
    UpstreamFailure,
};

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

/// This store's schema, and the one a restore replays.
pub const DDL: &str = r#"
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
CREATE TABLE IF NOT EXISTS cog_context_boards (
                task_id TEXT NOT NULL,
                field_name TEXT NOT NULL,
                field_value TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (task_id, field_name)
            )
;
CREATE TABLE IF NOT EXISTS cog_dag_state (
                workspace_id TEXT PRIMARY KEY,
                state JSONB NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
;
CREATE TABLE IF NOT EXISTS cog_dag_tasks (
                workspace_id TEXT NOT NULL,
                task_id TEXT NOT NULL,
                task JSONB NOT NULL,
                status TEXT NOT NULL,
                retry_history JSONB NOT NULL DEFAULT '[]'::jsonb,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (workspace_id, task_id)
            )
;
CREATE INDEX IF NOT EXISTS idx_cog_dag_tasks_status
            ON cog_dag_tasks(workspace_id, status)
;
-- The face and owner a task carries, read out of the task payload by the
-- database rather than written beside it.
--
-- A stored copy would be a second answer to "whose is this": every writer would
-- have to update the column and the JSON in step, and the one that forgot would
-- leave an index pointing at an owner the row no longer has. Generated columns
-- cannot drift from the row they are derived from.
--
-- They are declared here, next to the CREATE, rather than in a migration:
-- migrations run before this schema creates the table, so an ALTER in one would
-- fail on a fresh install. The ALTER form below is what upgrades an existing
-- database, and it is idempotent so every start may run it.
ALTER TABLE cog_dag_tasks ADD COLUMN IF NOT EXISTS face_id TEXT
            GENERATED ALWAYS AS (task->>'face_id') STORED
;
ALTER TABLE cog_dag_tasks ADD COLUMN IF NOT EXISTS owner_unit TEXT
            GENERATED ALWAYS AS (task->>'owner_unit') STORED
;
CREATE INDEX IF NOT EXISTS idx_cog_dag_tasks_face
            ON cog_dag_tasks(workspace_id, face_id, status)
;
CREATE INDEX IF NOT EXISTS idx_cog_dag_tasks_owner_unit
            ON cog_dag_tasks(workspace_id, owner_unit, status)
;
-- 一条任务的计划基线：它「打算怎么做」。
--
-- 与 cog_dag_tasks 并排而不是加列进去：后者每次状态流转都要整行重写，
-- 基线在分解那一刻就定了、之后不改，混在一张表里会让「计划」跟着
-- 「现状」一起被覆盖，甘特面就再也画不出偏差。
--
-- 这里**没有状态列**，这是刻意的：状态的唯一正本在上面的表里，
-- 两边各存一份就是两份真值，迟早互相矛盾。
CREATE TABLE IF NOT EXISTS task_plan (
                workspace_id TEXT NOT NULL,
                task_id TEXT NOT NULL,
                planned_start TIMESTAMPTZ,
                planned_end TIMESTAMPTZ,
                budget_tokens BIGINT NOT NULL,
                baseline_rev TEXT NOT NULL,
                acceptance TEXT[] NOT NULL DEFAULT '{}',
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (workspace_id, task_id)
            )
;
ALTER TABLE task_plan
            ADD COLUMN IF NOT EXISTS acceptance TEXT[] NOT NULL DEFAULT '{}'
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

pub struct PostgresStateBackend {
    pool: PgPool,
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
        crate::schema_ddl::apply(&self.pool, DDL).await
    }
}

/// TaskStatus 的 DB 文本形式：定义在 `cog_core::TaskStatus::as_str`，这里只做
/// 借用转换——写列与读列的字符串必须来自同一处，否则计数查询会按一套拼写
/// 去数另一套拼写写下的行。
fn status_str(status: &cog_core::TaskStatus) -> &'static str {
    status.as_str()
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
        let row: Option<(String,)> = self
            .retry(|| async {
                sqlx::query_as("SELECT state FROM cog_agent_states WHERE agent_id = $1")
                    .bind(agent_id)
                    .fetch_optional(&self.pool)
                    .await
            })
            .await?;

        match row {
            Some((s,)) => {
                let state: AgentState = serde_json::from_str(&s).map_err(SFError::Serialization)?;
                Ok(Some(state))
            }
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
        let row: Option<(String, i64, chrono::DateTime<Utc>)> = self
            .retry(|| async {
                sqlx::query_as(
                    "SELECT snapshot_id, event_offset, timestamp FROM cog_checkpoints WHERE task_id = $1",
                )
                .bind(task_id)
                .fetch_optional(&self.pool)
                .await
            })
            .await?;

        match row {
            Some((snapshot_id, event_offset, timestamp)) => Ok(Some(TaskCheckpoint {
                task_id: task_id.into(),
                snapshot_id,
                event_offset: event_offset as u64,
                timestamp,
            })),
            None => Ok(None),
        }
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
        let row: Option<(serde_json::Value,)> = self
            .retry(|| async {
                sqlx::query_as(
                    "SELECT task FROM cog_dag_tasks WHERE workspace_id = $1 AND task_id = $2",
                )
                .bind(workspace_id)
                .bind(task_id)
                .fetch_optional(&self.pool)
                .await
            })
            .await?;
        row.map(|(v,)| serde_json::from_value(v).map_err(SFError::Serialization))
            .transpose()
    }

    async fn dag_set_task(
        &self,
        workspace_id: &str,
        task_id: &str,
        task: &cog_core::Task,
    ) -> SFResult<()> {
        let value = serde_json::to_value(task)?;
        let status = status_str(&task.status);
        self.retry(|| async {
            sqlx::query(
                r#"
                INSERT INTO cog_dag_tasks (workspace_id, task_id, task, status, updated_at)
                VALUES ($1, $2, $3, $4, NOW())
                ON CONFLICT (workspace_id, task_id) DO UPDATE SET
                    task = EXCLUDED.task,
                    status = EXCLUDED.status,
                    updated_at = NOW()
                "#,
            )
            .bind(workspace_id)
            .bind(task_id)
            .bind(value.clone())
            .bind(status)
            .execute(&self.pool)
            .await
        })
        .await?;
        Ok(())
    }

    async fn dag_transition_task(
        &self,
        workspace_id: &str,
        task_id: &str,
        expected: &[cog_core::TaskStatus],
        updated: &cog_core::Task,
    ) -> SFResult<()> {
        let value = serde_json::to_value(updated)?;
        let new_status = status_str(&updated.status);
        let expected: Vec<&str> = expected.iter().map(status_str).collect();

        let result = self
            .retry(|| async {
                sqlx::query(
                    r#"
                    UPDATE cog_dag_tasks
                    SET task = $3, status = $4, updated_at = NOW()
                    WHERE workspace_id = $1 AND task_id = $2 AND status = ANY($5)
                    "#,
                )
                .bind(workspace_id)
                .bind(task_id)
                .bind(value.clone())
                .bind(new_status)
                .bind(expected.clone())
                .execute(&self.pool)
                .await
            })
            .await?;

        if result.rows_affected() == 1 {
            return Ok(());
        }
        // CAS 失败：区分"不存在"与"状态不符"，报出当前状态
        let current = self.dag_get_task(workspace_id, task_id).await?;
        match current {
            Some(t) => Err(SFError::TaskFailed {
                task_id: task_id.into(),
                reason: format!("Cannot transition task in {:?} state", t.status),
            }),
            None => Err(SFError::TaskFailed {
                task_id: task_id.into(),
                reason: "Task not found".into(),
            }),
        }
    }

    async fn dag_transition_owned_task(
        &self,
        workspace_id: &str,
        task_id: &str,
        expected: &[cog_core::TaskStatus],
        updated: &cog_core::Task,
        owner: &str,
    ) -> SFResult<()> {
        let value = serde_json::to_value(updated)?;
        let new_status = status_str(&updated.status);
        let expected: Vec<&str> = expected.iter().map(status_str).collect();

        // Both owner clauses are load-bearing. The first compares the row on
        // disk with the caller's unit, which is the ownership check at the
        // instant of the write; the second refuses a payload that would move
        // the task to another owner, since that decision belongs to the
        // handover transaction and not to a state transition. Both read the
        // owner out of the task document rather than through the generated
        // column beside it: the row is already located by its primary key, so
        // the check costs one JSON lookup on one row, and the guard stays valid
        // on a database whose columns have not been added yet.
        let result = self
            .retry(|| async {
                sqlx::query(
                    r#"
                    UPDATE cog_dag_tasks
                    SET task = $3, status = $4, updated_at = NOW()
                    WHERE workspace_id = $1 AND task_id = $2 AND status = ANY($5)
                      AND task ->> 'owner_unit' = $6
                      AND $3::jsonb ->> 'owner_unit' = $6
                    "#,
                )
                .bind(workspace_id)
                .bind(task_id)
                .bind(value.clone())
                .bind(new_status)
                .bind(expected.clone())
                .bind(owner)
                .execute(&self.pool)
                .await
            })
            .await?;

        if result.rows_affected() == 1 {
            return Ok(());
        }
        // The guard that failed decides what the caller should do: a row that
        // moved to another owner and a row that moved to another state are
        // both "re-read", but only the first one means this caller may no
        // longer act on the task at all.
        let current = self.dag_get_task(workspace_id, task_id).await?;
        match current {
            Some(t) if t.owner_unit.as_deref() != Some(owner) => Err(SFError::Conflict(format!(
                "task {task_id} is owned by {}, not {owner}",
                t.owner_unit.as_deref().unwrap_or("nobody")
            ))),
            Some(t) => Err(SFError::Conflict(format!(
                "task {task_id} is in {:?} state, not one of the states this transition accepts",
                t.status
            ))),
            None => Err(SFError::TaskFailed {
                task_id: task_id.into(),
                reason: "Task not found".into(),
            }),
        }
    }

    async fn dag_remove_task(&self, workspace_id: &str, task_id: &str) -> SFResult<()> {
        self.retry(|| async {
            sqlx::query("DELETE FROM cog_dag_tasks WHERE workspace_id = $1 AND task_id = $2")
                .bind(workspace_id)
                .bind(task_id)
                .execute(&self.pool)
                .await
        })
        .await?;
        Ok(())
    }

    async fn dag_list_tasks(&self, workspace_id: &str) -> SFResult<Vec<String>> {
        let rows: Vec<(String,)> = self
            .retry(|| async {
                sqlx::query_as("SELECT task_id FROM cog_dag_tasks WHERE workspace_id = $1")
                    .bind(workspace_id)
                    .fetch_all(&self.pool)
                    .await
            })
            .await?;
        Ok(rows.into_iter().map(|r| r.0).collect())
    }

    async fn dag_get_all_tasks(&self, workspace_id: &str) -> SFResult<Vec<cog_core::Task>> {
        let rows: Vec<(serde_json::Value,)> = self
            .retry(|| async {
                sqlx::query_as("SELECT task FROM cog_dag_tasks WHERE workspace_id = $1")
                    .bind(workspace_id)
                    .fetch_all(&self.pool)
                    .await
            })
            .await?;
        rows.into_iter()
            .map(|(v,)| serde_json::from_value(v).map_err(SFError::Serialization))
            .collect()
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
        let acceptance = plan.acceptance.clone();
        self.retry(|| async {
            sqlx::query(
                r#"
                INSERT INTO task_plan
                    (workspace_id, task_id, planned_start, planned_end,
                     budget_tokens, baseline_rev, acceptance, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
                ON CONFLICT (workspace_id, task_id) DO UPDATE SET
                    planned_start = EXCLUDED.planned_start,
                    planned_end = EXCLUDED.planned_end,
                    budget_tokens = EXCLUDED.budget_tokens,
                    baseline_rev = EXCLUDED.baseline_rev,
                    acceptance = EXCLUDED.acceptance,
                    updated_at = NOW()
                "#,
            )
            .bind(&workspace_id)
            .bind(&task_id)
            .bind(planned_start)
            .bind(planned_end)
            .bind(budget)
            .bind(&baseline_rev)
            .bind(&acceptance)
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
            Vec<String>,
            chrono::DateTime<Utc>,
        );
        let row: Option<Row> = self
            .retry(|| async {
                sqlx::query_as(
                    r#"
                    SELECT planned_start, planned_end, budget_tokens, baseline_rev,
                           acceptance, updated_at
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
            Some((_, _, budget, _, _, _)) if budget < 0 => {
                return Err(negative_budget(workspace_id, task_id, budget))
            }
            Some(row) => row,
        };
        let (planned_start, planned_end, budget, baseline_rev, acceptance, updated_at) = budget;
        Ok(Some(cog_core::TaskPlan {
            workspace_id: workspace_id.to_string(),
            task_id: task_id.to_string(),
            planned_start,
            planned_end,
            budget_tokens: budget as u64,
            baseline_rev,
            acceptance,
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
            Vec<String>,
            chrono::DateTime<Utc>,
        )> = self
            .retry(|| async {
                sqlx::query_as(
                    r#"
                    SELECT task_id, planned_start, planned_end, budget_tokens,
                           baseline_rev, acceptance, updated_at
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
                |(
                    task_id,
                    planned_start,
                    planned_end,
                    budget,
                    baseline_rev,
                    acceptance,
                    updated_at,
                )| {
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
                        acceptance,
                        updated_at,
                    })
                },
            )
            .collect()
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
                sqlx::query_as("SELECT status, COUNT(*)::bigint FROM cog_dag_tasks GROUP BY status")
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
        // jsonb `?` 算子：task->'blocked_by' 数组包含 task_id 元素
        let rows: Vec<(String,)> = self
            .retry(|| async {
                sqlx::query_as(
                    r#"
                    SELECT task_id FROM cog_dag_tasks
                    WHERE workspace_id = $1 AND task->'blocked_by' ? $2
                    "#,
                )
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

        // 锁住本 workspace 全部任务行，整批读到一致快照后再算就绪翻转
        let rows: Vec<(String, serde_json::Value)> = sqlx::query_as(
            "SELECT task_id, task FROM cog_dag_tasks WHERE workspace_id = $1 FOR UPDATE",
        )
        .bind(workspace_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        let mut tasks: std::collections::HashMap<String, cog_core::Task> = rows
            .into_iter()
            .map(|(id, v)| serde_json::from_value(v).map(|t: cog_core::Task| (id, t)))
            .collect::<Result<_, _>>()
            .map_err(SFError::Serialization)?;

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
                            tasks
                                .get(b)
                                .map(|t| t.status == cog_core::TaskStatus::Completed)
                                .unwrap_or(false)
                        })
                })
                .unwrap_or(false);
            if all_ready {
                // 只报告就绪，不翻转状态（Scheduled = 已发布到 ready 流，
                // 由发布方 schedule_task 翻转），同事务锁保证判定一致
                ready.push(dep_id);
            }
        }

        // 只回写根任务；就绪下游保持 Pending，等发布方 CAS 翻转
        {
            let t = &tasks[task_id];
            sqlx::query(
                "UPDATE cog_dag_tasks SET task = $3, status = $4, updated_at = NOW() WHERE workspace_id = $1 AND task_id = $2",
            )
            .bind(workspace_id)
            .bind(task_id)
            .bind(serde_json::to_value(t)?)
            .bind(status_str(&t.status))
            .execute(&mut *tx)
            .await
            .map_err(|e| SFError::Database(e.to_string()))?;
        }

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
        max_retries: u32,
        retry_delay: std::time::Duration,
    ) -> SFResult<(bool, Vec<String>)> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| SFError::Database(e.to_string()))?;

        let rows: Vec<(String, serde_json::Value)> = sqlx::query_as(
            "SELECT task_id, task FROM cog_dag_tasks WHERE workspace_id = $1 FOR UPDATE",
        )
        .bind(workspace_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        let mut tasks: std::collections::HashMap<String, cog_core::Task> = rows
            .into_iter()
            .map(|(id, v)| serde_json::from_value(v).map(|t: cog_core::Task| (id, t)))
            .collect::<Result<_, _>>()
            .map_err(SFError::Serialization)?;

        let task = tasks.get_mut(task_id).ok_or_else(|| SFError::TaskFailed {
            task_id: task_id.into(),
            reason: "Task not found".into(),
        })?;
        task.error = Some(error.clone());
        task.error_cause = cause;
        task.updated_at = Utc::now();

        let mut dirty: Vec<String> = vec![task_id.to_string()];
        let mut cancelled = Vec::new();
        let should_retry = task.retry_count < max_retries;
        if should_retry {
            task.retry_count += 1;
            task.status = cog_core::TaskStatus::Pending;
            task.retry_not_before = chrono::Duration::from_std(retry_delay)
                .ok()
                .map(|d| Utc::now() + d);
        } else {
            task.status = cog_core::TaskStatus::Failed;
            task.retry_not_before = None;
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
                        if !matches!(
                            t.status,
                            cog_core::TaskStatus::Cancelled
                                | cog_core::TaskStatus::Failed
                                | cog_core::TaskStatus::Completed
                        ) {
                            t.status = cog_core::TaskStatus::Cancelled;
                            t.error = Some(format!(
                                "Cascade cancelled: upstream task '{}' permanently failed with error: {}",
                                task_id, error
                            ));
                            t.error_cause = None;
                            t.updated_at = Utc::now();
                            cancelled.push(child.clone());
                            dirty.push(child.clone());
                        }
                    }
                    stack.push(child);
                }
            }
        }

        for id in &dirty {
            let t = &tasks[id];
            sqlx::query(
                "UPDATE cog_dag_tasks SET task = $3, status = $4, updated_at = NOW() WHERE workspace_id = $1 AND task_id = $2",
            )
            .bind(workspace_id)
            .bind(id)
            .bind(serde_json::to_value(t)?)
            .bind(status_str(&t.status))
            .execute(&mut *tx)
            .await
            .map_err(|e| SFError::Database(e.to_string()))?;
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

        let rows: Vec<(String, serde_json::Value)> = sqlx::query_as(
            "SELECT task_id, task FROM cog_dag_tasks WHERE workspace_id = $1 FOR UPDATE",
        )
        .bind(workspace_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        let mut tasks: std::collections::HashMap<String, cog_core::Task> = rows
            .into_iter()
            .map(|(id, v)| serde_json::from_value(v).map(|t: cog_core::Task| (id, t)))
            .collect::<Result<_, _>>()
            .map_err(SFError::Serialization)?;

        let task = tasks.get_mut(task_id).ok_or_else(|| SFError::TaskFailed {
            task_id: task_id.into(),
            reason: "Task not found".into(),
        })?;
        if matches!(
            task.status,
            cog_core::TaskStatus::Cancelled
                | cog_core::TaskStatus::Failed
                | cog_core::TaskStatus::Completed
        ) {
            return Err(SFError::TaskFailed {
                task_id: task_id.into(),
                reason: format!("Cannot cancel task in {:?} state", task.status),
            });
        }
        task.status = cog_core::TaskStatus::Cancelled;
        task.error = Some(reason);
        task.error_cause = None;
        task.updated_at = Utc::now();

        let mut dirty: Vec<String> = vec![task_id.to_string()];
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
                    if !matches!(
                        t.status,
                        cog_core::TaskStatus::Cancelled
                            | cog_core::TaskStatus::Failed
                            | cog_core::TaskStatus::Completed
                    ) {
                        t.status = cog_core::TaskStatus::Cancelled;
                        t.error = Some(format!(
                            "Cascade cancelled: upstream task '{}' was cancelled",
                            task_id
                        ));
                        t.updated_at = Utc::now();
                        cancelled.push(child.clone());
                        dirty.push(child.clone());
                    }
                }
                stack.push(child);
            }
        }

        for id in &dirty {
            let t = &tasks[id];
            sqlx::query(
                "UPDATE cog_dag_tasks SET task = $3, status = $4, updated_at = NOW() WHERE workspace_id = $1 AND task_id = $2",
            )
            .bind(workspace_id)
            .bind(id)
            .bind(serde_json::to_value(t)?)
            .bind(status_str(&t.status))
            .execute(&mut *tx)
            .await
            .map_err(|e| SFError::Database(e.to_string()))?;
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
        let value = serde_json::to_value(attempt)?;
        self.retry(|| async {
            sqlx::query(
                r#"
                UPDATE cog_dag_tasks
                SET retry_history = retry_history || $3::jsonb, updated_at = NOW()
                WHERE workspace_id = $1 AND task_id = $2
                "#,
            )
            .bind(workspace_id)
            .bind(task_id)
            .bind(value.clone())
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
        let row: Option<(serde_json::Value,)> = self
            .retry(|| async {
                sqlx::query_as(
                    "SELECT retry_history FROM cog_dag_tasks WHERE workspace_id = $1 AND task_id = $2",
                )
                .bind(workspace_id)
                .bind(task_id)
                .fetch_optional(&self.pool)
                .await
            })
            .await?;
        match row {
            Some((v,)) => serde_json::from_value(v).map_err(SFError::Serialization),
            None => Ok(Vec::new()),
        }
    }

    async fn dag_clear_workspace(&self, workspace_id: &str) -> SFResult<()> {
        self.retry(|| async {
            sqlx::query("DELETE FROM cog_dag_tasks WHERE workspace_id = $1")
                .bind(workspace_id)
                .execute(&self.pool)
                .await
        })
        .await?;
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
            WHERE status = 'promoted' AND updated_at >= $1
            "#,
        )
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
            WHERE cluster = $1 AND status = 'awaiting_approval'
            ORDER BY updated_at ASC
            LIMIT $2
            "#,
        )
        .bind(cluster)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        Ok(rows.into_iter().map(promotion_record).collect())
    }
}

#[cfg(test)]
mod plan_schema_tests {
    use super::DDL;

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
        let columns = task_plan_columns(DDL);
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
            task_plan_columns(DDL),
            vec![
                "workspace_id",
                "task_id",
                "planned_start",
                "planned_end",
                "budget_tokens",
                "baseline_rev",
                "acceptance",
                "updated_at",
            ]
        );
    }
}
