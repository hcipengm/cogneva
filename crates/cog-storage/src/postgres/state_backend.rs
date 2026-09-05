use async_trait::async_trait;
use chrono::Utc;
use serde_json;
use sqlx::PgPool;
use std::collections::HashMap;

use cog_core::{AgentState, ContextBoard, Event, SFError, SFResult, StateBackend, TaskCheckpoint};

/// PostgreSQL-backed state backend.
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
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS cog_agent_states (
                agent_id TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS cog_checkpoints (
                task_id TEXT PRIMARY KEY,
                snapshot_id TEXT NOT NULL,
                event_offset BIGINT NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS cog_events (
                id SERIAL PRIMARY KEY,
                task_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                payload JSONB NOT NULL,
                offset_num BIGINT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_cog_events_task_id ON cog_events(task_id)
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS cog_context_boards (
                task_id TEXT NOT NULL,
                field_name TEXT NOT NULL,
                field_value TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (task_id, field_name)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS cog_dag_state (
                workspace_id TEXT PRIMARY KEY,
                state JSONB NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        // 存储权威模式的单任务表：task 存完整序列化，status 冗余一列供
        // CAS 与就绪扫描，依赖边不落地（从 task.blocked_by 派生查询，
        // 消掉反向索引双写竞态），retry_history 随行原子追加。
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS cog_dag_tasks (
                workspace_id TEXT NOT NULL,
                task_id TEXT NOT NULL,
                task JSONB NOT NULL,
                status TEXT NOT NULL,
                retry_history JSONB NOT NULL DEFAULT '[]'::jsonb,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (workspace_id, task_id)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_cog_dag_tasks_status
            ON cog_dag_tasks(workspace_id, status)
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        // 晋级台账：每次晋级（推送端与各集群拉取端）全字段留档，
        // 配额/熔断/审计共用一份事实源。
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS cog_evolution_promotions (
                id TEXT PRIMARY KEY,
                change_id TEXT NOT NULL,
                level TEXT NOT NULL,
                decision_reason TEXT NOT NULL,
                cluster TEXT NOT NULL,
                status TEXT NOT NULL,
                outcome TEXT NOT NULL,
                eval_summary TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_cog_evolution_promotions_updated
            ON cog_evolution_promotions(updated_at DESC)
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        // 迁移：旧库台账列名 patch_id → change_id（新库无 patch_id 列，自动跳过）。
        sqlx::query(
            r#"
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
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        Ok(())
    }
}

/// TaskStatus 的 DB 文本形式（与 serde snake_case 一致）。
fn status_str(status: &cog_core::TaskStatus) -> &'static str {
    match status {
        cog_core::TaskStatus::Pending => "pending",
        cog_core::TaskStatus::Scheduled => "scheduled",
        cog_core::TaskStatus::Running => "running",
        cog_core::TaskStatus::Completed => "completed",
        cog_core::TaskStatus::Failed => "failed",
        cog_core::TaskStatus::Cancelled => "cancelled",
    }
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

        let count: (i64,) = self
            .retry(|| async {
                sqlx::query_as("SELECT COUNT(*) FROM cog_events WHERE task_id = $1")
                    .bind(task_id)
                    .fetch_one(&self.pool)
                    .await
            })
            .await?;

        let offset_num = count.0 + 1;

        self.retry(|| async {
            sqlx::query(
                r#"
                INSERT INTO cog_events (task_id, event_type, payload, offset_num, created_at)
                VALUES ($1, $2, $3, $4, NOW())
                "#,
            )
            .bind(task_id)
            .bind(event_type.clone())
            .bind(payload.clone())
            .bind(offset_num)
            .execute(&self.pool)
            .await
        })
        .await?;

        Ok(offset_num as u64)
    }

    async fn get_events(&self, task_id: &str, offset: u64, limit: usize) -> SFResult<Vec<Event>> {
        let rows: Vec<(String, serde_json::Value, i64, chrono::DateTime<Utc>)> = self
            .retry(|| async {
                sqlx::query_as(
                    r#"
                    SELECT event_type, payload, offset_num, created_at
                    FROM cog_events
                    WHERE task_id = $1 AND offset_num >= $2
                    ORDER BY offset_num ASC
                    LIMIT $3
                    "#,
                )
                .bind(task_id)
                .bind(offset as i64)
                .bind(limit as i64)
                .fetch_all(&self.pool)
                .await
            })
            .await?;

        let mut events = Vec::with_capacity(rows.len());
        for (event_type, payload, offset_num, created_at) in rows {
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
        max_retries: u32,
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
        task.updated_at = Utc::now();

        let mut dirty: Vec<String> = vec![task_id.to_string()];
        let mut cancelled = Vec::new();
        let should_retry = task.retry_count < max_retries;
        if should_retry {
            task.retry_count += 1;
            task.status = cog_core::TaskStatus::Pending;
        } else {
            task.status = cog_core::TaskStatus::Failed;
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

#[async_trait]
impl cog_core::PromotionLedger for PostgresStateBackend {
    async fn record(&self, rec: cog_core::PromotionRecord) -> SFResult<()> {
        sqlx::query(
            r#"
            INSERT INTO cog_evolution_promotions
                (id, change_id, level, decision_reason, cluster, status, outcome, eval_summary, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (id) DO NOTHING
            "#,
        )
        .bind(&rec.id)
        .bind(&rec.change_id)
        .bind(&rec.level)
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
        let rows: Vec<(
            String,
            String,
            String,
            String,
            String,
            String,
            String,
            Option<String>,
            chrono::DateTime<Utc>,
            chrono::DateTime<Utc>,
        )> = sqlx::query_as(
            r#"
            SELECT id, change_id, level, decision_reason, cluster, status, outcome,
                   eval_summary, created_at, updated_at
            FROM cog_evolution_promotions
            ORDER BY updated_at DESC
            LIMIT $1
            "#,
        )
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        Ok(rows
            .into_iter()
            .map(
                |(
                    id,
                    change_id,
                    level,
                    decision_reason,
                    cluster,
                    status,
                    outcome,
                    eval_summary,
                    created_at,
                    updated_at,
                )| {
                    cog_core::PromotionRecord {
                        id,
                        change_id,
                        level,
                        decision_reason,
                        cluster,
                        status: cog_core::PromotionStatus::parse(&status)
                            .unwrap_or(cog_core::PromotionStatus::Failed),
                        outcome,
                        eval_summary,
                        created_at,
                        updated_at,
                    }
                },
            )
            .collect())
    }
}
