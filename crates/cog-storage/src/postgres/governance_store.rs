//! Governance rows, read and written through the task table.
//!
//! There is no face table. A face is a non-executable task in one reserved
//! workspace, which is the same shape the platform already keeps for a
//! submitted goal whose decomposition has not run: a row that exists so that
//! something can be asked about it, and that nothing will ever execute. The
//! alternative -- a second table of faces -- was rejected because two stores
//! answering "who owns this" disagree the first time one of them is updated.
//!
//! The generated columns `face_id` and `owner_unit` are the task JSON read out
//! by the database itself, so the index and the row cannot describe different
//! bindings. They are declared in the state backend's schema, not here: that
//! schema is what creates the table, and a migration that altered a table it
//! had not created would fail on a fresh install.
//!
//! That also means a reader outside the application process cannot assume they
//! exist -- the columns appear when the application's own startup step runs, and
//! a host command reading the same database may be several revisions ahead of
//! the deployment. The queries below therefore read the binding out of the task
//! document, which is where it is written and is present on every row.
//!
//! Every read here is over rows the task system already keeps. The only thing
//! this store adds is the shape of the question.

use async_trait::async_trait;
use sqlx::{PgPool, Row};

use cog_core::{
    GovernanceStore, Handover, SFError, SFResult, Task, TaskStatus, FACE_TASK_TYPE,
    GOVERNANCE_WORKSPACE,
};

/// Face nodes, in one workspace, typed as faces.
///
/// The type is matched through its `Custom` variant because that is how the
/// task type is written: a variant with a payload serialises as an object, and
/// reading it as a bare string would match nothing and look like an empty
/// registry rather than a broken query.
const FACE_NODES_SQL: &str = "\
SELECT task FROM cog_dag_tasks \
WHERE workspace_id = $1 \
  AND task->'task_type'->>'custom' = $2 \
ORDER BY task_id";

/// Open work that names a face.
///
/// Terminal rows are excluded because a completed task cannot drift; rows
/// without a face are excluded because they belong to no face's reading; face
/// nodes themselves are excluded by workspace, since they are not work.
///
/// `COALESCE(..., true)`: rows written before `is_executable` existed omit the
/// field, and those rows are executable. Reading the missing value as false
/// would drop every older task from the reading and make the platform look
/// idle.
const OPEN_FACE_BOUND_SQL: &str = "\
SELECT task FROM cog_dag_tasks \
WHERE status IN ('pending', 'scheduled', 'running') \
  AND task ->> 'face_id' IS NOT NULL \
  AND workspace_id <> $1 \
  AND COALESCE((task->>'is_executable')::boolean, true) \
ORDER BY workspace_id, task_id";

/// Upsert one face node.
const SAVE_FACE_NODE_SQL: &str = "\
INSERT INTO cog_dag_tasks (workspace_id, task_id, task, status, updated_at) \
VALUES ($1, $2, $3, $4, NOW()) \
ON CONFLICT (workspace_id, task_id) DO UPDATE \
    SET task = EXCLUDED.task, status = EXCLUDED.status, updated_at = NOW()";

/// Write a face node only while the stored revision is still the one the
/// caller read.
///
/// The revision lives inside the task payload, so the guard reads it from
/// there. A handover that bumps it and an import that read the old value cannot
/// both win: whichever arrives second matches nothing and is told so.
const SAVE_FACE_NODE_IF_REVISION_SQL: &str = "\
UPDATE cog_dag_tasks \
   SET task = $3, status = $4, updated_at = NOW() \
 WHERE workspace_id = $1 AND task_id = $2 \
   AND COALESCE((task->'input'->'face'->>'revision')::bigint, 1) = $5";

/// Every row of one governance task type.
///
/// Handovers, decisions and votes are rows like the faces, so the same read
/// that finds faces finds them: the type is read out of the document, which
/// every row carries, rather than from a column a deployment may not have
/// added yet.
const GOVERNANCE_NODES_SQL: &str = "\
SELECT task FROM cog_dag_tasks \
WHERE workspace_id = $1 \
  AND task->'task_type'->>'custom' = $2 \
ORDER BY task_id";

/// Write one governance row, whatever state it is in.
const SAVE_GOVERNANCE_NODE_SQL: &str = "\
INSERT INTO cog_dag_tasks (workspace_id, task_id, task, status, updated_at) \
VALUES ($1, $2, $3, $4, NOW()) \
ON CONFLICT (workspace_id, task_id) DO UPDATE \
    SET task = EXCLUDED.task, status = EXCLUDED.status, updated_at = NOW()";

/// Write a governance row only while it is still in one of the states the
/// caller read.
const SAVE_GOVERNANCE_NODE_IF_STATUS_SQL: &str = "\
UPDATE cog_dag_tasks \
   SET task = $3, status = $4, updated_at = NOW() \
 WHERE workspace_id = $1 AND task_id = $2 AND status = ANY($5)";

/// Move a signed handover to its applied state, but only from `scheduled`.
const APPLY_HANDOVER_ROW_SQL: &str = "\
UPDATE cog_dag_tasks \
   SET task = $3, status = $4, updated_at = NOW() \
 WHERE workspace_id = $1 AND task_id = $2 AND status = 'scheduled'";

/// Hand the outgoing unit's unstarted work for one face to the incoming unit.
///
/// Only `pending` and `scheduled` rows move. A `running` task is executable
/// through its holder's lease; rewriting its owner would leave the lease and
/// the binding describing different units, so it is left to finish where it
/// is and reads as drifted until then.
///
/// `agent_id` and `lease_owner` are cleared because they name the old owner's
/// worker. Keeping them would hand the new unit a row it cannot execute (the
/// router checks the owner) while the row claims a worker that belongs to
/// somebody else.
const REBIND_FACE_TASKS_SQL: &str = "\
UPDATE cog_dag_tasks \
   SET task = (task - 'agent_id' - 'lease_owner') || jsonb_build_object( \
                'owner_unit', $3::text, \
                'bound_by_handover_id', $4::text), \
       updated_at = NOW() \
 WHERE workspace_id <> $1 \
   AND task ->> 'face_id' = $2 \
   AND task ->> 'owner_unit' = $5 \
   AND status IN ('pending', 'scheduled')";

/// PostgreSQL governance store.
#[derive(Clone)]
pub struct PostgresGovernanceStore {
    pool: PgPool,
}

impl PostgresGovernanceStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

fn rows_to_tasks(rows: Vec<sqlx::postgres::PgRow>) -> SFResult<Vec<Task>> {
    rows.into_iter()
        .map(|row| {
            let value: serde_json::Value = row
                .try_get("task")
                .map_err(|e| SFError::Database(format!("governance task column: {e}")))?;
            serde_json::from_value(value).map_err(SFError::Serialization)
        })
        .collect()
}

#[async_trait]
impl GovernanceStore for PostgresGovernanceStore {
    async fn face_nodes(&self) -> SFResult<Vec<Task>> {
        let rows = sqlx::query(FACE_NODES_SQL)
            .bind(GOVERNANCE_WORKSPACE)
            .bind(FACE_TASK_TYPE)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("governance face nodes: {e}")))?;
        rows_to_tasks(rows)
    }

    async fn open_face_bound_tasks(&self) -> SFResult<Vec<Task>> {
        let rows = sqlx::query(OPEN_FACE_BOUND_SQL)
            .bind(GOVERNANCE_WORKSPACE)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("governance open tasks: {e}")))?;
        rows_to_tasks(rows)
    }

    async fn save_face_node(&self, task: &Task) -> SFResult<()> {
        let value = serde_json::to_value(task).map_err(SFError::Serialization)?;
        sqlx::query(SAVE_FACE_NODE_SQL)
            .bind(GOVERNANCE_WORKSPACE)
            .bind(&task.id)
            .bind(value)
            .bind(task.status.as_str())
            .execute(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("governance save face node: {e}")))?;
        Ok(())
    }

    async fn save_face_node_if_revision(&self, task: &Task, expected: i64) -> SFResult<bool> {
        let value = serde_json::to_value(task).map_err(SFError::Serialization)?;
        let result = sqlx::query(SAVE_FACE_NODE_IF_REVISION_SQL)
            .bind(GOVERNANCE_WORKSPACE)
            .bind(&task.id)
            .bind(value)
            .bind(task.status.as_str())
            .bind(expected)
            .execute(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("governance save face node: {e}")))?;
        Ok(result.rows_affected() == 1)
    }

    async fn governance_nodes(&self, task_type: &str) -> SFResult<Vec<Task>> {
        let rows = sqlx::query(GOVERNANCE_NODES_SQL)
            .bind(GOVERNANCE_WORKSPACE)
            .bind(task_type)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("governance {task_type} nodes: {e}")))?;
        rows_to_tasks(rows)
    }

    async fn save_governance_node(&self, task: &Task) -> SFResult<()> {
        let value = serde_json::to_value(task).map_err(SFError::Serialization)?;
        sqlx::query(SAVE_GOVERNANCE_NODE_SQL)
            .bind(GOVERNANCE_WORKSPACE)
            .bind(&task.id)
            .bind(value)
            .bind(task.status.as_str())
            .execute(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("governance save node: {e}")))?;
        Ok(())
    }

    async fn save_governance_node_if_status(
        &self,
        task: &Task,
        expected: &[TaskStatus],
    ) -> SFResult<bool> {
        let value = serde_json::to_value(task).map_err(SFError::Serialization)?;
        let states: Vec<&str> = expected.iter().map(TaskStatus::as_str).collect();
        let result = sqlx::query(SAVE_GOVERNANCE_NODE_IF_STATUS_SQL)
            .bind(GOVERNANCE_WORKSPACE)
            .bind(&task.id)
            .bind(value)
            .bind(task.status.as_str())
            .bind(&states)
            .execute(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("governance save node: {e}")))?;
        Ok(result.rows_affected() == 1)
    }

    async fn save_governance_node_with_event(
        &self,
        event: &Task,
        node: &Task,
        expected: &[TaskStatus],
    ) -> SFResult<bool> {
        let node_value = serde_json::to_value(node).map_err(SFError::Serialization)?;
        let event_value = serde_json::to_value(event).map_err(SFError::Serialization)?;
        let states: Vec<&str> = expected.iter().map(TaskStatus::as_str).collect();
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| SFError::Database(format!("governance close with event: {e}")))?;

        let moved = sqlx::query(SAVE_GOVERNANCE_NODE_IF_STATUS_SQL)
            .bind(GOVERNANCE_WORKSPACE)
            .bind(&node.id)
            .bind(node_value)
            .bind(node.status.as_str())
            .bind(&states)
            .execute(&mut *tx)
            .await
            .map_err(|e| SFError::Database(format!("governance close with event: {e}")))?;
        if moved.rows_affected() != 1 {
            tx.rollback().await.ok();
            return Ok(false);
        }

        sqlx::query(SAVE_GOVERNANCE_NODE_SQL)
            .bind(GOVERNANCE_WORKSPACE)
            .bind(&event.id)
            .bind(event_value)
            .bind(event.status.as_str())
            .execute(&mut *tx)
            .await
            .map_err(|e| SFError::Database(format!("governance close with event: {e}")))?;

        tx.commit()
            .await
            .map_err(|e| SFError::Database(format!("governance close with event: {e}")))?;
        Ok(true)
    }

    async fn apply_handover(
        &self,
        handover: &Task,
        face: &Task,
        expected_revision: i64,
    ) -> SFResult<Option<usize>> {
        let spec = Handover::from_task(handover)
            .ok_or_else(|| SFError::Validation("the handover row carries no handover".into()))?
            .spec;
        let handover_value = serde_json::to_value(handover).map_err(SFError::Serialization)?;
        let face_value = serde_json::to_value(face).map_err(SFError::Serialization)?;
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| SFError::Database(format!("governance apply handover: {e}")))?;

        let moved_handover = sqlx::query(APPLY_HANDOVER_ROW_SQL)
            .bind(GOVERNANCE_WORKSPACE)
            .bind(&handover.id)
            .bind(handover_value)
            .bind(handover.status.as_str())
            .execute(&mut *tx)
            .await
            .map_err(|e| SFError::Database(format!("governance apply handover: {e}")))?;
        if moved_handover.rows_affected() != 1 {
            tx.rollback().await.ok();
            return Ok(None);
        }

        let moved_face = sqlx::query(SAVE_FACE_NODE_IF_REVISION_SQL)
            .bind(GOVERNANCE_WORKSPACE)
            .bind(&face.id)
            .bind(face_value)
            .bind(face.status.as_str())
            .bind(expected_revision)
            .execute(&mut *tx)
            .await
            .map_err(|e| SFError::Database(format!("governance apply handover: {e}")))?;
        if moved_face.rows_affected() != 1 {
            tx.rollback().await.ok();
            return Ok(None);
        }

        // Nothing to follow when the face was vacant: no unit held its work.
        let rebound = match spec.from_unit.as_deref() {
            Some(from) => sqlx::query(REBIND_FACE_TASKS_SQL)
                .bind(GOVERNANCE_WORKSPACE)
                .bind(&spec.face_id)
                .bind(&spec.to_unit)
                .bind(&handover.id)
                .bind(from)
                .execute(&mut *tx)
                .await
                .map_err(|e| SFError::Database(format!("governance apply handover: {e}")))?
                .rows_affected() as usize,
            None => 0,
        };

        tx.commit()
            .await
            .map_err(|e| SFError::Database(format!("governance apply handover: {e}")))?;
        Ok(Some(rebound))
    }
}
