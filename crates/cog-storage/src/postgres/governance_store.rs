//! Governance rows, read and written through the task ledger.
//!
//! There is no face table. A face is a non-executable task in one reserved
//! workspace, which is the same shape the platform already keeps for a
//! submitted goal whose decomposition has not run: a row that exists so that
//! something can be asked about it, and that nothing will ever execute. The
//! alternative -- a second table of faces -- was rejected because two stores
//! answering "who owns this" disagree the first time one of them is updated.
//!
//! Since the task graph became a ledger, the fields these rows are read by are
//! columns: `face_id` and `owner_unit` live on the definition revision, and the
//! state a row is in is its last transition. That is what makes the handover
//! paths below cheap -- moving a face's unstarted work appends one revision per
//! row instead of rewriting a document per row -- and it is also why nothing
//! here parses a task document to find out who owns what.
//!
//! Every read here is over rows the task system already keeps. The only thing
//! this store adds is the shape of the question.

use async_trait::async_trait;
use sqlx::PgPool;

use cog_core::{
    GovernanceStore, Handover, SFError, SFResult, Task, TaskStatus, FACE_TASK_TYPE,
    GOVERNANCE_WORKSPACE,
};

use super::task_ledger::{self, ledger_sql, LedgerRow};

/// Face nodes, in one workspace, typed as faces.
///
/// The kind is a column now, so "typed as a face" is an equality and not a
/// parse: a custom type is stored under its own name, which is exactly the
/// string [`cog_core::TaskType::kind_key`] writes.
fn face_nodes_sql() -> String {
    format!(
        "{} ORDER BY task_id",
        ledger_sql("workspace_id = $1 AND kind = $2", "")
    )
}

/// Open work that names a face.
///
/// Terminal states are excluded because a completed task cannot drift; rows
/// without a face are excluded because they belong to no face's reading; face
/// nodes themselves are excluded by workspace, since they are not work.
///
/// The state filter sits outside the definition subquery on purpose: what a
/// task's state is depends on its transitions, so filtering inside the subquery
/// would compare a definition column against a state that is not there.
fn open_face_bound_sql() -> String {
    ledger_sql(
        "workspace_id <> $1 AND face_id IS NOT NULL AND is_executable",
        "to_state IN ('{pending}', '{scheduled}', '{running}') \
         ORDER BY workspace_id, task_id",
    )
}

/// Every row of one governance task type. Handovers, decisions and votes are
/// rows like the faces, and they are told apart by their kind.
fn governance_nodes_sql() -> String {
    format!(
        "{} ORDER BY task_id",
        ledger_sql("workspace_id = $1 AND kind = $2", "")
    )
}

/// Give one face's unstarted work to the incoming unit.
///
/// Only `pending` and `scheduled` rows move. A `running` task is executable
/// through its holder's lease; rewriting its owner would leave the lease and
/// the binding describing different units, so it is left to finish where it is
/// and reads as drifted until then.
///
/// `agent_id` is cleared because it names the old owner's worker. Keeping it
/// would hand the new unit a row it cannot execute (the router checks the
/// owner) while the row claims a worker that belongs to somebody else. The
/// lease lives in the projection rather than the definition, so it is cleared
/// there after the revisions are appended.
///
/// The new revision carries the binding in both places the old document did --
/// the column and the payload the claimer reads -- because those two copies are
/// how the executor learns who it is working for.
const REBIND_FACE_TASKS_SQL: &str = "\
WITH latest AS (\
    SELECT DISTINCT ON (task_id) * FROM task_defined \
     WHERE workspace_id <> $1 \
     ORDER BY task_id, rev DESC), \
targets AS (\
    SELECT l.* FROM latest l \
      JOIN task_current c ON c.task_id = l.task_id \
     WHERE l.face_id = $2 AND l.owner_unit = $5 \
       AND c.state IN ('{pending}', '{scheduled}')) \
INSERT INTO task_defined (\
    task_id, rev, workspace_id, kind, input, created_at, priority, max_retries, \
    timeout_seconds, is_executable, acceptance, blocked_by, agent_id, goal_id, \
    parent_task_id, sink_task_id, face_id, venue, owner_unit, \
    bound_by_handover_id, submitted_by, planner_verified, planner_version, \
    planner_note, planner_source, planner_confidence, planner_at) \
SELECT task_id, rev + 1, workspace_id, kind, \
       CASE WHEN jsonb_typeof(input::jsonb) = 'object' \
            THEN jsonb_set(input::jsonb, '{owner_unit}', to_jsonb($3::text), true)::text \
            ELSE input END, \
       created_at, priority, max_retries, timeout_seconds, is_executable, \
       acceptance, blocked_by, NULL, goal_id, parent_task_id, sink_task_id, \
       face_id, venue, $3, $4, submitted_by, planner_verified, \
       planner_version, planner_note, planner_source, planner_confidence, planner_at \
  FROM targets \
ON CONFLICT (task_id, rev) DO NOTHING \
RETURNING task_id";

/// Hand one face's unstarted work to a unit that is present.
///
/// The same revision as a handover's rebind, without the handover: the face did
/// not change hands on paper, the unit that held its rows is simply gone.
///
/// The owner is compared before writing, so a face whose rows already belong to
/// the present unit matches nothing and the pass reports zero rows moved rather
/// than appending a revision every two minutes. The comparison is
/// `IS DISTINCT FROM` rather than `<>`: plain `<>` answers NULL against a
/// missing owner, which is not true, so the row would be silently skipped and
/// the face would keep reading idle while the pass reported that it looked.
const REASSIGN_FACE_WORK_SQL: &str = "\
WITH latest AS (\
    SELECT DISTINCT ON (task_id) * FROM task_defined \
     WHERE workspace_id <> $1 \
     ORDER BY task_id, rev DESC), \
targets AS (\
    SELECT l.* FROM latest l \
      JOIN task_current c ON c.task_id = l.task_id \
     WHERE l.face_id = $2 AND l.owner_unit IS DISTINCT FROM $3 \
       AND c.state IN ('{pending}', '{scheduled}')) \
INSERT INTO task_defined (\
    task_id, rev, workspace_id, kind, input, created_at, priority, max_retries, \
    timeout_seconds, is_executable, acceptance, blocked_by, agent_id, goal_id, \
    parent_task_id, sink_task_id, face_id, venue, owner_unit, \
    bound_by_handover_id, submitted_by, planner_verified, planner_version, \
    planner_note, planner_source, planner_confidence, planner_at) \
SELECT task_id, rev + 1, workspace_id, kind, \
       CASE WHEN jsonb_typeof(input::jsonb) = 'object' \
            THEN jsonb_set(input::jsonb, '{owner_unit}', to_jsonb($3::text), true)::text \
            ELSE input END, \
       created_at, priority, max_retries, timeout_seconds, is_executable, \
       acceptance, blocked_by, NULL, goal_id, parent_task_id, sink_task_id, \
       face_id, venue, $3, bound_by_handover_id, submitted_by, planner_verified, \
       planner_version, planner_note, planner_source, planner_confidence, planner_at \
  FROM targets \
ON CONFLICT (task_id, rev) DO NOTHING \
RETURNING task_id";

/// Give one face's unstarted work back to the shared queue.
///
/// Both copies of the owner go, for the same reason both were written: the row
/// and the payload are read by different hands, and a row whose payload still
/// names a unit tells the executor it is working for somebody the queue never
/// routed it from. `owner_unit` is set to NULL rather than to a name, which is
/// what puts the rows back in front of every worker.
const RELEASE_FACE_WORK_SQL: &str = "\
WITH latest AS (\
    SELECT DISTINCT ON (task_id) * FROM task_defined \
     WHERE workspace_id <> $1 \
     ORDER BY task_id, rev DESC), \
targets AS (\
    SELECT l.* FROM latest l \
      JOIN task_current c ON c.task_id = l.task_id \
     WHERE l.face_id = $2 AND l.owner_unit IS NOT NULL \
       AND c.state IN ('{pending}', '{scheduled}')) \
INSERT INTO task_defined (\
    task_id, rev, workspace_id, kind, input, created_at, priority, max_retries, \
    timeout_seconds, is_executable, acceptance, blocked_by, agent_id, goal_id, \
    parent_task_id, sink_task_id, face_id, venue, owner_unit, \
    bound_by_handover_id, submitted_by, planner_verified, planner_version, \
    planner_note, planner_source, planner_confidence, planner_at) \
SELECT task_id, rev + 1, workspace_id, kind, \
       CASE WHEN jsonb_typeof(input::jsonb) = 'object' \
            THEN ((input::jsonb) - 'owner_unit')::text \
            ELSE input END, \
       created_at, priority, max_retries, timeout_seconds, is_executable, \
       acceptance, blocked_by, NULL, goal_id, parent_task_id, sink_task_id, \
       face_id, venue, NULL, bound_by_handover_id, submitted_by, planner_verified, \
       planner_version, planner_note, planner_source, planner_confidence, planner_at \
  FROM targets \
ON CONFLICT (task_id, rev) DO NOTHING \
RETURNING task_id";

/// The status words a template may ask for, and the variant each stands for.
///
/// The left column is a label the template writes; the right is the variant
/// whose spelling [`TaskStatus::as_str`] decides. They are deliberately two
/// different strings: the placeholder names a slot, the constant decides what
/// goes in it.
const STATUS_PLACEHOLDERS: &[(&str, TaskStatus)] = &[
    ("pending", TaskStatus::Pending),
    ("scheduled", TaskStatus::Scheduled),
    ("running", TaskStatus::Running),
    ("completed", TaskStatus::Completed),
    ("failed", TaskStatus::Failed),
    ("cancelled", TaskStatus::Cancelled),
];

/// Fill a query template's status placeholders from [`TaskStatus`].
///
/// A status written straight into a query is a second spelling of a word
/// `as_str` already decides. Rename a variant there and the predicate keeps
/// matching the old spelling, which is not an error: a query matching no rows
/// answers the same as one over an empty table, so the face would read as
/// having no open work rather than as broken.
fn fill_statuses(template: &str) -> String {
    let mut sql = template.to_string();
    for (name, status) in STATUS_PLACEHOLDERS {
        sql = sql.replace(&format!("{{{name}}}"), status.as_str());
    }
    sql
}

/// PostgreSQL governance store.
#[derive(Clone)]
pub struct PostgresGovernanceStore {
    pool: PgPool,
}

impl PostgresGovernanceStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// The rows a ledger read returned, as tasks.
    ///
    /// A row this build cannot read is an error rather than a skipped row: a
    /// face that silently disappears from the registry is a face nobody
    /// staffs, and the reading would say "no faces" instead.
    fn rows_to_tasks(rows: Vec<LedgerRow>) -> SFResult<Vec<Task>> {
        rows.into_iter().map(LedgerRow::into_task).collect()
    }

    /// Clear the lease on rows just given a new owner.
    ///
    /// The lease is presence, not history, so it is not part of a definition
    /// revision -- and a row that changed hands must not keep pointing at the
    /// worker of the unit it left.
    async fn clear_leases(
        conn: &mut sqlx::PgConnection,
        task_ids: &[String],
    ) -> Result<(), SFError> {
        if task_ids.is_empty() {
            return Ok(());
        }
        sqlx::query(
            "UPDATE task_current SET lease_owner = NULL, lease_expires_at = NULL \
              WHERE task_id = ANY($1)",
        )
        .bind(task_ids)
        .execute(conn)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;
        Ok(())
    }

    /// Re-project the rows a bulk write touched.
    ///
    /// The projection carries `def_rev`, the watermark a reader compares to
    /// know its row is current. Appending a definition revision without
    /// re-projecting would leave that watermark pointing at the old revision --
    /// a reading that looks fresh and is one revision behind.
    async fn reproject(conn: &mut sqlx::PgConnection, task_ids: &[String]) -> Result<(), SFError> {
        for task_id in task_ids {
            task_ledger::project(conn, Some(task_id)).await?;
        }
        Ok(())
    }

    /// The revision number a face node's payload carries, defaulting to 1.
    ///
    /// The old document read this out of the row itself; here it is the same
    /// number, read from the latest definition's payload.
    async fn face_revision(conn: &mut sqlx::PgConnection, task_id: &str) -> Result<i64, SFError> {
        let input: Option<String> = sqlx::query_scalar(
            "SELECT input FROM task_defined WHERE task_id = $1 ORDER BY rev DESC LIMIT 1",
        )
        .bind(task_id)
        .fetch_optional(conn)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;
        let revision = input
            .and_then(|text| serde_json::from_str::<serde_json::Value>(&text).ok())
            .and_then(|value| value.pointer("/face/revision").and_then(|v| v.as_i64()));
        Ok(revision.unwrap_or(1))
    }
}

#[async_trait]
impl GovernanceStore for PostgresGovernanceStore {
    async fn face_nodes(&self) -> SFResult<Vec<Task>> {
        let rows: Vec<LedgerRow> = sqlx::query_as(&face_nodes_sql())
            .bind(GOVERNANCE_WORKSPACE)
            .bind(FACE_TASK_TYPE)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("governance face nodes: {e}")))?;
        Self::rows_to_tasks(rows)
    }

    async fn open_face_bound_tasks(&self) -> SFResult<Vec<Task>> {
        let rows: Vec<LedgerRow> = sqlx::query_as(&fill_statuses(&open_face_bound_sql()))
            .bind(GOVERNANCE_WORKSPACE)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("governance open tasks: {e}")))?;
        Self::rows_to_tasks(rows)
    }

    async fn save_face_node(&self, task: &Task) -> SFResult<()> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| SFError::Database(format!("governance save face node: {e}")))?;
        task_ledger::write_task(&mut tx, GOVERNANCE_WORKSPACE, task).await?;
        tx.commit()
            .await
            .map_err(|e| SFError::Database(format!("governance save face node: {e}")))?;
        Ok(())
    }

    async fn save_face_node_if_revision(&self, task: &Task, expected: i64) -> SFResult<bool> {
        let mut tx = self.pool.begin().await.map_err(|e| {
            SFError::Database(format!("governance save face node if revision: {e}"))
        })?;
        // The lock comes first: the revision is read and then written, and two
        // handovers reading the same one must not both find it current.
        task_ledger::lock_workspace(&mut tx, GOVERNANCE_WORKSPACE).await?;
        let current = Self::face_revision(&mut tx, &task.id).await?;
        if current != expected {
            tx.rollback().await.ok();
            return Ok(false);
        }
        task_ledger::write_task(&mut tx, GOVERNANCE_WORKSPACE, task).await?;
        tx.commit()
            .await
            .map_err(|e| SFError::Database(format!("governance save face node: {e}")))?;
        Ok(true)
    }

    async fn governance_nodes(&self, task_type: &str) -> SFResult<Vec<Task>> {
        let rows: Vec<LedgerRow> = sqlx::query_as(&governance_nodes_sql())
            .bind(GOVERNANCE_WORKSPACE)
            .bind(task_type)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("governance {task_type} nodes: {e}")))?;
        Self::rows_to_tasks(rows)
    }

    async fn save_governance_node(&self, task: &Task) -> SFResult<()> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| SFError::Database(format!("governance save node: {e}")))?;
        task_ledger::write_task(&mut tx, GOVERNANCE_WORKSPACE, task).await?;
        tx.commit()
            .await
            .map_err(|e| SFError::Database(format!("governance save node: {e}")))?;
        Ok(())
    }

    async fn save_governance_node_if_status(
        &self,
        task: &Task,
        expected: &[TaskStatus],
    ) -> SFResult<bool> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| SFError::Database(format!("governance save node if status: {e}")))?;
        task_ledger::lock_workspace(&mut tx, GOVERNANCE_WORKSPACE).await?;
        let state = task_ledger::current_state(&mut tx, &task.id).await?;
        match state {
            Some(state) if expected.contains(&state) => {
                task_ledger::write_task(&mut tx, GOVERNANCE_WORKSPACE, task).await?;
                tx.commit()
                    .await
                    .map_err(|e| SFError::Database(format!("governance save node: {e}")))?;
                Ok(true)
            }
            _ => {
                // No row answers the question the same way a row in another
                // state does: the caller asked to write only from the states it
                // read, and a missing row is not one of them.
                tx.rollback().await.ok();
                Ok(false)
            }
        }
    }

    async fn save_governance_node_with_event(
        &self,
        event: &Task,
        node: &Task,
        expected: &[TaskStatus],
    ) -> SFResult<bool> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| SFError::Database(format!("governance close with event: {e}")))?;
        task_ledger::lock_workspace(&mut tx, GOVERNANCE_WORKSPACE).await?;

        let state = task_ledger::current_state(&mut tx, &node.id).await?;
        if !matches!(state, Some(state) if expected.contains(&state)) {
            tx.rollback().await.ok();
            return Ok(false);
        }

        task_ledger::write_task(&mut tx, GOVERNANCE_WORKSPACE, node).await?;
        task_ledger::write_task(&mut tx, GOVERNANCE_WORKSPACE, event).await?;

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
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| SFError::Database(format!("governance apply handover: {e}")))?;
        task_ledger::lock_workspace(&mut tx, GOVERNANCE_WORKSPACE).await?;

        // The handover may only move out of `scheduled`: a signed handover that
        // arrived twice must apply once.
        let state = task_ledger::current_state(&mut tx, &handover.id).await?;
        if state != Some(TaskStatus::Scheduled) {
            tx.rollback().await.ok();
            return Ok(None);
        }
        task_ledger::write_task(&mut tx, GOVERNANCE_WORKSPACE, handover).await?;

        let current = Self::face_revision(&mut tx, &face.id).await?;
        if current != expected_revision {
            tx.rollback().await.ok();
            return Ok(None);
        }
        task_ledger::write_task(&mut tx, GOVERNANCE_WORKSPACE, face).await?;

        // Nothing to follow when the face was vacant: no unit held its work.
        let rebound = match spec.from_unit.as_deref() {
            Some(from) => {
                let moved: Vec<(String,)> = sqlx::query_as(&fill_statuses(REBIND_FACE_TASKS_SQL))
                    .bind(GOVERNANCE_WORKSPACE)
                    .bind(&spec.face_id)
                    .bind(&spec.to_unit)
                    .bind(&handover.id)
                    .bind(from)
                    .fetch_all(&mut *tx)
                    .await
                    .map_err(|e| SFError::Database(format!("governance apply handover: {e}")))?;
                let ids: Vec<String> = moved.into_iter().map(|(id,)| id).collect();
                Self::reproject(&mut tx, &ids).await?;
                Self::clear_leases(&mut tx, &ids).await?;
                ids.len()
            }
            None => 0,
        };

        tx.commit()
            .await
            .map_err(|e| SFError::Database(format!("governance apply handover: {e}")))?;
        Ok(Some(rebound))
    }

    async fn reassign_face_work(&self, face_id: &str, to_unit: &str) -> SFResult<usize> {
        let to_unit = to_unit.trim();
        if to_unit.is_empty() {
            return Err(SFError::Validation(
                "reassigning a face's work needs the unit taking it: an empty name would \
                 leave the rows owned by nobody while claiming they moved"
                    .into(),
            ));
        }
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| SFError::Database(format!("governance reassign face work: {e}")))?;
        let moved: Vec<(String,)> = sqlx::query_as(&fill_statuses(REASSIGN_FACE_WORK_SQL))
            .bind(GOVERNANCE_WORKSPACE)
            .bind(face_id)
            .bind(to_unit)
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| SFError::Database(format!("governance reassign face work: {e}")))?;
        let ids: Vec<String> = moved.into_iter().map(|(id,)| id).collect();
        Self::reproject(&mut tx, &ids).await?;
        Self::clear_leases(&mut tx, &ids).await?;
        tx.commit()
            .await
            .map_err(|e| SFError::Database(format!("governance reassign face work: {e}")))?;
        Ok(ids.len())
    }

    async fn release_face_work(&self, face_id: &str) -> SFResult<usize> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| SFError::Database(format!("governance release face work: {e}")))?;
        let released: Vec<(String,)> = sqlx::query_as(&fill_statuses(RELEASE_FACE_WORK_SQL))
            .bind(GOVERNANCE_WORKSPACE)
            .bind(face_id)
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| SFError::Database(format!("governance release face work: {e}")))?;
        let ids: Vec<String> = released.into_iter().map(|(id,)| id).collect();
        Self::reproject(&mut tx, &ids).await?;
        Self::clear_leases(&mut tx, &ids).await?;
        tx.commit()
            .await
            .map_err(|e| SFError::Database(format!("governance release face work: {e}")))?;
        Ok(ids.len())
    }

    async fn ensure_face_duty(&self, row: &Task) -> SFResult<bool> {
        let workspace = row.workspace_id.as_deref().unwrap_or_default().trim();
        if workspace.is_empty() || workspace == GOVERNANCE_WORKSPACE {
            return Err(SFError::Validation(format!(
                "a face's duty row belongs in a workspace the drift reading measures, \
                 not in {:?}",
                row.workspace_id
            )));
        }

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| SFError::Database(format!("governance ensure face duty: {e}")))?;
        task_ledger::lock_workspace(&mut tx, workspace).await?;

        // The two ways a duty row stops serving are both silent: a terminal
        // row is not open work, and a row in another shape is eaten by the
        // dispatcher and turned terminal. Both are replaced; a live row in the
        // right shape bound to the same unit is left exactly as it is, which is
        // what makes this safe to run every pass.
        //
        // A `running` row is never rewritten: it is executing under the lease
        // that makes it executable, and it fails on its own to be replaced on
        // the next pass.
        let existing: Option<LedgerRow> =
            sqlx::query_as(&ledger_sql("workspace_id = $1 AND task_id = $2", ""))
                .bind(workspace)
                .bind(&row.id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| SFError::Database(format!("governance ensure face duty: {e}")))?;

        let replace = match existing {
            None => true,
            Some(current) => {
                let task = current.into_task()?;
                task.status != TaskStatus::Running
                    && (matches!(
                        task.status,
                        TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Cancelled
                    ) || task.task_type != row.task_type
                        || task.owner_unit != row.owner_unit)
            }
        };

        if replace {
            task_ledger::write_task(&mut tx, workspace, row).await?;
        }
        tx.commit()
            .await
            .map_err(|e| SFError::Database(format!("governance ensure face duty: {e}")))?;
        Ok(replace)
    }
}

#[cfg(test)]
mod status_templates {
    use super::*;

    /// Every query that names a state has it filled in, from the declaration.
    #[test]
    fn every_template_fills_its_state_placeholders() {
        let templates = [
            open_face_bound_sql(),
            REBIND_FACE_TASKS_SQL.to_string(),
            REASSIGN_FACE_WORK_SQL.to_string(),
            RELEASE_FACE_WORK_SQL.to_string(),
        ];
        let mut filled = 0;
        for template in &templates {
            let sql = fill_statuses(template);
            for (name, _) in STATUS_PLACEHOLDERS {
                let slot = format!("{{{name}}}");
                filled += template.matches(&slot).count();
                assert!(
                    !sql.contains(&slot),
                    "模板里还有没填上的状态占位符 {slot}：{sql}"
                );
            }
        }
        // 少到这一步说明有人把模板改回了直接拼状态词，或者换了占位符的名字
        // 却没登记；两种都让下面那几条查询读一个不存在的状态。
        assert!(
            filled >= 9,
            "四条模板里一共只有 {filled} 个状态占位符，比应有的少"
        );
    }

    /// The word that lands in the query is the one `as_str` decides.
    #[test]
    fn the_filled_word_is_the_spelling_as_str_decides() {
        for (name, status) in STATUS_PLACEHOLDERS {
            let sql = fill_statuses(&format!("status = '{{{name}}}'"));
            assert_eq!(sql, format!("status = '{}'", status.as_str()));
        }
        // 没登记的槽不会被悄悄当成空串填掉。
        assert!(fill_statuses("status = '{nope}'").contains("{nope}"));
    }
}
