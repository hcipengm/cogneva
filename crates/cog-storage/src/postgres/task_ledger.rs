//! The task ledger: definition revisions, state transitions, attempts, and the
//! projection the dispatcher reads.
//!
//! A task used to be one jsonb document rewritten in place under a
//! compare-and-set on its status column. Three failures came out of that shape
//! and each of them is a property of a mutable document rather than of the
//! code that wrote it:
//!
//!   * A lost update is one interleaving away. Two writers that read the same
//!     document and each change a different field overwrite each other, and no
//!     constraint can say which of the two the second one meant to keep.
//!   * A value read once is frozen forever. `timeout_seconds` was computed from
//!     measured samples at planning time and copied into the document; nothing
//!     recomputed it, so tasks planned on 2026-11-08 still carried 3600 when
//!     the export of that hour said 3463.
//!   * A key that is not there answers NULL instead of failing. A query using
//!     `task->>'goal_id'` returned zero rows without an error and was read as
//!     "nothing was completed today".
//!
//! So: every state change is a row that is never updated, the definition is
//! versioned rather than overwritten, each execution is its own row, and the
//! row the dispatcher reads is a projection that only holds tasks that can
//! still move.
//!
//! # What a rebuild can and cannot restore
//!
//! [`project_sql`] is the whole projection, and the write path and the rebuild
//! command run the same statement -- a projection whose rebuild is written
//! twice is a projection whose rebuild is wrong once. Everything it produces is
//! derived from the ledger except the lease: `lease_owner` and
//! `lease_expires_at` are presence, not history, and a rebuild leaves them
//! NULL. That reads as "no evidence this is running", which is the conservative
//! answer and the one the reclaim path already handles (it falls back to
//! `started_at` plus the lease length for exactly this case).

use chrono::{DateTime, Utc};
use sqlx::PgConnection;

use cog_core::{SFError, Task, TaskStatus, TaskType, UpstreamFailure};

/// The spelling an upstream failure takes in the attempt's `error_code`, and
/// the spelling back.
///
/// Written out rather than derived from the enum's `Debug`, because this string
/// is a reading: a caller groups failures by it and must not have the grouping
/// move when a variant is renamed for readability. The two directions live in
/// one place for the same reason -- a write and a read that spell a code
/// differently do not fail, they quietly lose the cause.
pub(crate) fn cause_code(cause: Option<UpstreamFailure>) -> Option<&'static str> {
    cause.map(|c| match c {
        UpstreamFailure::RateLimited => "rate_limited",
        UpstreamFailure::QuotaExhausted => "quota_exhausted",
        UpstreamFailure::Auth => "auth",
        UpstreamFailure::ServerError => "server_error",
        UpstreamFailure::BadRequest => "bad_request",
        UpstreamFailure::Transport => "transport",
    })
}

/// The cause a stored code names. A code this build does not write reads as
/// `None` rather than as a guess: the failure still says what it says, and
/// inventing a cause would credit it to an environment nobody measured.
pub(crate) fn cause_from_code(code: Option<&str>) -> Option<UpstreamFailure> {
    Some(match code? {
        "rate_limited" => UpstreamFailure::RateLimited,
        "quota_exhausted" => UpstreamFailure::QuotaExhausted,
        "auth" => UpstreamFailure::Auth,
        "server_error" => UpstreamFailure::ServerError,
        "bad_request" => UpstreamFailure::BadRequest,
        "transport" => UpstreamFailure::Transport,
        _ => return None,
    })
}

/// Turn the column back into the enum it came from.
///
/// A kind outside the list is a custom one, which is what the old document
/// spelled as `{"custom": "..."}`. Keeping that reading here is what lets a
/// task type added by a caller survive a round trip without this table knowing
/// it.
pub(crate) fn task_type_from_kind(kind: &str) -> TaskType {
    match kind {
        cog_core::roles::PLANNER => TaskType::Planner,
        cog_core::roles::GENERATOR => TaskType::Generator,
        cog_core::roles::EVALUATOR => TaskType::Evaluator,
        "reviewer" => TaskType::Reviewer,
        "llm_call" => TaskType::LlmCall,
        "tool_call" => TaskType::ToolCall,
        "file_op" => TaskType::FileOp,
        "db_transaction" => TaskType::DbTransaction,
        "network_request" => TaskType::NetworkRequest,
        "wasm_skill" => TaskType::WasmSkill,
        "skill" => TaskType::Skill,
        "dag_node" => TaskType::DagNode,
        other => TaskType::Custom(other.to_string()),
    }
}

/// The spelling a [`TaskType`] takes in the `kind` column.
pub(crate) fn kind_of(task_type: &TaskType) -> String {
    match task_type {
        TaskType::Planner => cog_core::roles::PLANNER,
        TaskType::Generator => cog_core::roles::GENERATOR,
        TaskType::Evaluator => cog_core::roles::EVALUATOR,
        TaskType::Reviewer => "reviewer",
        TaskType::LlmCall => "llm_call",
        TaskType::ToolCall => "tool_call",
        TaskType::FileOp => "file_op",
        TaskType::DbTransaction => "db_transaction",
        TaskType::NetworkRequest => "network_request",
        TaskType::WasmSkill => "wasm_skill",
        TaskType::Skill => "skill",
        TaskType::DagNode => "dag_node",
        TaskType::Custom(name) => name,
    }
    .to_string()
}

/// The SQL grouping key of a kind, matching [`TaskType::kind_key`]: a custom
/// One row of [`LEDGER_SELECT`], in its column order.
///
/// A named struct rather than a tuple because the read is wider than the
/// sixteen columns a tuple decode stops at, and because a missing field would
/// otherwise fail at the call site rather than here.
#[derive(sqlx::FromRow)]
pub(crate) struct LedgerRow {
    pub workspace_id: String,
    pub task_id: String,
    pub kind: String,
    pub input: String,
    pub created_at: DateTime<Utc>,
    pub priority: i32,
    pub max_retries: i32,
    pub timeout_seconds: i64,
    pub is_executable: bool,
    pub acceptance: Vec<String>,
    pub blocked_by: Vec<String>,
    pub agent_id: Option<String>,
    pub goal_id: Option<String>,
    pub parent_task_id: Option<String>,
    pub sink_task_id: Option<String>,
    pub face_id: Option<String>,
    pub venue: Option<String>,
    pub owner_unit: Option<String>,
    pub bound_by_handover_id: Option<String>,
    pub submitted_by: Option<String>,
    pub planner_verified: Option<bool>,
    pub planner_version: Option<String>,
    pub planner_note: Option<String>,
    pub planner_source: Option<String>,
    pub planner_confidence: Option<f64>,
    pub planner_at: Option<DateTime<Utc>>,
    pub to_state: String,
    pub state_at: DateTime<Utc>,
    pub not_before: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub lease_owner: Option<String>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub retry_count: i32,
    pub error_code: Option<String>,
    pub error_text: Option<String>,
    pub attempt_agent: Option<String>,
    pub attempt_result: Option<String>,
    pub blocks: Vec<String>,
}

impl LedgerRow {
    /// The task as the rest of the platform knows it.
    ///
    /// A state this build cannot name is an error rather than a default: filing
    /// an unrecognised state under one of the six would keep counting rows
    /// while the states stopped being told apart.
    pub(crate) fn into_task(self) -> Result<Task, SFError> {
        let status = TaskStatus::parse(&self.to_state).ok_or_else(|| {
            SFError::Database(format!(
                "task {} is in state {:?}, which this build does not name",
                self.task_id, self.to_state
            ))
        })?;
        let input: serde_json::Value = serde_json::from_str(&self.input)?;
        let result = match self.attempt_result.as_deref() {
            Some(text) => Some(serde_json::from_str(text)?),
            None => None,
        };
        let action_planner_meta = match self.planner_verified {
            Some(verified) => Some(cog_core::ActionPlannerMeta {
                verified,
                version: self.planner_version,
                note: self.planner_note,
                source: self
                    .planner_source
                    .as_deref()
                    .and_then(planner_source_from_str),
                confidence: self.planner_confidence,
                timestamp: self.planner_at,
            }),
            None => None,
        };
        Ok(Task {
            id: self.task_id,
            task_type: task_type_from_kind(&self.kind),
            status,
            input,
            result,
            error: self.error_text,
            // The cause is read from its code, never guessed from the text:
            // `error` is prose the upstream wrote, and a machine grouping
            // failures by what it says would be reading a description.
            error_cause: cause_from_code(self.error_code.as_deref()),
            failure_payer: None,
            blocked_by: self.blocked_by,
            blocks: self.blocks,
            priority: self.priority,
            created_at: self.created_at,
            updated_at: self.state_at,
            agent_id: self.attempt_agent.or(self.agent_id),
            workspace_id: Some(self.workspace_id),
            retry_count: self.retry_count.max(0) as u32,
            max_retries: self.max_retries.max(0) as u32,
            retry_not_before: self.not_before,
            started_at: self.started_at,
            timeout_seconds: self.timeout_seconds.max(0) as u64,
            lease_owner: self.lease_owner,
            lease_expires_at: self.lease_expires_at,
            action_planner_meta,
            goal_id: self.goal_id,
            parent_task_id: self.parent_task_id,
            sink_task_id: self.sink_task_id,
            is_executable: self.is_executable,
            face_id: self.face_id,
            venue: self.venue,
            owner_unit: self.owner_unit,
            bound_by_handover_id: self.bound_by_handover_id,
            submitted_by: self.submitted_by,
            acceptance: self.acceptance,
        })
    }
}

/// The variant a planner source spelling names, or nothing.
fn planner_source_from_str(text: &str) -> Option<cog_core::ActionPlannerSource> {
    match text {
        "UserProvided" | "user_provided" => Some(cog_core::ActionPlannerSource::UserProvided),
        "Optimized" | "optimized" => Some(cog_core::ActionPlannerSource::Optimized),
        "Decomposed" | "decomposed" => Some(cog_core::ActionPlannerSource::Decomposed),
        _ => None,
    }
}

/// Everything a read of the ledger selects, and the only place that knows the
/// shape of the join.
///
/// `filter` is interpolated into the definition subquery rather than applied
/// outside it: the latest revision of a task is decided per task id, so a
/// filter that ran after the `DISTINCT ON` would first fold every revision in
/// the table and then throw almost all of them away.
pub(crate) fn ledger_sql(inner: &str, outer: &str) -> String {
    let outer = if outer.trim().is_empty() {
        String::new()
    } else {
        format!(" WHERE {outer}")
    };
    format!(
        r#"
SELECT d.workspace_id       AS workspace_id,
       d.task_id            AS task_id,
       d.kind               AS kind,
       d.input              AS input,
       d.created_at         AS created_at,
       d.priority           AS priority,
       d.max_retries        AS max_retries,
       d.timeout_seconds    AS timeout_seconds,
       d.is_executable      AS is_executable,
       d.acceptance         AS acceptance,
       d.blocked_by         AS blocked_by,
       d.agent_id           AS agent_id,
       d.goal_id            AS goal_id,
       d.parent_task_id     AS parent_task_id,
       d.sink_task_id       AS sink_task_id,
       d.face_id            AS face_id,
       d.venue              AS venue,
       d.owner_unit         AS owner_unit,
       d.bound_by_handover_id AS bound_by_handover_id,
       d.submitted_by       AS submitted_by,
       d.planner_verified   AS planner_verified,
       d.planner_version    AS planner_version,
       d.planner_note       AS planner_note,
       d.planner_source     AS planner_source,
       d.planner_confidence AS planner_confidence,
       d.planner_at         AS planner_at,
       t.to_state           AS to_state,
       t.at                 AS state_at,
       t.not_before         AS not_before,
       COALESCE(c.started_at, a.started_at)   AS started_at,
       c.lease_owner                          AS lease_owner,
       c.lease_expires_at                     AS lease_expires_at,
       COALESCE(c.retry_count, GREATEST(a.attempt_no - 1, 0)) AS retry_count,
       COALESCE(c.error_code, a.error_code)   AS error_code,
       COALESCE(c.error_text, a.error_text)   AS error_text,
       a.agent_id                             AS attempt_agent,
       a.result                               AS attempt_result,
       COALESCE(b.blocks, '{{}}'::text[])     AS blocks
  FROM (SELECT DISTINCT ON (task_id) * FROM task_defined WHERE {inner}
         ORDER BY task_id, rev DESC) d
  JOIN LATERAL (
        SELECT to_state, at, seq, not_before FROM task_transition
         WHERE task_id = d.task_id ORDER BY seq DESC LIMIT 1
       ) t ON TRUE
  LEFT JOIN task_current c
         ON c.task_id = d.task_id AND c.workspace_id = d.workspace_id
  LEFT JOIN LATERAL (
        SELECT attempt_no, started_at, owner, agent_id, outcome, error_code,
               error_text, result
          FROM task_attempt WHERE task_id = d.task_id
         ORDER BY attempt_no DESC LIMIT 1
       ) a ON TRUE
  LEFT JOIN LATERAL (
        SELECT array_agg(x.task_id ORDER BY x.task_id) AS blocks
          FROM (SELECT DISTINCT ON (task_id) task_id, blocked_by
                  FROM task_defined
                 WHERE blocked_by @> ARRAY[d.task_id]
                 ORDER BY task_id, rev DESC) x
         WHERE d.task_id = ANY(x.blocked_by)
       ) b ON TRUE{outer}
"#
    )
}

/// Which tasks name `$2` in their `blocked_by`, in workspace `$1`.
///
/// The same reverse read [`ledger_sql`] does for one task's `blocks` column,
/// asked on its own. It goes through the GIN index on `blocked_by`: reading the
/// whole definition table and testing containment in Rust would make the
/// dispatcher's dependency check cost the size of the ledger.
pub(crate) const DEPENDENTS_SQL: &str = r#"
SELECT x.task_id
  FROM (SELECT DISTINCT ON (task_id) task_id, workspace_id, blocked_by
          FROM task_defined
         WHERE blocked_by @> ARRAY[$2]
         ORDER BY task_id, rev DESC) x
 WHERE x.workspace_id = $1
 ORDER BY x.task_id
"#;

/// The projection, as one statement that either inserts a row or removes it.
///
/// Run for one task by the write path and for all of them by the rebuild, so
/// there is exactly one definition of what the projection means.
pub(crate) const PROJECT_SQL_TEMPLATE: &str = r#"
WITH latest AS (
    SELECT DISTINCT ON (task_id) task_id, workspace_id, rev FROM task_defined
     WHERE ($1::text IS NULL OR task_id = $1)
     ORDER BY task_id, rev DESC
), state AS (
    SELECT DISTINCT ON (task_id) task_id, to_state, seq, at, not_before
      FROM task_transition
     WHERE ($1::text IS NULL OR task_id = $1)
     ORDER BY task_id, seq DESC
), attempt AS (
    SELECT DISTINCT ON (task_id) task_id, attempt_no, started_at, owner,
           error_code, error_text, outcome
      FROM task_attempt
     WHERE ($1::text IS NULL OR task_id = $1)
     ORDER BY task_id, attempt_no DESC
), projected AS (
    SELECT l.workspace_id, l.task_id, s.to_state, s.seq, l.rev, s.at,
           COALESCE(a.attempt_no, 0) AS attempt_no,
           a.started_at,
           s.not_before,
           GREATEST(COALESCE(a.attempt_no, 0) - 1, 0) AS retry_count,
           a.error_code, a.error_text
      FROM latest l
      JOIN state s ON s.task_id = l.task_id
      LEFT JOIN attempt a ON a.task_id = l.task_id
     WHERE s.to_state IN ('{task_status:pending}', '{task_status:scheduled}',
                          '{task_status:running}')
)
INSERT INTO task_current (
    workspace_id, task_id, state, as_of_seq, def_rev, updated_at, attempt_no,
    started_at, retry_not_before, retry_count, error_code, error_text)
SELECT workspace_id, task_id, to_state, seq, rev, at, attempt_no, started_at,
       not_before, retry_count, error_code, error_text
  FROM projected
ON CONFLICT (workspace_id, task_id) DO UPDATE SET
    state = EXCLUDED.state,
    as_of_seq = EXCLUDED.as_of_seq,
    def_rev = EXCLUDED.def_rev,
    updated_at = EXCLUDED.updated_at,
    attempt_no = EXCLUDED.attempt_no,
    started_at = EXCLUDED.started_at,
    retry_not_before = EXCLUDED.retry_not_before,
    retry_count = EXCLUDED.retry_count,
    error_code = EXCLUDED.error_code,
    error_text = EXCLUDED.error_text
"#;

/// The other half of [`project_sql`]: a task that reached a terminal state
/// leaves the projection. Its outcome is the transition and the attempt that
/// closed it, and a terminal row sitting in the table the dispatcher locks is
/// the thing that made that table grow without bound.
pub(crate) const UNPROJECT_SQL_TEMPLATE: &str = r#"
DELETE FROM task_current c
 WHERE ($1::text IS NULL OR c.task_id = $1)
   AND NOT EXISTS (
        SELECT 1 FROM (SELECT DISTINCT ON (task_id) task_id, to_state
                         FROM task_transition
                        WHERE ($1::text IS NULL OR task_id = $1)
                        ORDER BY task_id, seq DESC) s
         WHERE s.task_id = c.task_id
           AND s.to_state IN ('{task_status:pending}', '{task_status:scheduled}',
                              '{task_status:running}'))
"#;

/// The states a projection row may be in: everything it is still possible to
/// leave. Written from the enum rather than copied beside the queries, because
/// the two copies agree until a variant is renamed, and then the statement
/// matches no rows -- which reads as "no task is waiting" to every caller.
pub(crate) fn live_status_slots() -> Vec<(&'static str, &'static str)> {
    TaskStatus::ALL
        .iter()
        .filter(|status| !status.is_terminal())
        .map(|status| ("task_status", status.as_str()))
        .collect()
}

/// [`PROJECT_SQL_TEMPLATE`] with the live states filled in.
pub(crate) fn project_sql() -> String {
    crate::postgres::fill_slots(PROJECT_SQL_TEMPLATE, &live_status_slots())
}

/// [`UNPROJECT_SQL_TEMPLATE`] with the live states filled in.
pub(crate) fn unproject_sql() -> String {
    crate::postgres::fill_slots(UNPROJECT_SQL_TEMPLATE, &live_status_slots())
}

/// Serialise the ledger write for one workspace.
///
/// Every write to a task takes this before it reads anything it then decides
/// on. The document shape got its serialisation from a row lock on the row
/// being rewritten; a ledger has no such row -- an insert does not block
/// another insert -- so the serialisation has to be named. Keyed on the
/// workspace rather than the task because the bulk paths (complete, fail,
/// cancel) decide for a whole workspace at once and a per-task key would let
/// them race the single-task writes.
pub(crate) async fn lock_workspace(
    conn: &mut PgConnection,
    workspace_id: &str,
) -> Result<(), SFError> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("task-ws:{workspace_id}"))
        .execute(conn)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;
    Ok(())
}

/// Append a definition revision if the definition changed, and answer the
/// revision the task now has.
///
/// "Changed" is every column this table holds, compared with the newest row.
/// An unchanged save is the common case -- a task moves through five states
/// without its definition moving once -- and writing a revision for each would
/// make the table's growth a function of state traffic instead of of
/// re-planning.
pub(crate) async fn save_definition(
    conn: &mut PgConnection,
    workspace_id: &str,
    task: &Task,
) -> Result<i64, SFError> {
    let kind = kind_of(&task.task_type);
    let input = serde_json::to_string(&task.input)?;
    let (rev, same): (i64, bool) = sqlx::query_as(
        r#"
        SELECT COALESCE(MAX(rev), 0),
               COALESCE(bool_or(
                   kind = $3 AND input = $4 AND created_at = $5
                   AND priority = $6 AND max_retries = $7
                   AND timeout_seconds = $8 AND is_executable = $9
                   AND acceptance = $10 AND blocked_by = $11
                   AND agent_id IS NOT DISTINCT FROM $12
                   AND goal_id IS NOT DISTINCT FROM $13
                   AND parent_task_id IS NOT DISTINCT FROM $14
                   AND sink_task_id IS NOT DISTINCT FROM $15
                   AND face_id IS NOT DISTINCT FROM $16
                   AND venue IS NOT DISTINCT FROM $17
                   AND owner_unit IS NOT DISTINCT FROM $18
                   AND bound_by_handover_id IS NOT DISTINCT FROM $19
                   AND submitted_by IS NOT DISTINCT FROM $20
                   AND planner_verified IS NOT DISTINCT FROM $21
                   AND planner_source IS NOT DISTINCT FROM $22
               ), FALSE)
          FROM task_defined
         WHERE task_id = $1 AND rev = (SELECT MAX(rev) FROM task_defined WHERE task_id = $1)
           AND workspace_id = $2
        "#,
    )
    .bind(&task.id)
    .bind(workspace_id)
    .bind(&kind)
    .bind(&input)
    .bind(task.created_at)
    .bind(task.priority)
    .bind(task.max_retries as i32)
    .bind(task.timeout_seconds as i64)
    .bind(task.is_executable)
    .bind(&task.acceptance)
    .bind(&task.blocked_by)
    .bind(&task.agent_id)
    .bind(&task.goal_id)
    .bind(&task.parent_task_id)
    .bind(&task.sink_task_id)
    .bind(&task.face_id)
    .bind(&task.venue)
    .bind(&task.owner_unit)
    .bind(&task.bound_by_handover_id)
    .bind(&task.submitted_by)
    .bind(task.action_planner_meta.as_ref().map(|m| m.verified))
    .bind(
        task.action_planner_meta
            .as_ref()
            .and_then(|m| m.source.as_ref())
            .map(|s| format!("{s:?}")),
    )
    .fetch_one(&mut *conn)
    .await
    .map_err(|e| SFError::Database(e.to_string()))?;

    if same && rev > 0 {
        return Ok(rev);
    }

    let meta = task.action_planner_meta.as_ref();
    sqlx::query(
        r#"
        INSERT INTO task_defined (
            task_id, rev, at, workspace_id, kind, input, created_at, priority,
            max_retries, timeout_seconds, is_executable, acceptance, blocked_by,
            agent_id, goal_id, parent_task_id, sink_task_id, face_id, venue,
            owner_unit, bound_by_handover_id, submitted_by, planner_verified,
            planner_version, planner_note, planner_source, planner_confidence,
            planner_at)
        VALUES ($1, $2, NOW(), $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24,
                $25, $26, $27)
        "#,
    )
    .bind(&task.id)
    .bind(rev + 1)
    .bind(workspace_id)
    .bind(&kind)
    .bind(&input)
    .bind(task.created_at)
    .bind(task.priority)
    .bind(task.max_retries as i32)
    .bind(task.timeout_seconds as i64)
    .bind(task.is_executable)
    .bind(&task.acceptance)
    .bind(&task.blocked_by)
    .bind(&task.agent_id)
    .bind(&task.goal_id)
    .bind(&task.parent_task_id)
    .bind(&task.sink_task_id)
    .bind(&task.face_id)
    .bind(&task.venue)
    .bind(&task.owner_unit)
    .bind(&task.bound_by_handover_id)
    .bind(&task.submitted_by)
    .bind(meta.map(|m| m.verified))
    .bind(meta.and_then(|m| m.version.clone()))
    .bind(meta.and_then(|m| m.note.clone()))
    .bind(meta.and_then(|m| m.source.as_ref().map(|s| format!("{s:?}"))))
    .bind(meta.and_then(|m| m.confidence))
    .bind(meta.and_then(|m| m.timestamp))
    .execute(&mut *conn)
    .await
    .map_err(|e| SFError::Database(e.to_string()))?;
    Ok(rev + 1)
}

/// One state transition the writer has in hand.
///
/// A struct rather than eight positional arguments: every field is a short
/// word, and a call that passed `cause` where `actor` belonged would insert a
/// row that reads as a different act without failing anything.
pub(crate) struct Transition<'a> {
    pub task_id: &'a str,
    pub from: Option<TaskStatus>,
    pub to: TaskStatus,
    pub actor: &'a str,
    pub cause: &'a str,
    pub detail: Option<&'a str>,
    pub not_before: Option<DateTime<Utc>>,
}

/// Append one state transition and answer the sequence it took.
///
/// The caller has already checked that the row is in one of the states this
/// move accepts; [`lock_workspace`] is what makes that check still true by the
/// time this runs.
pub(crate) async fn append_transition(
    conn: &mut PgConnection,
    move_: &Transition<'_>,
) -> Result<i64, SFError> {
    let (seq,): (i64,) = sqlx::query_as(
        r#"
        INSERT INTO task_transition (task_id, seq, at, from_state, to_state,
                                     actor, cause, detail, not_before)
        SELECT $1,
               COALESCE((SELECT MAX(seq) FROM task_transition WHERE task_id = $1), 0) + 1,
               NOW(), $2, $3, $4, $5, $6, $7
        RETURNING seq
        "#,
    )
    .bind(move_.task_id)
    .bind(move_.from.map(|s| s.as_str()))
    .bind(move_.to.as_str())
    .bind(move_.actor)
    .bind(move_.cause)
    .bind(move_.detail)
    .bind(move_.not_before)
    .fetch_one(&mut *conn)
    .await
    .map_err(|e| SFError::Database(e.to_string()))?;
    Ok(seq)
}

/// The state the last transition left the task in, if the task has one.
pub(crate) async fn current_state(
    conn: &mut PgConnection,
    task_id: &str,
) -> Result<Option<TaskStatus>, SFError> {
    let row: Option<(String,)> = sqlx::query_as(
        "SELECT to_state FROM task_transition WHERE task_id = $1 ORDER BY seq DESC LIMIT 1",
    )
    .bind(task_id)
    .fetch_optional(&mut *conn)
    .await
    .map_err(|e| SFError::Database(e.to_string()))?;
    Ok(row.and_then(|(s,)| TaskStatus::parse(&s)))
}

/// Open the attempt a move into `running` starts.
pub(crate) async fn open_attempt(
    conn: &mut PgConnection,
    task_id: &str,
    owner: Option<&str>,
) -> Result<i32, SFError> {
    let (no,): (i32,) = sqlx::query_as(
        r#"
        INSERT INTO task_attempt (task_id, attempt_no, started_at, owner)
        SELECT $1,
               COALESCE((SELECT MAX(attempt_no) FROM task_attempt WHERE task_id = $1), 0) + 1,
               NOW(), $2
        RETURNING attempt_no
        "#,
    )
    .bind(task_id)
    .bind(owner)
    .fetch_one(&mut *conn)
    .await
    .map_err(|e| SFError::Database(e.to_string()))?;
    Ok(no)
}

/// Close the attempt that is still open, if there is one.
///
/// The outcome is the state the task moved to, so a reader that counts
/// outcomes never has to join back to the transition table to learn what the
/// attempt produced.
pub(crate) async fn close_attempt(
    conn: &mut PgConnection,
    task_id: &str,
    outcome: TaskStatus,
    error_code: Option<&str>,
    error_text: Option<&str>,
    result: Option<&serde_json::Value>,
    agent_id: Option<&str>,
) -> Result<(), SFError> {
    let result = match result {
        Some(value) => Some(serde_json::to_string(value)?),
        None => None,
    };
    sqlx::query(
        r#"
        UPDATE task_attempt
           SET ended_at = NOW(),
               outcome = $2,
               error_code = COALESCE($3, error_code),
               error_text = COALESCE($4, error_text),
               result = COALESCE($5, result),
               agent_id = COALESCE($6, agent_id)
         WHERE task_id = $1 AND attempt_no = (
               SELECT MAX(attempt_no) FROM task_attempt WHERE task_id = $1)
           AND ended_at IS NULL
        "#,
    )
    .bind(task_id)
    .bind(outcome.as_str())
    .bind(error_code)
    .bind(error_text)
    .bind(result)
    .bind(agent_id)
    .execute(&mut *conn)
    .await
    .map_err(|e| SFError::Database(e.to_string()))?;
    Ok(())
}

/// Record a failure as a closed attempt without starting the next one.
///
/// Used by the retry path: the attempt that failed ends with the error, and the
/// attempt that follows is opened when the task moves back to `running`. Keeping
/// them separate is what makes "how many attempts did this task take" a count
/// rather than a field that has to be incremented in step with everything else.
pub(crate) async fn record_failed_attempt(
    conn: &mut PgConnection,
    task_id: &str,
    error_code: Option<&str>,
    error_text: &str,
) -> Result<(), SFError> {
    let open = sqlx::query(
        r#"
        UPDATE task_attempt
           SET ended_at = NOW(), outcome = $2, error_code = $3,
               error_text = $4
         WHERE task_id = $1 AND attempt_no = (
               SELECT MAX(attempt_no) FROM task_attempt WHERE task_id = $1)
           AND ended_at IS NULL
        "#,
    )
    .bind(task_id)
    .bind(TaskStatus::Failed.as_str())
    .bind(error_code)
    .bind(error_text)
    .execute(&mut *conn)
    .await
    .map_err(|e| SFError::Database(e.to_string()))?;

    if open.rows_affected() == 0 {
        // No attempt was open: a failure that arrived for a task nobody opened
        // an attempt for is still a fact about the task, so it gets a row of
        // its own rather than disappearing.
        let (no,): (i32,) = sqlx::query_as(
            r#"
            INSERT INTO task_attempt (task_id, attempt_no, started_at, ended_at,
                                      outcome, error_code, error_text)
            SELECT $1,
                   COALESCE((SELECT MAX(attempt_no) FROM task_attempt WHERE task_id = $1), 0) + 1,
                   NOW(), NOW(), $2, $3, $4
            RETURNING attempt_no
            "#,
        )
        .bind(task_id)
        .bind(TaskStatus::Failed.as_str())
        .bind(error_code)
        .bind(error_text)
        .fetch_one(&mut *conn)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;
        let _ = no;
    }
    Ok(())
}

/// Rewrite one task's projection row from the ledger.
pub(crate) async fn project(conn: &mut PgConnection, task_id: Option<&str>) -> Result<(), SFError> {
    sqlx::query(&project_sql())
        .bind(task_id)
        .execute(&mut *conn)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;
    sqlx::query(&unproject_sql())
        .bind(task_id)
        .execute(&mut *conn)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;
    Ok(())
}

/// Write one whole task: its definition, its state if that moved, and its
/// projection row.
///
/// The one place that spells "a task was written", shared by the state backend
/// and the governance store. Two copies would be two answers to what a write
/// means -- and the governance rows are tasks too, so a copy that forgot to
/// open an attempt would make their durations unmeasurable while every other
/// task's were fine.
pub(crate) async fn write_task(
    tx: &mut PgConnection,
    workspace_id: &str,
    task: &Task,
) -> Result<(), SFError> {
    lock_workspace(tx, workspace_id).await?;
    save_definition(tx, workspace_id, task).await?;

    let current = current_state(tx, &task.id).await?;
    if current != Some(task.status) {
        match (current, task.status) {
            (_, TaskStatus::Running) => {
                open_attempt(tx, &task.id, task.lease_owner.as_deref()).await?;
            }
            (Some(TaskStatus::Running), landed) => {
                close_attempt(
                    tx,
                    &task.id,
                    landed,
                    None,
                    task.error.as_deref(),
                    task.result.as_ref(),
                    task.agent_id.as_deref(),
                )
                .await?;
            }
            _ => {}
        }
        append_transition(
            tx,
            &Transition {
                task_id: &task.id,
                from: current,
                to: task.status,
                actor: "store",
                cause: "set_task",
                detail: None,
                not_before: task.retry_not_before,
            },
        )
        .await?;
    }

    project(tx, Some(&task.id)).await?;
    // Presence is not derivable from history and a rebuild is right to drop
    // it, so the write path is the only place it is set.
    sqlx::query(
        "UPDATE task_current SET lease_owner = $2, lease_expires_at = $3 WHERE task_id = $1",
    )
    .bind(&task.id)
    .bind(&task.lease_owner)
    .bind(task.lease_expires_at)
    .execute(&mut *tx)
    .await
    .map_err(|e| SFError::Database(e.to_string()))?;
    Ok(())
}

/// Rebuild the projection from the ledger, for every task or for one.
///
/// This is the command the projection's claim of being rebuildable rests on.
/// It runs the same statements the write path runs, so a rebuild cannot drift
/// from what the writes mean.
pub(crate) async fn rebuild(pool: &sqlx::PgPool, task_id: Option<&str>) -> Result<u64, SFError> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e: sqlx::Error| SFError::Database(e.to_string()))?;
    // 先投影再撤回：两半合起来才是「投影恰好是账本上还活着的那批任务」。
    // 只跑前半条，一个已经走到终态、却被某个旧写入留了行的任务会在重建之后
    // 继续待在派发要锁的那张表里。
    project(&mut tx, task_id).await?;
    sqlx::query(&unproject_sql())
        .bind(task_id)
        .execute(&mut *tx)
        .await
        .map_err(|e: sqlx::Error| SFError::Database(e.to_string()))?;
    let done: i64 = sqlx::query_scalar("SELECT count(*) FROM task_current")
        .fetch_one(&mut *tx)
        .await
        .map_err(|e: sqlx::Error| SFError::Database(e.to_string()))?;
    tx.commit()
        .await
        .map_err(|e: sqlx::Error| SFError::Database(e.to_string()))?;
    Ok(done as u64)
}
