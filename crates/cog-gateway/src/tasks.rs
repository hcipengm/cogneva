//! principle: feedback_a_claim_checkable_at_write_time_must_be_checked_there

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::{error::ApiError, GatewayState};
use cog_core::{AgentEvent, Task, TaskFilter, TaskType};
use std::collections::HashSet;

/// Put a task's new state on the bus.
///
/// Takes the state itself, not a word for it: the frame crosses to readers in
/// other crates and to the browser, and they match on [`TaskStatus::as_str`]'s
/// spelling -- the level filter marks `failed` as an error, the comms panel
/// folds `completed`/`failed` into its edges. A literal here was spelled the
/// other way round, so every one of those comparisons was quietly false and
/// each of them read as "nothing failed" rather than as a mismatch.
fn broadcast_task_status(
    state: &GatewayState,
    task_id: &str,
    status: cog_core::TaskStatus,
    agent_id: Option<String>,
) {
    let _ = state
        .event_tx
        .send(task_status_frame(task_id, status, agent_id));
}

/// The frame a transition puts on the bus.
///
/// Split out from the send so the word it carries can be judged without a
/// gateway state behind it.
fn task_status_frame(
    task_id: &str,
    status: cog_core::TaskStatus,
    agent_id: Option<String>,
) -> AgentEvent {
    AgentEvent::TaskStatusChange {
        task_id: task_id.into(),
        status: status.as_str().into(),
        agent_id,
        crew_id: None,
        squad_id: None,
        timestamp: chrono::Utc::now(),
    }
}

fn parse_task_type(s: &str) -> TaskType {
    match s {
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
        _ => TaskType::Custom(s.into()),
    }
}

pub(crate) fn task_type_str(tt: &TaskType) -> String {
    match tt {
        TaskType::Planner => cog_core::roles::PLANNER.into(),
        TaskType::Generator => cog_core::roles::GENERATOR.into(),
        TaskType::Evaluator => cog_core::roles::EVALUATOR.into(),
        TaskType::Reviewer => "reviewer".into(),
        TaskType::LlmCall => "llm_call".into(),
        TaskType::ToolCall => "tool_call".into(),
        TaskType::FileOp => "file_op".into(),
        TaskType::DbTransaction => "db_transaction".into(),
        TaskType::NetworkRequest => "network_request".into(),
        TaskType::WasmSkill => "wasm_skill".into(),
        TaskType::Skill => "skill".into(),
        TaskType::DagNode => "dag_node".into(),
        TaskType::Custom(s) => s.clone(),
    }
}

async fn record_task_op(_state: &GatewayState, _op: &str) {
    // MetricsExporter currently only exposes encode(); counter recording is
    // deferred until the exporter interface is extended.
}

// ─── Request / Response DTOs ─────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct CreateTaskRequest {
    #[serde(default)]
    pub goal_id: Option<String>,
    /// The work to do: a short title line, then any details.
    ///
    /// The first line becomes the landing commit's subject, so it has to read
    /// as a title -- one line, at most 50 characters, ending on a word
    /// boundary, and carrying whatever tells this work apart from the changes
    /// before it. A blank line and the details follow; the landing writes the
    /// whole goal into the commit body verbatim. A goal submitted as one long
    /// sentence still lands, but its header has to be shortened, and the part
    /// that did not fit is only a scroll below rather than on the first line.
    pub goal: String,
    #[serde(default)]
    pub tasks: Option<Vec<TaskItem>>,
    #[serde(default)]
    pub workspace_id: String,
    pub priority: Option<i32>,
    /// What this submission is: a goal to be planned, or a task to be carried
    /// out as given. It is declared by the submitter, never inferred from who
    /// sent it -- the owner's own input takes this same field with the same
    /// meaning.
    #[serde(default)]
    pub intent_kind: IntentKind,
}

/// The two input forms. A `Goal` is decomposed: the platform decides which
/// steps it consists of. A `Task` is submitted as already decided, and is
/// injected as-is only when the submitter also marks it verified -- an
/// unverified task still goes through planning, because "here is a task" and
/// "here is a task that is right" are different claims.
#[derive(Debug, Deserialize, Default, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IntentKind {
    #[default]
    Goal,
    Task,
}

#[derive(Debug, Deserialize)]
pub struct TaskItem {
    pub id: String,
    /// Task type. Use "self_evolution" to trigger the collaborative change
    /// generation and auto-deploy pipeline.
    pub task_type: String,
    pub input: serde_json::Value,
    #[serde(default)]
    pub blocked_by: Vec<String>,
    #[serde(default)]
    pub priority: i32,
    /// The submitter's claim that this row needs no planning. Honoured only
    /// for `intent_kind=task`; a goal submission is decomposed even if a row
    /// arrives carrying it.
    #[serde(default)]
    pub planner: Option<PlannerClaim>,
    /// The executor this row is declared for. Recorded on the task; routing it
    /// to exactly that executor is a separate step.
    #[serde(default)]
    pub agent_id: Option<String>,
    /// The face this row serves. Naming a face binds the row to that face's
    /// owner, so the row is checked against the roster before it is queued
    /// rather than reported as drifted after it runs.
    #[serde(default)]
    pub face_id: Option<String>,
    /// The unit the submitter says will do the work. Allowed only together
    /// with a face and only when it agrees with the face's owner.
    #[serde(default)]
    pub unit_id: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct PlannerClaim {
    #[serde(default)]
    pub verified: bool,
    /// `user_provided` | `optimized` | `decomposed`; anything else falls back
    /// to `user_provided`, which is what an API submission is.
    #[serde(default)]
    pub source: Option<String>,
}

fn parse_planner_source(s: Option<&str>) -> cog_core::ActionPlannerSource {
    match s.unwrap_or("user_provided") {
        "optimized" => cog_core::ActionPlannerSource::Optimized,
        "decomposed" => cog_core::ActionPlannerSource::Decomposed,
        _ => cog_core::ActionPlannerSource::UserProvided,
    }
}

/// Credential-shaped key names an input may not carry. The task graph is
/// readable by every route that lists tasks, and a credential pasted into an
/// input is a credential written to Postgres and to every log that records the
/// input. A reference (`{"credential": "llm-upstream"}`) is the supported form;
/// the values themselves live in the security gateway.
const CREDENTIAL_KEY_SUFFIXES: [&str; 7] = [
    "token",
    "secret",
    "password",
    "passwd",
    "apikey",
    "privatekey",
    "accesskey",
];
const CREDENTIAL_KEY_EXACT: [&str; 3] = ["key", "credentials", "authorization"];

/// Return the first credential-shaped key path in `input`, if any.
///
/// Matching is on the normalised key (lower-cased, separators removed) and by
/// suffix, so `github_token`, `client-secret` and `password` are caught while
/// `token_budget` and `max_tokens` -- which describe a quantity, not a secret
/// -- are not. Key *names* only: a value is not sniffed for looking like a
/// credential, because that would reject honest payloads and still miss a
/// renamed one.
pub(crate) fn credential_key_in_input(input: &serde_json::Value) -> Option<String> {
    fn walk(value: &serde_json::Value, path: &str) -> Option<String> {
        match value {
            serde_json::Value::Object(map) => {
                for (k, v) in map {
                    let norm: String = k
                        .chars()
                        .filter(|c| !matches!(c, '_' | '-' | '.' | ' '))
                        .flat_map(|c| c.to_lowercase())
                        .collect();
                    if CREDENTIAL_KEY_EXACT.contains(&norm.as_str())
                        || CREDENTIAL_KEY_SUFFIXES.iter().any(|s| norm.ends_with(s))
                    {
                        return Some(if path.is_empty() {
                            k.clone()
                        } else {
                            format!("{path}.{k}")
                        });
                    }
                    let next = if path.is_empty() {
                        k.clone()
                    } else {
                        format!("{path}.{k}")
                    };
                    if let Some(found) = walk(v, &next) {
                        return Some(found);
                    }
                }
                None
            }
            serde_json::Value::Array(items) => items
                .iter()
                .enumerate()
                .find_map(|(i, v)| walk(v, &format!("{path}[{i}]"))),
            _ => None,
        }
    }
    walk(input, "")
}

#[derive(Debug, Serialize)]
pub struct CreateTaskResponse {
    pub goal: String,
    pub task_count: usize,
    pub task_ids: Vec<String>,
    pub message_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CompleteTaskRequest {
    pub result: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct FailTaskRequest {
    pub error: String,
}

#[derive(Debug, Serialize)]
pub struct TaskView {
    pub id: String,
    pub task_type: String,
    /// 状态词，取自 `TaskStatus::as_str`：行里存的、总线上的、客户端比的是同一个拼法。
    /// 读面曾经印 `Debug` 那一种拼法，于是「完成」「失败」这两种读法在客户端并存，
    /// 谁在哪一边比较，谁就只认得自己那一种。
    pub status: String,
    pub input: serde_json::Value,
    pub result: Option<serde_json::Value>,
    pub error: Option<String>,
    pub blocked_by: Vec<String>,
    pub priority: i32,
    pub retry_count: u32,
    pub max_retries: u32,
    pub agent_id: Option<String>,
    /// 这道任务指定由谁执行（`owner_unit`）。与 `agent_id` 不是同一列：前者是
    /// 「谁被指定来做」，后者是「结果是谁交的」。带归属的任务不进共享就绪队列，
    /// 只等它点名的那个 unit 来开工，所以这一列是读侧唯一能看出「这道活已经有人
    /// 认领、别人抢不到」的地方——不印出来，认领成功的响应与没认领过一模一样。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner_unit: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    /// 所属 goal。提交时网关生成（未带则用 UUID），响应里以 `message_id` 交回；同一 goal
    /// 下分解出的子任务带同一个值。以前这个字段只在写侧存在，读侧看不到，于是「按下发的
    /// goal 找回它派生的那批任务」这条路尽管设计过也从没接通。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub goal_id: Option<String>,
    /// 父任务 ID。分解出的原子任务指向被它替代的那条原始任务。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_task_id: Option<String>,
    /// 这条目标行的答案落在哪条任务上。只有不可执行的占位行会带它；分解没给出恰好一条
    /// 收口任务时整份提交判退，所以它要么缺席，要么指向唯一的那条。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sink_task_id: Option<String>,
    /// 能否被 DagExecutor 调度。原始任务被重新注入成占位时是 false——它只做层级与查询，
    /// 不能被当成「还没跑的活」。
    pub is_executable: bool,
    /// 这道任务解锁了谁。与 `blocked_by` 是同一条边的两个方向，读的人不必为了
    /// 「我卡住了谁」再发一次请求。
    #[serde(default)]
    pub blocks: Vec<String>,
    /// 失败原因的类型（来自传输层的 HTTP 状态），与给人看的 `error` 文本分开。
    /// 读面丢掉它就只剩措辞：同一句「请求失败」可能该等、该换 key、该改请求。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_cause: Option<cog_core::contract::llm::UpstreamFailure>,
    /// 执行权持有者与租约到期时刻。状态是 Running 而租约已过期，说明持有它的进程
    /// 已经不在了——这是从行里唯一能读出来的「孤儿」证据。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lease_owner: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lease_expires_at: Option<String>,
    /// 失败后的退避到期时刻：早于此刻不会重试。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_not_before: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    pub timeout_seconds: u64,
    /// 服务哪个面（面节点的这一列指向自己）。`None` 是不服务任何面，不是默认面。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub face_id: Option<String>,
    /// 由哪张交接单签来的归属。与 `owner_unit` 一起读，才知道归属是改派还是漂移。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bound_by_handover_id: Option<String>,
}

impl From<Task> for TaskView {
    fn from(t: Task) -> Self {
        Self {
            id: t.id,
            task_type: task_type_str(&t.task_type),
            status: t.status.as_str().to_string(),
            input: t.input,
            result: t.result,
            error: t.error,
            blocked_by: t.blocked_by,
            priority: t.priority,
            retry_count: t.retry_count,
            max_retries: t.max_retries,
            agent_id: t.agent_id,
            owner_unit: t.owner_unit,
            created_at: t.created_at.to_rfc3339(),
            updated_at: t.updated_at.to_rfc3339(),
            goal_id: t.goal_id,
            parent_task_id: t.parent_task_id,
            sink_task_id: t.sink_task_id,
            is_executable: t.is_executable,
            blocks: t.blocks,
            error_cause: t.error_cause,
            lease_owner: t.lease_owner,
            lease_expires_at: t.lease_expires_at.map(|at| at.to_rfc3339()),
            retry_not_before: t.retry_not_before.map(|at| at.to_rfc3339()),
            started_at: t.started_at.map(|at| at.to_rfc3339()),
            timeout_seconds: t.timeout_seconds,
            face_id: t.face_id,
            bound_by_handover_id: t.bound_by_handover_id,
        }
    }
}

/// `/api/v1/tasks/list` 与 `/api/v1/tasks/graph` 的过滤参数。不带 query 时与从前逐字相同。
#[derive(Debug, Deserialize, Default)]
pub struct TaskQuery {
    /// 只看这一个 goal 下的任务。值就是提交响应里的 `message_id`；占位任务与它分解出的
    /// 子任务带同一个值，所以按它过滤能拿到一次提交的全貌。
    pub goal_id: Option<String>,
    /// 只看这个执行者手上的（或声明给它的）任务。
    pub agent_id: Option<String>,
    /// 只看这一类任务：一个状态词，或就绪队列（`ready`，队列本身，不是某行携带的状态）。
    /// 大小写不敏感，取值就是 [`TaskFilter::ALL`] 印出来的那些词。
    pub status: Option<String>,
}

/// 一条任务是否属于被查询的 goal。没有 goal 条件时一律留下（既有行为不变）。
fn in_goal(query: &TaskQuery, task: &Task) -> bool {
    match &query.goal_id {
        Some(want) => task.goal_id.as_deref() == Some(want.as_str()),
        None => true,
    }
}

/// The goal and executor half of the filter. Separate from [`matches_status`]
/// because the tests that pin "no goal means everything" are about that one
/// dimension, and folding the state into it would let a change to either
/// dimension turn them red for an unrelated reason.
fn matches_query(query: &TaskQuery, task: &Task) -> bool {
    in_goal(query, task)
        && query
            .agent_id
            .as_deref()
            .is_none_or(|want| task.agent_id.as_deref() == Some(want))
}

/// The state half of the filter, read out of [`TaskFilter`] rather than out of
/// whatever the caller typed.
///
/// An unknown word here would otherwise select nothing, and an empty body is
/// what an empty state returns too -- a tab whose word no row carries would
/// report "nothing waiting" instead of "that is not a word I know". So the
/// caller is told what it can ask for.
fn status_filter(query: &TaskQuery) -> Result<Option<TaskFilter>, ApiError> {
    match query.status.as_deref() {
        None => Ok(None),
        Some(word) => TaskFilter::parse(word).map(Some).ok_or_else(|| {
            ApiError::bad_request(format!(
                "unknown status '{word}': expected one of {}",
                TaskFilter::ALL
                    .iter()
                    .map(TaskFilter::as_str)
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        }),
    }
}

/// Whether `task` is in the state the query asked for.
///
/// `Ready` is the queue, not a state: it matches the ids the orchestrator's own
/// ready predicate returned, so the tab and the count above it cannot disagree
/// about what "ready" means -- a second definition here would have them drift
/// the moment one side changed.
fn matches_status(
    filter: Option<TaskFilter>,
    ready_ids: Option<&HashSet<String>>,
    task: &Task,
) -> bool {
    match filter {
        None => true,
        Some(TaskFilter::Status(want)) => task.status == want,
        Some(TaskFilter::Ready) => ready_ids.is_some_and(|ids| ids.contains(task.id.as_str())),
    }
}

/// The ids the ready queue holds, when that is what was asked for.
///
/// Read once per request from the orchestrator, not recomputed per row: the
/// predicate walks the whole graph (it needs the blockers' states), and doing
/// that once per candidate row would turn a list of a few hundred tasks into a
/// few hundred graph walks.
async fn ready_ids(
    state: &Arc<GatewayState>,
    filter: Option<TaskFilter>,
) -> Option<HashSet<String>> {
    match filter {
        Some(TaskFilter::Ready) => Some(
            state
                .orchestrator
                .get_ready_tasks()
                .await
                .into_iter()
                .map(|task| task.id)
                .collect(),
        ),
        _ => None,
    }
}

/// Whether a reader holding `claims` may see `task`.
///
/// An empty scope is the same convention the submission side already uses (a
/// token that declares no workspaces is not restricted by them), so unscoped
/// platform operators still see the whole graph.
///
/// A task that names no workspace is **not** visible to a scoped reader. That
/// is the opposite of what this used to do, and the reversal is the point: a
/// row with no owner cannot be shown to one tenant without showing it to every
/// tenant, so "nobody owns it" has to read as "only unowned callers see it".
/// The rows written before the field existed do not disappear from the
/// platform -- the unscoped readers still fold them in -- but they stop
/// appearing inside a tenant's view, where nothing could tell them apart from
/// that tenant's own work.
pub(crate) fn visible_to_claims(claims: Option<&cog_core::Claims>, task: &Task) -> bool {
    let Some(claims) = claims else { return true };
    if claims.workspace_ids.is_empty() {
        return true;
    }
    match task.workspace_id.as_deref() {
        Some(ws) => claims.workspace_ids.iter().any(|held| held == ws),
        None => false,
    }
}

#[derive(Debug, Serialize)]
pub struct TaskSummary {
    pub total: usize,
    pub ready: usize,
    pub running: usize,
    pub completed: usize,
    pub failed: usize,
    pub all_completed: bool,
}

// ─── Handlers ────────────────────────────────────────────────────────

/// 估算一次 goal 提交的 token 规模：goal 文本 + 各 task input 序列化长度 / 4。
fn estimate_goal_tokens(req: &CreateTaskRequest) -> u64 {
    let mut chars = req.goal.len();
    if let Some(ref items) = req.tasks {
        for item in items {
            chars += item.input.to_string().len();
        }
    }
    ((chars / 4) as u64).max(1)
}

/// 把一次提交展开成进 DAG 的初始任务。
///
/// 显式给出的任务带着客户端的 `goal_id` 原样下去。**裸 goal（没有任何可执行行）也必须
/// 有一条承载 `goal_id` 的行**：分解在协调器里是从 `tasks.first()` 取 goal_id 的，一个空的
/// `tasks` 会让它当场另生成一个 uuid，客户端拿到的 goal_id 就永远匹配不到任何行——
/// `?goal_id=` 恒空，那条「按下发的 goal 找回它派生的那批任务」的路等于没建。
///
/// 判据是「展开后一条可执行行都没有」，**不是**「`tasks` 字段缺席」：显式 `tasks: []` 与
/// 缺省一样落到这里——两者都表示「这是一次要平台自己规划的裸 goal」，不能一条行都不给。
///
/// 合成的这条 goal 行（id 与 goal_id 都取客户端那一个）交下去后，由协调器存成占位行
/// （分解成功）或 Failed 行（分解没产出），返回值本身因此就是可寻址的那一行。
/// 它设 `is_executable=false`：它是这次目标的标记，不是活——不接 planner 的回退路径
/// 也不该把它当任务调度出去（协调器组 hints 时按这个键把它排除在规划输入之外）。
///
/// 行本身由 [`Task::goal_row`] 一处构造：接口这条路要带客户端的 id，所以在这里合成；
/// 一条行都不带的调用方（例如聊天入口）由分解侧补出同样的行。
/// What a submitted row asked to be bound to, or why the two claims cannot
/// both be true.
///
/// A face's owner is the answer to "who does this face's work", so a row that
/// names a face and no unit is bound to that owner instead of being left free:
/// a face-bound row with no owner reads as drifted from the moment it is
/// queued. Naming both is allowed only when they agree -- accepting a mismatch
/// here and letting the drift pass report it later is a row written with an
/// answer the writer already knew to be wrong.
fn binding_of(
    item: &TaskItem,
    face: Option<&cog_core::Face>,
) -> Result<(Option<String>, Option<String>), String> {
    match (item.face_id.as_deref(), item.unit_id.as_deref()) {
        (None, None) => Ok((None, None)),
        (None, Some(unit)) => Err(format!(
            "task {} names unit '{unit}' but no face; ownership is checked against the face the \
             work serves",
            item.id
        )),
        (Some(face_id), _) => {
            let Some(face) = face else {
                return Err(format!(
                    "task {} names face '{face_id}', which is not on the roster",
                    item.id
                ));
            };
            match (face.owner_unit.as_deref(), item.unit_id.as_deref()) {
                (None, _) => Err(format!(
                    "face '{face_id}' has no owner; a task cannot be bound to it until it is \
                     staffed"
                )),
                (Some(owner), Some(unit)) if owner != unit => Err(format!(
                    "task {} names unit '{unit}' but face '{face_id}' is owned by '{owner}'",
                    item.id
                )),
                (Some(owner), _) => Ok((Some(face_id.to_string()), Some(owner.to_string()))),
            }
        }
    }
}

/// Whether this row is injected as it was submitted, and so can carry a
/// binding through to the graph.
///
/// Only a verified `intent_kind=task` row is. A goal's rows, and an unverified
/// task's rows, are hints to the planner; the rows that run are the planner's,
/// and they know nothing about a face the submitter named.
fn binding_carrier(item: &TaskItem, intent_kind: IntentKind) -> bool {
    intent_kind == IntentKind::Task && item.planner.as_ref().is_some_and(|claim| claim.verified)
}

/// Check every row that names a face or a unit against the roster, and fill in
/// the owner a face-bound row would otherwise be queued without.
///
/// A binding is accepted only on a row that will be injected as submitted.
/// Every other row is a hint to the planner and comes back as a different
/// row, so a face and owner written on it would be dropped without a word --
/// the caller would believe it bound work to a face that never received any.
async fn resolve_bindings(
    state: &GatewayState,
    items: &mut [TaskItem],
    intent_kind: IntentKind,
) -> Result<(), ApiError> {
    if !items
        .iter()
        .any(|item| item.face_id.is_some() || item.unit_id.is_some())
    {
        return Ok(());
    }
    for item in items.iter() {
        if !binding_carrier(item, intent_kind) {
            return Err(ApiError::bad_request(format!(
                "task {} names a face or a unit, but a row that goes through planning is \
                 replaced by what the planner returns; submit it as intent_kind=task with \
                 planner.verified, or leave the binding off",
                item.id
            )));
        }
    }
    let store = state.governance_store.as_ref().ok_or_else(|| {
        ApiError::service_unavailable(
            "no shared database is published, so a submitted task cannot be checked \
                 against the face it names",
        )
    })?;
    let nodes = store.face_nodes().await?;
    let faces: std::collections::HashMap<String, cog_core::Face> = nodes
        .iter()
        .filter_map(cog_core::Face::from_task)
        .map(|face| (face.face_id.clone(), face))
        .collect();
    for item in items.iter_mut() {
        let face = item.face_id.as_deref().and_then(|id| faces.get(id));
        let (face_id, unit) = binding_of(item, face).map_err(ApiError::owner_mismatch)?;
        item.face_id = face_id;
        item.unit_id = unit;
    }
    Ok(())
}

fn submitted_tasks(req: &CreateTaskRequest, goal_id: &str) -> Vec<Task> {
    let mut rows: Vec<Task> = req
        .tasks
        .as_ref()
        .map(|items| {
            items
                .iter()
                .map(|item| {
                    let mut task = Task::new(
                        item.id.clone(),
                        parse_task_type(&item.task_type),
                        item.input.clone(),
                    );
                    task.blocked_by = item.blocked_by.clone();
                    task.priority = item.priority;
                    task.workspace_id = Some(req.workspace_id.clone());
                    task.goal_id = Some(goal_id.to_string());
                    task.agent_id = item.agent_id.clone();
                    task.face_id = item.face_id.clone();
                    task.owner_unit = item.unit_id.clone();
                    // Only a submission that says it is a task can carry the
                    // "no planning needed" claim. A goal is decomposed by
                    // definition; letting a verified flag on one of its rows
                    // skip that would make the branch depend on a marker
                    // rather than on the input's form.
                    if req.intent_kind == IntentKind::Task {
                        if let Some(claim) = item.planner.as_ref().filter(|c| c.verified) {
                            task.action_planner_meta = Some(cog_core::ActionPlannerMeta {
                                verified: true,
                                version: None,
                                note: Some(
                                    "submitted as a verified task; injected without decomposition"
                                        .into(),
                                ),
                                source: Some(parse_planner_source(claim.source.as_deref())),
                                confidence: None,
                                timestamp: Some(chrono::Utc::now()),
                            });
                        }
                    }
                    task
                })
                .collect()
        })
        .unwrap_or_default();
    if rows.is_empty() {
        let mut goal_row = Task::goal_row(goal_id, &req.goal);
        goal_row.workspace_id = Some(req.workspace_id.clone());
        rows.push(goal_row);
    }
    rows
}

pub async fn create_task_handler(
    State(state): State<Arc<GatewayState>>,
    claims: Option<axum::Extension<cog_core::Claims>>,
    Json(req): Json<CreateTaskRequest>,
) -> Result<(StatusCode, Json<CreateTaskResponse>), ApiError> {
    let goal_id = req
        .goal_id
        .clone()
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    // 审计 3.4：workspace 级资源隔离 —— 认证用户只能向自己所属的 workspace 提交。
    if let Some(axum::Extension(c)) = claims.as_ref() {
        if !req.workspace_id.is_empty()
            && !c.workspace_ids.is_empty()
            && !c.workspace_ids.contains(&req.workspace_id)
        {
            if let Some(ref stream) = state.audit_stream {
                let _ = stream
                    .append(
                        cog_core::AuditKind::Authz,
                        &c.sub,
                        &req.workspace_id,
                        "authz.workspace_denied",
                        serde_json::json!({ "goal_id": goal_id }),
                    )
                    .await;
            }
            return Err(ApiError::forbidden(format!(
                "workspace '{}' is outside your workspace scope",
                req.workspace_id
            )));
        }
    }

    // A credential pasted into an input is a credential written to Postgres, to
    // every task listing, and to every log that records a submission. Reject it
    // before anything is queued: a task the platform accepted is a task it then
    // copies around.
    if let Some(items) = req.tasks.as_ref() {
        for item in items {
            if let Some(path) = credential_key_in_input(&item.input) {
                return Err(ApiError::bad_request(format!(
                    "task {} carries a credential-shaped key '{}' in its input; \
                     task input may hold a reference to a credential, never its value",
                    item.id, path
                )));
            }
        }
    }

    // "Here is a task" with no rows is a goal stated in the wrong field; it
    // would be decomposed anyway, and the caller would read a 202 as its task.
    if req.intent_kind == IntentKind::Task
        && req.tasks.as_ref().is_none_or(|items| items.is_empty())
    {
        return Err(ApiError::bad_request(
            "intent_kind=task needs at least one task; a submission with no rows is a goal",
        ));
    }

    // A row that names a face or a unit is checked against the roster here,
    // before anything is queued: the drift pass would otherwise be the first
    // reader to notice a mismatch, and its verdict would describe work that
    // already ran under the wrong owner.
    let mut req = req;
    let intent_kind = req.intent_kind;
    if let Some(items) = req.tasks.as_mut() {
        resolve_bindings(&state, items, intent_kind).await?;
    }

    // 按任务配额执法（审计 3.6）：创建前经 5 级 HierarchyManager 检查。
    // 这是一次只读评估：它读的是各级自己的「今日已用」，而今天没有任何代码往那里
    // 记数（原来记账的那层管理面中间件已撤掉，理由见 `GatewayState::quota_manager`），
    // 所以它此刻实际拦不下东西。留着它是因为声明出来的限额本身是可读的事实，且真正
    // 的消耗要等「任务 → 属主」归属落地后才记；在那之前不另起一套按估值扣账的执法。
    if let Some(ref hierarchy) = state.hierarchy_manager {
        let user_id = claims.as_ref().map(|axum::Extension(c)| c.sub.clone());
        let quota_ctx = cog_core::QuotaContext {
            user_id,
            workspace_id: Some(req.workspace_id.clone()),
            team_id: None,
            organization_id: None,
            global_id: None,
        };
        let estimated = estimate_goal_tokens(&req);
        let decision = hierarchy.check(&quota_ctx, estimated).await;
        if !decision.allowed {
            if let Some(ref stream) = state.audit_stream {
                let _ = stream
                    .append(
                        cog_core::AuditKind::QuotaEnforcement,
                        quota_ctx.user_id.as_deref().unwrap_or("anonymous"),
                        &goal_id,
                        "quota.task_rejected",
                        serde_json::json!({
                            "estimated_tokens": estimated,
                            "blocked_by": decision.blocked_by.len(),
                        }),
                    )
                    .await;
            }
            return Err(ApiError::too_many_requests(format!(
                "quota exceeded: estimated {} tokens blocked by {} scope(s)",
                estimated,
                decision.blocked_by.len()
            )));
        }
    }

    let tasks: Vec<Task> = submitted_tasks(&req, &goal_id);

    // 异步受理：goal 分解需要多轮 LLM 调用（实测 50-90s），远超网关超时层。
    // 同步等待会在超时后丢弃 future，分解结果永远注入不了 DAG（2026-08-06
    // 双副本验证时两次裸 goal 提交均 408 丢任务）。改为立即 202 受理、
    // 后台 spawn 完成分解+注入，客户端按 goal_id 轮询任务图。
    let orchestrator = state.orchestrator.clone();
    let goal = req.goal.clone();
    let spawn_goal_id = goal_id.clone();
    tokio::spawn(async move {
        match orchestrator.submit_goal_auto(&goal, tasks).await {
            Ok(ids) => {
                tracing::info!(
                    goal_id = %spawn_goal_id,
                    task_count = ids.len(),
                    "goal decomposed and injected"
                );
            }
            Err(e) => {
                tracing::error!(goal_id = %spawn_goal_id, "submit_goal_auto failed: {}", e);
            }
        }
    });
    record_task_op(&state, "submit").await;

    Ok((
        StatusCode::ACCEPTED,
        Json(CreateTaskResponse {
            goal: req.goal,
            task_count: 0,
            task_ids: Vec::new(),
            message_id: Some(goal_id),
        }),
    ))
}

pub async fn get_task_handler(
    State(state): State<Arc<GatewayState>>,
    claims: Option<axum::Extension<cog_core::Claims>>,
    Path(id): Path<String>,
) -> Result<Json<TaskView>, ApiError> {
    let claims = claims.as_ref().map(|axum::Extension(c)| c);
    match state.orchestrator.get_task(&id).await {
        Some(task) if !visible_to_claims(claims, &task) => {
            Err(ApiError::not_found(format!("task '{id}' not found")))
        }
        Some(task) => {
            let view: TaskView = task.into();
            Ok(Json(view))
        }
        None => Err(ApiError::not_found(format!("task '{}' not found", id))),
    }
}

pub async fn list_tasks_handler(
    State(state): State<Arc<GatewayState>>,
    claims: Option<axum::Extension<cog_core::Claims>>,
    Query(query): Query<TaskQuery>,
) -> Result<Json<Vec<TaskView>>, ApiError> {
    let claims = claims.as_ref().map(|axum::Extension(c)| c);
    let filter = status_filter(&query)?;
    let ready = ready_ids(&state, filter).await;
    let tasks = state.orchestrator.get_all_tasks().await;
    let views: Vec<TaskView> = tasks
        .into_iter()
        .filter(|t| {
            matches_query(&query, t)
                && matches_status(filter, ready.as_ref(), t)
                && visible_to_claims(claims, t)
        })
        .map(|t| t.into())
        .collect();
    Ok(Json(views))
}

pub async fn get_ready_tasks_handler(
    State(state): State<Arc<GatewayState>>,
    claims: Option<axum::Extension<cog_core::Claims>>,
) -> Result<Json<Vec<TaskView>>, ApiError> {
    let claims = claims.as_ref().map(|axum::Extension(c)| c);
    let tasks = state.orchestrator.get_ready_tasks().await;
    let views: Vec<TaskView> = tasks
        .into_iter()
        .filter(|t| visible_to_claims(claims, t))
        .map(|t| t.into())
        .collect();
    Ok(Json(views))
}

pub async fn schedule_task_handler(
    State(state): State<Arc<GatewayState>>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    state
        .orchestrator
        .schedule_task(&id)
        .await
        .map_err(ApiError::from)?;
    broadcast_task_status(&state, &id, cog_core::TaskStatus::Scheduled, None);
    Ok(StatusCode::NO_CONTENT)
}

pub async fn start_task_handler(
    State(state): State<Arc<GatewayState>>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    state
        .orchestrator
        .start_task(&id)
        .await
        .map_err(ApiError::from)?;
    broadcast_task_status(&state, &id, cog_core::TaskStatus::Running, None);
    Ok(StatusCode::NO_CONTENT)
}

pub async fn complete_task_handler(
    State(state): State<Arc<GatewayState>>,
    Path(id): Path<String>,
    Json(req): Json<CompleteTaskRequest>,
) -> Result<Json<Vec<String>>, ApiError> {
    let scheduled = state
        .orchestrator
        .complete_task(&id, req.result)
        .await
        .map_err(|e| ApiError::internal(format!("complete failed: {}", e)))?;
    broadcast_task_status(&state, &id, cog_core::TaskStatus::Completed, None);
    Ok(Json(scheduled))
}

pub async fn fail_task_handler(
    State(state): State<Arc<GatewayState>>,
    Path(id): Path<String>,
    Json(req): Json<FailTaskRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let (retried, cancelled, dlq) = state
        .orchestrator
        // HTTP 调用方只给了文本，没有传输层信号可依——类型留空，不猜。
        .fail_task(&id, req.error, None)
        .await
        .map_err(|e| ApiError::internal(format!("fail failed: {}", e)))?;
    broadcast_task_status(&state, &id, cog_core::TaskStatus::Failed, None);
    Ok(Json(serde_json::json!({
        "retried": retried,
        "cancelled": cancelled,
        "dlq_pushed": dlq,
    })))
}

pub async fn cancel_task_handler(
    State(state): State<Arc<GatewayState>>,
    Path(id): Path<String>,
) -> Result<Json<Vec<String>>, ApiError> {
    let cancelled = state
        .orchestrator
        .cancel_task(&id)
        .await
        .map_err(|e| ApiError::internal(format!("cancel failed: {}", e)))?;
    broadcast_task_status(&state, &id, cog_core::TaskStatus::Cancelled, None);
    Ok(Json(cancelled))
}

#[derive(Debug, Deserialize)]
pub struct AssignTaskRequest {
    pub agent_id: String,
}

/// POST /api/v1/tasks/{id}/assign -- name the executor a task belongs to.
///
/// Assignment is a step of its own: the planner says what the steps are, this
/// says who carries one out. It binds the owner column the ready queue reads,
/// so the task stops being offered to whoever is idle and waits for the unit
/// named here. Only a task still `Pending` can be bound: one already offered to
/// the shared queue may have been picked up, and a binding made after that
/// would be refused by the completion path rather than honoured.
pub async fn assign_task_handler(
    State(state): State<Arc<GatewayState>>,
    claims: Option<axum::Extension<cog_core::Claims>>,
    Path(id): Path<String>,
    Json(req): Json<AssignTaskRequest>,
) -> Result<StatusCode, ApiError> {
    // Checked before the write: an unreadable task is one the caller may not
    // assign either, and answering "not found" for both keeps the route from
    // confirming which ids exist elsewhere.
    let claims = claims.as_ref().map(|axum::Extension(c)| c);
    match state.orchestrator.get_task(&id).await {
        Some(task) if visible_to_claims(claims, &task) => {}
        _ => return Err(ApiError::not_found(format!("task '{id}' not found"))),
    }
    if req.agent_id.trim().is_empty() {
        return Err(ApiError::bad_request("agent_id must not be empty"));
    }
    state
        .orchestrator
        .assign_task(&id, req.agent_id.trim())
        .await
        .map_err(|e| ApiError::bad_request(format!("assign failed: {}", e)))?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn retry_task_handler(
    State(state): State<Arc<GatewayState>>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    state
        .orchestrator
        .retry_task(&id)
        .await
        .map_err(ApiError::from)?;
    Ok(StatusCode::NO_CONTENT)
}

// ─── Bulk backlog purge ────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct PurgeTasksRequest {
    /// Eligible statuses; default `["pending", "scheduled"]`. Terminal
    /// states are rejected: cancelling a finished task would rewrite its
    /// recorded outcome.
    pub statuses: Option<Vec<String>>,
    /// Optional task-type prefix filter, e.g. `"platform_issue_fix"` or
    /// `"intent-assess"` (task ids embed the same words, so a prefix on
    /// either the type or the id matches).
    pub task_type_prefix: Option<String>,
    /// Only tasks last updated more than this many seconds ago.
    pub older_than_secs: Option<i64>,
    /// Default true: report what would be cancelled without touching
    /// anything. Pass false to actually cancel.
    pub dry_run: Option<bool>,
}

/// Statuses the purge may act on. Terminal states are excluded: cancelling a
/// finished task would rewrite its recorded outcome, and a purge is a
/// backlog broom, not a history editor.
fn parse_task_status(s: &str) -> Result<cog_core::TaskStatus, ApiError> {
    let status = cog_core::TaskStatus::parse(&s.to_ascii_lowercase())
        .ok_or_else(|| ApiError::bad_request(format!("unknown status: {s}")))?;
    if status.is_terminal() {
        return Err(ApiError::bad_request(format!(
            "status {s} is terminal; purge only acts on backlog ({})",
            cog_core::TaskStatus::ALL
                .iter()
                .filter(|status| !status.is_terminal())
                .map(|status| status.as_str())
                .collect::<Vec<_>>()
                .join("/")
        )));
    }
    Ok(status)
}

/// Pure eligibility predicate for the purge: status set, optional type/id
/// prefix, optional age cutoff. Extracted so the matching semantics are
/// testable without a running orchestrator.
fn purge_matches(
    task: &Task,
    statuses: &[cog_core::TaskStatus],
    task_type_prefix: Option<&str>,
    cutoff: Option<chrono::DateTime<chrono::Utc>>,
) -> bool {
    statuses.contains(&task.status)
        && task_type_prefix.is_none_or(|prefix| {
            task_type_str(&task.task_type).starts_with(prefix) || task.id.starts_with(prefix)
        })
        && cutoff.is_none_or(|c| task.updated_at < c)
}

/// POST /api/v1/tasks/purge — interactive backlog cleanup. Backlog piles up
/// silently (a stalled pipeline can queue thousands of stale intents); this
/// endpoint is the operator's broom. Dry-run by default so an operator can
/// inspect the match set before cancelling; the response is the audit
/// record (what matched, what was cancelled, what resisted).
pub async fn purge_tasks_handler(
    State(state): State<Arc<GatewayState>>,
    claims: Option<axum::Extension<cog_core::Claims>>,
    Json(req): Json<PurgeTasksRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let claims = claims.as_ref().map(|axum::Extension(c)| c);
    let dry_run = req.dry_run.unwrap_or(true);
    let statuses = match &req.statuses {
        Some(list) => list
            .iter()
            .map(|s| parse_task_status(s))
            .collect::<Result<Vec<_>, _>>()?,
        None => vec![
            cog_core::TaskStatus::Pending,
            cog_core::TaskStatus::Scheduled,
        ],
    };
    let cutoff = req
        .older_than_secs
        .map(|s| chrono::Utc::now() - chrono::Duration::seconds(s));

    let matched: Vec<Task> = state
        .orchestrator
        .get_all_tasks()
        .await
        .into_iter()
        .filter(|t| {
            visible_to_claims(claims, t)
                && purge_matches(t, &statuses, req.task_type_prefix.as_deref(), cutoff)
        })
        .collect();

    let mut cancelled: Vec<String> = Vec::new();
    let mut failed: Vec<String> = Vec::new();
    if !dry_run {
        for task in &matched {
            match state.orchestrator.cancel_task(&task.id).await {
                Ok(_) => {
                    broadcast_task_status(&state, &task.id, cog_core::TaskStatus::Cancelled, None);
                    cancelled.push(task.id.clone());
                }
                Err(_) => failed.push(task.id.clone()),
            }
        }
        tracing::warn!(
            matched = matched.len(),
            cancelled = cancelled.len(),
            resisted = failed.len(),
            statuses = ?statuses,
            task_type_prefix = req.task_type_prefix,
            older_than_secs = req.older_than_secs,
            "task backlog purge executed"
        );
    }

    Ok(Json(serde_json::json!({
        "dry_run": dry_run,
        "matched": matched.len(),
        "matched_ids": matched.iter().take(100).map(|t| t.id.clone()).collect::<Vec<_>>(),
        "cancelled": cancelled.len(),
        "resisted": failed,
    })))
}

pub async fn task_summary_handler(
    State(state): State<Arc<GatewayState>>,
    claims: Option<axum::Extension<cog_core::Claims>>,
) -> Result<Json<TaskSummary>, ApiError> {
    let claims = claims.as_ref().map(|axum::Extension(c)| c);
    let all: Vec<Task> = state
        .orchestrator
        .get_all_tasks()
        .await
        .into_iter()
        .filter(|t| visible_to_claims(claims, t))
        .collect();
    let ready: Vec<Task> = state
        .orchestrator
        .get_ready_tasks()
        .await
        .into_iter()
        .filter(|t| visible_to_claims(claims, t))
        .collect();
    // Counted over what this reader can see, not the whole graph: a summary
    // whose totals and body disagree sends the reader looking for tasks it is
    // never shown.
    let all_completed = !all.is_empty()
        && all
            .iter()
            .all(|t| t.status == cog_core::TaskStatus::Completed);
    // Counted by comparing states, not their `Debug` spelling: a word retyped
    // here would keep counting after a rename turned it into a comparison that
    // is never true, and a column reading zero is the same shape as a column
    // with nothing in it.
    let counted = |want: cog_core::TaskStatus| all.iter().filter(|t| t.status == want).count();

    Ok(Json(TaskSummary {
        total: all.len(),
        ready: ready.len(),
        running: counted(cog_core::TaskStatus::Running),
        completed: counted(cog_core::TaskStatus::Completed),
        failed: counted(cog_core::TaskStatus::Failed),
        all_completed,
    }))
}

pub async fn get_task_dependents_handler(
    State(state): State<Arc<GatewayState>>,
    claims: Option<axum::Extension<cog_core::Claims>>,
    Path(id): Path<String>,
) -> Result<Json<Vec<TaskView>>, ApiError> {
    let claims = claims.as_ref().map(|axum::Extension(c)| c);
    let tasks = state
        .orchestrator
        .get_dependents(&id)
        .await
        .unwrap_or_default();
    let views: Vec<TaskView> = tasks
        .into_iter()
        .filter(|t| visible_to_claims(claims, t))
        .map(|t| t.into())
        .collect();
    Ok(Json(views))
}

pub async fn get_task_dependencies_handler(
    State(state): State<Arc<GatewayState>>,
    claims: Option<axum::Extension<cog_core::Claims>>,
    Path(id): Path<String>,
) -> Result<Json<Vec<TaskView>>, ApiError> {
    let claims = claims.as_ref().map(|axum::Extension(c)| c);
    let tasks = state
        .orchestrator
        .get_dependencies(&id)
        .await
        .unwrap_or_default();
    let views: Vec<TaskView> = tasks
        .into_iter()
        .filter(|t| visible_to_claims(claims, t))
        .map(|t| t.into())
        .collect();
    Ok(Json(views))
}

pub async fn get_task_graph_handler(
    State(state): State<Arc<GatewayState>>,
    claims: Option<axum::Extension<cog_core::Claims>>,
    Query(query): Query<TaskQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let claims = claims.as_ref().map(|axum::Extension(c)| c);
    let filter = status_filter(&query)?;
    let ready = ready_ids(&state, filter).await;
    let (tasks, edges) = state.orchestrator.get_graph().await;
    let kept: Vec<Task> = tasks
        .into_iter()
        .filter(|t| {
            matches_query(&query, t)
                && matches_status(filter, ready.as_ref(), t)
                && visible_to_claims(claims, t)
        })
        .collect();
    // 过滤时边跟着节点收口：只留两端都还在的边，否则图里会出现指向已被滤掉节点的悬边，
    // 读的人会以为那条边通向一张没被过滤掉的任务。工作区过滤也会滤掉节点，所以这一段
    // 从「给了 goal_id 才收口」改成「只要滤掉了任何一个节点就收口」。
    let edges: Vec<(String, String)> = {
        let ids: std::collections::HashSet<&str> = kept.iter().map(|t| t.id.as_str()).collect();
        edges
            .into_iter()
            .filter(|(from, to)| ids.contains(from.as_str()) && ids.contains(to.as_str()))
            .collect()
    };
    let nodes: Vec<TaskView> = kept.into_iter().map(|t| t.into()).collect();
    Ok(Json(serde_json::json!({
        "nodes": nodes,
        "edges": edges,
    })))
}

pub async fn delete_task_handler(
    State(state): State<Arc<GatewayState>>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    state
        .orchestrator
        .delete_task(&id)
        .await
        .map_err(ApiError::from)?;
    Ok(StatusCode::NO_CONTENT)
}

// ─── 范围结构：把一条任务重挂到另一个父节点下 ─────────────────────────

/// 改一条任务的范围结构字段。
///
/// 与上面那十一个状态迁移端点不是一类：那些回答「这条活跑到哪了」，这一条回答
/// 「这条活挂在谁下面」。范围结构只能由分解侧产出的那段时间里，客户的树一旦拆错
/// 就没有任何路径能拆回来。
#[derive(Debug, Deserialize)]
pub struct UpdateTaskRequest {
    /// 三态：字段缺席＝这次不改父节点，`null`＝把它提到顶层，字符串＝挂到那个
    /// 父节点下。`Option<Option<_>>` 是唯一能同时说出这三种意思的形状——
    /// 单层 `Option` 会把「不改」和「提到顶层」压成同一个 `None`。
    #[serde(default, deserialize_with = "double_option")]
    pub parent_task_id: Option<Option<String>>,
    #[serde(default)]
    pub title: Option<String>,
    /// 全部替换，不是追加：验收标准是一份清单，改它的人手里有完整的那一版。
    #[serde(default)]
    pub acceptance: Option<Vec<String>>,
}

fn double_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

#[derive(Debug, Serialize)]
pub struct UpdateTaskResponse {
    pub id: String,
    pub parent_task_id: Option<String>,
    pub title: String,
    pub acceptance: Vec<String>,
}

/// 新父节点会不会把这个任务接到它自己的子树里。
///
/// 走上去而不是走下去：任务图里向上只有一条链（每个任务一个父节点），判断
/// 「新父节点是不是我的后代」只要从新父节点往上爬到顶，看有没有撞见自己。
/// 撞见就成环——把 task 挂到它自己的孙子下面，那个孙子的祖先链里就有 task。
///
/// 数据里本来就有环时不死循环：走过的节点记下来，第二次踩到同一个节点说明
/// 这条既有链自己成环、而且不经过 task，那不是这次改动造成的。
pub(crate) fn would_create_cycle(tasks: &[Task], task_id: &str, new_parent: &str) -> bool {
    if task_id == new_parent {
        return true;
    }
    let by_id: std::collections::HashMap<&str, &Task> =
        tasks.iter().map(|t| (t.id.as_str(), t)).collect();
    let mut seen: HashSet<&str> = HashSet::new();
    let mut cursor = Some(new_parent);
    while let Some(id) = cursor {
        if id == task_id {
            return true;
        }
        if !seen.insert(id) {
            return false;
        }
        cursor = by_id
            .get(id)
            .and_then(|t| t.parent_task_id.as_deref())
            .filter(|p| !p.is_empty());
    }
    false
}

pub async fn update_task_handler(
    State(state): State<Arc<GatewayState>>,
    claims: Option<axum::Extension<cog_core::Claims>>,
    Path(id): Path<String>,
    Json(req): Json<UpdateTaskRequest>,
) -> Result<Json<UpdateTaskResponse>, ApiError> {
    let claims = claims.as_ref().map(|axum::Extension(c)| c);
    let all = state.orchestrator.get_all_tasks().await;
    let visible: Vec<Task> = all
        .into_iter()
        .filter(|t| visible_to_claims(claims, t))
        .collect();
    let mut task = visible
        .iter()
        .find(|t| t.id == id)
        .cloned()
        .ok_or_else(|| ApiError::not_found(format!("task '{id}' does not exist")))?;
    // 改前的样子先留一份：审计问的是「改成了什么」，只有改后的值答不了。
    let before = serde_json::json!({
        "parent_task_id": task.parent_task_id,
        "title": crate::projects::task_title(&task),
        "acceptance": task.acceptance,
    });

    if let Some(title) = req.title.as_ref() {
        let title = title.trim();
        if title.is_empty() {
            return Err(ApiError::bad_request(
                "title is empty: a work package with no name draws as its id everywhere",
            )
            .with_code("empty_title"));
        }
        if title.chars().count() > crate::projects::MAX_TITLE_CHARS {
            return Err(ApiError::bad_request(format!(
                "title is {} characters; the cap is {}",
                title.chars().count(),
                crate::projects::MAX_TITLE_CHARS
            ))
            .with_code("title_too_long"));
        }
        // 任务没有独立的名字列，标题住在 `input` 里——写回同一处，读面才读得到。
        // `name` 排在读面取词的第一个，所以人改的名字会压过输入里原有的描述。
        let mut input = if task.input.is_object() {
            task.input.clone()
        } else {
            serde_json::json!({})
        };
        input["name"] = serde_json::json!(title);
        task.input = input;
    }
    if let Some(acceptance) = req.acceptance.as_ref() {
        task.acceptance = acceptance.clone();
    }

    if let Some(parent) = req.parent_task_id.as_ref() {
        let is_project_root = task.goal_id.as_deref() == Some(task.id.as_str());
        match parent.as_deref() {
            None => {
                if is_project_root {
                    return Err(ApiError::bad_request(
                        "this row is the root of a project: it has no parent to be moved to, \
                         and giving it one would put a whole project inside another",
                    )
                    .with_code("root_cannot_be_reparented"));
                }
                task.parent_task_id = None;
            }
            Some(new_parent) => {
                if is_project_root {
                    return Err(ApiError::bad_request(
                        "this row is the root of a project: it has no parent to be moved to, \
                         and giving it one would put a whole project inside another",
                    )
                    .with_code("root_cannot_be_reparented"));
                }
                let target = visible.iter().find(|t| t.id == new_parent).ok_or_else(|| {
                    ApiError::bad_request(format!(
                        "parent '{new_parent}' does not exist in anything you can see"
                    ))
                    .with_code("unknown_parent")
                })?;
                if target.project_key() != task.project_key() {
                    return Err(ApiError::bad_request(format!(
                        "parent '{new_parent}' belongs to project '{}', not to '{}': a \
                         re-parent across projects makes one scope tree reach into another",
                        target.project_key(),
                        task.project_key()
                    ))
                    .with_code("parent_in_another_project"));
                }
                if would_create_cycle(&visible, &task.id, new_parent) {
                    return Err(ApiError::bad_request(format!(
                        "moving '{}' under '{new_parent}' would make the task its own \
                         ancestor",
                        task.id
                    ))
                    .with_code("would_create_cycle"));
                }
                task.parent_task_id = Some(new_parent.to_string());
            }
        }
    }

    state.orchestrator.replace_task(task.clone()).await?;

    if let Some(ref stream) = state.audit_stream {
        let _ = stream
            .append(
                cog_core::AuditKind::Custom("project_task".into()),
                claims.map(|c| c.sub.as_str()).unwrap_or("system"),
                &task.id,
                "project.task_updated",
                serde_json::json!({
                    "parent_task_id": task.parent_task_id,
                    "title": crate::projects::task_title(&task),
                    "acceptance": task.acceptance,
                }),
            )
            .await;
    }
    // 哈希链记顺序与完整性，这张表记「这一张任务改过什么」——两份的读者不同。
    crate::project_audit::record(cog_core::AuditEntry {
        actor: claims
            .map(|c| c.sub.clone())
            .unwrap_or_else(|| "system".into()),
        action: "project.task_updated".into(),
        resource_type: "task".into(),
        resource_id: task.id.clone(),
        project_id: task.project_key().to_string(),
        before,
        after: serde_json::json!({
            "parent_task_id": task.parent_task_id,
            "title": crate::projects::task_title(&task),
            "acceptance": task.acceptance,
        }),
    })
    .await;

    Ok(Json(UpdateTaskResponse {
        id: task.id.clone(),
        parent_task_id: task.parent_task_id.clone(),
        title: crate::projects::task_title(&task),
        acceptance: task.acceptance.clone(),
    }))
}

// ─── 计划基线：把「当初打算怎么做」写进去 ──────────────────────────────

#[derive(Debug, Deserialize)]
pub struct SaveTaskPlanRequest {
    #[serde(default)]
    pub planned_start: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default)]
    pub planned_end: Option<chrono::DateTime<chrono::Utc>>,
    /// 这一版计划打算花多少 token。缺席时沿用上一版基线的预算；连上一版都没有
    /// 就拒绝——一份没有预算的基线在挣值里贡献一个恒为零的计划价值，读面分不出
    /// 「还没排期」和「排了但没给钱」。
    #[serde(default)]
    pub budget_tokens: Option<u64>,
}

fn plan_refusal(message: impl Into<String>, code: &str) -> ApiError {
    ApiError::bad_request(message).with_code(code)
}

pub async fn save_task_plan_handler(
    State(state): State<Arc<GatewayState>>,
    claims: Option<axum::Extension<cog_core::Claims>>,
    Path(id): Path<String>,
    Json(req): Json<SaveTaskPlanRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let claims = claims.as_ref().map(|axum::Extension(c)| c);
    let tasks: Vec<Task> = state
        .orchestrator
        .get_all_tasks()
        .await
        .into_iter()
        .filter(|t| visible_to_claims(claims, t))
        .collect();
    let task = tasks
        .iter()
        .find(|t| t.id == id)
        .ok_or_else(|| ApiError::not_found(format!("task '{id}' does not exist")))?;

    // 没有验收标准的工作包没有「做完」的判据：给它排一条期，等于给一件谁也
    // 说不清完成没有的活定一个计划价值。
    if task.acceptance.is_empty() {
        return Err(plan_refusal(
            format!(
                "task '{id}' has no acceptance criteria: a work package whose 'done' has no \
                 judge cannot carry a planned value"
            ),
            "no_acceptance",
        ));
    }
    if let (Some(start), Some(end)) = (req.planned_start, req.planned_end) {
        if end < start {
            return Err(plan_refusal(
                format!("planned_end {end} is before planned_start {start}"),
                "plan_window_inverted",
            ));
        }
    }

    let workspace = task
        .workspace_id
        .clone()
        .or_else(|| {
            claims
                .filter(|c| c.workspace_ids.len() == 1)
                .map(|c| c.workspace_ids[0].clone())
        })
        .ok_or_else(|| {
            plan_refusal(
                format!(
                    "task '{id}' carries no workspace and the caller holds no single one: \
                     a baseline filed under a guessed workspace is read back by nobody"
                ),
                "workspace_ambiguous",
            )
        })?;

    let existing = state
        .orchestrator
        .task_plans()
        .await
        .ok()
        .and_then(|plans| plans.into_iter().find(|p| p.task_id == id));
    let budget_tokens = match (req.budget_tokens, existing.as_ref()) {
        (Some(budget), _) => budget,
        (None, Some(prev)) => prev.budget_tokens,
        (None, None) => {
            return Err(plan_refusal(
                format!(
                    "task '{id}' has no budget: pass `budget_tokens`, or a planned value of \
                     zero will read as 'nothing was planned'"
                ),
                "budget_required",
            ))
        }
    };

    let plan = cog_core::TaskPlan {
        workspace_id: workspace,
        task_id: id.clone(),
        planned_start: req.planned_start,
        planned_end: req.planned_end,
        budget_tokens,
        // 每一笔写就是一版新基线：版本号由写入方按时刻生成，单调前进。让调用方
        // 自己带版本号，旧版本就能盖掉新版本，读者也分不出哪一版是后来的。
        baseline_rev: format!(
            "rev-{}",
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ),
        updated_at: chrono::Utc::now(),
    };
    state.orchestrator.save_task_plan(&plan).await?;

    if let Some(ref stream) = state.audit_stream {
        let _ = stream
            .append(
                cog_core::AuditKind::Custom("project_task".into()),
                claims.map(|c| c.sub.as_str()).unwrap_or("system"),
                &id,
                "project.plan_saved",
                serde_json::json!({
                    "planned_start": plan.planned_start,
                    "planned_end": plan.planned_end,
                    "budget_tokens": plan.budget_tokens,
                    "baseline_rev": plan.baseline_rev,
                }),
            )
            .await;
    }
    crate::project_audit::record(cog_core::AuditEntry {
        actor: claims
            .map(|c| c.sub.clone())
            .unwrap_or_else(|| "system".into()),
        action: "project.plan_saved".into(),
        resource_type: "plan".into(),
        resource_id: plan.task_id.clone(),
        project_id: task.project_key().to_string(),
        before: existing
            .as_ref()
            .map(|p| {
                serde_json::json!({
                    "planned_start": p.planned_start,
                    "planned_end": p.planned_end,
                    "budget_tokens": p.budget_tokens,
                    "baseline_rev": p.baseline_rev,
                })
            })
            .unwrap_or(serde_json::Value::Null),
        after: serde_json::json!({
            "planned_start": plan.planned_start,
            "planned_end": plan.planned_end,
            "budget_tokens": plan.budget_tokens,
            "baseline_rev": plan.baseline_rev,
        }),
    })
    .await;

    Ok(Json(serde_json::json!({
        "task_id": plan.task_id,
        "workspace_id": plan.workspace_id,
        "planned_start": plan.planned_start,
        "planned_end": plan.planned_end,
        "budget_tokens": plan.budget_tokens,
        "baseline_rev": plan.baseline_rev,
        "updated_at": plan.updated_at,
    })))
}

// ─── 看板：拖动是状态迁移，不是浏览器里的位置 ──────────────────────────

/// 一次拖动对应哪一个状态迁移。
///
/// 只有一步的才认：从「待办」拖到「进行中」在真实看板上是两跳（先入队再开跑），
/// 悄悄替调用方走两步，会让一次拖动在日志里变成两个不相干的事件。中间那一列应
/// 该由人再拖一次，或者由派发侧自己走——这两件事都不该藏在一次 PATCH 里。
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum MoveStep {
    Schedule,
    Start,
    Complete,
    Fail,
    Cancel,
    Retry,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum MoveRefusal {
    SameColumn,
    NotAdjacent { from: String, to: String },
}

impl MoveRefusal {
    fn code(&self) -> &'static str {
        match self {
            Self::SameColumn => "same_column",
            Self::NotAdjacent { .. } => "not_adjacent",
        }
    }

    fn message(&self) -> String {
        match self {
            Self::SameColumn => "the card is already in that column".into(),
            Self::NotAdjacent { from, to } => format!(
                "'{from}' cannot move straight to '{to}' in one step: a board move is a \
                 status transition, and this pair is not one"
            ),
        }
    }
}

pub(crate) fn move_step(from: &str, to: &str) -> Result<MoveStep, MoveRefusal> {
    use MoveStep::*;
    if from == to {
        return Err(MoveRefusal::SameColumn);
    }
    match (from, to) {
        ("pending", "scheduled") => Ok(Schedule),
        ("scheduled", "running") => Ok(Start),
        ("running", "completed") => Ok(Complete),
        ("running", "failed") => Ok(Fail),
        (_, "cancelled") => Ok(Cancel),
        ("failed", "pending") => Ok(Retry),
        _ => Err(MoveRefusal::NotAdjacent {
            from: from.to_string(),
            to: to.to_string(),
        }),
    }
}

#[derive(Debug, Deserialize)]
pub struct MoveTaskRequest {
    /// 目标列，取值就是任务状态的六个拼写——看板与状态机共用一套词。
    pub status: String,
}

pub async fn move_task_handler(
    State(state): State<Arc<GatewayState>>,
    claims: Option<axum::Extension<cog_core::Claims>>,
    Path(id): Path<String>,
    Json(req): Json<MoveTaskRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let claims = claims.as_ref().map(|axum::Extension(c)| c);
    let target = cog_core::TaskStatus::parse(req.status.trim()).ok_or_else(|| {
        ApiError::bad_request(format!(
            "'{}' is not a column: the six are {}",
            req.status,
            cog_core::TaskStatus::ALL
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ))
        .with_code("unknown_column")
    })?;

    let tasks: Vec<Task> = state
        .orchestrator
        .get_all_tasks()
        .await
        .into_iter()
        .filter(|t| visible_to_claims(claims, t))
        .collect();
    let task = tasks
        .iter()
        .find(|t| t.id == id)
        .ok_or_else(|| ApiError::not_found(format!("task '{id}' does not exist")))?;

    let step = move_step(task.status.as_str(), target.as_str())
        .map_err(|refusal| ApiError::bad_request(refusal.message()).with_code(refusal.code()))?;

    // 目标列满了就拦在写入之前：到达上限这一刻，发布侧已经不再往这一列放新活，
    // 看板还能把卡片拖进去的话，两边的读数当场就分家。
    let (limits, _problems) = cog_core::BoardWipLimits::from_env();
    if let Some(limit) = limits.limit_for(target.as_str()) {
        let occupants = tasks
            .iter()
            .filter(|t| t.project_key() == task.project_key() && t.status == target)
            .count();
        if occupants >= limit {
            return Err(ApiError::bad_request(format!(
                "column '{}' already holds {occupants} of its {limit} cards",
                target.as_str()
            ))
            .with_code("column_full"));
        }
    }

    let actor = claims.map(|c| c.sub.as_str()).unwrap_or("system");
    match step {
        MoveStep::Schedule => state.orchestrator.schedule_task(&id).await?,
        MoveStep::Start => state.orchestrator.start_task(&id).await?,
        MoveStep::Complete => {
            state
                .orchestrator
                .complete_task(&id, serde_json::json!({ "via": "board_move", "by": actor }))
                .await?;
        }
        MoveStep::Fail => {
            state
                .orchestrator
                .fail_task(
                    &id,
                    format!("moved to failed from the board by {actor}"),
                    None,
                )
                .await?;
        }
        MoveStep::Cancel => {
            state.orchestrator.cancel_task(&id).await?;
        }
        MoveStep::Retry => state.orchestrator.retry_task(&id).await?,
    }
    broadcast_task_status(&state, &id, target, task.agent_id.clone());

    if let Some(ref stream) = state.audit_stream {
        let _ = stream
            .append(
                cog_core::AuditKind::Custom("project_task".into()),
                actor,
                &id,
                "project.card_moved",
                serde_json::json!({ "from": task.status.as_str(), "to": target.as_str() }),
            )
            .await;
    }
    // 「这片卡是哪一天被谁挪走的」——事故复盘问的第一个问题，按资源一秒查出来。
    crate::project_audit::record(cog_core::AuditEntry {
        actor: actor.to_string(),
        action: "project.card_moved".into(),
        resource_type: "card".into(),
        resource_id: id.clone(),
        project_id: task.project_key().to_string(),
        before: serde_json::json!({ "status": task.status.as_str() }),
        after: serde_json::json!({ "status": target.as_str() }),
    })
    .await;

    Ok(Json(serde_json::json!({
        "id": id,
        "from": task.status.as_str(),
        "to": target.as_str(),
    })))
}

pub async fn check_timeouts_handler(
    State(state): State<Arc<GatewayState>>,
) -> Result<Json<Vec<serde_json::Value>>, ApiError> {
    let results = state.orchestrator.check_timeouts().await;
    let out: Vec<serde_json::Value> = results
        .into_iter()
        .map(|(id, retried, cancelled, dlq)| {
            serde_json::json!({
                "task_id": id,
                "retried": retried,
                "cancelled": cancelled,
                "dlq_pushed": dlq,
            })
        })
        .collect();
    Ok(Json(out))
}

// ─── Batch ops ───────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct BatchCompleteRequest {
    pub completions: Vec<CompletionItem>,
}

#[derive(Debug, Deserialize)]
pub struct CompletionItem {
    pub task_id: String,
    pub result: serde_json::Value,
}

pub async fn batch_complete_handler(
    State(state): State<Arc<GatewayState>>,
    Json(req): Json<BatchCompleteRequest>,
) -> Result<Json<Vec<String>>, ApiError> {
    let mut scheduled = Vec::new();
    for item in req.completions {
        match state
            .orchestrator
            .complete_task(&item.task_id, item.result)
            .await
        {
            Ok(mut s) => scheduled.append(&mut s),
            Err(e) => tracing::warn!("batch complete failed for {}: {}", item.task_id, e),
        }
    }
    Ok(Json(scheduled))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::claims;

    fn task(id: &str, status: cog_core::TaskStatus, task_type: &str, age_secs: i64) -> Task {
        let mut t = Task::new(
            id.to_string(),
            parse_task_type(task_type),
            serde_json::json!({}),
        );
        t.status = status;
        t.updated_at = chrono::Utc::now() - chrono::Duration::seconds(age_secs);
        t
    }

    /// 读面印的状态词就是行里携带的那一个。印 `Debug` 的拼法时，客户端按
    /// `'failed'` 比的那一处永远为假，一张失败的任务于是以「完成」的样子出现。
    #[test]
    fn the_task_view_prints_the_status_word_the_row_carries() {
        for status in cog_core::TaskStatus::ALL {
            let view: TaskView = task("a", status, "t", 0).into();
            assert_eq!(
                view.status,
                status.as_str(),
                "the read surface must print the word the row stores, not a formatting of it"
            );
        }
    }

    /// 总线帧里带的是同一个词，而且网关自己的等级过滤器认得它：`failed` 帧要被
    /// 标成 error。帧里写死另一种拼法时，这一层永远标不出 error，读起来像
    /// 「没有一次失败」，而不是像一处拼错。
    #[test]
    fn the_frame_a_transition_broadcasts_carries_the_declared_word() {
        let failed = task_status_frame("t-1", cog_core::TaskStatus::Failed, None);
        let AgentEvent::TaskStatusChange { status, .. } = &failed else {
            panic!("a task transition produces a status frame");
        };
        assert_eq!(status, cog_core::TaskStatus::Failed.as_str());
        assert_eq!(
            crate::observability::agent_event_level(&failed),
            "error",
            "the gateway's own level filter must recognize the word its own producer writes"
        );

        let completed = task_status_frame("t-1", cog_core::TaskStatus::Completed, None);
        assert_eq!(
            crate::observability::agent_event_level(&completed),
            "info",
            "a completed task is not an error"
        );
    }

    /// 每个 `TaskFilter` 词都要被自己认下来：客户端标签页按这份声明取词，
    /// 声明里有一个读不回来的词，就等于那个标签页恒空。
    #[test]
    fn every_declared_status_filter_word_parses_back_to_itself() {
        for filter in TaskFilter::ALL {
            assert_eq!(
                TaskFilter::parse(filter.as_str()),
                Some(filter),
                "{} is declared but not readable",
                filter.as_str()
            );
            assert_eq!(
                TaskFilter::parse(&filter.as_str().to_uppercase()),
                Some(filter),
                "the parameter always was case-insensitive"
            );
        }
    }

    /// 状态词必须与读面印出来的那个词逐字一致：`TaskFilter` 用 `as_str` 印行，
    /// 用 serde 送往客户端，两侧拼法一旦分叉，按词取行就再也取不到。
    #[test]
    fn a_status_filter_word_is_the_one_the_row_carries() {
        let row = serde_json::to_value(task("a", cog_core::TaskStatus::Scheduled, "t", 0))
            .expect("a task serializes");
        assert_eq!(row["status"], "scheduled");
        assert_eq!(
            TaskFilter::parse("scheduled"),
            Some(TaskFilter::Status(cog_core::TaskStatus::Scheduled))
        );
    }

    /// 认不出的词是错误，不是空列表：空列表正是「这个状态没有任务」的同一个身体，
    /// 于是打错一个词的标签页会读成「一切安静」。错误里要列出能问的词。
    #[test]
    fn an_unknown_status_word_is_refused_with_the_words_that_work() {
        let query = TaskQuery {
            status: Some("ready_to_run".into()),
            ..Default::default()
        };
        let err = status_filter(&query).expect_err("ready_to_run is not a word the endpoints know");
        assert_eq!(err.error, "BadRequest");
        let text = err.message;
        for word in TaskFilter::ALL {
            assert!(
                text.contains(word.as_str()),
                "the refusal must name {word:?}; got {text}"
            );
        }
    }

    /// `ready` 是队列不是状态：判定按队列给回的那批 id 走，不按行上的状态。
    /// 拿它跟状态比会一条都匹配不上——那正是修之前标签页恒空的原因。
    #[test]
    fn the_ready_filter_selects_the_queue_not_a_status() {
        let pending = task("queued", cog_core::TaskStatus::Pending, "t", 0);
        let blocked = task("blocked", cog_core::TaskStatus::Pending, "t", 0);
        let ready: HashSet<String> = ["queued".to_string()].into_iter().collect();

        assert!(matches_status(
            Some(TaskFilter::Ready),
            Some(&ready),
            &pending
        ));
        assert!(
            !matches_status(Some(TaskFilter::Ready), Some(&ready), &blocked),
            "pending with an unfinished blocker is not in the queue"
        );
        assert!(
            matches_status(None, Some(&ready), &blocked),
            "no filter keeps everything, queue or not"
        );
        assert!(matches_status(
            Some(TaskFilter::Status(cog_core::TaskStatus::Pending)),
            Some(&ready),
            &blocked
        ));
    }

    #[test]
    fn purge_matches_default_backlog_only() {
        let backlog = cog_core::TaskStatus::Pending;
        let running = cog_core::TaskStatus::Running;
        let statuses = vec![
            cog_core::TaskStatus::Pending,
            cog_core::TaskStatus::Scheduled,
        ];
        let pending = task("a", backlog, "platform_issue_fix", 10);
        let active = task("b", running, "platform_issue_fix", 10);
        assert!(purge_matches(&pending, &statuses, None, None));
        assert!(
            !purge_matches(&active, &statuses, None, None),
            "running tasks are never touched by the default backlog purge"
        );
    }

    #[test]
    fn purge_matches_prefix_on_type_or_id() {
        let statuses = vec![cog_core::TaskStatus::Pending];
        let by_type = task(
            "x-1",
            cog_core::TaskStatus::Pending,
            "platform_issue_fix",
            0,
        );
        let by_id = task(
            "intent-assess-gitee-issue-7-abc",
            cog_core::TaskStatus::Pending,
            "custom_thing",
            0,
        );
        let other = task("x-2", cog_core::TaskStatus::Pending, "self_audit", 0);
        assert!(purge_matches(
            &by_type,
            &statuses,
            Some("platform_issue"),
            None
        ));
        assert!(purge_matches(
            &by_id,
            &statuses,
            Some("intent-assess"),
            None
        ));
        assert!(!purge_matches(
            &other,
            &statuses,
            Some("platform_issue"),
            None
        ));
    }

    /// 写出去的拼法与读回来的拼法必须是同一组词：提交时 `parse_task_type`
    /// 认不出一个词就把它当成自定义类型，于是那道任务以 `Custom("llm-call")`
    /// 的样子进图，而规划、限流、重试矩阵都按真类型找它——找不到，读作「没有
    /// 这一类任务」。
    #[test]
    fn every_task_type_word_reads_back_as_the_type_that_wrote_it() {
        let builtin = [
            TaskType::Planner,
            TaskType::Generator,
            TaskType::Evaluator,
            TaskType::Reviewer,
            TaskType::LlmCall,
            TaskType::ToolCall,
            TaskType::FileOp,
            TaskType::DbTransaction,
            TaskType::NetworkRequest,
            TaskType::WasmSkill,
            TaskType::Skill,
            TaskType::DagNode,
        ];
        for task_type in builtin {
            let word = task_type_str(&task_type);
            assert_eq!(
                parse_task_type(&word),
                task_type,
                "{word:?} does not read back as the type that wrote it"
            );
            let serde_word = serde_json::to_value(&task_type)
                .expect("a task type serializes")
                .as_str()
                .expect("a builtin type serializes as a word")
                .to_string();
            assert_eq!(
                word, serde_word,
                "the read surface and the row must spell the type the same way"
            );
        }
        // 自定义类型原样往返：写出去的是它自己的名字，不是 `custom(\"x\")`。
        let custom = TaskType::Custom("my_thing".into());
        assert_eq!(task_type_str(&custom), "my_thing");
        assert_eq!(parse_task_type("my_thing"), custom);
    }

    #[test]
    fn purge_matches_age_cutoff() {
        let statuses = vec![cog_core::TaskStatus::Pending];
        let old = task("old", cog_core::TaskStatus::Pending, "t", 7200);
        let fresh = task("fresh", cog_core::TaskStatus::Pending, "t", 60);
        let cutoff = Some(chrono::Utc::now() - chrono::Duration::seconds(3600));
        assert!(purge_matches(&old, &statuses, None, cutoff));
        assert!(
            !purge_matches(&fresh, &statuses, None, cutoff),
            "fresh tasks survive an age-cutoff purge"
        );
    }

    #[test]
    fn parse_task_status_rejects_unknown() {
        assert!(parse_task_status("pending").is_ok());
        assert!(parse_task_status("nonsense").is_err());
    }

    #[test]
    fn parse_task_status_rejects_terminal() {
        // Purging a finished task would rewrite its recorded outcome.
        for s in ["completed", "failed", "cancelled"] {
            assert!(parse_task_status(s).is_err(), "{s} must be rejected");
        }
    }

    /// 写侧一直在记的 goal 链接，读侧必须看得见——否则「按下发的 goal 找回它派生的那批
    /// 任务」这条路就还是断的。
    #[test]
    fn the_task_view_carries_the_goal_link_the_write_side_records() {
        let mut t = task("child", cog_core::TaskStatus::Completed, "custom_thing", 0);
        t.goal_id = Some("goal-1".into());
        t.parent_task_id = Some("orig".into());
        t.is_executable = false;

        let view: TaskView = t.into();
        assert_eq!(view.goal_id.as_deref(), Some("goal-1"));
        assert_eq!(view.parent_task_id.as_deref(), Some("orig"));
        assert!(
            !view.is_executable,
            "a re-injected placeholder is not schedulable work"
        );
    }

    #[test]
    fn a_goal_filter_keeps_only_that_goals_tasks() {
        let mut in_g1 = task("a", cog_core::TaskStatus::Pending, "t", 0);
        in_g1.goal_id = Some("g1".into());
        let mut in_g2 = task("b", cog_core::TaskStatus::Pending, "t", 0);
        in_g2.goal_id = Some("g2".into());
        let goal_less = task("c", cog_core::TaskStatus::Pending, "t", 0);

        let unfiltered = TaskQuery::default();
        assert!(
            in_goal(&unfiltered, &in_g1)
                && in_goal(&unfiltered, &in_g2)
                && in_goal(&unfiltered, &goal_less),
            "no goal filter keeps every task, exactly as before"
        );

        let only_g1 = TaskQuery {
            goal_id: Some("g1".into()),
            ..Default::default()
        };
        assert!(in_goal(&only_g1, &in_g1));
        assert!(!in_goal(&only_g1, &in_g2));
        assert!(
            !in_goal(&only_g1, &goal_less),
            "a task with no goal belongs to no goal"
        );
    }

    /// 裸 goal：客户端拿到的 goal_id 必须落在一条行上，否则 `?goal_id=` 恒空，
    /// 「按下发的 goal 找回它派生的那批任务」这条路就还是断的。
    #[test]
    fn a_bare_goal_carries_its_id_onto_a_row_so_the_client_can_poll_it() {
        let req = CreateTaskRequest {
            goal_id: Some("goal-1".into()),
            goal: "do the thing".into(),
            tasks: None,
            workspace_id: "ws".into(),
            priority: None,
            intent_kind: IntentKind::Goal,
        };
        let tasks = submitted_tasks(&req, "goal-1");
        assert_eq!(tasks.len(), 1, "a bare goal must still be addressable");
        assert_eq!(
            tasks[0].id, "goal-1",
            "the row id is the id the client was handed, so its own response id polls back"
        );
        assert_eq!(tasks[0].goal_id.as_deref(), Some("goal-1"));
        assert!(
            !tasks[0].is_executable,
            "the goal row is a marker for the goal, not schedulable work"
        );
    }

    /// 显式任务：各自保留自己的 id，同时都带同一个 goal_id（这是分解用的载体，
    /// 也是 `?goal_id=` 的连接键）。
    #[test]
    fn explicit_tasks_keep_their_ids_and_carry_the_goal_id() {
        let req = CreateTaskRequest {
            goal_id: None,
            goal: "g".into(),
            tasks: Some(vec![crate::tasks::TaskItem {
                id: "case-1".into(),
                task_type: "benchmark_case".into(),
                input: serde_json::json!({ "x": 1 }),
                blocked_by: vec![],
                priority: 0,
                planner: None,
                agent_id: None,
                face_id: None,
                unit_id: None,
            }]),
            workspace_id: "ws".into(),
            priority: None,
            intent_kind: IntentKind::Goal,
        };
        let tasks = submitted_tasks(&req, "goal-9");
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].id, "case-1");
        assert_eq!(tasks[0].goal_id.as_deref(), Some("goal-9"));
    }

    /// 显式空数组 `tasks: []` 与字段缺省是同一件事：没有可执行行 ⇒ 裸 goal，
    /// 同样要有一条承载 id 的 goal 行。按「字段缺席」判就会漏掉这一条，`?goal_id=` 又变恒空。
    #[test]
    fn an_explicit_empty_task_list_is_the_same_bare_goal() {
        let req = CreateTaskRequest {
            goal_id: Some("goal-empty".into()),
            goal: "plan this for me".into(),
            tasks: Some(vec![]),
            workspace_id: "ws".into(),
            priority: None,
            intent_kind: IntentKind::Goal,
        };
        let tasks = submitted_tasks(&req, "goal-empty");
        assert_eq!(tasks.len(), 1, "an empty array is still a bare goal");
        assert_eq!(tasks[0].id, "goal-empty");
        assert_eq!(tasks[0].goal_id.as_deref(), Some("goal-empty"));
        assert!(!tasks[0].is_executable);
    }

    fn task_item(id: &str, verified: Option<bool>) -> TaskItem {
        TaskItem {
            id: id.into(),
            task_type: "benchmark_case".into(),
            input: serde_json::json!({ "x": 1 }),
            blocked_by: vec![],
            priority: 0,
            planner: verified.map(|v| PlannerClaim {
                verified: v,
                source: Some("user_provided".into()),
            }),
            agent_id: None,
            face_id: None,
            unit_id: None,
        }
    }

    fn face(face_id: &str, owner: Option<&str>) -> cog_core::Face {
        cog_core::Face {
            face_id: face_id.into(),
            owner_unit: owner.map(Into::into),
            bound_by_handover_id: None,
            spec: cog_core::FaceSpec::first(face_id, cog_core::FaceKind::Face),
            updated_at: chrono::Utc::now(),
        }
    }

    /// 一张面绑定的行只能落在面主手里：命名了面就继承面主，命名了别的 unit 就当场拒，
    /// 而不是先入队、再让漂移读数过一会儿把它读红。后者等于把已经知道错的答案写进账。
    #[test]
    fn a_face_bound_row_takes_the_face_owner_or_is_refused_at_submission() {
        let owned = face("task-management", Some("worker"));
        let vacant = face("security", None);

        let mut bound = task_item("t1", None);
        bound.face_id = Some("task-management".into());
        assert_eq!(
            binding_of(&bound, Some(&owned)),
            Ok((Some("task-management".into()), Some("worker".into()))),
            "naming only the face binds the row to that face's owner"
        );

        let mut mismatched = task_item("t2", None);
        mismatched.face_id = Some("task-management".into());
        mismatched.unit_id = Some("6e".into());
        let refusal = binding_of(&mismatched, Some(&owned)).unwrap_err();
        assert!(refusal.contains("owned by 'worker'"), "{refusal}");

        let mut unknown = task_item("t3", None);
        unknown.face_id = Some("no-such-face".into());
        assert!(binding_of(&unknown, None)
            .unwrap_err()
            .contains("not on the roster"));

        let mut to_vacancy = task_item("t4", None);
        to_vacancy.face_id = Some("security".into());
        assert!(binding_of(&to_vacancy, Some(&vacant))
            .unwrap_err()
            .contains("has no owner"));

        let mut ownerless = task_item("t5", None);
        ownerless.unit_id = Some("worker".into());
        assert!(binding_of(&ownerless, None)
            .unwrap_err()
            .contains("but no face"));

        assert_eq!(binding_of(&task_item("t6", None), None), Ok((None, None)));
    }

    /// 走规划的行会被规划器返回的东西替换，点在上面的面与 unit 一个字都到不了图。
    /// 接受它等于让提交者以为绑好了，而那一行从来没有存在过——所以带绑定的行只收
    /// 「按提交原样注入」的那一种形状。
    #[test]
    fn a_binding_is_refused_on_a_row_the_planner_will_replace() {
        let mut bound = task_item("t1", Some(true));
        bound.face_id = Some("task-management".into());
        assert!(binding_carrier(&bound, IntentKind::Task));
        assert!(
            !binding_carrier(&bound, IntentKind::Goal),
            "a goal's rows are hints; the planner's rows carry no binding"
        );

        let unverified = task_item("t2", None);
        assert!(
            !binding_carrier(&unverified, IntentKind::Task),
            "an unverified task still goes through planning"
        );
    }

    /// 绑定过的字段要真的落到行上；否则校验通过、提交成功，而任务图里仍是一张没有
    /// 归属的行，下一拍又被读成漂移。
    #[test]
    fn a_bound_row_carries_its_face_and_owner_onto_the_task() {
        let mut item = task_item("t1", Some(true));
        item.face_id = Some("task-management".into());
        item.unit_id = Some("worker".into());
        let req = CreateTaskRequest {
            goal_id: None,
            goal: "do the thing".into(),
            tasks: Some(vec![item]),
            workspace_id: "ws".into(),
            priority: None,
            intent_kind: IntentKind::Task,
        };
        let tasks = submitted_tasks(&req, "g");
        assert_eq!(tasks[0].face_id.as_deref(), Some("task-management"));
        assert_eq!(tasks[0].owner_unit.as_deref(), Some("worker"));
    }

    /// 声明为 task 且自称已验证的提交，逐行带上 `verified`：这是协调器直注分支
    /// 唯一看的标记，少了它，一次「拿去执行」的提交会被再规划一遍。
    #[test]
    fn a_verified_task_submission_marks_every_row_for_direct_injection() {
        let req = CreateTaskRequest {
            goal_id: None,
            goal: "run these".into(),
            tasks: Some(vec![
                task_item("t1", Some(true)),
                task_item("t2", Some(true)),
            ]),
            workspace_id: "ws".into(),
            priority: None,
            intent_kind: IntentKind::Task,
        };
        let tasks = submitted_tasks(&req, "g");
        assert_eq!(tasks.len(), 2);
        for t in &tasks {
            let meta = t
                .action_planner_meta
                .as_ref()
                .unwrap_or_else(|| panic!("{} lost its verified claim", t.id));
            assert!(meta.verified);
        }
    }

    /// 反过来：goal 提交不认这条声明。输入形态由 `intent_kind` 说，不由某一行上
    /// 偶然带的标记说——否则「这条是目标」与「这一行别规划了」会互相顶掉。
    #[test]
    fn a_goal_submission_ignores_a_verified_claim_on_its_rows() {
        let req = CreateTaskRequest {
            goal_id: None,
            goal: "plan this".into(),
            tasks: Some(vec![task_item("t1", Some(true))]),
            workspace_id: "ws".into(),
            priority: None,
            intent_kind: IntentKind::Goal,
        };
        let tasks = submitted_tasks(&req, "g");
        assert!(
            tasks[0].action_planner_meta.is_none(),
            "a goal's rows are decomposed even when one of them claims to be verified"
        );
    }

    /// 凭据类键名一律拒；只描述数量的名字（`token_budget`、`max_tokens`）不拒——
    /// 按子串判会把它们一起打掉，而它们恰是正常载荷。
    #[test]
    fn credential_keys_are_rejected_but_quantities_are_not() {
        for key in [
            "api_key",
            "github_token",
            "client-secret",
            "password",
            "Authorization",
        ] {
            let input = serde_json::json!({ "step": { key: "x" } });
            assert_eq!(
                credential_key_in_input(&input).as_deref(),
                Some(format!("step.{key}").as_str()),
                "{key} is a credential key and must be refused"
            );
        }
        for key in ["token_budget", "max_tokens", "keyword", "monkey"] {
            let input = serde_json::json!({ key: 1 });
            assert!(
                credential_key_in_input(&input).is_none(),
                "{key} describes a quantity or a word, not a secret"
            );
        }
        assert!(
            credential_key_in_input(&serde_json::json!([{"user": {"password": "x"}}]))
                .is_some_and(|p| p.contains("password"))
        );
    }

    /// 读面的隔离：声明了 workspace 的 token 看不到别人的任务；没声明范围的口径
    /// 与提交侧一致（不按范围限制）；**没有归属的行对声明了范围的读者不可见**。
    ///
    /// 最后那一条以前是反的：旧行没有 workspace 就让所有声明范围的读者都看得见，
    /// 理由是「不因隔离而消失」。代价是任何一条没记归属的行都出现在每一家客户的
    /// 看板上，而且看起来就像是他家的——把别人的活读成自己的，比读不到更坏。
    /// 看不见的判据是「看得见才算」：范围之外的一切都不返回，包括没归属的。
    #[test]
    fn a_scoped_reader_sees_only_its_own_workspaces() {
        let claims = claims(&["ws-a"]);
        let mut mine = task("a", cog_core::TaskStatus::Pending, "t", 0);
        mine.workspace_id = Some("ws-a".into());
        let mut theirs = task("b", cog_core::TaskStatus::Pending, "t", 0);
        theirs.workspace_id = Some("ws-b".into());
        let unattributed = task("c", cog_core::TaskStatus::Pending, "t", 0);

        assert!(visible_to_claims(Some(&claims), &mine));
        assert!(!visible_to_claims(Some(&claims), &theirs));
        assert!(
            !visible_to_claims(Some(&claims), &unattributed),
            "a row with no workspace belongs to nobody: showing it to a scoped reader \
             puts somebody else's work on their board"
        );
        assert!(visible_to_claims(None, &theirs));
        let unscoped = crate::test_support::claims(&[]);
        assert!(
            visible_to_claims(Some(&unscoped), &theirs),
            "a token that declares no scope is not restricted by one"
        );
    }

    /// 读面丢掉调度器与回收判据真正读的那几列，界面上就只剩一个状态词：
    /// 「为什么失败」「谁还拿着执行权」「什么时候才许重试」三件事都不在响应里。
    #[test]
    fn the_view_keeps_the_columns_the_scheduler_acts_on() {
        let mut t = task("a", cog_core::TaskStatus::Running, "t", 0);
        t.blocks = vec!["b".into()];
        t.error_cause = Some(cog_core::contract::llm::UpstreamFailure::QuotaExhausted);
        t.lease_owner = Some("gateway-1".into());
        t.lease_expires_at = Some(chrono::Utc::now() + chrono::Duration::seconds(30));
        t.retry_not_before = Some(chrono::Utc::now() + chrono::Duration::seconds(60));
        t.started_at = Some(chrono::Utc::now());
        t.timeout_seconds = 900;
        t.face_id = Some("face-resources".into());
        t.bound_by_handover_id = Some("handover-7".into());

        let view = TaskView::from(t);
        assert_eq!(view.blocks, vec!["b".to_string()]);
        assert_eq!(
            view.error_cause,
            Some(cog_core::contract::llm::UpstreamFailure::QuotaExhausted)
        );
        assert_eq!(view.lease_owner.as_deref(), Some("gateway-1"));
        assert!(view.lease_expires_at.is_some());
        assert!(view.retry_not_before.is_some());
        assert!(view.started_at.is_some());
        assert_eq!(view.timeout_seconds, 900);
        assert_eq!(view.face_id.as_deref(), Some("face-resources"));
        assert_eq!(view.bound_by_handover_id.as_deref(), Some("handover-7"));
    }

    /// 一条不属于任何 workspace 的任务，对一个有 workspace 范围的读者不可见。
    ///
    /// 反过来的写法——无主即放行——让每一条历史行同时出现在每个租户的视图里，
    /// 而视图里没有任何东西能把它们和这个租户自己的活分开。
    #[test]
    fn unowned_tasks_are_not_visible() {
        let mut t = task("unowned", cog_core::TaskStatus::Pending, "t", 0);
        t.workspace_id = None;
        let reader = claims(&["acme"]);
        assert!(
            !visible_to_claims(Some(&reader), &t),
            "无主的行不许落进有范围的读者眼里"
        );
        assert!(
            visible_to_claims(None, &t),
            "没有范围的读者（平台运维）仍然看得见它，否则这些行就没有任何读面"
        );
        assert!(
            visible_to_claims(Some(&claims(&[])), &t),
            "空范围＝不受范围限制，与提交侧同一个约定"
        );
    }

    /// 有主的行只给主人看。
    #[test]
    fn a_scoped_reader_sees_only_its_own_workspace_rows() {
        let mut mine = task("mine", cog_core::TaskStatus::Pending, "t", 0);
        mine.workspace_id = Some("acme".into());
        let mut theirs = task("theirs", cog_core::TaskStatus::Pending, "t", 0);
        theirs.workspace_id = Some("globex".into());
        let reader = claims(&["acme"]);
        assert!(visible_to_claims(Some(&reader), &mine));
        assert!(!visible_to_claims(Some(&reader), &theirs));
    }

    /// 把一条任务挂到它自己的后代下面会成环；挂到无关的任务下面不会。
    #[test]
    fn a_task_cannot_become_its_own_ancestor() {
        let mut root = task("root", cog_core::TaskStatus::Pending, "t", 0);
        root.parent_task_id = None;
        let mut mid = task("mid", cog_core::TaskStatus::Pending, "t", 0);
        mid.parent_task_id = Some("root".into());
        let mut leaf = task("leaf", cog_core::TaskStatus::Pending, "t", 0);
        leaf.parent_task_id = Some("mid".into());
        let other = task("other", cog_core::TaskStatus::Pending, "t", 0);
        let tasks = vec![root, mid, leaf, other];

        assert!(would_create_cycle(&tasks, "root", "leaf"), "孙子下面挂祖父");
        assert!(would_create_cycle(&tasks, "root", "mid"));
        assert!(would_create_cycle(&tasks, "root", "root"), "自己挂自己");
        assert!(
            !would_create_cycle(&tasks, "leaf", "other"),
            "无关的父节点不算环"
        );
    }

    /// 数据里本来就有一个不走这条链的环时，判据要停下来，不能死循环。
    #[test]
    fn a_pre_existing_cycle_elsewhere_does_not_hang_the_check() {
        let mut a = task("a", cog_core::TaskStatus::Pending, "t", 0);
        a.parent_task_id = Some("b".into());
        let mut b = task("b", cog_core::TaskStatus::Pending, "t", 0);
        b.parent_task_id = Some("a".into());
        let c = task("c", cog_core::TaskStatus::Pending, "t", 0);
        let tasks = vec![a, b, c];
        assert!(!would_create_cycle(&tasks, "c", "a"));
    }

    /// 拖动只有一步时才落状态：两跳的拖动会让一次操作变成两个事件。
    #[test]
    fn a_board_move_is_one_transition_or_a_refusal() {
        assert_eq!(
            move_step("pending", "scheduled").unwrap(),
            MoveStep::Schedule
        );
        assert_eq!(move_step("scheduled", "running").unwrap(), MoveStep::Start);
        assert_eq!(
            move_step("running", "completed").unwrap(),
            MoveStep::Complete
        );
        assert_eq!(move_step("failed", "pending").unwrap(), MoveStep::Retry);
        assert_eq!(move_step("running", "cancelled").unwrap(), MoveStep::Cancel);
        assert_eq!(
            move_step("running", "running").unwrap_err(),
            MoveRefusal::SameColumn
        );
        match move_step("pending", "running") {
            Err(MoveRefusal::NotAdjacent { from, to }) => {
                assert_eq!((from.as_str(), to.as_str()), ("pending", "running"));
            }
            other => panic!("两跳的拖动必须拒绝，实际 {other:?}"),
        }
        assert!(
            move_step("completed", "pending").is_err(),
            "做完的不能靠拖回去重开"
        );
    }
}
