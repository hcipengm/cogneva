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
    /// 这条活的终态描述。
    ///
    /// 三格随行一起来、是任务的一等字段，不放进 `input` 的开集载荷：判据、界面
    /// 与定批都按键读它们，埋在文本里就不可查询。老调用方仍可把三格放在
    /// `input.ledger_item` 里，写事实前会被提到字段上。
    #[serde(default)]
    pub terminal: Option<String>,
    /// 此刻的样子（写明来源）。
    #[serde(default)]
    pub now: Option<String>,
    /// 缺什么；空 = 到终态。
    #[serde(default)]
    pub gap: Option<String>,
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
    /// The place this row is worked in: memory, wiki, eval …
    ///
    /// A label on the row, not an object: the workbench folds rows by it to
    /// answer "where is this work done", and a venue nobody registered still
    /// appears as long as some row carries it. Absent means the row belongs to
    /// no venue, which is where platform-internal work and older rows live —
    /// the reading must not fold that into a default column.
    ///
    /// A submitter that files work here is saying which column of the board it
    /// belongs in; nothing downstream can infer that from the task type, and a
    /// row filed without one is visible only in the unfiltered task list.
    #[serde(default)]
    pub venue: Option<String>,
    /// How long the submitter declares this work will take, in seconds.
    ///
    /// A declaration, not a deadline: nothing in the platform ends a running row
    /// by comparing its age to this number. Reclaim reads two things instead --
    /// whether the holder's lease expired, and whether the attempt stopped
    /// moving (`cog_core::declared_wall::stall`) -- because a row whose owner is
    /// demonstrably alive and whose work is demonstrably progressing must not be
    /// failed for having taken a while. What this field does is size the
    /// expectations around the row: producers read it back as "how long this
    /// kind of work should take", and the ledger counts rows that outlived their
    /// own declaration (`past_its_declaration`), which is a reading and not a
    /// verdict.
    ///
    /// Zero is not "no limit": it declares a duration of nothing, so every
    /// reader that sizes its patience from the row reads the smallest duration
    /// there is before the work has started. A submitter that means "no limit"
    /// has to say a length.
    #[serde(default)]
    pub declared_duration_seconds: Option<u64>,
    /// The field's old name, kept only so the door can refuse it by name.
    ///
    /// A rename that lets the old spelling fall through would be read as an
    /// unknown field and silently replaced by the default -- a submitter who
    /// asked for an hour would get the row's five minutes and a 202. Naming it
    /// here turns that into a 400 that says what to send instead.
    #[serde(default, rename = "timeout_seconds")]
    pub renamed_timeout_seconds: Option<serde_json::Value>,
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
    pub declared_duration_seconds: u64,
    /// 服务哪个面（面节点的这一列指向自己）。`None` 是不服务任何面，不是默认面。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub face_id: Option<String>,
    /// 由哪张交接单签来的归属。与 `owner_unit` 一起读，才知道归属是改派还是漂移。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bound_by_handover_id: Option<String>,
    /// 这道活落在哪个场地（memory / wiki / eval …）。与 `face_id` 不是一列：
    /// 面是组织上的责任归属，场地是活本身落在哪一栏。`None` 是不属于任何场地。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub venue: Option<String>,
    /// 这道活怎样算做完，一句一句可证伪地写在行上。读任务的人要看的就是它：
    /// 没有这一列，「做完了」只能靠读结果自己猜。
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub acceptance: Vec<String>,
    /// 这一行带着的待办三格（终态／现在／差距），状态按行自己的结局现算：
    /// **完成且差距已空**才是 `已落`，其余仍 `未落`，差距不动。没带三格的行
    /// 不出这一格——「这一条没有差距」与「这一行不是待办」在读面上是两件事。
    ///
    /// 现算而不回写 `input`：行上只有 `status` 一处写结局，读的时候折进来，
    /// 重试一次读出来的状态就跟着变，没有第二份要同步的副本。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ledger_item: Option<cog_core::contract::ledger_item::LedgerItem>,
}

impl From<Task> for TaskView {
    fn from(t: Task) -> Self {
        let ledger_item = t.ledger_item();
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
            declared_duration_seconds: t.declared_duration_seconds,
            face_id: t.face_id,
            bound_by_handover_id: t.bound_by_handover_id,
            venue: t.venue,
            acceptance: t.acceptance,
            ledger_item,
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
    /// 只看这几行，逗号分隔的 id。对话里那份任务列表带的就是这一轮挂上去的几个 id：
    /// 它要的是自己派的那几行，不是整张表——整张表今天九千多行，读过之后只会截掉。
    /// 不认的 id 静默略过：列表上的行可能已经被清理，一个已经不存在的 id 不该让整次
    /// 读取被拒。
    pub ids: Option<String>,
    /// 只看这一类任务：一个状态词，或就绪队列（`ready`，队列本身，不是某行携带的状态）。
    /// 大小写不敏感，取值就是 [`TaskFilter::ALL`] 印出来的那些词。
    pub status: Option<String>,
    /// 最多要几行，最新的在前。不写就是不翻页，整张表。
    ///
    /// 从前这个参数根本不在这一面：浏览器带着 `?limit=300` 发过来，服务端当作
    /// 不认识的 query 丢掉，整张表照发，客户端收完再截掉。屏上写着「最近三百
    /// 条」，网络上走的是九千行、三十八兆——截在收完之后，省下的只是内存里的
    /// 那一截。现在截在这里，代价是传输之前就没了。
    ///
    /// 只对 `/tasks/list` 生效：`/tasks/graph` 也吃这个结构，但它画的是整张图，
    /// 少一半节点就不是那张图了，所以那边照旧读全量。
    ///
    /// `0` 就是零行，不是「不限」——零当不限是个陷阱，两种意思共用一个值，写错
    /// 的一方永远看不出来。
    pub limit: Option<usize>,
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
        && in_ids(query, task)
}

/// 一条任务是否在被点名的这一组里。不给 id 就一律留下。
///
/// 一次调用读了几个 id，就只读这几个 id 的行——列表要的是它自己派的那几行。
fn in_ids(query: &TaskQuery, task: &Task) -> bool {
    match query.ids.as_deref() {
        None => true,
        Some(want) => want.split(',').map(str::trim).any(|id| id == task.id),
    }
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
) -> Result<Option<HashSet<String>>, ApiError> {
    match filter {
        Some(TaskFilter::Ready) => {
            let ids = ready_ids_from(state.orchestrator.get_ready_tasks_checked().await)?;
            Ok(Some(ids))
        }
        _ => Ok(None),
    }
}

/// The ids a ready read carries, with a failed read refusing instead of
/// answering an empty queue.
///
/// The `ready` filter asks for the queue itself, not for a state word, so the
/// answer is a bare set of ids and the reader has no rows to compare it
/// against: an empty set is exactly what a quiet deployment looks like. Folding
/// a store error into it makes a gateway that cannot read the graph report
/// "nothing waiting" to the tab and the count above it. Absence of the answer
/// is a 503, the same way every other unreadable read on this surface answers.
fn ready_ids_from(reading: cog_core::SFResult<Vec<Task>>) -> Result<HashSet<String>, ApiError> {
    Ok(task_rows_from_reading(reading, "ready queue")?
        .into_iter()
        .map(|task| task.id)
        .collect())
}

/// The rows a task read carries, with a failed read refusing instead of
/// answering an empty list.
///
/// The task list, the summary above it, the purge preview and the process
/// overview all count rows; folding a store failure into an empty set makes
/// each of them report the cleanest possible number about a graph nobody read
/// -- "0 tasks", "0 ready", "nothing to purge", an empty flow. Absence of the
/// answer is a 503; an empty row set that was actually read stays a 200 with
/// zeros.
/// 一行任务的读：读失败留成 503，读到了才轮到「有没有这一行」。
pub(crate) fn task_from_reading(
    reading: cog_core::SFResult<Option<Task>>,
) -> Result<Option<Task>, ApiError> {
    reading.map_err(|e| ApiError::service_unavailable(format!("the task could not be read: {e}")))
}

pub(crate) fn task_rows_from_reading(
    reading: cog_core::SFResult<Vec<Task>>,
    what: &str,
) -> Result<Vec<Task>, ApiError> {
    reading.map_err(|e| ApiError::service_unavailable(format!("the {what} could not be read: {e}")))
}

/// The task a write handler is about to change, read under the caller's scope.
///
/// The edit handlers used to find their row in the folded graph, so a store
/// that could not be read made an existing task answer 404 "does not exist" --
/// a statement about the task that is really a statement about the read, on
/// paths that then wrote nothing and reported a missing resource. A failed read
/// is a 503; 404 stays for a row that was looked for among the rows this caller
/// may see and was not there.
fn task_to_change_from_reading(
    reading: cog_core::SFResult<Vec<Task>>,
    claims: Option<&cog_core::Claims>,
    id: &str,
) -> Result<Task, ApiError> {
    let rows: Vec<Task> = task_rows_from_reading(reading, "task list")?
        .into_iter()
        .filter(|t| visible_to_claims(claims, t))
        .collect();
    Ok(task_to_change(&rows, id)?.clone())
}

/// The same lookup once the rows are in hand: a row that is not among rows
/// this caller may see is a 404, and only a completed read reaches here.
fn task_to_change<'a>(rows: &'a [Task], id: &str) -> Result<&'a Task, ApiError> {
    rows.iter()
        .find(|t| t.id == id)
        .ok_or_else(|| ApiError::not_found(format!("task '{id}' does not exist")))
}

/// The graph a read carries, with a failed read refusing instead of answering
/// an empty graph.
///
/// No nodes and no edges draws exactly like a deployment that has not made a
/// task yet: the view is blank either way, so a store failure folded here is a
/// graph view that says "nothing exists" about a graph it never read. Absence
/// of the answer is a 503, the same way every other unreadable read on this
/// surface answers.
/// The tasks and the edges between them, as the store hands both over.
type TaskGraphRows = (Vec<Task>, Vec<(String, String)>);

fn graph_from_reading(
    reading: cog_core::SFResult<TaskGraphRows>,
) -> Result<TaskGraphRows, ApiError> {
    reading.map_err(|e| {
        ApiError::service_unavailable(format!("the task graph could not be read: {e}"))
    })
}

/// The rows a dependents or dependencies read carries, with a failed read
/// refusing instead of answering an empty list.
///
/// On the task detail page a failed read and a task with no related rows draw
/// the same empty list, so folding the failure in says "this task depends on
/// nothing" about a graph nobody managed to read. `None` still answers an empty
/// list: that is a read answer (no such relation), not a failure.
fn related_tasks_from_reading(
    reading: cog_core::SFResult<Option<Vec<Task>>>,
    what: &str,
) -> Result<Vec<Task>, ApiError> {
    let rows = reading.map_err(|e| {
        ApiError::service_unavailable(format!("the task's {what} could not be read: {e}"))
    })?;
    Ok(rows.unwrap_or_default())
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
    let payload_chars: usize = req
        .tasks
        .as_ref()
        .map(|items| items.iter().map(|item| item.input.to_string().len()).sum())
        .unwrap_or(0);
    estimated_goal_tokens(&req.goal, payload_chars)
}

/// 上面那条估算规则本身，按「一句话 + 它带的载荷有多长」取。
///
/// 界面上那句对话派活时走的是同一条准入（同一份 5 级配额、同一份估值口径），
/// 只是它手里没有 `CreateTaskRequest`：把规则放这里，两处读的是同一个数，
/// 而不是各写一条除法。
pub fn estimated_goal_tokens(goal: &str, payload_chars: usize) -> u64 {
    (((goal.len() + payload_chars) / 4) as u64).max(1)
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
/// queued. Naming a unit on a face-bound row is allowed only when the roster
/// already names that unit for the face -- accepting a mismatch here and
/// letting the drift pass report it later is a row written with an answer the
/// writer already knew to be wrong.
///
/// A unit with no face is a different row, not a mismatched one. It serves no
/// face, so there is no binding to contradict; it says only who holds it, and
/// an owned row is not offered to the shared queue -- the unit it names asks
/// for it. That is how a unit that is on no face still authors its own work:
/// the alternative, refusing the name, left every session that is not a face's
/// owner able to file nothing at all, so its work existed nowhere but in its
/// transcript.
fn binding_of(
    item: &TaskItem,
    face: Option<&cog_core::Face>,
) -> Result<(Option<String>, Option<String>), String> {
    match (item.face_id.as_deref(), item.unit_id.as_deref()) {
        (None, None) => Ok((None, None)),
        (None, Some(unit)) if unit.trim().is_empty() => Err(format!(
            "task {} names an empty unit; a row held by nobody in particular has no owner to \
             ask for it",
            item.id
        )),
        (None, Some(unit)) => Ok((None, Some(unit.to_string()))),
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

/// The acceptance criteria a submission states under `acceptance` in a row's
/// input.
///
/// Blank entries are dropped: a list of empty strings is a list that says
/// nothing, and a dispatcher that only asks whether the list is empty would take
/// it as an answer.
fn stated_acceptance(input: &serde_json::Value) -> Vec<String> {
    input
        .get("acceptance")
        .and_then(|v| v.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

/// The first row in this submission that is injected as submitted and states no
/// acceptance criterion.
///
/// Such a row is dispatched by nobody: the graph holds it until a criterion
/// appears, and the producer learns that only from a log line it never reads.
/// Two live rows were sitting that way when this check was written -- one that
/// did state its criteria in the input and lost them here, one that never stated
/// any. The first is fixed by carrying the statement onto the row; this check is
/// what keeps the second from being accepted in silence.
fn injected_row_without_acceptance(req: &CreateTaskRequest) -> Option<&str> {
    if req.intent_kind != IntentKind::Task {
        return None;
    }
    req.tasks.as_ref()?.iter().find_map(|item| {
        let injected = item.planner.as_ref().is_some_and(|claim| claim.verified);
        (injected && stated_acceptance(&item.input).is_empty()).then_some(item.id.as_str())
    })
}

/// The first row in this submission that states a duration of zero seconds.
///
/// Zero is not "no limit". A duration of zero declares that this work is
/// expected to take no time at all, and every reader of the field -- the
/// ledger's `past_its_declaration`, the producers sizing a wait against it --
/// reads it as the smallest duration there is. A submitter that meant "no
/// limit" has to say a length, and this check is what tells them their row was
/// read the other way.
fn row_with_zero_duration(req: &CreateTaskRequest) -> Option<&str> {
    req.tasks
        .as_ref()?
        .iter()
        .find(|item| item.declared_duration_seconds == Some(0))
        .map(|item| item.id.as_str())
}

/// The first row that still spells the declared duration `timeout_seconds`.
///
/// The name moved when the meaning did: the row's number is a declaration, and
/// `timeout` in this tree means the point at which something is killed. A body
/// that still says `timeout_seconds` parses into nothing, so without this check
/// the row would be filed on its default while its submitter read a 202 -- the
/// quiet half of a rename.
fn row_with_the_old_duration_name(req: &CreateTaskRequest) -> Option<&str> {
    req.tasks
        .as_ref()?
        .iter()
        .find(|item| item.renamed_timeout_seconds.is_some())
        .map(|item| item.id.as_str())
}

fn submitted_tasks(
    req: &CreateTaskRequest,
    goal_id: &str,
    submitted_by: Option<&str>,
) -> Vec<Task> {
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
                    if let Some(terminal) = item.terminal.as_deref() {
                        task.terminal = terminal.to_string();
                    }
                    if let Some(now) = item.now.as_deref() {
                        task.now = now.to_string();
                    }
                    if let Some(gap) = item.gap.as_deref() {
                        task.gap = gap.to_string();
                    }
                    task.blocked_by = item.blocked_by.clone();
                    task.priority = item.priority;
                    task.workspace_id = Some(req.workspace_id.clone());
                    task.goal_id = Some(goal_id.to_string());
                    task.agent_id = item.agent_id.clone();
                    task.face_id = item.face_id.clone();
                    task.owner_unit = item.unit_id.clone();
                    // Blank is absent rather than a venue whose name is the
                    // empty string: the board would draw a nameless column
                    // for it, and every reading that groups by venue would
                    // carry one group no reader can name.
                    task.venue = item
                        .venue
                        .as_deref()
                        .map(str::trim)
                        .filter(|venue| !venue.is_empty())
                        .map(str::to_string);
                    // 提交人从令牌来，不从请求体来：这一格决定谁的当日额度，一个
                    // 能自己填它的调用方等于能把自己的花销记到别人头上。
                    task.submitted_by = submitted_by.map(str::to_string);
                    // The stated budget replaces the row default rather than
                    // being layered under it: a submitter that says "this stays
                    // open for a week" is answering the same question the
                    // default answers.
                    if let Some(secs) = item.declared_duration_seconds {
                        task.declared_duration_seconds = secs;
                    }
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
                                    "Plan: the submission as written (verified by the submitter)"
                                        .into(),
                                ),
                                source: Some(parse_planner_source(claim.source.as_deref())),
                                confidence: None,
                                timestamp: Some(chrono::Utc::now()),
                            });
                            // This row never passes through decomposition, so no
                            // planner will ever write "what done means" onto it.
                            // The submitter's own statement is the only source,
                            // and `acceptance` in the input is where a submission
                            // says it. The copy in the input stays: it is what
                            // the caller sent, and an audit that reads the row
                            // back has to see the submission as it arrived. The
                            // row field is what dispatch and execution read.
                            task.acceptance = stated_acceptance(&item.input);
                        }
                    }
                    // 这一行是提交面唯一把一件活写进账的地方：在这里把它交什么
                    // 落成一份带来源的记录，后面的派发、判尺、输出形状都读那一格。
                    // 提交人此时已经知道（令牌），再晚就没有这一格了。
                    //
                    // 请求体里自带的记录先丢掉：那一格是服务端写的，里面有「谁说的」。
                    // 收下一个调用方自己填的 `by`，等于让任何人把一句声明记到别人头上。
                    // 丢掉之后仍按提交内容重新落定，声明本身不受影响。
                    if let Some(fields) = task.input.as_object_mut() {
                        fields.remove(cog_core::types::task::INPUT_DELIVERABLE_RECORD);
                        fields.remove(cog_core::types::task::INPUT_DELIVERABLE_SUPERSEDED);
                    }
                    task.settle_deliverable();
                    task
                })
                .collect()
        })
        .unwrap_or(Default::default());
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
                crate::audit::append_or_warn(
                    stream,
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

    // A row injected as submitted and stating no acceptance criterion is
    // refused here rather than queued: nothing downstream will ever fill one in,
    // and a row that sits on the board forever reads to its submitter exactly
    // like work in progress.
    if let Some(id) = injected_row_without_acceptance(&req) {
        return Err(ApiError::bad_request(format!(
            "task {id} is submitted as verified, so no planner runs on it and nothing else \
             will state what done means. Put the acceptance criteria under `acceptance` in \
             the task's input; without them the row is held and never runs."
        ))
        .with_code("acceptance_required"));
    }

    // The field was called `timeout_seconds`, and a submitter who still sends
    // that spelling must be told so: an unknown key would be dropped and the
    // row would fall back to its default while the submitter read a 202.
    if let Some(id) = row_with_the_old_duration_name(&req) {
        return Err(ApiError::bad_request(format!(
            "task {id} carries `timeout_seconds`, which is now `declared_duration_seconds`: \
             the row's number is the length its submitter declares, not a point at which \
             anything is killed. Send the same number under the new name."
        ))
        .with_code("renamed_field"));
    }

    // A duration of zero is refused here rather than read as "no limit": the
    // number is what a reader sizes its own patience from, so a zero hands
    // every reader the smallest duration there is before the work starts --
    // while the submitter read a 202.
    if let Some(id) = row_with_zero_duration(&req) {
        return Err(ApiError::bad_request(format!(
            "task {id} states a declared duration of 0 seconds, which declares the work takes \
             no time at all; say how long the row declares it will take"
        ))
        .with_code("duration_required"));
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
    // 它读的是各级自己的「今日已用」，而今天往那里记数的只有一条路：LLM 代理按
    // 调用方声明的属主，把真实 token 记到「人」那一级。人这一格因此是活的——有人
    // 给某个人配过上限，这个人越线之后连任务都创建不了；工作区、团队、组织三级
    // 仍然没有任何写者（没有哪一处说得清一次调用属于哪一个），配在那里的上限会
    // 一直判不出东西。这里是一次按估值的只读评估，不扣账。
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
                crate::audit::append_or_warn(
                    stream,
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

    // 提交人取自已验过的令牌，和上面那两处判据读的是同一个字段：一处判权限，
    // 一处记在账上，两处要是各读各的，会出现「按 A 判了权限、记到 B 头上」。
    // 没有令牌的提交（平台自己的零凭证面）不编一个名字。
    let submitted_by = claims.as_ref().map(|axum::Extension(c)| c.sub.clone());
    let tasks: Vec<Task> = submitted_tasks(&req, &goal_id, submitted_by.as_deref());

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

/// 一次阻塞读最多等多久。
///
/// `override`：人定的窗口，与智能体那一侧 `await_task` 的上限取同一个决定——
/// 超过它的等待方早就不在了（它自己那一轮有停滞超时），连接只是挂在那里。
/// 门这一侧再钳一次，是因为这个读口不问调用方是谁：一个任意长的 `wait_ms`
/// 就是一条能被外面拿住的连接。
// cap: fixed — 人定的（override）：与 await_task 的上限同一个决定，超过它等待方早已不在
// keep: 删掉后回到「人定的（override）：与 await_task 的上限同一个决定，超过它等待方早已不在」所描述的情形；由这条约定兜住
pub const MAX_TASK_WAIT_MS: u64 = 600_000;

/// `GET /api/v1/tasks/{id}` 的等待参数。
#[derive(Debug, Default, serde::Deserialize)]
pub struct TaskWaitQuery {
    /// 最多等这条任务走到终局多少毫秒；缺省是「读当下这一眼」。
    pub wait_ms: Option<u64>,
}

/// 一条任务是不是走到了终局。
fn task_reached_an_end(status: Option<cog_core::TaskStatus>) -> bool {
    matches!(
        status,
        Some(cog_core::TaskStatus::Completed)
            | Some(cog_core::TaskStatus::Failed)
            | Some(cog_core::TaskStatus::Cancelled)
    )
}

/// 等这条任务走到终局，或者到点。
///
/// 订阅编排器广播的那一份任务事件，不在读口里反复读库：一次等 = 一次订阅。
/// 掉队（`Lagged`）时立刻返回让调用方重读一行，而不是在这里一直等到点——
/// 掉队说明的是读者跟不上，这一行的终局可能就在掉队的那几条里；重读一次状态
/// 比「等一个已经发生过的终局」便宜。
async fn wait_for_task_end(state: &GatewayState, id: &str, wait_ms: u64) {
    let Some(task) = state.orchestrator.get_task(id).await else {
        return;
    };
    if task_reached_an_end(Some(task.status)) {
        return;
    }
    let mut events = state.subscribe_task_events();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(wait_ms);
    let waited = tokio::time::timeout_at(deadline, async {
        loop {
            match events.recv().await {
                Ok(event) => {
                    let arrived = match &event {
                        cog_core::TaskEvent::TaskCompleted { task_id, .. }
                        | cog_core::TaskEvent::TaskFailed { task_id, .. }
                        | cog_core::TaskEvent::TaskCancelled { task_id, .. } => task_id,
                        _ => continue,
                    };
                    if arrived == id {
                        return;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => return,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
            }
        }
    })
    .await;
    if waited.is_err() {
        tracing::debug!(
            task_id = id,
            wait_ms,
            "no end arrived for this task within the wait; the caller reads the row as it stands"
        );
    }
}

pub async fn get_task_handler(
    State(state): State<Arc<GatewayState>>,
    claims: Option<axum::Extension<cog_core::Claims>>,
    Path(id): Path<String>,
    Query(wait): Query<TaskWaitQuery>,
) -> Result<Json<TaskView>, ApiError> {
    if let Some(wait_ms) = wait.wait_ms.filter(|ms| *ms > 0) {
        wait_for_task_end(&state, &id, wait_ms.min(MAX_TASK_WAIT_MS)).await;
    }
    let claims = claims.as_ref().map(|axum::Extension(c)| c);
    // 读失败不能被这一页答成「没有这条任务」：404 是一个对客户端有后果的答案
    // （它据此判断那一行已经被删），而一次读不出来的库完全可能还压着这一行。
    let task = task_from_reading(state.orchestrator.get_task_checked(&id).await)?;
    match task {
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
    let page = page_limit(&query)?;
    let ready = ready_ids(&state, filter).await?;
    let tasks = task_rows_from_reading(
        state.orchestrator.get_all_tasks_checked().await,
        "task list",
    )?;
    let mut rows: Vec<Task> = tasks
        .into_iter()
        .filter(|t| {
            matches_query(&query, t)
                && matches_status(filter, ready.as_ref(), t)
                && visible_to_claims(claims, t)
        })
        .collect();
    if let Some(limit) = page {
        rows = newest_page(rows, limit);
    }
    Ok(Json(rows.into_iter().map(TaskView::from).collect()))
}

/// 一页最多几行。
///
/// 这个数不是从哪条读数推出来的，是「一个读者真的会读几条」定的：浏览器上的
/// 那一屏最多同时看得到三百行，台账读口一页五十行。超了不截、不静默降级——
/// 一个要一万行的调用方想要的不是一页，是整张表，而那张表今天九千多行、三十
/// 八兆，收到之后它只会截掉再看，字节一个没少。
// cap: fixed — 一页多大由读者一屏读得完多少定，不由表的行数定；要整张表的人要的是整张表，不是更大的一页
// keep: 删掉后一页跟着表的行数走，而不是跟着一屏读得完多少；由「要整张表的人要的是整张表」兜住
// not-a-bound: 单位换算（定义性常量：改它只改单位，不改任何阈值行为）
pub const MAX_TASKS_PER_PAGE: usize = 1000;

/// 调用方要的这一页有几行。`None` 是「不翻页」，与从前逐字相同。
///
/// 不认 `offset`：今天没有任何读者在读第二页——工作台看的是最近三百行，台账
/// 读口看的是最老的五十行且不翻页。真有人要往下走的时候，要加的不止一个
/// `offset`：还得有一条总数，因为裸数组说不出「后面还有」。
fn page_limit(query: &TaskQuery) -> Result<Option<usize>, ApiError> {
    match query.limit {
        None => Ok(None),
        Some(want) if want > MAX_TASKS_PER_PAGE => Err(ApiError::bad_request(format!(
            "limit {want} is above the page cap {MAX_TASKS_PER_PAGE}: ask for a page, not the table"
        ))),
        Some(want) => Ok(Some(want)),
    }
}

/// 最新的 `limit` 行，新的在前。
///
/// 定这个顺序是因为「前 N 行」只有在顺序定下来之后才是一个说法：从前这里是存
/// 储层给的顺序，浏览器那头照样截前三百行，屏上写的是「最近」，拿到的却是任
/// 意三行。创建时刻相同的两行按 id 排，否则同一份数据两次读到两个顺序。
fn newest_page(mut rows: Vec<Task>, limit: usize) -> Vec<Task> {
    rows.sort_by(|a, b| {
        b.created_at
            .cmp(&a.created_at)
            .then_with(|| a.id.cmp(&b.id))
    });
    rows.truncate(limit);
    rows
}

pub async fn get_ready_tasks_handler(
    State(state): State<Arc<GatewayState>>,
    claims: Option<axum::Extension<cog_core::Claims>>,
) -> Result<Json<Vec<TaskView>>, ApiError> {
    let claims = claims.as_ref().map(|axum::Extension(c)| c);
    let tasks = task_rows_from_reading(
        state.orchestrator.get_ready_tasks_checked().await,
        "ready queue",
    )?;
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

    let matched: Vec<Task> = task_rows_from_reading(
        state.orchestrator.get_all_tasks_checked().await,
        "task list",
    )?
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
    let all: Vec<Task> = task_rows_from_reading(
        state.orchestrator.get_all_tasks_checked().await,
        "task list",
    )?
    .into_iter()
    .filter(|t| visible_to_claims(claims, t))
    .collect();
    let ready: Vec<Task> = task_rows_from_reading(
        state.orchestrator.get_ready_tasks_checked().await,
        "ready queue",
    )?
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
    let tasks = related_tasks_from_reading(
        state.orchestrator.get_dependents_checked(&id).await,
        "dependents",
    )?;
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
    let tasks = related_tasks_from_reading(
        state.orchestrator.get_dependencies_checked(&id).await,
        "dependencies",
    )?;
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
    let ready = ready_ids(&state, filter).await?;
    let (tasks, edges) = graph_from_reading(state.orchestrator.get_graph_checked().await)?;
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
    let visible: Vec<Task> = task_rows_from_reading(
        state.orchestrator.get_all_tasks_checked().await,
        "task list",
    )?
    .into_iter()
    .filter(|t| visible_to_claims(claims, t))
    .collect();
    let mut task = task_to_change(&visible, &id)?.clone();
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
        crate::audit::append_or_warn(
            stream,
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
    let task = task_to_change_from_reading(
        state.orchestrator.get_all_tasks_checked().await,
        claims,
        &id,
    )?;

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
            ));
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
        crate::audit::append_or_warn(
            stream,
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

    let tasks: Vec<Task> = task_rows_from_reading(
        state.orchestrator.get_all_tasks_checked().await,
        "task list",
    )?
    .into_iter()
    .filter(|t| visible_to_claims(claims, t))
    .collect();
    let task = task_to_change(&tasks, &id)?;

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
        crate::audit::append_or_warn(
            stream,
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
    use axum::response::IntoResponse;

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

    /// 任务行带着的三格随行自己的结局现算：只有「完成且差距已空」读作 `已落`；
    /// 完成却还写着差距、失败、还在跑，都是 `未落`。没带三格的行不出这一格——
    /// 「没有差距」与「不是一条待办」在读面上是两件事。
    #[test]
    fn the_task_view_reads_the_ledger_state_from_the_rows_own_outcome() {
        use cog_core::contract::ledger_item::{LedgerItem, LedgerState, ITEM_INPUT_KEY};
        let item = LedgerItem {
            id: "a".into(),
            terminal: "树上是这个样子".into(),
            now: "现在还不是".into(),
            gap: "还差那一处".into(),
            state: LedgerState::Open,
        };
        for (status, want) in [
            (cog_core::TaskStatus::Completed, "未落"),
            (cog_core::TaskStatus::Failed, "未落"),
            (cog_core::TaskStatus::Cancelled, "未落"),
            (cog_core::TaskStatus::Running, "未落"),
        ] {
            let mut t = task("a", status, "t", 0);
            t.input = serde_json::json!({
                ITEM_INPUT_KEY: serde_json::to_value(&item).expect("待办该序列化得出来"),
            });
            let view: TaskView = t.into();
            let got = view.ledger_item.expect("带三格的行该读得出待办");
            assert_eq!(
                got.state.as_str(),
                want,
                "{status:?} 在任务读面上读错了状态"
            );
            assert_eq!(got.gap, "还差那一处", "一次运行的结局不抹掉世界上的缺口");
            let wire = serde_json::to_value(&got).expect("读面该序列化得出来");
            assert!(
                wire["state"] == "已落" || wire["state"] == "未落",
                "读面印的不是台账上的词：{wire}"
            );
        }
        // 完成且差距已空：这时才读作已落——差距是被读树的人清掉的，不是被
        // 一次运行报告成功抹掉的。
        let mut reached = item.clone();
        reached.gap = String::new();
        let mut done = task("c", cog_core::TaskStatus::Completed, "t", 0);
        done.input = serde_json::json!({
            ITEM_INPUT_KEY: serde_json::to_value(&reached).expect("待办该序列化得出来"),
        });
        let view: TaskView = done.into();
        assert_eq!(
            view.ledger_item
                .expect("带三格的行该读得出待办")
                .state
                .as_str(),
            "已落"
        );
        let bare: TaskView = task("b", cog_core::TaskStatus::Completed, "t", 0).into();
        assert!(
            bare.ledger_item.is_none(),
            "没带三格的行不该凭空长出一条待办"
        );
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

    /// 就绪队列读不出来，不是「队列是空的」。
    ///
    /// `ready` 是唯一一个没有行可以对照的筛法：它问的就是队列本身，所以空集
    /// 合正是安静时的答案。读失败被折进去时，一个读不动图的网关会把「没有活
    /// 等着」印在标签页和它上方的计数上——那是本节最不能给的数。读不出来要
    /// 拒绝，理由带着原始错误，读得出来时要交回那些 id。
    /// 一行任务读不出来时，这一编要答 503，而不是「没有这条任务」。
    ///
    /// 判据：把 `task_from_reading` 换回 `unwrap_or(None)`，第一条断言会红。
    #[test]
    fn a_single_task_that_cannot_be_read_is_not_a_task_that_does_not_exist() {
        let error = task_from_reading(Err(cog_core::SFError::Database("store unavailable".into())))
            .expect_err("读失败不许折成「没有这一行」");
        let response = error.into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(task_from_reading(Ok(None))
            .expect("读到了「没有这一行」")
            .is_none());
    }

    #[test]
    fn an_unreadable_ready_queue_is_not_an_empty_queue() {
        let unreadable: cog_core::SFResult<Vec<Task>> = Err(cog_core::SFError::Database(
            "connection reset while reading the graph".into(),
        ));
        let refusal = ready_ids_from(unreadable).expect_err("读不出来时要交回拒绝，不能交回空集合");
        assert!(
            refusal.message.contains("connection reset"),
            "拒绝的理由要带原始错误，让人看得出是读失败: {}",
            refusal.message
        );
        assert_eq!(
            refusal.into_response().status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "图读不出来是「这个部署现在回答不了」，不是一场 200"
        );

        let quiet = ready_ids_from(Ok(Vec::new())).expect("一条都没有是一份读得出来的答案");
        assert!(quiet.is_empty(), "空队列原样交回，不编造任何 id");

        let ids = ready_ids_from(Ok(vec![task(
            "t-ready",
            cog_core::TaskStatus::Pending,
            "t",
            0,
        )]))
        .expect("读得出来时要交回队列里的那些 id");
        assert_eq!(
            ids,
            HashSet::from(["t-ready".to_string()]),
            "交回的必须是队列里的 id 本身"
        );
    }

    /// 图画不出来，不是「图上什么都没有」。
    ///
    /// 图读接口交回的是一对集合，零节点零边在视图上正是一张干净的空图——
    /// 与一个刚建好的部署同形。读失败被折进去时，任务图会把「读不动」画成
    /// 「还没有任务」。读不出来要拒绝，读得出来时节点与边原样交回。
    #[test]
    fn an_unreadable_graph_read_is_not_an_empty_task_graph() {
        let unreadable: cog_core::SFResult<TaskGraphRows> = Err(cog_core::SFError::Database(
            "connection reset while reading the graph".into(),
        ));
        let refusal =
            graph_from_reading(unreadable).expect_err("读不出来时要交回拒绝，不能画成一张空图");
        assert!(
            refusal.message.contains("connection reset"),
            "拒绝的理由要带原始错误，让人看得出是读失败: {}",
            refusal.message
        );
        assert_eq!(
            refusal.into_response().status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "图读不出来是「这个部署现在回答不了」，不是一场 200"
        );

        let (nodes, edges) =
            graph_from_reading(Ok((Vec::new(), Vec::new()))).expect("空图是一份读得出来的答案");
        assert!(
            nodes.is_empty() && edges.is_empty(),
            "空图原样交回，不编造任何节点或边"
        );

        let row = task("t-node", cog_core::TaskStatus::Scheduled, "t", 0);
        let edge = ("t-upstream".to_string(), "t-node".to_string());
        let (nodes, edges) =
            graph_from_reading(Ok((vec![row], vec![edge.clone()]))).expect("读得出来时原样交回");
        assert_eq!(nodes.len(), 1, "节点是读回来的那一行");
        assert_eq!(edges, vec![edge], "边是读回来的那一条");
    }

    /// 关联名单读不出来，不是「这条任务没有关联」。
    ///
    /// 详情页把「读不出来」与「没有下游 / 没有依赖」画成同一张空列表。读失败
    /// 被折进去时，页面会替一条读都没读到的图作证「它谁也不依赖」。读不出来要
    /// 拒绝，`None` 仍是一份读得到的答案（空列表），`Some` 原样交回。
    #[test]
    fn an_unreadable_relation_list_is_not_a_task_with_no_relations() {
        for what in ["dependents", "dependencies"] {
            let unreadable: cog_core::SFResult<Option<Vec<Task>>> = Err(
                cog_core::SFError::Database("connection reset while reading the rows".into()),
            );
            let refusal = related_tasks_from_reading(unreadable, what)
                .expect_err("读不出来时要交回拒绝，不能交回一张空列表");
            assert!(
                refusal.message.contains("connection reset"),
                "拒绝的理由要带原始错误，让人看得出是读失败: {}",
                refusal.message
            );
            assert!(
                refusal.message.contains(what),
                "拒绝的理由要点名是哪一份名单读不出来: {}",
                refusal.message
            );
            assert_eq!(
                refusal.into_response().status(),
                StatusCode::SERVICE_UNAVAILABLE,
                "名单读不出来是「这个部署现在回答不了」，不是一场 200"
            );
        }

        let none = related_tasks_from_reading(Ok(None), "dependents")
            .expect("没有关联是一份读得出来的答案");
        assert!(none.is_empty(), "空名单原样交回，不编造任何行");

        let rows = related_tasks_from_reading(
            Ok(Some(vec![task(
                "t-up",
                cog_core::TaskStatus::Completed,
                "t",
                0,
            )])),
            "dependencies",
        )
        .expect("读得出来时原样交回");
        assert_eq!(rows.len(), 1, "交回的是读回来的那一行");
    }

    /// 任务表读不出来，不是「一条任务都没有」。
    ///
    /// 任务列表、它上方那张汇总、以及清理接口的预览都在这同一读上数行：
    /// 把这些数折成零，一个读不动库的部署会报告「0 条任务」「0 条就绪」「没
    /// 有什么可清理」——本节里最干净、也最不可质疑的那几个数。读不出来要
    /// 拒绝，读得出来的空行集仍是 200 的零。
    #[test]
    fn an_unreadable_task_list_is_not_an_empty_task_list() {
        for what in ["task list", "ready queue"] {
            let unreadable: cog_core::SFResult<Vec<Task>> = Err(cog_core::SFError::Database(
                "connection reset while reading the tasks".into(),
            ));
            let refusal = task_rows_from_reading(unreadable, what)
                .expect_err("读不出来时要交回拒绝，不能交回一张空表");
            assert!(
                refusal.message.contains("connection reset"),
                "拒绝的理由要带原始错误，让人看得出是读失败: {}",
                refusal.message
            );
            assert!(
                refusal.message.contains(what),
                "拒绝的理由要点名是哪一份读数读不出来: {}",
                refusal.message
            );
            assert_eq!(
                refusal.into_response().status(),
                StatusCode::SERVICE_UNAVAILABLE,
                "任务表读不出来是「这个部署现在回答不了」，不是一场 200"
            );
        }

        let rows = task_rows_from_reading(
            Ok(vec![task("t-one", cog_core::TaskStatus::Pending, "t", 0)]),
            "task list",
        )
        .expect("读得出来时照常交回");
        assert_eq!(rows.len(), 1, "交回的是读回来的那一行");

        let empty = task_rows_from_reading(Ok(Vec::new()), "task list")
            .expect("一条都没有是一份读得出来的答案");
        assert!(empty.is_empty(), "空表原样交回，不编造任何行");
    }

    /// 读不出来的任务不是「不存在的任务」。
    ///
    /// 改任务、存计划、挪列这三条写路径都在同一读上找自己要改的那一行。旧读
    /// 把库故障折成空表，于是一条真实存在的任务被告知「does not exist」——那是
    /// 一句关于这条任务的话，而实际发生的是另一件事。读不出来要 503；404 留给
    /// 「真读到了、而且在可见范围内确实没有这一行」。
    #[test]
    fn a_task_that_cannot_be_read_is_not_a_task_that_does_not_exist() {
        let unreadable: cog_core::SFResult<Vec<Task>> = Err(cog_core::SFError::Database(
            "connection refused while reading the tasks".into(),
        ));
        let refusal = task_to_change_from_reading(unreadable, None, "t-1")
            .expect_err("读不出来时不能判这条任务不存在");
        assert!(
            refusal.message.contains("connection refused"),
            "拒绝的理由要带原始错误，让人看得出是读失败: {}",
            refusal.message
        );
        let response = refusal.into_response();
        assert_eq!(
            response.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "读不出来是「这个部署现在回答不了」，不是 404"
        );

        // 真读到、但没有这一行：仍是 404——那是读到的答案。
        let missing = task_to_change_from_reading(Ok(Vec::new()), None, "t-1")
            .expect_err("读过之后确实没有这一行");
        assert_eq!(
            missing.into_response().status(),
            StatusCode::NOT_FOUND,
            "见过这一份行、里面没有它，才是 404"
        );

        // 别人的任务读得到、但不是这个调用方可见的：照旧 404，不泄露存在性。
        let scoped = claims(&["ws-a"]);
        let hidden = task_to_change_from_reading(
            Ok(vec![task("t-1", cog_core::TaskStatus::Pending, "t", 0)]),
            Some(&scoped),
            "t-1",
        )
        .expect_err("可见范围之外的行与不存在是同一种回答");
        assert_eq!(hidden.into_response().status(), StatusCode::NOT_FOUND);

        let found = task_to_change_from_reading(
            Ok(vec![task("t-1", cog_core::TaskStatus::Pending, "t", 0)]),
            None,
            "t-1",
        )
        .expect("读得出来、读得到、也看得见，就该交回那一行");
        assert_eq!(found.id, "t-1");
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

    /// 点名一组 id 就只读这几行，其余一律不出现；不给就与从前逐字相同。
    ///
    /// 对话里那份任务列表拿的是这一轮自己派的几个 id——整张表读过来只会再截掉，
    /// 而网络上一个字节都省不下。
    #[test]
    fn an_id_filter_keeps_exactly_the_rows_it_names() {
        let wanted = task("wanted-1", cog_core::TaskStatus::Running, "t", 0);
        let also = task("wanted-2", cog_core::TaskStatus::Pending, "t", 0);
        let other = task("someone-else", cog_core::TaskStatus::Pending, "t", 0);

        let named = TaskQuery {
            ids: Some("wanted-1, wanted-2".into()),
            ..Default::default()
        };
        assert!(in_ids(&named, &wanted) && in_ids(&named, &also));
        assert!(
            !in_ids(&named, &other),
            "an id that was not named must not appear in the list"
        );
        assert!(
            in_ids(&TaskQuery::default(), &other),
            "no id filter keeps every task, exactly as before"
        );
        // 一个已经不存在的 id 静默略过：列表上的一行可能已经被清理，那不是读取失败。
        assert!(!in_ids(&named, &other));
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

    /// 一页就是最新的那几行，新的在前。`age_secs` 越小的越新。
    fn at(id: &str, age_secs: i64) -> Task {
        let mut t = task(id, cog_core::TaskStatus::Pending, "t", age_secs);
        t.created_at = chrono::Utc::now() - chrono::Duration::seconds(age_secs);
        t
    }

    /// 不写 `limit` 就是从前那一份：每一行都在，顺序不动。翻页是个可选的说法，
    /// 不是给所有调用方换一种读法。
    #[test]
    fn no_limit_still_answers_the_whole_set() {
        assert_eq!(page_limit(&TaskQuery::default()).unwrap(), None);
    }

    /// 一页是「最新的 N 行」，不是存储层给的前 N 行。
    ///
    /// 从前这个截断在浏览器里做，做的方向是「把收到的数组切一段」——收到的顺序
    /// 没有任何人定义过。屏上写「最近」而拿到的是任意三行，这是这个参数在服务
    /// 端不存在时的全部后果。
    #[test]
    fn a_page_is_the_newest_rows_with_the_newest_first() {
        let rows = vec![at("old", 900), at("new", 10), at("middle", 300)];
        let page = newest_page(rows, 2);
        let ids: Vec<&str> = page.iter().map(|t| t.id.as_str()).collect();
        assert_eq!(ids, vec!["new", "middle"], "the page is the newest two");
    }

    /// 同一时刻创建的两行按 id 排。没有这一条，同一份数据两次读到两个顺序，
    /// 而「前 N 行」在两次读数之间换了内容。
    #[test]
    fn rows_created_at_the_same_instant_keep_one_order() {
        let moment = chrono::Utc::now();
        let mut a = at("a", 0);
        let mut b = at("b", 0);
        a.created_at = moment;
        b.created_at = moment;
        let first: Vec<String> = newest_page(vec![a.clone(), b.clone()], 2)
            .iter()
            .map(|t| t.id.clone())
            .collect();
        let second: Vec<String> = newest_page(vec![b, a], 2)
            .iter()
            .map(|t| t.id.clone())
            .collect();
        assert_eq!(first, second, "the same rows must read in the same order");
    }

    /// 上限之上是拒绝，不是静默截到上限。静默截断的读数是假的：读者以为要到的
    /// 是一千行，其实是整张表的前一千行，而它没有别的地方能看出这个差。
    #[test]
    fn a_limit_above_the_cap_is_refused_rather_than_silently_cut() {
        let query = TaskQuery {
            limit: Some(MAX_TASKS_PER_PAGE + 1),
            ..Default::default()
        };
        let err = page_limit(&query).expect_err("above the cap is not a page");
        assert_eq!(err.error, "BadRequest");
        assert!(
            err.message.contains(&MAX_TASKS_PER_PAGE.to_string()),
            "the refusal must name the cap; got {}",
            err.message
        );
    }

    /// 零行就是零行。让它读成「不限」是个陷阱：两种意思共用一个值，写错的那
    /// 一方永远看不出来。
    #[test]
    fn a_limit_of_zero_is_zero_rows_not_every_row() {
        let query = TaskQuery {
            limit: Some(0),
            ..Default::default()
        };
        assert_eq!(page_limit(&query).unwrap(), Some(0));
        assert!(newest_page(vec![at("a", 1), at("b", 2)], 0).is_empty());
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
        let tasks = submitted_tasks(&req, "goal-1", None);
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
                terminal: None,
                now: None,
                gap: None,
                blocked_by: vec![],
                priority: 0,
                planner: None,
                agent_id: None,
                face_id: None,
                unit_id: None,
                venue: None,
                declared_duration_seconds: None,
                renamed_timeout_seconds: None,
            }]),
            workspace_id: "ws".into(),
            priority: None,
            intent_kind: IntentKind::Goal,
        };
        let tasks = submitted_tasks(&req, "goal-9", None);
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].id, "case-1");
        assert_eq!(tasks[0].goal_id.as_deref(), Some("goal-9"));
    }

    /// 门交上来的三格落在任务的一等字段上，不留在 `input` 的开集载荷里。
    #[test]
    fn a_rows_cells_land_on_the_task_not_in_input() {
        let req = CreateTaskRequest {
            goal_id: None,
            goal: "g".into(),
            tasks: Some(vec![crate::tasks::TaskItem {
                id: "case-2".into(),
                task_type: "benchmark_case".into(),
                input: serde_json::json!({ "x": 1 }),
                terminal: Some("到终态".into()),
                now: Some("此刻".into()),
                gap: Some("缺的是：接线".into()),
                blocked_by: vec![],
                priority: 0,
                planner: None,
                agent_id: None,
                face_id: None,
                unit_id: None,
                venue: None,
                declared_duration_seconds: None,
                renamed_timeout_seconds: None,
            }]),
            workspace_id: "ws".into(),
            priority: None,
            intent_kind: IntentKind::Goal,
        };
        let tasks = submitted_tasks(&req, "goal-9", None);
        assert_eq!(tasks[0].terminal, "到终态");
        assert_eq!(tasks[0].now, "此刻");
        assert_eq!(tasks[0].gap, "缺的是：接线");
        assert!(
            tasks[0]
                .input
                .get(cog_core::contract::ledger_item::ITEM_INPUT_KEY)
                .is_none(),
            "三格不该留在 input 里"
        );
    }

    /// 提交人点名的场地要落到行上：工作台按这一列把活分开看，而这个标签没有
    /// 任何下游能重新推出来——丢了它，这条活就只出现在不带场地的总表里。
    #[test]
    fn a_row_keeps_the_venue_its_submitter_named() {
        let mut item = task_item("v-1", Some(true));
        item.venue = Some("  memory  ".into());
        let req = CreateTaskRequest {
            goal_id: None,
            goal: "fold these entries".into(),
            tasks: Some(vec![item]),
            workspace_id: "ws".into(),
            priority: None,
            intent_kind: IntentKind::Task,
        };
        let tasks = submitted_tasks(&req, "g", None);
        assert_eq!(
            tasks[0].venue.as_deref(),
            Some("memory"),
            "the label travels trimmed; a padded copy would open a second column"
        );

        let mut blank = task_item("v-2", Some(true));
        blank.venue = Some("   ".into());
        let req = CreateTaskRequest {
            goal_id: None,
            goal: "fold these entries".into(),
            tasks: Some(vec![blank]),
            workspace_id: "ws".into(),
            priority: None,
            intent_kind: IntentKind::Task,
        };
        let tasks = submitted_tasks(&req, "g", None);
        assert_eq!(
            tasks[0].venue, None,
            "a blank label is no label, not a venue whose name nobody can read"
        );
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
        let tasks = submitted_tasks(&req, "goal-empty", None);
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
            terminal: None,
            now: None,
            gap: None,
            blocked_by: vec![],
            priority: 0,
            planner: verified.map(|v| PlannerClaim {
                verified: v,
                source: Some("user_provided".into()),
            }),
            agent_id: None,
            face_id: None,
            unit_id: None,
            venue: None,
            declared_duration_seconds: None,
            renamed_timeout_seconds: None,
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

        // A row that serves no face has no binding to contradict, so naming its
        // holder is not a claim about any face. Refusing it left a unit that is
        // on no face unable to author any row at all.
        let mut own_work = task_item("t5", None);
        own_work.unit_id = Some("89".into());
        assert_eq!(binding_of(&own_work, None), Ok((None, Some("89".into()))));

        let mut nameless = task_item("t5b", None);
        nameless.unit_id = Some("  ".into());
        assert!(binding_of(&nameless, None)
            .unwrap_err()
            .contains("names an empty unit"));

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
        let tasks = submitted_tasks(&req, "g", None);
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
        let tasks = submitted_tasks(&req, "g", None);
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
        let tasks = submitted_tasks(&req, "g", None);
        assert!(
            tasks[0].action_planner_meta.is_none(),
            "a goal's rows are decomposed even when one of them claims to be verified"
        );
    }

    /// 直注的行不经过分解，没有规划器会替它写「怎样算做完」；提交者在输入里写的
    /// 那一条就该落在行上。少了这一步，行进了图也永远不被派发，而提交者读到的
    /// 是 202——一次「已受理」与一件永远不开工的事在回执上同形。
    #[test]
    fn a_verified_row_carries_the_acceptance_stated_in_its_input() {
        let mut item = task_item("t1", Some(true));
        item.input = serde_json::json!({
            "goal": "do the thing",
            "acceptance": ["接口对这条请求返回 200", "  ", "删掉这一步用例当场红"],
        });
        let req = CreateTaskRequest {
            goal_id: None,
            goal: "do the thing".into(),
            tasks: Some(vec![item]),
            workspace_id: "ws".into(),
            priority: None,
            intent_kind: IntentKind::Task,
        };
        let tasks = submitted_tasks(&req, "g", None);
        assert_eq!(
            tasks[0].acceptance,
            vec![
                "接口对这条请求返回 200".to_string(),
                "删掉这一步用例当场红".to_string()
            ],
            "the criteria the submitter stated are the row's criteria; a blank \
             entry is not one of them"
        );
    }

    /// 提交者声明的时长要真的落到行上。行上是**声明**，不是刀：回收读的是租约
    /// 和实测的步间窗口，这个数只喂给台账里「过了自己声明的时长」那条读数。丢掉
    /// 它不会砍行，但会让那条读数对着一个别人编的值说话。
    #[test]
    fn a_stated_duration_lands_on_the_row() {
        let mut item = task_item("t1", Some(true));
        item.declared_duration_seconds = Some(7 * 24 * 60 * 60);
        let req = CreateTaskRequest {
            goal_id: None,
            goal: "hold this for a while".into(),
            tasks: Some(vec![item]),
            workspace_id: "ws".into(),
            priority: None,
            intent_kind: IntentKind::Task,
        };
        let tasks = submitted_tasks(&req, "g", None);
        assert_eq!(
            tasks[0].declared_duration_seconds,
            7 * 24 * 60 * 60,
            "the submitter's declaration is the one the ledger reading will compare"
        );
    }

    /// 没声明时长的行保持原来的默认值：这条口子不许把「没说话」读成「零」。
    #[test]
    fn a_row_that_states_no_duration_keeps_the_executor_default() {
        let req = CreateTaskRequest {
            goal_id: None,
            goal: "one round".into(),
            tasks: Some(vec![task_item("t1", Some(true))]),
            workspace_id: "ws".into(),
            priority: None,
            intent_kind: IntentKind::Task,
        };
        let tasks = submitted_tasks(&req, "g", None);
        assert_eq!(tasks[0].declared_duration_seconds, 300);
    }

    /// 0 秒不是「不限时」：它声明这件活不花时间，台账里那条读数就会在行刚开时
    /// 说它已经过了自己的声明。这一条要在门口点名，而不是收下以后让读数去说。
    #[test]
    fn a_zero_duration_is_named_at_the_door() {
        let mut item = task_item("t1", Some(true));
        item.declared_duration_seconds = Some(0);
        let req = CreateTaskRequest {
            goal_id: None,
            goal: "no limit".into(),
            tasks: Some(vec![item]),
            workspace_id: "ws".into(),
            priority: None,
            intent_kind: IntentKind::Task,
        };
        assert_eq!(row_with_zero_duration(&req), Some("t1"));
    }

    /// 旧名必须被门口认出来、回 400 点名新名。serde 默认会把未知键静静丢掉，
    /// 于是提交者写 `timeout_seconds` 的行会落回默认值，而他读到的是一个 202。
    #[test]
    fn the_old_budget_name_is_named_at_the_door_not_dropped() {
        let body = serde_json::json!({
            "goal": "do the thing",
            "workspace_id": "ws",
            "intent_kind": "task",
            "tasks": [{
                "id": "t1",
                "task_type": "benchmark_case",
                "input": { "x": 1 },
                "timeout_seconds": 3600,
            }],
        });
        let req: CreateTaskRequest = serde_json::from_value(body).unwrap();
        assert_eq!(row_with_the_old_duration_name(&req), Some("t1"));
        assert!(
            row_with_zero_duration(&req).is_none(),
            "旧名不是零值错误：两条拒绝各有各的码"
        );

        let body = serde_json::json!({
            "goal": "do the thing",
            "workspace_id": "ws",
            "intent_kind": "task",
            "tasks": [{
                "id": "t1",
                "task_type": "benchmark_case",
                "input": { "x": 1 },
                "declared_duration_seconds": 3600,
            }],
        });
        let req: CreateTaskRequest = serde_json::from_value(body).unwrap();
        assert!(row_with_the_old_duration_name(&req).is_none());
        let tasks = submitted_tasks(&req, "g", None);
        assert_eq!(
            tasks[0].declared_duration_seconds, 3600,
            "新名是收的，而且落下的是提交者说的那个数"
        );
    }

    /// 一条什么都不说的直注行在门口就被点名，而不是被收下以后静静躺在板上。
    #[test]
    fn a_verified_row_that_states_no_acceptance_is_named_at_the_door() {
        let submission = |items: Vec<TaskItem>| CreateTaskRequest {
            goal_id: None,
            goal: "do the thing".into(),
            tasks: Some(items),
            workspace_id: "ws".into(),
            priority: None,
            intent_kind: IntentKind::Task,
        };
        let mut stated = task_item("stated", Some(true));
        stated.input = serde_json::json!({ "acceptance": ["有产出"] });
        // 说过的行不被误伤：只有什么都不说的那一行该被点出来，而且点的是它。
        assert_eq!(
            injected_row_without_acceptance(&submission(vec![stated])),
            None
        );
        assert_eq!(
            injected_row_without_acceptance(&submission(vec![
                task_item("silent-1", Some(true)),
                task_item("silent-2", Some(true)),
            ])),
            Some("silent-1")
        );
    }

    /// 提交人跟着任务行走，而且只从令牌来。
    ///
    /// 这一格决定谁的当日额度，所以「没有令牌」必须读作「没有这个人」而不是某个
    /// 默认名：编一个出来会让平台自己的活记到一个不存在的人头上，比不记更难查。
    #[test]
    fn a_submitted_row_carries_the_person_the_token_named() {
        let req = CreateTaskRequest {
            goal_id: None,
            goal: "do the thing".into(),
            tasks: Some(vec![task_item("t1", None)]),
            workspace_id: "ws".into(),
            priority: None,
            intent_kind: IntentKind::Task,
        };
        let named = submitted_tasks(&req, "goal-1", Some("u-1"));
        assert_eq!(named[0].submitted_by.as_deref(), Some("u-1"));
        let anonymous = submitted_tasks(&req, "goal-1", None);
        assert_eq!(anonymous[0].submitted_by, None);
    }

    /// 提交面把「这件活交什么」在门口落成一份带来源的记录。
    ///
    /// 提交人从令牌来，只有门口这个时刻同时知道声明与提交人；过了这一格再想补，
    /// 就只能编一个名字或者留空。说了的行记成 `stated`，没说的行记成 `unstated`
    /// ——「没人说」不在这里被推成一个默认的交付物。
    #[test]
    fn a_submitted_row_settles_what_it_delivers_with_its_source() {
        let mut stated = task_item("stated", None);
        stated.input = serde_json::json!({
            cog_core::types::task::INPUT_DELIVERS: "judgement",
            // 调用方自带一份「谁说的」：门口必须丢掉它，按令牌重新落定。
            cog_core::types::task::INPUT_DELIVERABLE_RECORD: {
                "state": "stated",
                "conclusion": "answer",
                "by": "someone-else",
                "via": "delivers",
            },
        });
        let req = CreateTaskRequest {
            goal_id: None,
            goal: "judge the change".into(),
            tasks: Some(vec![stated, task_item("silent", None)]),
            workspace_id: "ws".into(),
            priority: None,
            intent_kind: IntentKind::Task,
        };
        let rows = submitted_tasks(&req, "goal-1", Some("u-1"));

        assert_eq!(
            rows[0].deliverable_declaration(),
            cog_core::DeliverableDeclaration::Stated {
                conclusion: cog_core::Deliverable::Judgement,
                by: Some("u-1".into()),
                via: cog_core::StatementVia::Delivers,
            }
        );
        assert_eq!(rows[0].deliverable(), cog_core::Deliverable::Judgement);

        assert_eq!(
            rows[1].deliverable_declaration(),
            cog_core::DeliverableDeclaration::Unstated
        );
        assert!(rows[1]
            .input
            .get(cog_core::types::task::INPUT_DELIVERABLE_RECORD)
            .is_some());
    }

    /// 目标提交不受这条约束：它交给分解，验收标准由规划给出，提交时不要求写。
    #[test]
    fn a_goal_submission_is_not_asked_for_acceptance() {
        let req = CreateTaskRequest {
            goal_id: None,
            goal: "plan this".into(),
            tasks: Some(vec![task_item("t1", Some(true))]),
            workspace_id: "ws".into(),
            priority: None,
            intent_kind: IntentKind::Goal,
        };
        assert_eq!(injected_row_without_acceptance(&req), None);
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
        t.declared_duration_seconds = 900;
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
        assert_eq!(view.declared_duration_seconds, 900);
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
