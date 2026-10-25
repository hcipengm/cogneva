//! 项目面：把任务图按 goal 根折成项目。
//!
//! 一个项目就是一个 goal 根连同它的后代。这里**不建第二张表**——范围、执行态、
//! 时间都是任务图的投影，读的时候现算。另建一张项目表会立刻长出第二份任务清单，
//! 两边各自自洽、互不校验，读出来都像答案。
//!
//! 三个读面回答三个不同的问题：
//!   * `/api/v1/projects`        有哪些项目、各自积压多少；
//!   * `/api/v1/projects/{id}/board`  这些活在六个状态上怎么分布；
//!   * `/api/v1/projects/{id}/gantt`  各自什么时候开始、什么时候动过。
//!
//! 每个响应都带 `source`：读的是哪张表、哪一刻读的。界面上的每个数字都要能点开
//! 来路，缺哪一件就写缺哪一件——一个说不出自己从哪来的数字，读者只能选择相信或
//! 不信，而这两种都不是读数的用法。

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::{Extension, Json};
use serde::Serialize;

use cog_core::{Task, TaskStatus};

use crate::error::ApiError;
use crate::tasks::visible_to_claims;
use crate::GatewayState;

/// 没有 `goal_id` 的任务归到这里。它是一个**具名**的桶，不是被丢掉的那些：
/// 把无归属的任务悄悄排除，全部项目加起来会小于总数，而那个差没有任何地方会说。
pub const UNGROUPED: &str = cog_core::UNGROUPED_PROJECT;

#[derive(Debug, Serialize)]
pub struct Source {
    /// 读的载体。项目面没有自己的表，读的是任务图。
    pub table: &'static str,
    pub reader: &'static str,
    pub read_at: String,
}

#[derive(Debug, Serialize)]
pub struct StatusCount {
    pub status: String,
    pub count: usize,
}

#[derive(Debug, Serialize)]
pub struct ProjectRow {
    pub id: String,
    /// `goal` 或 `ungrouped`。前端据此决定这一行能不能点进任务图。
    pub kind: &'static str,
    pub tasks: usize,
    /// 可被调度的任务数。与 `tasks` 的差是那些只做层级与查询的占位行——
    /// 把两者混成一个数，读者会把「没有活可干」读成「活都干完了」。
    pub executables: usize,
    pub by_status: Vec<StatusCount>,
    /// 依赖还没满足的任务数。
    pub blocked: usize,
    pub first_created_at: Option<String>,
    pub last_updated_at: Option<String>,
    /// 有没有计划基线。基线表还没落地，所以今天是恒 false——这不是「没有项目
    /// 需要排期」，是「排期这一件还读不到」，两者在界面上必须分开写。
    pub planned: bool,
}

#[derive(Debug, Serialize)]
pub struct ProjectsResponse {
    pub projects: Vec<ProjectRow>,
    /// 读者可见的任务总数。它必须与各行之和相等，否则有任务落在了所有桶之外。
    pub total_tasks: usize,
    pub source: Source,
}

#[derive(Debug, Serialize)]
pub struct BoardColumn {
    pub status: String,
    pub count: usize,
    /// 这一列的通行上限。`None` 表示**没有配**，不是「不限」也不是「没超」——
    /// 把「没配上限」印成 `over_limit: false`，读者会以为上限正在生效。
    pub wip_limit: Option<usize>,
    pub over_limit: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct BoardResponse {
    pub project_id: String,
    pub columns: Vec<BoardColumn>,
    pub total: usize,
    pub source: Source,
}

#[derive(Debug, Serialize)]
pub struct GanttRow {
    pub id: String,
    pub title: String,
    pub status: String,
    pub created_at: String,
    pub started_at: Option<String>,
    pub updated_at: String,
    /// 计划基线。没有基线时为 `None` 且 `unplanned` 为真；界面据此画成
    /// 「未排期」，不许画成零长度条——一根零长度的条会被读成「计划已完成」。
    pub planned_start: Option<String>,
    pub planned_end: Option<String>,
    pub unplanned: bool,
}

#[derive(Debug, Serialize)]
pub struct GanttResponse {
    pub project_id: String,
    /// 这个项目有没有任何一条带基线的任务。全都没有时界面直接说「未排期」，
    /// 而不是把一屏空白当成正常。
    pub has_plan: bool,
    /// 读计划基线失败时的原因；读到了就是 `None`。
    ///
    /// 它存在是因为「读不到」和「没排期」在行上长得一模一样（都是两个计划字段
    /// 为空、`unplanned` 为真）。不给这个字段，一个存储读不了基线的部署会把整屏
    /// 画成「都还没排期」，而真相是没人问得出。判读顺序：先看这里，再看行。
    pub plan_error: Option<String>,
    pub rows: Vec<GanttRow>,
    pub source: Source,
}

/// 挣值的一个量。
///
/// `value` 与 `unmeasurable` 是互斥的两态，不是「有值 + 备注」：一个量不出来
/// 的绩效如果印成 0，读者会把它当成「绩效为零」——成本为零是好消息，计划价值
/// 为零是没开工，两者都与「还没法算」差得很远。
#[derive(Debug, Serialize, PartialEq)]
pub struct EvmReading {
    pub value: Option<u64>,
    pub unmeasurable: Option<String>,
    /// 这个数怎么来的，一句给人读的话。
    pub basis: String,
}

impl EvmReading {
    fn measured(value: u64, basis: impl Into<String>) -> Self {
        Self {
            value: Some(value),
            unmeasurable: None,
            basis: basis.into(),
        }
    }

    fn unmeasurable(reason: impl Into<String>) -> Self {
        Self {
            value: None,
            unmeasurable: Some(reason.into()),
            basis: String::new(),
        }
    }
}

/// 实际成本的归因面：成本只在账上带任务身份的那些行里，剩下的单独报。
///
/// 每一格都是 `Option`：`None` 是「这一格没量过」，不是零。这个区分在这里比在别处
/// 更重要——一屏挣值里有好几处零，而「有多少条任务在挣值之外」写成 0 是在断言
/// 「一条都没有」，与「基线读不到」正好相反。
#[derive(Debug, Serialize, PartialEq)]
pub struct EvmAttribution {
    /// 项目里账上有花销记录的任务条数；逐笔账读不到时为 `None`。
    pub tasks_with_cost: Option<usize>,
    /// 有基线预算、账上却一条花销都没有的**已完成**任务条数。它不是「免费」，
    /// 是这笔账上看不到它——两个都不该被读成省下来的钱。基线或账读不到时为 `None`。
    pub baselined_tasks_without_cost: Option<usize>,
    /// 项目里没有基线的任务条数：它们的预算不在挣值里，所以挣值必然少于项目
    /// 的全部工作量。不报这个数，读者会把「挣值还小」读成「干得少」。
    /// 基线读不到时为 `None`——那时一条也判不出在不在基线之外。
    pub tasks_outside_baseline: Option<usize>,
}

#[derive(Debug, Serialize)]
pub struct EvmResponse {
    pub project_id: String,
    /// 成本的单位。三个量同一把尺子，不引入第二套计价。
    pub unit: &'static str,
    pub planned_value: EvmReading,
    pub earned_value: EvmReading,
    /// 整个项目的花销。它不是成本绩效的分母——分母只取有基线的那一部分，
    /// 见 [`EvmResponse::baselined_actual_cost`]。
    pub actual_cost: EvmReading,
    /// 有基线的那些任务花掉的 token。**与挣值覆盖同一批任务**，所以它才是
    /// 成本绩效的分母。
    ///
    /// 拿项目总花销当分母会把两个总体相除：一条基线加一百条没基线的任务时，
    /// 挣值只认那一条，成本认全部——比值被没参与挣值的那部分压低，看起来像
    /// 「花得多挣得少」，而没有任何一项测量支持这个结论。
    pub baselined_actual_cost: EvmReading,
    /// 成本绩效指数 = 挣值 / 有基线任务的实际成本。任一量不出来时为 `None`。
    pub cost_performance_index: Option<f64>,
    /// 进度绩效指数 = 挣值 / 计划价值。
    pub schedule_performance_index: Option<f64>,
    /// 同一窗口里**没有**任务身份的用量。
    ///
    /// 它不属于任何一个项目，所以不并进上面的成本，也不折成 0——那样成本绩效
    /// 会把归因缺口读成省钱。`None` 表示这笔账读不到。
    ///
    /// 这里**只报数，不报比例**：这条账没有项目维度，任何「本项目 X% 的成本
    /// 看不到出处」的分母都是本项目而分子是全平台，项目越小越接近 100%。
    pub unattributed_tokens: Option<u64>,
    /// 上面那个数的统计窗口**起点**，终点就是 `source.read_at`。少了它，这个数
    /// 会被当成常数带走。
    pub unattributed_window_start: Option<String>,
    pub attribution: EvmAttribution,
    /// 算这三个量时用的假设，明写出来。
    pub assumption: String,
    pub source: Source,
}

/// 任务图的正本表名。项目面自己不带表，读的就是它。
const TASK_TABLE: &str = "cog_dag_tasks";

/// 实际成本的出处：逐笔 token 账。与配额、告警读的是同一张，所以三个量同一把尺子。
const USAGE_TABLE: &str = "gateway_llm_usage";

/// 挣值一次要读三张表，`source.table` 把三张都写出来。
const EVM_TABLE: &str = "cog_dag_tasks + task_plan + gateway_llm_usage";

/// 甘特面要多读一张：计划基线。两个表名一起写进 `source`，读者才知道这一屏
/// 是「现状 + 当初的计划」两份读数拼出来的。
const GANTT_TABLE: &str = "cog_dag_tasks + task_plan";

fn now_source(reader: &'static str) -> Source {
    Source {
        table: TASK_TABLE,
        reader,
        read_at: chrono::Utc::now().to_rfc3339(),
    }
}

/// 取读者能看到的全部任务。可见性判定与任务面同一个函数——项目面不该有自己
/// 一套隔离规则，两套规则迟早会不一致，而不一致的那一天，一边会漏读。
async fn visible_tasks(state: &Arc<GatewayState>, claims: Option<&cog_core::Claims>) -> Vec<Task> {
    state
        .orchestrator
        .get_all_tasks()
        .await
        .into_iter()
        .filter(|t| visible_to_claims(claims, t))
        .collect()
}

/// 读这个部署里的全部计划基线，读不到时返回 `None` 而不是空表。
///
/// 空表的意思是「这些任务都没排过期」，读不到的意思是「没人问得出」。两个都
/// 折成空表，甘特与项目列表就会把整个部署的排期状态报成一片空白。
async fn readable_plans(
    orchestrator: &Arc<dyn cog_core::OrchestratorControl>,
) -> Option<BTreeMap<String, cog_core::TaskPlan>> {
    match orchestrator.task_plans().await {
        Ok(v) => Some(v.into_iter().map(|p| (p.task_id.clone(), p)).collect()),
        Err(e) => {
            tracing::warn!(error = %e, "plan baselines are not readable on this deployment");
            None
        }
    }
}

fn project_key(task: &Task) -> &str {
    // 项目身份只有一处定义：发布侧的列上限与这里的看板读面必须按同一个 id
    // 记账，否则某项目的上限按 A 记、超限按 B 报。
    task.project_key()
}

/// 一条任务在界面上叫什么。任务没有独立的名字列，标题只能从输入里取；取不到就
/// 退回任务 id——编一个名字比露出 id 更坏，读者会把编出来的当成真的。
fn task_title(task: &Task) -> String {
    for key in ["name", "task", "description", "goal"] {
        if let Some(v) = task.input.get(key).and_then(|v| v.as_str()) {
            let v = v.trim();
            if !v.is_empty() {
                return v.chars().take(120).collect();
            }
        }
    }
    task.id.clone()
}

fn status_counts(tasks: &[&Task]) -> Vec<StatusCount> {
    let mut counts: BTreeMap<&'static str, usize> = BTreeMap::new();
    for t in tasks {
        *counts.entry(t.status.as_str()).or_insert(0) += 1;
    }
    // 六态固定顺序输出：同一份数据两次读出的行要逐字相同，界面才不会跳。
    TaskStatus::ALL
        .iter()
        .map(|s| StatusCount {
            status: s.as_str().to_string(),
            count: counts.get(s.as_str()).copied().unwrap_or(0),
        })
        .collect()
}

/// 把一批任务折成项目行。与 handler 分开是为了能被测试直接喂数据——一个只能
/// 通过起整套网关才能验证的投影，实际上不会被验证。
/// `plans` 为 `None` 表示这一部署读不到基线，此时每行的 `planned` 只能是
/// `false`——与「确实没排期」同形，所以调用方要在响应头把原因写出来。
fn build_project_rows(
    tasks: &[Task],
    plans: Option<&BTreeMap<String, cog_core::TaskPlan>>,
) -> Vec<ProjectRow> {
    // 谁已经完成，用来判「这条任务的依赖都满足了吗」。判据与 `Task::is_ready`
    // 同一套：每条依赖都完成才放开，依赖不在这一批里也算没放开——看不到的依赖
    // 不能当成已完成，那会把一条卡住的任务读成可以开跑。
    let done: BTreeMap<&str, bool> = tasks
        .iter()
        .map(|t| (t.id.as_str(), t.status == TaskStatus::Completed))
        .collect();
    let blocked_count = |rows: &[&Task]| -> usize {
        rows.iter()
            .filter(|t| {
                !t.blocked_by.is_empty()
                    && !t
                        .blocked_by
                        .iter()
                        .all(|d| done.get(d.as_str()).copied().unwrap_or(false))
            })
            .count()
    };

    let mut grouped: BTreeMap<String, Vec<&Task>> = BTreeMap::new();
    for t in tasks {
        grouped
            .entry(project_key(t).to_string())
            .or_default()
            .push(t);
    }

    let mut projects: Vec<ProjectRow> = grouped
        .iter()
        .map(|(id, rows)| {
            let mut first: Option<&chrono::DateTime<chrono::Utc>> = None;
            let mut last: Option<&chrono::DateTime<chrono::Utc>> = None;
            for t in rows {
                first = Some(match first {
                    Some(f) if f <= &t.created_at => f,
                    _ => &t.created_at,
                });
                last = Some(match last {
                    Some(l) if l >= &t.updated_at => l,
                    _ => &t.updated_at,
                });
            }
            ProjectRow {
                id: id.clone(),
                kind: if id == UNGROUPED { "ungrouped" } else { "goal" },
                tasks: rows.len(),
                executables: rows.iter().filter(|t| t.is_executable).count(),
                by_status: status_counts(rows),
                blocked: blocked_count(rows),
                first_created_at: first.map(|d| d.to_rfc3339()),
                last_updated_at: last.map(|d| d.to_rfc3339()),
                planned: plans
                    .map(|p| rows.iter().any(|t| p.contains_key(&t.id)))
                    .unwrap_or(false),
            }
        })
        .collect();

    // 任务多的排前面，同数按 id 升序：顺序稳定，读者两次刷新能对上。
    projects.sort_by(|a, b| b.tasks.cmp(&a.tasks).then_with(|| a.id.cmp(&b.id)));
    projects
}

pub async fn list_projects_handler(
    State(state): State<Arc<GatewayState>>,
    claims: Option<Extension<cog_core::Claims>>,
) -> Result<Json<ProjectsResponse>, ApiError> {
    let claims = claims.as_ref().map(|Extension(c)| c);
    let tasks = visible_tasks(&state, claims).await;
    let total_tasks = tasks.len();
    let plans = readable_plans(&state.orchestrator).await;
    Ok(Json(ProjectsResponse {
        projects: build_project_rows(&tasks, plans.as_ref()),
        total_tasks,
        source: now_source("in-memory task graph"),
    }))
}

fn tasks_of_project<'a>(tasks: &'a [Task], id: &str) -> Vec<&'a Task> {
    tasks.iter().filter(|t| project_key(t) == id).collect()
}

fn build_board_columns(rows: &[&Task], limits: &cog_core::BoardWipLimits) -> Vec<BoardColumn> {
    status_counts(rows)
        .into_iter()
        .map(|c| {
            // 没配上限的列两格都是 `None`：印成 0 会让「没有上限」看起来像
            // 「上限为零」，印成 `false` 会让「没有东西在拦」读成「拦过、没超」。
            let limit = limits.limit_for(&c.status);
            BoardColumn {
                wip_limit: limit,
                // 满了就拦：到达上限这一刻，发布侧已经不再往这一列放新活。读面
                // 与拦住它的那道判据用同一个比较，读数才不会说「未超」而派发
                // 已经被拦下。
                over_limit: limit.map(|limit| c.count >= limit),
                status: c.status,
                count: c.count,
            }
        })
        .collect()
}

fn build_gantt_rows(
    rows: &[&Task],
    plans: Option<&BTreeMap<String, cog_core::TaskPlan>>,
) -> Vec<GanttRow> {
    let mut out: Vec<GanttRow> = rows
        .iter()
        .map(|t| {
            // 基线按 (workspace, task) 存的，任务行这边只按 id 取：同一个 id 在同一
            // 张任务图里唯一，而读面只看得到这一张图。
            let plan = plans.and_then(|p| p.get(&t.id));
            GanttRow {
                id: t.id.clone(),
                title: task_title(t),
                status: t.status.as_str().to_string(),
                created_at: t.created_at.to_rfc3339(),
                started_at: t.started_at.map(|d| d.to_rfc3339()),
                updated_at: t.updated_at.to_rfc3339(),
                planned_start: plan.and_then(|p| p.planned_start).map(|d| d.to_rfc3339()),
                planned_end: plan.and_then(|p| p.planned_end).map(|d| d.to_rfc3339()),
                // 有没有这一行基线，就是「排没排期」。只有一端有估算的基线仍然算
                // 排过期：起算时刻是知道的，止期未知不等于没有计划。
                unplanned: plan.is_none(),
            }
        })
        .collect();
    out.sort_by(|a, b| {
        a.created_at
            .cmp(&b.created_at)
            .then_with(|| a.id.cmp(&b.id))
    });
    out
}

pub async fn project_board_handler(
    State(state): State<Arc<GatewayState>>,
    claims: Option<Extension<cog_core::Claims>>,
    Path(id): Path<String>,
) -> Result<Json<BoardResponse>, ApiError> {
    let claims = claims.as_ref().map(|Extension(c)| c);
    let tasks = visible_tasks(&state, claims).await;
    let rows = tasks_of_project(&tasks, &id);

    // 项目不存在与项目为空是两件事：前者是路径写错了，后者是这个 goal 还没
    // 分解出任何任务。都返回空列的话，读者分不出该去改 URL 还是该去看分解。
    if rows.is_empty() && id != UNGROUPED {
        return Err(ApiError::not_found(format!("project {id}")));
    }

    // 上限是本进程的环境配置，与发布侧读的是同一个变量、同一个解析：看板上
    // 写着的上限，就是拦住派发的那个数。
    let (limits, problems) = cog_core::BoardWipLimits::from_env();
    for problem in problems {
        tracing::warn!(
            variable = cog_core::config::BOARD_WIP_LIMITS_ENV,
            problem = %problem,
            "board WIP limit entry ignored"
        );
    }
    let columns = build_board_columns(&rows, &limits);

    Ok(Json(BoardResponse {
        project_id: id,
        columns,
        total: rows.len(),
        source: now_source("in-memory task graph"),
    }))
}

pub async fn project_gantt_handler(
    State(state): State<Arc<GatewayState>>,
    claims: Option<Extension<cog_core::Claims>>,
    Path(id): Path<String>,
) -> Result<Json<GanttResponse>, ApiError> {
    let claims = claims.as_ref().map(|Extension(c)| c);
    let tasks = visible_tasks(&state, claims).await;
    let rows = tasks_of_project(&tasks, &id);

    if rows.is_empty() && id != UNGROUPED {
        return Err(ApiError::not_found(format!("project {id}")));
    }

    // 基线读不到时不是「没有基线」：把原因带回去，行才不会被整屏读成未排期。
    let plans = readable_plans(&state.orchestrator).await;
    Ok(Json(build_gantt_response(id, &rows, plans.as_ref())))
}

pub async fn project_evm_handler(
    State(state): State<Arc<GatewayState>>,
    claims: Option<Extension<cog_core::Claims>>,
    Path(id): Path<String>,
) -> Result<Json<EvmResponse>, ApiError> {
    let claims = claims.as_ref().map(|Extension(c)| c);
    let tasks = visible_tasks(&state, claims).await;
    let rows = tasks_of_project(&tasks, &id);

    if rows.is_empty() && id != UNGROUPED {
        return Err(ApiError::not_found(format!("project {id}")));
    }

    let plans = readable_plans(&state.orchestrator).await;
    let now = chrono::Utc::now();

    // 成本从逐笔账按任务取。账没接上时 `spends` 与 `unattributed` 都是 `None`，
    // 于是成本这一列写「量不了」——比 0 诚实：0 会让成本绩效好看得不像真的。
    let (spends, unattributed) = match state.llm_usage.as_ref() {
        Some(store) => {
            let ids: Vec<String> = rows.iter().map(|t| t.id.clone()).collect();
            let spends = match store.spend_by_task(&ids).await {
                Ok(v) => Some(v.into_iter().collect::<BTreeMap<String, u64>>()),
                Err(e) => {
                    tracing::warn!(error = %e, "evm: task spend is not readable from the ledger");
                    None
                }
            };
            // 归因缺口的窗口：从这些任务最早的那一刻（有基线就用计划起点）到现在。
            // 账上没有任务身份的行无法归属到任何项目，只能报这个窗口里的总量。
            let since = evm_window_start(&rows, plans.as_ref());
            let unattributed = match store.unattributed_between(since, now).await {
                Ok(v) => Some(v),
                Err(e) => {
                    tracing::warn!(error = %e, "evm: unattributed spend is not readable");
                    None
                }
            };
            (spends, unattributed)
        }
        None => (None, None),
    };

    Ok(Json(build_evm_response(
        id,
        &rows,
        plans.as_ref(),
        spends.as_ref(),
        unattributed,
        now,
    )))
}

/// 归因缺口的统计窗口起点：这一批任务里最早的那一刻。
///
/// 有基线时用计划起点（这批活「本来」从那时开始），没有就用创建时刻。比这个
/// 更早的无归因用量与这个项目无关，算进来只会把缺口报大。
fn evm_window_start(
    rows: &[&Task],
    plans: Option<&BTreeMap<String, cog_core::TaskPlan>>,
) -> chrono::DateTime<chrono::Utc> {
    let mut start = rows
        .iter()
        .map(|t| t.created_at)
        .min()
        .unwrap_or_else(chrono::Utc::now);
    if let Some(plans) = plans {
        for t in rows {
            if let Some(planned) = plans.get(&t.id).and_then(|p| p.planned_start) {
                start = start.min(planned);
            }
        }
    }
    start
}

/// 挣值响应的拼装与 handler 分开：三个量的算法是这一面唯一的实质，而它只有在
/// 能被直接喂数据时才验得了。
fn build_evm_response(
    project_id: String,
    rows: &[&Task],
    plans: Option<&BTreeMap<String, cog_core::TaskPlan>>,
    spends: Option<&BTreeMap<String, u64>>,
    unattributed: Option<u64>,
    now: chrono::DateTime<chrono::Utc>,
) -> EvmResponse {
    let spent: Option<std::collections::BTreeSet<&str>> =
        spends.map(|s| s.keys().map(String::as_str).collect());
    // 账读不到时这一格也不算：0 会被读成「一条都没有花过钱」。
    let tasks_with_cost = spent.as_ref().map(|spent| {
        rows.iter()
            .filter(|t| spent.contains(t.id.as_str()))
            .count()
    });

    let (planned_value, earned_value, baselined, attribution) = match plans {
        None => (
            EvmReading::unmeasurable("计划基线读不到：这一部署回答不了「本来该完成多少」"),
            EvmReading::unmeasurable("计划基线读不到：挣值没有分母可比"),
            Vec::new(),
            EvmAttribution {
                tasks_with_cost,
                // 基线读不到时一条也判不出在不在基线之外：写 0 是断言「全都有基线」。
                baselined_tasks_without_cost: None,
                tasks_outside_baseline: None,
            },
        ),
        Some(plans) => {
            let baselined: Vec<(&Task, &cog_core::TaskPlan)> = rows
                .iter()
                .filter_map(|t| plans.get(&t.id).map(|p| (*t, p)))
                .collect();
            let outside = rows.len() - baselined.len();
            if baselined.is_empty() {
                (
                    EvmReading::unmeasurable(
                        "这个项目里没有一条任务有基线：不是进度为零，是没排过期",
                    ),
                    EvmReading::unmeasurable("没有基线就没有可挣的计划价值"),
                    baselined,
                    EvmAttribution {
                        tasks_with_cost,
                        baselined_tasks_without_cost: spent.as_ref().map(|_| 0),
                        tasks_outside_baseline: Some(outside),
                    },
                )
            } else {
                let (pv, with_duration, without_duration) = planned_value_at(&baselined, now);
                let ev: u64 = baselined
                    .iter()
                    .filter(|(t, _)| t.status == TaskStatus::Completed)
                    .map(|(_, p)| p.budget_tokens)
                    .sum();
                let completed = baselined
                    .iter()
                    .filter(|(t, _)| t.status == TaskStatus::Completed)
                    .count();
                (
                    EvmReading::measured(
                        pv,
                        format!(
                            "{with_duration} 条带时长的基线按线性比例折算到此刻\
                             （无工作日历）；{without_duration} 条只有起期、时长未知，不计入"
                        ),
                    ),
                    EvmReading::measured(
                        ev,
                        format!(
                            "{completed} 条已完成任务的基线预算之和，共 {} 条有基线；\
                             另外 {outside} 条没有基线，不在挣值里",
                            baselined.len()
                        ),
                    ),
                    baselined.clone(),
                    EvmAttribution {
                        tasks_with_cost,
                        baselined_tasks_without_cost: spent.as_ref().map(|spent| {
                            baselined
                                .iter()
                                .filter(|(t, _)| {
                                    !spent.contains(t.id.as_str())
                                        && t.status == TaskStatus::Completed
                                })
                                .count()
                        }),
                        tasks_outside_baseline: Some(outside),
                    },
                )
            }
        }
    };

    let actual_cost = match spends {
        None => EvmReading::unmeasurable(format!(
            "逐笔 token 账读不到（`{USAGE_TABLE}` 没有接上这一部署）：成本量不出来"
        )),
        Some(spends) => {
            let total: u64 = rows.iter().filter_map(|t| spends.get(&t.id).copied()).sum();
            EvmReading::measured(
                total,
                format!(
                    "整个项目 {} 条任务在 `{USAGE_TABLE}` 上按 task_id 聚合的花销；\
                     失败调用也计（token 已经烧掉）",
                    spends.len()
                ),
            )
        }
    };

    // 分母与挣值覆盖同一批任务。项目总花销里有一部分买了没有预算的活，把它算进
    // 分母就是这样两个总体相除，比值被没参与挣值的那部分压低。
    let baselined_actual_cost = match spends {
        None => EvmReading::unmeasurable(format!(
            "逐笔 token 账读不到（`{USAGE_TABLE}` 没有接上这一部署）：成本量不出来"
        )),
        Some(spends) => {
            let total: u64 = baselined
                .iter()
                .filter_map(|(t, _)| spends.get(&t.id).copied())
                .sum();
            EvmReading::measured(
                total,
                format!(
                    "有基线的 {} 条任务的花销，与挣值覆盖同一批任务（整个项目的花销见 actual_cost）",
                    baselined.len()
                ),
            )
        }
    };

    let cost_performance_index = ratio(earned_value.value, baselined_actual_cost.value);
    let schedule_performance_index = ratio(earned_value.value, planned_value.value);

    EvmResponse {
        project_id,
        unit: "token",
        planned_value,
        earned_value,
        actual_cost,
        baselined_actual_cost,
        cost_performance_index,
        schedule_performance_index,
        unattributed_tokens: unattributed,
        // 只在真读到缺口时给窗口：量不出来的时候给一个窗口，会让读者以为
        // 「这个窗口里一条无归因用量都没有」。
        unattributed_window_start: unattributed.map(|_| evm_window_start(rows, plans).to_rfc3339()),
        attribution,
        assumption: "计划价值按「已过时间 / 计划总时长」线性折算，没有工作日历；\
                     挣值只认已完成且有基线的任务；成本绩效的分子分母都是这批有基线的任务\
                     （实际成本另报整个项目的花销）；无归因用量只报数：这条账没有项目维度，\
                     拿它比本项目的花销得到的是一个反向比例"
            .to_string(),
        source: Source {
            table: EVM_TABLE,
            reader: "task graph + plan baselines + per-call token ledger",
            read_at: now.to_rfc3339(),
        },
    }
}

/// 除法的三个拒绝：分母缺失、分母为零、以及结果没有意义的情形。全部返回
/// `None` 而不是 0——一个 0 的绩效指数读起来像「一样快」，而真相是算不出来。
fn ratio(numerator: Option<u64>, denominator: Option<u64>) -> Option<f64> {
    match (numerator, denominator) {
        (Some(n), Some(d)) if d > 0 => Some(n as f64 / d as f64),
        _ => None,
    }
}

/// 到此刻按基线本该完成多少，返回 (计划价值, 计入的条数, 时长未知的条数)。
///
/// 线性折算：`已过 / 计划总时长`。没有工作日历，所以这是唯一诚实的算法——
/// 一个「按工作日」算出来的数在这里没有任何来源。
fn planned_value_at(
    baselined: &[(&Task, &cog_core::TaskPlan)],
    now: chrono::DateTime<chrono::Utc>,
) -> (u64, usize, usize) {
    let mut pv = 0f64;
    let mut with_duration = 0usize;
    let mut without_duration = 0usize;
    for (_, plan) in baselined {
        let Some(start) = plan.planned_start else {
            without_duration += 1;
            continue;
        };
        let Some(end) = plan.planned_end else {
            without_duration += 1;
            continue;
        };
        // 零长度的计划不是「立刻完成」：没有时长就折不出比例，按未知计。
        if end <= start {
            without_duration += 1;
            continue;
        }
        let total = (end - start).num_milliseconds().max(1) as f64;
        let elapsed = (now - start).num_milliseconds() as f64;
        let fraction = (elapsed / total).clamp(0.0, 1.0);
        pv += plan.budget_tokens as f64 * fraction;
        with_duration += 1;
    }
    (pv.round() as u64, with_duration, without_duration)
}

/// 甘特响应的拼装与 handler 分开，好让「读不到基线」这一态能直接被测到——
/// 只能靠起整套网关才验得到的读面，实际上不会被验。
fn build_gantt_response(
    project_id: String,
    rows: &[&Task],
    plans: Option<&BTreeMap<String, cog_core::TaskPlan>>,
) -> GanttResponse {
    let has_plan = plans
        .map(|p| rows.iter().any(|t| p.contains_key(&t.id)))
        .unwrap_or(false);
    GanttResponse {
        project_id,
        has_plan,
        plan_error: plans.is_none().then(|| {
            "plan baselines could not be read on this deployment; the unplanned marks below \
             mean unreadable, not unscheduled"
                .to_string()
        }),
        rows: build_gantt_rows(rows, plans),
        source: Source {
            table: GANTT_TABLE,
            reader: "in-memory task graph + plan baselines",
            read_at: chrono::Utc::now().to_rfc3339(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::TaskType;

    fn task(id: &str, goal: Option<&str>) -> Task {
        let mut t = Task::new(id, TaskType::Generator, serde_json::json!({}));
        t.goal_id = goal.map(|g| g.to_string());
        t
    }

    /// 未分组的任务必须落在一个**具名**的桶里。把这些行丢掉，所有项目加起来会
    /// 小于总数，而那个差没有任何地方会说——读者只能看到一堆加起来不对的数。
    #[test]
    fn ungrouped_tasks_land_in_a_named_bucket_rather_than_being_dropped() {
        let tasks = vec![
            task("a", Some("g1")),
            task("b", Some("g1")),
            task("c", None),
            task("d", Some("")),
        ];
        let rows = build_project_rows(&tasks, None);

        let summed: usize = rows.iter().map(|r| r.tasks).sum();
        assert_eq!(
            summed,
            tasks.len(),
            "每个任务都要落在某个桶里，否则总数对不上"
        );

        let ungrouped = rows
            .iter()
            .find(|r| r.id == UNGROUPED)
            .expect("没有 goal_id 的任务要有一个具名桶");
        assert_eq!(ungrouped.kind, "ungrouped");
        assert_eq!(ungrouped.tasks, 2, "空字符串的 goal_id 与缺失同义");
    }

    /// 可执行数与总数必须分开报：把两者混成一个数，读者会把「都是占位行、
    /// 没有活可干」读成「活都干完了」。
    #[test]
    fn executables_are_counted_apart_from_the_row_total() {
        let mut goal_row = Task::goal_row("g1", "做一个东西");
        goal_row.goal_id = Some("g1".into());
        let mut child = task("c1", Some("g1"));
        child.is_executable = true;
        let mut placeholder = task("c2", Some("g1"));
        placeholder.is_executable = false;

        let tasks = vec![goal_row, child, placeholder];
        let rows = build_project_rows(&tasks, None);
        let row = rows.iter().find(|r| r.id == "g1").expect("g1 要有一行");
        assert_eq!(row.tasks, 3);
        assert_eq!(row.executables, 1);
    }

    /// 堵住的判据是「依赖没全部完成」，不是「有依赖边」。只看边会让一条依赖早就
    /// 完成、随时可以开跑的任务被算成积压，读者会去查一个并不存在的堵点。
    #[test]
    fn blocked_means_a_dependency_is_not_yet_completed() {
        let mut dep_done = task("dep1", Some("g1"));
        dep_done.status = TaskStatus::Completed;
        let dep_open = task("dep2", Some("g1"));

        // 依赖已完成：不堵。
        let mut satisfied = task("b", Some("g1"));
        satisfied.blocked_by = vec!["dep1".into()];
        // 依赖还没完成：堵着。
        let mut waiting = task("a", Some("g1"));
        waiting.blocked_by = vec!["dep2".into()];
        // 依赖不在这一批里：看不到它完成，所以仍然算堵着。
        let mut unknown = task("c", Some("g1"));
        unknown.blocked_by = vec!["not-in-this-batch".into()];

        let rows = build_project_rows(&[satisfied, waiting, dep_done, dep_open, unknown], None);
        let row = rows.iter().find(|r| r.id == "g1").expect("g1 要有一行");
        assert_eq!(
            row.blocked, 2,
            "a 的依赖没完成、c 的依赖不在图里；b 的依赖已完成不算堵"
        );
    }

    /// 依赖边为空的行永远不算堵；它的「没有依赖」与「依赖都满足了」在读面上
    /// 是同一件事，都不该进积压数。
    #[test]
    fn a_task_without_edges_is_never_counted_as_blocked() {
        let rows = build_project_rows(&[task("a", Some("g1"))], None);
        assert_eq!(rows[0].blocked, 0);
    }

    /// 没有计划基线的行必须标成未排期，且计划两头都是 None。画成一根零长度的
    /// 条会被读成「计划已完成」——空白的形状在这里不能含糊。
    #[test]
    fn gantt_rows_without_a_baseline_are_marked_unplanned_not_zero_length() {
        let t = task("a", Some("g1"));
        let rows = build_gantt_rows(&[&t], None);
        assert_eq!(rows.len(), 1);
        assert!(rows[0].unplanned);
        assert_eq!(rows[0].planned_start, None);
        assert_eq!(rows[0].planned_end, None);
    }

    fn plan(task_id: &str, tokens: u64) -> cog_core::TaskPlan {
        cog_core::TaskPlan {
            workspace_id: "default".into(),
            task_id: task_id.into(),
            planned_start: Some(chrono::Utc::now()),
            planned_end: Some(chrono::Utc::now() + chrono::Duration::minutes(10)),
            budget_tokens: tokens,
            baseline_rev: "rev-1".into(),
            updated_at: chrono::Utc::now(),
        }
    }

    /// 有基线的行要按基线画，没基线的行仍然是未排期。两件事必须在一屏里同时
    /// 成立：把有基线的行也标成未排期，计划就白写了；把没基线的行按零长度画，
    /// 读者会以为计划已经完成。
    #[test]
    fn a_baselined_row_is_planned_and_an_unbaselined_one_stays_unplanned() {
        let planned = task("a", Some("g1"));
        let unbaselined = task("b", Some("g1"));
        let plans: BTreeMap<String, cog_core::TaskPlan> =
            [("a".to_string(), plan("a", 10_000))].into_iter().collect();

        let rows = build_gantt_rows(&[&planned, &unbaselined], Some(&plans));
        let a = rows.iter().find(|r| r.id == "a").unwrap();
        let b = rows.iter().find(|r| r.id == "b").unwrap();

        assert!(!a.unplanned, "有基线就是排过期了");
        assert!(a.planned_start.is_some(), "基线给了起期");
        assert!(a.planned_end.is_some(), "基线给了止期");
        assert!(b.unplanned, "没有基线行的任务仍然是未排期");
        assert_eq!(b.planned_start, None);
        assert_eq!(b.planned_end, None);
    }

    /// 读不到基线时的行与「没排期」在行上完全一样，唯一区分它们的是响应级的
    /// `plan_error`。少了它，一个读不了基线的部署会把整屏报成未排期。
    #[test]
    fn an_unreadable_plan_store_is_reported_above_the_rows_not_as_unplanned() {
        let t = task("a", Some("g1"));
        let response = build_gantt_response("g1".into(), &[&t], None);
        assert!(response.plan_error.is_some(), "读不到要说读不到");
        assert!(!response.has_plan);

        let read_ok = build_gantt_response("g1".into(), &[&t], Some(&BTreeMap::new()));
        assert!(read_ok.plan_error.is_none(), "读到了、只是空的，不是读不到");
        assert!(!read_ok.has_plan, "一条基线都没有，界面仍要说未排期");
    }

    /// 标题取不到就露出 id，不编一个名字：编出来的名字读者会当成真的。
    #[test]
    fn gantt_title_falls_back_to_the_id_instead_of_inventing_one() {
        let mut t = task("task-42", Some("g1"));
        t.input = serde_json::json!({ "unrelated": 1 });
        let rows = build_gantt_rows(&[&t], None);
        assert_eq!(rows[0].title, "task-42");
    }

    /// 没配上限就读作「没配」，不能读作「没超」。`wip_limit: Some(默认数)` 会让
    /// 界面宣称一个并不存在的准入控制正在生效。
    #[test]
    fn board_columns_report_no_limit_as_absent_rather_than_as_no_overflow() {
        let t = task("a", Some("g1"));
        let (limits, _) = cog_core::BoardWipLimits::from_spec("");
        let columns = build_board_columns(&[&t], &limits);
        assert!(columns.iter().all(|c| c.wip_limit.is_none()));
        assert!(columns.iter().all(|c| c.over_limit.is_none()));
    }

    /// 配了上限的列要把数读出来，并给出「满了没有」。判据与发布侧同一条：
    /// 到达上限就是满了——那一刻发布侧已经不往这一列放新活，读面说「未超」
    /// 就是两处口径不一致。
    #[test]
    fn a_configured_limit_is_reported_with_the_same_comparison_the_gate_uses() {
        let mut a = task("a", Some("g1"));
        a.status = cog_core::TaskStatus::Running;
        let mut b = task("b", Some("g1"));
        b.status = cog_core::TaskStatus::Running;
        let (limits, problems) = cog_core::BoardWipLimits::from_spec("running=2,scheduled=8");
        assert!(problems.is_empty(), "{problems:?}");
        let columns = build_board_columns(&[&a, &b], &limits);
        let running = columns.iter().find(|c| c.status == "running").unwrap();
        assert_eq!(running.count, 2);
        assert_eq!(running.wip_limit, Some(2));
        assert_eq!(running.over_limit, Some(true), "到了上限就等于满了");
        let scheduled = columns.iter().find(|c| c.status == "scheduled").unwrap();
        assert_eq!(scheduled.wip_limit, Some(8));
        assert_eq!(scheduled.over_limit, Some(false));
        let pending = columns.iter().find(|c| c.status == "pending").unwrap();
        assert_eq!(pending.wip_limit, None, "没配的列不许借隔壁的数");
        assert_eq!(pending.over_limit, None);
    }

    /// 六列的集合与顺序固定：缺的列要显式出 0，否则「这一列没有任务」与
    /// 「这一列不在读数里」在读面上同形。
    #[test]
    fn board_columns_are_all_six_statuses_in_a_fixed_order() {
        let t = task("a", Some("g1"));
        let (limits, _) = cog_core::BoardWipLimits::from_spec("");
        let columns = build_board_columns(&[&t], &limits);
        let order: Vec<&str> = columns.iter().map(|c| c.status.as_str()).collect();
        let expected: Vec<&str> = cog_core::TaskStatus::ALL
            .iter()
            .map(|s| s.as_str())
            .collect();
        assert_eq!(order, expected);
        assert_eq!(
            columns.iter().map(|c| c.count).sum::<usize>(),
            1,
            "只有一列有任务，其余列必须显式为 0"
        );
    }

    fn plan_between(
        task_id: &str,
        tokens: u64,
        start: chrono::DateTime<chrono::Utc>,
        end: chrono::DateTime<chrono::Utc>,
    ) -> cog_core::TaskPlan {
        cog_core::TaskPlan {
            workspace_id: "default".into(),
            task_id: task_id.into(),
            planned_start: Some(start),
            planned_end: Some(end),
            budget_tokens: tokens,
            baseline_rev: "rev-1".into(),
            updated_at: start,
        }
    }

    /// 三个量都量不出来的时候必须说「量不了」，不能填 0。0 在这三个位置上各有
    /// 各的误读：计划价值 0 读作「还没开工」，挣值 0 读作「一点没干成」，成本 0
    /// 读作「一分钱没花」；而真相是这个部署回答不了这个问题。
    #[test]
    fn without_a_baseline_all_three_quantities_read_unmeasurable_not_zero() {
        let mut done = task("a", Some("g1"));
        done.status = TaskStatus::Completed;
        let now = chrono::Utc::now();

        // 基线读不到、账也读不到：三个量全是「量不了」。
        let blind = build_evm_response("g1".into(), &[&done], None, None, None, now);
        assert_eq!(blind.planned_value.value, None);
        assert_eq!(blind.earned_value.value, None);
        assert_eq!(blind.actual_cost.value, None);
        assert!(blind.planned_value.unmeasurable.is_some());
        assert!(blind.earned_value.unmeasurable.is_some());
        assert!(blind.actual_cost.unmeasurable.is_some());
        assert_eq!(blind.unattributed_tokens, None, "读不到不等于零");
        assert_eq!(blind.unattributed_window_start, None, "没量就没有窗口");
        assert_eq!(blind.cost_performance_index, None, "分母量不出来就没有指数");
        assert_eq!(blind.schedule_performance_index, None);
        assert_eq!(
            blind.attribution.tasks_outside_baseline, None,
            "基线读不到时一条也判不出在不在基线之外；写 0 是断言「全都有基线」"
        );
        assert_eq!(blind.attribution.tasks_with_cost, None, "账也读不到");
        assert_eq!(blind.attribution.baselined_tasks_without_cost, None);

        // 基线读到了、这个项目一条都没有：进度不是零，是没排过期。
        let empty = build_evm_response(
            "g1".into(),
            &[&done],
            Some(&BTreeMap::new()),
            None,
            None,
            now,
        );
        assert_eq!(empty.planned_value.value, None);
        assert_eq!(empty.earned_value.value, None);
        assert!(empty.planned_value.unmeasurable.is_some());
        assert!(empty.earned_value.unmeasurable.is_some());

        // 只有账读不到时，成本单独是「量不了」，另两个照常给数。
        let plans: BTreeMap<String, cog_core::TaskPlan> = [(
            "a".to_string(),
            plan_between(
                "a",
                1000,
                now - chrono::Duration::minutes(5),
                now + chrono::Duration::minutes(5),
            ),
        )]
        .into_iter()
        .collect();
        let half = build_evm_response("g1".into(), &[&done], Some(&plans), None, None, now);
        assert_eq!(
            half.earned_value.value,
            Some(1000),
            "已完成任务的预算全额计入"
        );
        assert_eq!(half.planned_value.value, Some(500), "过了半个计划时长");
        assert_eq!(half.actual_cost.value, None);
        assert!(half.actual_cost.unmeasurable.is_some());
        assert_eq!(half.cost_performance_index, None);
        assert!(half.schedule_performance_index.is_some());
    }

    /// 账上不带任务身份的花销不属于任何项目，所以单独成字段、不并进成本。
    /// 折进成本会把「归因缺口」读成「这个项目很贵」，折成 0 又会读成「省钱」。
    #[test]
    fn unattributed_spend_is_reported_separately_and_never_folded_into_actual_cost() {
        let now = chrono::Utc::now();
        let mut a = task("a", Some("g1"));
        a.status = TaskStatus::Completed;
        let b = task("b", Some("g1"));
        let window = (
            now - chrono::Duration::minutes(5),
            now + chrono::Duration::minutes(5),
        );
        let plans: BTreeMap<String, cog_core::TaskPlan> = [
            ("a".to_string(), plan_between("a", 1000, window.0, window.1)),
            ("b".to_string(), plan_between("b", 400, window.0, window.1)),
        ]
        .into_iter()
        .collect();
        let spends: BTreeMap<String, u64> = [("a".to_string(), 700u64), ("b".to_string(), 300u64)]
            .into_iter()
            .collect();

        let reading = build_evm_response(
            "g1".into(),
            &[&a, &b],
            Some(&plans),
            Some(&spends),
            Some(120),
            now,
        );

        assert_eq!(
            reading.actual_cost.value,
            Some(1000),
            "成本只算账上带任务身份的行"
        );
        assert_eq!(reading.unattributed_tokens, Some(120));
        assert_eq!(
            reading.actual_cost.value.unwrap() + reading.unattributed_tokens.unwrap(),
            1120,
            "两个数加起来才是这个窗口的全部花销"
        );
        assert!(
            reading.unattributed_window_start.is_some(),
            "不带窗口起点的数会被下一个读者当成常数带走"
        );
        assert_eq!(reading.attribution.tasks_with_cost, Some(2));
        assert_eq!(
            reading.attribution.baselined_tasks_without_cost,
            Some(0),
            "两条都在账上找到了"
        );
        assert!(
            EVM_TABLE.contains(USAGE_TABLE),
            "成本那一列的出处必须点名逐笔 token 账"
        );
    }

    /// 成本绩效的分子分母必须覆盖同一批任务。一条基线加一百条没基线的任务时，
    /// 拿项目总花销当分母会把比值压到接近 0——读作「花得多挣得少」，而没有任何
    /// 一项测量支持这个结论：那些钱买的是没参与挣值的活。
    #[test]
    fn cost_performance_divides_two_quantities_measured_over_the_same_tasks() {
        let now = chrono::Utc::now();
        let mut a = task("a", Some("g1"));
        a.status = TaskStatus::Completed;
        // 没有基线的任务：花了很多钱，但不在挣值里。
        let unbudgeted = task("u", Some("g1"));
        let window = (
            now - chrono::Duration::minutes(5),
            now + chrono::Duration::minutes(5),
        );
        let plans: BTreeMap<String, cog_core::TaskPlan> =
            [("a".to_string(), plan_between("a", 1000, window.0, window.1))]
                .into_iter()
                .collect();
        let spends: BTreeMap<String, u64> = [("a".to_string(), 500u64), ("u".to_string(), 9000u64)]
            .into_iter()
            .collect();

        let reading = build_evm_response(
            "g1".into(),
            &[&a, &unbudgeted],
            Some(&plans),
            Some(&spends),
            None,
            now,
        );

        assert_eq!(reading.earned_value.value, Some(1000));
        assert_eq!(reading.actual_cost.value, Some(9500), "项目总花销照报");
        assert_eq!(
            reading.baselined_actual_cost.value,
            Some(500),
            "分母只取有基线的那一条"
        );
        assert_eq!(
            reading.cost_performance_index,
            Some(2.0),
            "1000 挣值 / 500 同批成本；用 9500 会算出 0.105，读作「花得多挣得少」"
        );
        assert_eq!(
            reading.attribution.tasks_outside_baseline,
            Some(1),
            "读者要知道挣值少算了多少条"
        );
        assert_eq!(
            reading.attribution.tasks_with_cost,
            Some(2),
            "账上有花销的是两条，与挣值覆盖的那一批不是一个数"
        );
    }

    /// 计划只有起期、没有时长时，折不出比例，只能算作「时长未知」而不是
    /// 「已经全部完成」——后者会让计划价值凭空涨一截，进度看起来超前。
    #[test]
    fn a_baseline_without_a_duration_is_left_out_of_planned_value_not_counted_as_done() {
        let mut t = task("a", Some("g1"));
        t.status = TaskStatus::Completed;
        let now = chrono::Utc::now();
        let mut zero_length = plan_between("a", 1000, now, now);
        let (pv, counted, unknown) = planned_value_at(&[(&t, &zero_length)], now);
        assert_eq!(pv, 0);
        assert_eq!(counted, 0);
        assert_eq!(unknown, 1);

        zero_length.planned_end = None;
        let (pv, counted, unknown) = planned_value_at(&[(&t, &zero_length)], now);
        assert_eq!(pv, 0);
        assert_eq!(counted, 0);
        assert_eq!(unknown, 1, "只有起期也算不出比例");
    }

    /// 两次读同一份数据，行序必须逐行相同：顺序不稳定，界面每次刷新都在跳，
    /// 读者没法比对前后两张图。
    #[test]
    fn project_order_is_stable_across_reads() {
        let tasks = vec![
            task("a", Some("g2")),
            task("b", Some("g1")),
            task("c", Some("g2")),
        ];
        let first = build_project_rows(&tasks, None);
        let second = build_project_rows(&tasks, None);
        let ids: Vec<&str> = first.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(ids, vec!["g2", "g1"], "任务多的在前，同数按 id 升序");
        assert_eq!(
            ids,
            second.iter().map(|r| r.id.as_str()).collect::<Vec<_>>()
        );
    }
}
