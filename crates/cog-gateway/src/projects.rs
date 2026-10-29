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

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::{Extension, Json};
use serde::Serialize;

use cog_core::{Task, TaskStatus};

use crate::error::ApiError;
use crate::tasks::visible_to_claims;
use crate::GatewayState;

/// 没有 `goal_id` 的任务归到这里。它是一个**具名**的桶，不是被丢掉的那些：
/// 把无归属的任务悄悄排除，全部项目加起来会小于总数，而那个差没有任何地方会说。
pub const UNGROUPED: &str = cog_core::UNGROUPED_PROJECT;

/// What a row of the project listing is.
///
/// The panel prints one of these words and decides from it whether the row
/// opens a task graph, so the word is part of the response's meaning rather
/// than a label: a row called something the client does not know draws with no
/// way into its tasks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectKind {
    /// A goal root: the id is the submission's `message_id`.
    Goal,
    /// The named bucket for tasks submitted outside any goal.
    Ungrouped,
}

impl ProjectKind {
    /// Every kind, so a gate or a client can cover the set.
    pub const ALL: [ProjectKind; 2] = [ProjectKind::Goal, ProjectKind::Ungrouped];

    /// The word this response carries.
    pub fn as_str(self) -> &'static str {
        match self {
            ProjectKind::Goal => "goal",
            ProjectKind::Ungrouped => UNGROUPED,
        }
    }

    /// Read a kind back from its word.
    pub fn parse(word: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.as_str() == word)
    }

    /// Which kind a project id stands for.
    ///
    /// The bucket's name is [`UNGROUPED`], the same constant the id is built
    /// from -- a second literal here would be a second thing to rename.
    pub fn of(id: &str) -> Self {
        if id == UNGROUPED {
            ProjectKind::Ungrouped
        } else {
            ProjectKind::Goal
        }
    }
}

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

/// CSV 的一格。含逗号、引号或换行的字段整体加引号、内部引号翻倍——这是
/// RFC 4180 的写法。不转义的话，一个带逗号的任务标题会把一行撑成两列，而
/// 下游按列数校验的表会被整表拒收。
fn csv_field(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') || value.contains('\r') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn csv_row(fields: &[String]) -> String {
    fields
        .iter()
        .map(|f| csv_field(f))
        .collect::<Vec<_>>()
        .join(",")
        + "\r\n"
}

#[derive(Debug, serde::Deserialize)]
pub struct ExportQuery {
    /// `wbs` | `board` | `gantt` | `evm`。就是四个读面的名字。
    pub view: String,
}

/// 把一个读面导出成 CSV。
///
/// 走的是同几个折叠函数，不是另算一遍：导出的数与会话里看到的数必须逐格一致，
/// 否则客户拿表去核对时，两边谁对谁错无法裁决。
pub async fn export_csv_handler(
    State(state): State<Arc<GatewayState>>,
    claims: Option<Extension<cog_core::Claims>>,
    Path(id): Path<String>,
    axum::extract::Query(q): axum::extract::Query<ExportQuery>,
) -> Result<axum::response::Response, ApiError> {
    let claims = claims.as_ref().map(|Extension(c)| c);
    let tasks = visible_tasks(&state, claims).await;
    let rows = tasks_of_project(&tasks, &id);
    if rows.is_empty() && id != UNGROUPED {
        return Err(ApiError::not_found(format!("project {id}")));
    }

    let mut body = String::new();
    match q.view.as_str() {
        "wbs" => {
            let tree = build_wbs_tree(id.clone(), &rows);
            body.push_str(&csv_row(&[
                "id".into(),
                "parent_id".into(),
                "depth".into(),
                "title".into(),
                "status".into(),
                "acceptance".into(),
            ]));
            fn walk(nodes: &[WbsNode], parent: &str, out: &mut String) {
                for n in nodes {
                    out.push_str(&csv_row(&[
                        n.id.clone(),
                        parent.to_string(),
                        n.depth.to_string(),
                        n.title.clone(),
                        n.status.clone(),
                        n.acceptance.to_string(),
                    ]));
                    walk(&n.children, &n.id, out);
                }
            }
            walk(&tree.roots, "", &mut body);
            // 悬空与成环的节点也导出来：它们在树里没有位置，但在账上有名有姓，
            // 少导出它们等于让一份导出比读面更干净——而脏的正是要修的那些行。
            for n in tree.orphan_parents.iter().chain(tree.cycles.iter()) {
                body.push_str(&csv_row(&[
                    n.id.clone(),
                    "(detached)".into(),
                    n.depth.to_string(),
                    n.title.clone(),
                    n.status.clone(),
                    n.acceptance.to_string(),
                ]));
            }
        }
        "board" => {
            let (limits, _problems) = cog_core::BoardWipLimits::from_env();
            body.push_str(&csv_row(&[
                "status".into(),
                "count".into(),
                "wip_limit".into(),
                "over_limit".into(),
            ]));
            for c in build_board_columns(&rows, &tasks, &limits) {
                body.push_str(&csv_row(&[
                    c.status,
                    c.count.to_string(),
                    c.wip_limit.map(|v| v.to_string()).unwrap_or_default(),
                    c.over_limit.map(|v| v.to_string()).unwrap_or_default(),
                ]));
            }
        }
        "gantt" => {
            let plans = readable_plans(&state.orchestrator).await;
            let gantt = build_gantt_response(id.clone(), &rows, plans.as_ref());
            body.push_str(&csv_row(&[
                "id".into(),
                "title".into(),
                "status".into(),
                "created_at".into(),
                "started_at".into(),
                "updated_at".into(),
                "planned_start".into(),
                "planned_end".into(),
                "unplanned".into(),
            ]));
            for r in gantt.rows {
                body.push_str(&csv_row(&[
                    r.id,
                    r.title,
                    r.status,
                    r.created_at,
                    r.started_at.unwrap_or_default(),
                    r.updated_at,
                    r.planned_start.unwrap_or_default(),
                    r.planned_end.unwrap_or_default(),
                    r.unplanned.to_string(),
                ]));
            }
        }
        "evm" => {
            let evm = evm_view(&state, id.clone(), &rows).await;
            body.push_str(&csv_row(&[
                "metric".into(),
                "value".into(),
                "unmeasurable".into(),
                "basis".into(),
            ]));
            for (name, reading) in [
                ("planned_value", &evm.planned_value),
                ("earned_value", &evm.earned_value),
                ("actual_cost", &evm.actual_cost),
                ("baselined_actual_cost", &evm.baselined_actual_cost),
            ] {
                body.push_str(&csv_row(&[
                    name.into(),
                    reading.value.map(|v| v.to_string()).unwrap_or_default(),
                    reading.unmeasurable.clone().unwrap_or_default(),
                    reading.basis.clone(),
                ]));
            }
        }
        other => {
            return Err(ApiError::bad_request(format!(
                "'{other}' is not a view: the four are wbs, board, gantt, evm"
            ))
            .with_code("unknown_view"))
        }
    }

    let filename = format!("{}-{}.csv", id, q.view);
    Ok((
        [
            (
                axum::http::header::CONTENT_TYPE,
                "text/csv; charset=utf-8".to_string(),
            ),
            (
                axum::http::header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"{filename}\""),
            ),
        ],
        body,
    )
        .into_response())
}

#[derive(Debug, Serialize)]
pub struct ProjectsResponse {
    pub projects: Vec<ProjectRow>,
    /// 本页各行 `tasks` 之和。它与 `projects` 逐行对得上，是这一页自己的账。
    pub counted_tasks: usize,
    /// 读者可见的任务总数，**不随分页变化**。它与本页各行之和不必相等——分页
    /// 之后 `projects` 只是一部分；两者放在一起，读者才分得出「还有更多」。
    pub total_tasks: usize,
    /// 读者可见的项目总数，同样不随分页变化。
    pub total_projects: usize,
    /// 下一页的位置；`None` 表示这一页就是最后一页。
    ///
    /// 它是一**个位置**而不是一个 id：列表顺序在两次读之间可能变（任务被删、
    /// 新项目进来），按 id 取游标会在顺序变化时跳过或重复行。位置语义下最坏的
    /// 情况是同一个项目出现在相邻两页，而读者按 id 去重就能收回。
    pub next_cursor: Option<String>,
    pub source: Source,
}

/// 项目列表的一页。`limit` 有上限：不设上限的分页等于没分页。
#[derive(Debug, serde::Deserialize)]
pub struct ListProjectsQuery {
    pub limit: Option<usize>,
    pub cursor: Option<usize>,
}

const PROJECTS_PAGE_DEFAULT: usize = 200;
const PROJECTS_PAGE_MAX: usize = 1000;

/// 每列最多带回多少张卡片。
///
/// 看板是给人挪的，不是给人数的：只回计数的话，界面画得出六列数字，画不出任何一张
/// 能拖的卡，而「这一列有 37 个」在客户眼里等于「我不知道里面是什么」。上限之外
/// 的张数由 `cards_hidden` 如实报出来——静默截断会让一列看起来正好装得下。
const BOARD_CARDS_PER_COLUMN: usize = 100;

/// 看板上的一张卡片。
///
/// 只带读面要画、写面要判的那几格：标题（人认卡靠它）、父节点（树上的位置）、
/// 还缺哪些依赖（能不能现在开工）、有没有验收标准（能不能给它写计划基线）。
/// 任务的全部字段不在这里——那是任务详情的读面，看板重复一份迟早与那边分家。
#[derive(Debug, Serialize)]
pub struct BoardCard {
    pub id: String,
    pub title: String,
    /// 它在哪一列。列本身已经说过一次，卡片再带一份：块按列渲染之后，卡片
    /// 被单独传给「能挪到哪」的判据时，不必再让调用方把列名一路带下来。
    pub status: String,
    pub parent_task_id: Option<String>,
    /// 依赖还没全部完成。依赖不在可见的这一批里时也算卡住——看不见的依赖不能
    /// 当成已完成，那会把一条卡住的活读成可以开跑。
    pub blocked: bool,
    /// 有没有验收标准。没有的工作包「做完」没有判据，写计划基线会被拒。
    pub has_acceptance: bool,
}

#[derive(Debug, Serialize)]
pub struct BoardColumn {
    pub status: String,
    pub count: usize,
    /// 这一列的通行上限。`None` 表示**没有配**，不是「不限」也不是「没超」——
    /// 把「没配上限」印成 `over_limit: false`，读者会以为上限正在生效。
    pub wip_limit: Option<usize>,
    pub over_limit: Option<bool>,
    /// 这一列里最久没动的那一批卡片。`count` 是这一列的总数，与本数组长度不必
    /// 相等——差的那部分是 `cards_hidden`。
    pub cards: Vec<BoardCard>,
    /// 因为超过每列上限而没带回来的张数。0 表示这一列的张数就是全部。
    pub cards_hidden: usize,
}

#[derive(Debug, Serialize)]
pub struct BoardResponse {
    pub project_id: String,
    pub columns: Vec<BoardColumn>,
    pub total: usize,
    /// 这一台进程的上限是从哪来的。
    ///
    /// 每列的 `wip_limit` 全是 `null` 时，只看看板分不出两件事：「这台按设计
    /// 不限流」与「这台上限没接上」。两者对读者的下一步完全相反——前者不用动，
    /// 后者要去查部署——而 `null` 那一格把它们的形状抹成了同一个。所以把变量的
    /// 原文带回来，让「没声明」自己说出来。
    pub wip_limits: WipLimitsReading,
    pub source: Source,
}

#[derive(Debug, Serialize)]
pub struct WipLimitsReading {
    /// 变量名。读者要拿它去查自己的部署，所以它不是文档里的一句话，是读数。
    pub variable: String,
    /// 这一台进程读到的原文；没设这个变量时是 `null`。
    pub declared: Option<String>,
    /// 声明了却用不上的条目，一条一行。它们不出现在任何一列的 `wip_limit`
    /// 上，不点名就等于「配了、没生效、没人知道」。
    pub refused: Vec<String>,
    /// 一句话讲清现状：读者可能只看这一句。
    pub basis: String,
}

impl WipLimitsReading {
    /// `declared` 由调用方从环境读出后传进来，不在这里读：这一句判词的三档要
    /// 能被逐档验，而一个只能靠设进程环境变量才验得了的判据，会和同进程里别的
    /// 用例抢同一份环境。
    fn read(
        declared: Option<String>,
        limits: &cog_core::BoardWipLimits,
        refused: Vec<String>,
    ) -> Self {
        let variable = cog_core::config::BOARD_WIP_LIMITS_ENV.to_string();
        let usable = limits.iter().count();
        let basis = match &declared {
            // 没声明与「声明了但一条都用不上」必须分开说：后者的看板与前者
            // 逐格同形，而它是一次失败的配置，不是一次决定。
            None => format!(
                "没声明 {variable}：这台没有列上限，六列都不拦人。看板上的 `null` 是「没配」，不是「不限」也不是「没超」"
            ),
            Some(raw) if usable == 0 => format!(
                "声明了 {variable}=`{raw}`，但一条都用不上{}：这台事实上没有列上限",
                if refused.is_empty() {
                    String::new()
                } else {
                    format!("（{} 条被拒，见 refused）", refused.len())
                }
            ),
            Some(raw) => format!(
                "由 {variable}=`{raw}` 声明，其中 {usable} 条生效{}",
                if refused.is_empty() {
                    String::new()
                } else {
                    format!("、{} 条被拒（见 refused）", refused.len())
                }
            ),
        };
        Self {
            variable,
            declared,
            refused,
            basis,
        }
    }
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
    /// 同一窗口里**归不到任何一件任务**的用量：账上没写身份，或者写的是一次
    /// 运行的 id 而任务图里没有这件活。
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
    /// 超支的节点：花销已经超过基线预算的那些。
    ///
    /// `None` 表示三张表里有一张读不到（基线或逐笔账），此时**不能**给一个空
    /// 列表：空读作「一条也没超支」，那是另一个结论。
    pub overruns: Option<Vec<OverrunRow>>,
    /// 已经过了计划止期、却还没跑完的节点。
    ///
    /// 只数还在等或在跑的：失败与取消的不算「落后」，它们各有各的回路（重规划、
    /// 放弃），把结局算成进度会让落后这一列永远清不掉。
    pub behind_schedule: Option<Vec<BehindRow>>,
    /// 算这三个量时用的假设，明写出来。
    pub assumption: String,
    pub source: Source,
}

/// 一条超支的节点。预算与花销一起给：只报差额，读者还得回去找那两行。
#[derive(Debug, Clone, serde::Serialize)]
pub struct OverrunRow {
    pub task_id: String,
    pub title: String,
    pub budget_tokens: u64,
    pub spent_tokens: u64,
}

/// 一条过了计划止期还没跑完的节点。
#[derive(Debug, Clone, serde::Serialize)]
pub struct BehindRow {
    pub task_id: String,
    pub title: String,
    pub status: String,
    pub planned_end: String,
    /// 过了止期多久。差三分钟与差三天是两件事，只给止期这个数读者得自己减。
    pub overdue_secs: i64,
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
pub(crate) async fn readable_plans(
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
pub(crate) fn task_title(task: &Task) -> String {
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
                kind: ProjectKind::of(id).as_str(),
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
    axum::extract::Query(page): axum::extract::Query<ListProjectsQuery>,
) -> Result<Json<ProjectsResponse>, ApiError> {
    let claims = claims.as_ref().map(|Extension(c)| c);
    let tasks = visible_tasks(&state, claims).await;
    let total_tasks = tasks.len();
    let plans = readable_plans(&state.orchestrator).await;

    let all = build_project_rows(&tasks, plans.as_ref());
    let total_projects = all.len();
    let limit = page
        .limit
        .unwrap_or(PROJECTS_PAGE_DEFAULT)
        .clamp(1, PROJECTS_PAGE_MAX);
    let start = page.cursor.unwrap_or(0).min(total_projects);
    let end = (start + limit).min(total_projects);
    let projects: Vec<ProjectRow> = all.into_iter().skip(start).take(end - start).collect();
    let counted_tasks: usize = projects.iter().map(|p| p.tasks).sum();

    Ok(Json(ProjectsResponse {
        projects,
        counted_tasks,
        total_tasks,
        total_projects,
        next_cursor: (end < total_projects).then(|| end.to_string()),
        source: now_source("in-memory task graph"),
    }))
}

fn tasks_of_project<'a>(tasks: &'a [Task], id: &str) -> Vec<&'a Task> {
    tasks.iter().filter(|t| project_key(t) == id).collect()
}

fn build_board_columns(
    rows: &[&Task],
    all: &[Task],
    limits: &cog_core::BoardWipLimits,
) -> Vec<BoardColumn> {
    // 「谁已经完成」按全部可见任务判，不按这个项目的那一批：一条依赖挂在项目外面
    // 时，按项目内判会把它当成没完成；那是保守的方向，但按整张图判才是真的。
    let done: BTreeMap<&str, bool> = all
        .iter()
        .map(|t| (t.id.as_str(), t.status == TaskStatus::Completed))
        .collect();

    status_counts(rows)
        .into_iter()
        .map(|c| {
            // 没配上限的列两格都是 `None`：印成 0 会让「没有上限」看起来像
            // 「上限为零」，印成 `false` 会让「没有东西在拦」读成「拦过、没超」。
            let limit = limits.limit_for(&c.status);
            // 卡片按最近动过的排前面：看板上最上面那张应当是刚刚在动的那张，
            // 而不是 id 最小的那张。截断时丢掉的也就是最久没动的那一批。
            let mut cards: Vec<&Task> = rows
                .iter()
                .copied()
                .filter(|t| t.status.as_str() == c.status)
                .collect();
            cards.sort_by(|a, b| {
                b.updated_at
                    .cmp(&a.updated_at)
                    .then_with(|| a.id.cmp(&b.id))
            });
            let cards_hidden = cards.len().saturating_sub(BOARD_CARDS_PER_COLUMN);
            let cards: Vec<BoardCard> = cards
                .into_iter()
                .take(BOARD_CARDS_PER_COLUMN)
                .map(|t| BoardCard {
                    id: t.id.clone(),
                    title: task_title(t),
                    status: t.status.as_str().to_string(),
                    parent_task_id: t.parent_task_id.clone().filter(|p| !p.is_empty()),
                    blocked: !t.blocked_by.is_empty()
                        && !t
                            .blocked_by
                            .iter()
                            .all(|d| done.get(d.as_str()).copied().unwrap_or(false)),
                    has_acceptance: !t.acceptance.is_empty(),
                })
                .collect();
            BoardColumn {
                wip_limit: limit,
                // 满了就拦：到达上限这一刻，发布侧已经不再往这一列放新活。读面
                // 与拦住它的那道判据用同一个比较，读数才不会说「未超」而派发
                // 已经被拦下。
                over_limit: limit.map(|limit| c.count >= limit),
                status: c.status,
                count: c.count,
                cards,
                cards_hidden,
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
    for problem in &problems {
        tracing::warn!(
            variable = cog_core::config::BOARD_WIP_LIMITS_ENV,
            problem = %problem,
            "board WIP limit entry ignored"
        );
    }
    let columns = build_board_columns(&rows, &tasks, &limits);

    Ok(Json(BoardResponse {
        project_id: id,
        columns,
        total: rows.len(),
        wip_limits: WipLimitsReading::read(
            std::env::var(cog_core::config::BOARD_WIP_LIMITS_ENV).ok(),
            &limits,
            problems,
        ),
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

    Ok(Json(evm_view(&state, id, &rows).await))
}

/// 一次挣值的完整读数：三张表的取数与拼装。
///
/// 与 handler 分开，是因为导出也要这一份——导出另算一遍的话，表里的数和页面
/// 上的数会在某一天悄悄分家，而分家的那一天没人能裁决谁对。
async fn evm_view(state: &Arc<GatewayState>, id: String, rows: &[&Task]) -> EvmResponse {
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
            // 账上归不到任何一件任务的行无法归属到任何项目，只能报这个窗口里的总量。
            let since = evm_window_start(rows, plans.as_ref());
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

    build_evm_response(id, rows, plans.as_ref(), spends.as_ref(), unattributed, now)
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
pub(crate) fn build_evm_response(
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
                let attribution = EvmAttribution {
                    tasks_with_cost,
                    baselined_tasks_without_cost: spent.as_ref().map(|spent| {
                        baselined
                            .iter()
                            .filter(|(t, _)| {
                                !spent.contains(t.id.as_str()) && t.status == TaskStatus::Completed
                            })
                            .count()
                    }),
                    tasks_outside_baseline: Some(outside),
                };
                // 一条预算都没量出来时，这两个量不是 0 而是量不出来。基线上的
                // `budget_tokens = 0` 写的是「这件活没有成本计划」——估算器要等同类
                // 任务量出过花销才写得下预算，而一次都没跑完过的那种还写不下。把
                // 它折成 `measured(0)` 与「一条都没量出来」在屏幕上是一模一样的
                // 两个 0，读者却会读成「计划价值是零」「一条活都没挣到」，再往下
                // 把「没量过」当成「没干活」。时长那一列的情形不同：`planned_end`
                // 由时长中位数给，缺时长的行止期本来就空着，折比例时会被数成
                // 「时长未知」而不是零。
                if baselined.iter().all(|(_, p)| p.budget_tokens == 0) {
                    let reason = format!(
                        "这个项目 {} 条基线一条也没带预算：估算器要等同类任务量出过花销才写\
                         得下预算，所以这两个量是量不出来，不是 0（时长那一列仍按 \
                         {with_duration} 条基线折算到此刻，见 `behind_schedule`）",
                        baselined.len()
                    );
                    (
                        EvmReading::unmeasurable(reason.clone()),
                        EvmReading::unmeasurable(reason),
                        baselined,
                        attribution,
                    )
                } else {
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
                        baselined,
                        attribution,
                    )
                }
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
        overruns: overruns(rows, plans, spends),
        behind_schedule: behind_schedule(rows, plans, now),
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

/// 哪些节点超支了，按超出的量从大到小。
///
/// 三个「不算」各自有原因：预算为 0 的行读作「没有成本计划」（不是「计划花零个
/// token」），谈不上超支；账上没有这条任务的行是**没记到**而不是花了 0，把它当 0
/// 会漏掉真正超支的那条；超过而没有到（`spent > budget`）才算，到线不算超。
fn overruns(
    rows: &[&Task],
    plans: Option<&BTreeMap<String, cog_core::TaskPlan>>,
    spends: Option<&BTreeMap<String, u64>>,
) -> Option<Vec<OverrunRow>> {
    let plans = plans?;
    let spends = spends?;
    let mut out: Vec<OverrunRow> = rows
        .iter()
        .filter_map(|t| {
            let plan = plans.get(&t.id)?;
            if plan.budget_tokens == 0 {
                return None;
            }
            let spent = spends.get(&t.id).copied()?;
            (spent > plan.budget_tokens).then(|| OverrunRow {
                task_id: t.id.clone(),
                title: task_title(t),
                budget_tokens: plan.budget_tokens,
                spent_tokens: spent,
            })
        })
        .collect();
    out.sort_by(|a, b| {
        (b.spent_tokens - b.budget_tokens)
            .cmp(&(a.spent_tokens - a.budget_tokens))
            .then_with(|| a.task_id.cmp(&b.task_id))
    });
    Some(out)
}

/// 哪些节点已经过了计划止期还没跑完，按过了多久从大到小。
///
/// 没有基线的任务不进这张表——它们不是准时，是没排过期。止期相等的两条按 id
/// 排，好让两次读取给出同一个顺序。
fn behind_schedule(
    rows: &[&Task],
    plans: Option<&BTreeMap<String, cog_core::TaskPlan>>,
    now: chrono::DateTime<chrono::Utc>,
) -> Option<Vec<BehindRow>> {
    let plans = plans?;
    let mut out: Vec<BehindRow> = rows
        .iter()
        .filter_map(|t| {
            let end = plans.get(&t.id)?.planned_end?;
            if end >= now {
                return None;
            }
            if matches!(
                t.status,
                TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Cancelled
            ) {
                return None;
            }
            Some(BehindRow {
                task_id: t.id.clone(),
                title: task_title(t),
                status: t.status.as_str().to_string(),
                planned_end: end.to_rfc3339(),
                overdue_secs: (now - end).num_seconds(),
            })
        })
        .collect();
    out.sort_by(|a, b| {
        b.overdue_secs
            .cmp(&a.overdue_secs)
            .then_with(|| a.task_id.cmp(&b.task_id))
    });
    Some(out)
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

// ─── 范围树：谁挂在谁下面 ────────────────────────────────────────────

/// 范围树上的一个节点。
///
/// 嵌套已经表达了层级；`depth` 再写一遍是因为前端要按深度配缩进与竖线，而从
/// 嵌套里数深度得在渲染时维护状态——渲染器一旦被虚拟滚动裁掉一段，那个状态就
/// 断了，缩进会错位。
#[derive(Debug, Serialize, PartialEq)]
pub struct WbsNode {
    pub id: String,
    pub title: String,
    pub status: String,
    /// 根是 0。
    pub depth: usize,
    /// 验收标准的条数。0 不是「不用验收」，是「这一片活没有做完的判据」。
    pub acceptance: usize,
    pub children: Vec<WbsNode>,
}

impl WbsNode {
    fn leaf(t: &Task, depth: usize) -> Self {
        Self {
            id: t.id.clone(),
            title: task_title(t),
            status: t.status.as_str().to_string(),
            depth,
            acceptance: t.acceptance.len(),
            children: Vec::new(),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct WbsResponse {
    pub project_id: String,
    /// 本来就没有父节点的那些。
    pub roots: Vec<WbsNode>,
    /// 有父节点、而父节点不在这个项目的可见集合里的那些。
    ///
    /// 它们**不是根**：根读作「这一片活顶在最上面」，这里读作「这一片活的上面
    /// 少了一块」。两者在界面上必须分开写，否则一棵缺了一块树的树会被当成一棵
    /// 完整的树来读。
    pub orphan_parents: Vec<WbsNode>,
    /// 父指针成环的那些：从任何根都走不到，渲染时如果按「递归画子节点」硬走
    /// 会直接卡死。
    pub cycles: Vec<WbsNode>,
    /// 这个项目可见的任务总数。它必须等于三个桶之和——不等就是有行落在了所有
    /// 桶之外。
    pub total: usize,
    pub source: Source,
}

/// 递归的最大深度。超过它按环处理，不继续往下走。
///
/// 真实的分解深度个位数；一个四千层的链只可能来自坏数据或有人手工造环，那时
/// 递归会把网关的栈打穿——而栈溢出不是一个能被 `Result` 接住的错误。
const WBS_DEPTH_CAP: usize = 256;

/// 把一批任务折成一棵范围树。
///
/// 判据与拼装分开，好直接喂数据：一棵只能起整套网关才验得到的树，实际上不会
/// 被验。
pub(crate) fn build_wbs_tree(project_id: String, rows: &[&Task]) -> WbsResponse {
    let by_id: BTreeMap<&str, &Task> = rows.iter().map(|t| (t.id.as_str(), *t)).collect();

    // 子节点按 (创建时刻, id) 排：与甘特同一把尺子，两次读要逐行相同。
    let order = |v: &mut Vec<&Task>| {
        v.sort_by(|a, b| {
            a.created_at
                .cmp(&b.created_at)
                .then_with(|| a.id.cmp(&b.id))
        });
    };

    let mut children_of: BTreeMap<&str, Vec<&Task>> = BTreeMap::new();
    let mut root_tasks: Vec<&Task> = Vec::new();
    let mut orphan_tasks: Vec<&Task> = Vec::new();
    for t in rows {
        match t.parent_task_id.as_deref() {
            None | Some("") => root_tasks.push(t),
            // 自指不是非法父节点，是环里最短的一种；下面按环处理，不在这里放行。
            Some(p) if p == t.id.as_str() => {}
            Some(p) => match by_id.get(p) {
                Some(_) => children_of.entry(p).or_default().push(t),
                None => orphan_tasks.push(t),
            },
        }
    }
    order(&mut root_tasks);
    order(&mut orphan_tasks);

    let mut seen: std::collections::BTreeSet<&str> = std::collections::BTreeSet::new();
    let roots: Vec<WbsNode> = root_tasks
        .iter()
        .map(|t| build_subtree(t, 0, &children_of, &mut seen))
        .collect();
    let orphan_parents: Vec<WbsNode> = orphan_tasks
        .iter()
        .map(|t| build_subtree(t, 0, &children_of, &mut seen))
        .collect();

    // 剩下的：从任何根都走不到、父指针又不是「找不到」的那些。它们只可能在环里。
    let mut cycle_tasks: Vec<&Task> = rows
        .iter()
        .filter(|t| !seen.contains(t.id.as_str()))
        .copied()
        .collect();
    order(&mut cycle_tasks);
    let cycles: Vec<WbsNode> = cycle_tasks.iter().map(|t| WbsNode::leaf(t, 0)).collect();

    // 每一行都要么在一棵树里、要么在环里：三桶之和不等于行数就是有行静静地
    // 掉了——掉在哪一个桶外，界面上没有任何一处会说。
    debug_assert_eq!(seen.len() + cycles.len(), rows.len());

    WbsResponse {
        project_id,
        roots,
        orphan_parents,
        cycles,
        total: rows.len(),
        source: now_source("in-memory task graph"),
    }
}

/// 一棵子树。`seen` 防重入：一棵树里同一个 id 出现两次只会来自坏数据，但它
/// 一旦出现，按「节点数不等于行数」去报比让渲染器无限展开要好。
fn build_subtree<'a>(
    task: &'a Task,
    depth: usize,
    children_of: &BTreeMap<&'a str, Vec<&'a Task>>,
    seen: &mut std::collections::BTreeSet<&'a str>,
) -> WbsNode {
    let mut node = WbsNode::leaf(task, depth);
    if !seen.insert(task.id.as_str()) {
        return node;
    }
    if depth >= WBS_DEPTH_CAP {
        return node;
    }
    if let Some(children) = children_of.get(task.id.as_str()) {
        node.children = children
            .iter()
            .map(|c| build_subtree(c, depth + 1, children_of, seen))
            .collect();
    }
    node
}

pub async fn project_wbs_handler(
    State(state): State<Arc<GatewayState>>,
    claims: Option<Extension<cog_core::Claims>>,
    Path(id): Path<String>,
) -> Result<Json<WbsResponse>, ApiError> {
    let claims = claims.as_ref().map(|Extension(c)| c);
    let tasks = visible_tasks(&state, claims).await;
    let rows = tasks_of_project(&tasks, &id);
    if rows.is_empty() && id != UNGROUPED {
        return Err(ApiError::not_found(format!("project {id}")));
    }
    Ok(Json(build_wbs_tree(id, &rows)))
}

/// 一个项目的改动时间线：谁、何时、改前改后，最近的在前。
///
/// 写操作从这一版起都留一行审计，写进 `audit_logs`；这个端点是那些行的读者。
/// 没有读者时「写审计」只是往一张表里堆字节——而这一面最常犯的错，恰恰是有一个
/// 看起来完整的面、没有人从里面取过东西。
#[derive(Debug, serde::Deserialize)]
pub struct ProjectAuditQuery {
    /// 取多少行，默认 100，最多 500。
    #[serde(default)]
    pub limit: Option<i64>,
}

pub async fn project_audit_handler(
    State(state): State<Arc<GatewayState>>,
    claims: Option<Extension<cog_core::Claims>>,
    Path(id): Path<String>,
    Query(q): Query<ProjectAuditQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let claims = claims.as_ref().map(|Extension(c)| c);
    // 看不到的项目不给时间线：告诉一个租户「这个项目存在但你没权限」，
    // 与告诉它「不存在」是不同的信息泄漏。
    let tasks = visible_tasks(&state, claims).await;
    if tasks_of_project(&tasks, &id).is_empty() && id != UNGROUPED {
        return Err(ApiError::not_found(format!("project {id}")));
    }
    let limit = q.limit.unwrap_or(100).clamp(1, 500);
    let rows = match crate::project_audit::recent(&id, limit).await {
        Ok(Some(rows)) => rows,
        Ok(None) => {
            return Err(ApiError::service_unavailable(
                "this deployment has no audit store wired: the timeline of a project \
                 cannot be answered from anywhere else",
            )
            .with_code("audit_store_missing"))
        }
        // 有存储、读失败。与上面那条分开：那条说这台部署没有审计，这条说这一次没读到。
        // 客户看到「没有审计」会去问运维为什么关掉了审计，而真正的事是一次查询失败了。
        Err(e) => {
            return Err(ApiError::service_unavailable(format!(
                "the audit store did not answer: {e}"
            ))
            .with_code("audit_read_failed"))
        }
    };
    Ok(Json(serde_json::json!({
        "project_id": id,
        "count": rows.len(),
        "rows": rows,
        "source": now_source("audit_logs"),
    })))
}

/// 新建一个项目：它就是一整棵 goal 树的根。
///
/// 在此之前「项目」只能是提交一次 goal 的副产物，而那条路要经过分解（实测
/// 50–90 秒，异步），拿它建一个空项目既慢又会在树里长出十几条别人写的活。
/// 建项目与拆分是两件事，前者是范围管理的第一步。
#[derive(Debug, serde::Deserialize)]
pub struct CreateProjectRequest {
    /// 项目的名字。它就是根那一行在列表、树、看板上的标题。
    pub name: String,
    /// 归属租户。不填时只有在调用方恰好只属于一个 workspace 时才继承——猜一个
    /// 的代价是项目悄悄落到另一家名下。
    #[serde(default)]
    pub workspace_id: Option<String>,
}

#[derive(Debug, serde::Serialize)]
pub struct CreateProjectResponse {
    pub id: String,
    pub name: String,
    pub workspace_id: String,
    pub source: Source,
}

/// 新项目落在哪个租户。与 [`project_workspace`] 同一套口径：能继承就继承，
/// 不能继承就要求调用方说，绝不挑一个。
fn new_project_workspace(
    explicit: Option<&str>,
    claims: Option<&cog_core::Claims>,
) -> Result<String, String> {
    if let Some(ws) = explicit.map(str::trim).filter(|w| !w.is_empty()) {
        return Ok(ws.to_string());
    }
    match claims.map(|c| c.workspace_ids.as_slice()) {
        Some([one]) if !one.is_empty() => Ok(one.clone()),
        Some(many) if !many.is_empty() => Err(format!(
            "you belong to {} workspaces ({}): pass `workspace_id` to say which one this \
             project belongs to",
            many.len(),
            many.join(", ")
        )),
        _ => Err(
            "cannot decide which workspace this project belongs to: pass `workspace_id`"
                .to_string(),
        ),
    }
}

pub async fn create_project_handler(
    State(state): State<Arc<GatewayState>>,
    claims: Option<Extension<cog_core::Claims>>,
    Json(req): Json<CreateProjectRequest>,
) -> Result<(StatusCode, Json<CreateProjectResponse>), ApiError> {
    let claims = claims.as_ref().map(|Extension(c)| c);
    let name = req.name.trim();
    if name.is_empty() {
        return Err(ApiError::bad_request(
            "name is empty: a project with no name draws as its id everywhere",
        ));
    }
    if name.chars().count() > MAX_TITLE_CHARS {
        return Err(ApiError::bad_request(format!(
            "name is {} characters; the cap is {MAX_TITLE_CHARS}",
            name.chars().count()
        )));
    }
    let workspace = new_project_workspace(req.workspace_id.as_deref(), claims)
        .map_err(ApiError::bad_request)?;

    if let Some(c) = claims {
        if !c.workspace_ids.is_empty() && !c.workspace_ids.contains(&workspace) {
            if let Some(ref stream) = state.audit_stream {
                let _ = stream
                    .append(
                        cog_core::AuditKind::Authz,
                        &c.sub,
                        &workspace,
                        "authz.workspace_denied",
                        serde_json::json!({ "intent": "project.create" }),
                    )
                    .await;
            }
            return Err(ApiError::forbidden(format!(
                "workspace '{workspace}' is outside your workspace scope"
            )));
        }
    }

    let project_id = uuid::Uuid::new_v4().to_string();
    let mut root = Task::goal_row(project_id.clone(), name);
    root.workspace_id = Some(workspace.clone());
    state.orchestrator.add_task(root).await?;

    if let Some(ref stream) = state.audit_stream {
        let _ = stream
            .append(
                cog_core::AuditKind::Custom("project".into()),
                claims.map(|c| c.sub.as_str()).unwrap_or("system"),
                &project_id,
                "project.created",
                serde_json::json!({ "name": name, "workspace_id": workspace }),
            )
            .await;
    }
    crate::project_audit::record(cog_core::AuditEntry {
        actor: claims
            .map(|c| c.sub.clone())
            .unwrap_or_else(|| "system".into()),
        action: "project.created".into(),
        resource_type: "project".into(),
        resource_id: project_id.clone(),
        project_id: project_id.clone(),
        before: serde_json::Value::Null,
        after: serde_json::json!({ "name": name, "workspace_id": workspace }),
    })
    .await;

    Ok((
        StatusCode::CREATED,
        Json(CreateProjectResponse {
            id: project_id,
            name: name.to_string(),
            workspace_id: workspace,
            source: now_source("in-memory task graph"),
        }),
    ))
}

// ─── 写面：人能把活放进范围树 ──────────────────────────────────────────

/// 在项目下新建一条任务。
///
/// 这是这一面的第一个写端点。在此之前范围结构只能由编排侧的分解产出：任务图的
/// 十一个写端点都是状态机操作，没有一个是「新建一片活、挂在谁下面」。于是一个
/// 客户能看到一棵树，却没有一条路径能把树改对。
#[derive(Debug, serde::Deserialize)]
pub struct CreateProjectTaskRequest {
    /// 一行标题，就是它在树、看板、甘特上的名字。
    pub title: String,
    /// 挂在哪一片下面。不给就是项目的顶层。
    #[serde(default)]
    pub parent_task_id: Option<String>,
    /// 验收标准。工作包没有它，做完与没做完就没有判据。
    #[serde(default)]
    pub acceptance: Vec<String>,
    /// 上下游关系：这些任务必须先完成。
    #[serde(default)]
    pub blocked_by: Vec<String>,
    /// 只有项目自己的行跨了多个 workspace 时才需要：那时没有「项目的租户」
    /// 可以继承，必须在请求里说清楚这条活落在谁家。
    #[serde(default)]
    pub workspace_id: Option<String>,
}

#[derive(Debug, serde::Serialize)]
pub struct CreateProjectTaskResponse {
    pub id: String,
    pub project_id: String,
    pub parent_task_id: Option<String>,
    pub status: &'static str,
    pub source: Source,
}

/// 一条新任务放不进这棵树的哪一种原因。
///
/// 做成闭集而不是一串字符串：每一种的改法不同（换个父节点、换个项目、补一个
/// workspace），而读者只能从词上分辨它们。
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PlacementError {
    /// 未分组桶装的是「没有 goal 的行」，它不是容器，不能往里放新活。
    UngroupedIsNotAContainer,
    UnknownParent(String),
    ParentInAnotherProject {
        parent: String,
        project: String,
    },
    UnknownDependency(String),
    /// 项目自己的行没有 workspace，或者跨了几个：没有可继承的租户。
    WorkspaceAmbiguous(String),
}

impl PlacementError {
    /// 稳定的机器标识。改它等于改判据的身份，客户端按它分支。
    pub(crate) fn code(&self) -> &'static str {
        match self {
            Self::UngroupedIsNotAContainer => "ungrouped_is_not_a_container",
            Self::UnknownParent(_) => "unknown_parent",
            Self::ParentInAnotherProject { .. } => "parent_in_another_project",
            Self::UnknownDependency(_) => "unknown_dependency",
            Self::WorkspaceAmbiguous(_) => "workspace_ambiguous",
        }
    }

    fn message(&self) -> String {
        match self {
            Self::UngroupedIsNotAContainer => format!(
                "`{UNGROUPED}` is a bucket for rows that have no goal, not a container: \
                 a new task has to land inside a real project"
            ),
            Self::UnknownParent(parent) => format!(
                "parent `{parent}` does not exist in anything you can see: \
                 a parent pointer that names nothing would hang the new task off a node \
                 no reader will ever render"
            ),
            Self::ParentInAnotherProject { parent, project } => format!(
                "parent `{parent}` belongs to project `{project}`, not to this one: \
                 a cross-project parent makes the scope tree of one project reach into another"
            ),
            Self::UnknownDependency(dep) => format!(
                "blocked_by names `{dep}`, which is not a task of this project: \
                 the new task would wait on something that will never run"
            ),
            Self::WorkspaceAmbiguous(why) => format!(
                "cannot decide which workspace this task belongs to ({why}); \
                 pass `workspace_id` explicitly"
            ),
        }
    }

    fn into_api(self) -> ApiError {
        // 400 而不是 500：改的那一处是请求本身（换父节点、补 workspace），
        // 重试同一条请求不会自己变好。码走 `code` 那一格，不塞进 message——
        // 客户端按码分支，不按措辞。
        ApiError::bad_request(self.message()).with_code(self.code())
    }
}

/// 把一条新任务放进这棵树之前要过的三关：父节点存在、父节点在同一项目、
/// 依赖都在同一项目里。
///
/// 判据与拼装分开，是因为它才是这一面唯一的实质：一个只能起整套网关才验得到
/// 的校验，实际上不会被验。
pub(crate) fn check_placement(
    project_id: &str,
    tasks: &[Task],
    parent: Option<&str>,
    blocked_by: &[String],
) -> Result<(), PlacementError> {
    if project_id == UNGROUPED {
        return Err(PlacementError::UngroupedIsNotAContainer);
    }
    if let Some(parent) = parent {
        let found = tasks.iter().find(|t| t.id == parent);
        match found {
            None => return Err(PlacementError::UnknownParent(parent.to_string())),
            Some(p) if p.project_key() != project_id => {
                return Err(PlacementError::ParentInAnotherProject {
                    parent: parent.to_string(),
                    project: p.project_key().to_string(),
                })
            }
            Some(_) => {}
        }
    }
    for dep in blocked_by {
        match tasks.iter().find(|t| &t.id == dep) {
            // 跨项目的依赖同样不成立：它在另一个项目里，永远不会被当成这一片的上游
            // 去推进，而这条新任务会静静地等它到天荒地老。
            Some(t) if t.project_key() == project_id => {}
            _ => return Err(PlacementError::UnknownDependency(dep.clone())),
        }
    }
    Ok(())
}

/// 这条新任务落在哪个租户。
///
/// 继承优先于猜：项目自己那些行都写着同一个 workspace 时，新行跟着它；跨了几个
/// 或者一个都没有时不挑一个，而是要求调用方说清楚。挑一个的代价是数据悄悄落到
/// 另一个租户名下，而租户错了之后没有任何读面会红。
pub(crate) fn project_workspace(
    rows: &[&Task],
    explicit: Option<&str>,
) -> Result<String, PlacementError> {
    if let Some(ws) = explicit.map(str::trim).filter(|w| !w.is_empty()) {
        return Ok(ws.to_string());
    }
    let mut seen: Vec<&str> = rows
        .iter()
        .filter_map(|t| t.workspace_id.as_deref())
        .filter(|w| !w.is_empty())
        .collect();
    seen.sort_unstable();
    seen.dedup();
    match seen.as_slice() {
        [one] => Ok((*one).to_string()),
        [] => Err(PlacementError::WorkspaceAmbiguous(
            "none of this project's rows carries a workspace".into(),
        )),
        many => Err(PlacementError::WorkspaceAmbiguous(format!(
            "this project's rows span {} workspaces: {}",
            many.len(),
            many.join(", ")
        ))),
    }
}

/// 新的范围行。`id` 由调用方给（handler 用 uuid），这样同一段拼装也能在测试里
/// 拿固定 id 断言。
pub(crate) fn build_project_task(
    id: String,
    project_id: &str,
    workspace: &str,
    req: &CreateProjectTaskRequest,
) -> Task {
    let mut task = Task::new(
        id,
        cog_core::TaskType::DagNode,
        serde_json::json!({ "name": req.title.trim() }),
    );
    task.goal_id = Some(project_id.to_string());
    task.parent_task_id = req.parent_task_id.clone();
    task.workspace_id = Some(workspace.to_string());
    task.acceptance = req.acceptance.clone();
    task.blocked_by = req.blocked_by.clone();
    // 人建出来的一片活默认是可执行的叶子；它是不是父节点由别人挂不挂在它下面
    // 决定，不由这一行自己声明。
    task.is_executable = true;
    task
}

/// 标题的上限。超了拒绝而不是截断：截断之后存下来的就不是用户写的那个名字，
/// 而界面上没有任何地方会说它被改过。
pub(crate) const MAX_TITLE_CHARS: usize = 200;

pub async fn create_project_task_handler(
    State(state): State<Arc<GatewayState>>,
    claims: Option<Extension<cog_core::Claims>>,
    Path(id): Path<String>,
    Json(req): Json<CreateProjectTaskRequest>,
) -> Result<(StatusCode, Json<CreateProjectTaskResponse>), ApiError> {
    let claims = claims.as_ref().map(|Extension(c)| c);
    let title = req.title.trim();
    if title.is_empty() {
        return Err(ApiError::bad_request(
            "title is empty: a task with no name draws on the board as its id",
        ));
    }
    if title.chars().count() > MAX_TITLE_CHARS {
        return Err(ApiError::bad_request(format!(
            "title is {} characters; the cap is {MAX_TITLE_CHARS}",
            title.chars().count()
        )));
    }

    let tasks = visible_tasks(&state, claims).await;
    let rows = tasks_of_project(&tasks, &id);
    if rows.is_empty() && id != UNGROUPED {
        return Err(ApiError::not_found(format!("project {id}")));
    }

    check_placement(&id, &tasks, req.parent_task_id.as_deref(), &req.blocked_by)
        .map_err(PlacementError::into_api)?;
    let workspace =
        project_workspace(&rows, req.workspace_id.as_deref()).map_err(PlacementError::into_api)?;

    // 与任务面同一条判据：往别人的租户里放活的请求，连写都没开始就被拒。
    if let Some(c) = claims {
        if !c.workspace_ids.is_empty() && !c.workspace_ids.contains(&workspace) {
            if let Some(ref stream) = state.audit_stream {
                let _ = stream
                    .append(
                        cog_core::AuditKind::Authz,
                        &c.sub,
                        &workspace,
                        "authz.workspace_denied",
                        serde_json::json!({ "project_id": id }),
                    )
                    .await;
            }
            return Err(ApiError::forbidden(format!(
                "workspace '{workspace}' is outside your workspace scope"
            )));
        }
    }

    let task_id = uuid::Uuid::new_v4().to_string();
    let task = build_project_task(task_id.clone(), &id, &workspace, &req);
    let parent = task.parent_task_id.clone();
    state.orchestrator.add_task(task).await?;

    // 写下来就要查得到「谁在什么时候把这片活放进了哪棵树」。审计流是这一面
    // 唯一不会被后续写覆盖的地方。
    if let Some(ref stream) = state.audit_stream {
        let _ = stream
            .append(
                cog_core::AuditKind::Custom("project_task".into()),
                claims.map(|c| c.sub.as_str()).unwrap_or("system"),
                &task_id,
                "project.task_created",
                serde_json::json!({
                    "project_id": id,
                    "parent_task_id": parent,
                    "workspace_id": workspace,
                    "title": title,
                }),
            )
            .await;
    }
    crate::project_audit::record(cog_core::AuditEntry {
        actor: claims
            .map(|c| c.sub.clone())
            .unwrap_or_else(|| "system".into()),
        action: "project.task_created".into(),
        resource_type: "task".into(),
        resource_id: task_id.clone(),
        project_id: id.clone(),
        before: serde_json::Value::Null,
        after: serde_json::json!({
            "parent_task_id": parent,
            "workspace_id": workspace,
            "title": title,
        }),
    })
    .await;

    Ok((
        StatusCode::CREATED,
        Json(CreateProjectTaskResponse {
            id: task_id,
            project_id: id,
            parent_task_id: parent,
            status: TaskStatus::Pending.as_str(),
            source: now_source("in-memory task graph"),
        }),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::claims;
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

    /// 六列全是 `null` 有两种意思，读面必须分得开。
    ///
    /// 「这台没声明上限」与「这台声明了但一条都没接上」在每一格上逐字同形，
    /// 前者不用动、后者要去查部署。哪一档被抹成另一档都不行：把没声明读成
    /// 配好了，读的人会以为上限正在生效；把配坏了读成没声明，那一行错配置
    /// 就永远没人看见。
    #[test]
    fn an_undeclared_limit_and_a_declaration_that_parsed_to_nothing_read_differently() {
        // 一档：环境变量没设。
        let (limits, refused) = cog_core::BoardWipLimits::from_spec("");
        let unset = WipLimitsReading::read(None, &limits, refused);
        assert_eq!(unset.declared, None);
        assert!(
            unset.basis.contains("没声明") && unset.basis.contains("六列都不拦人"),
            "没设变量要自己说出来：{}",
            unset.basis
        );

        // 另一档：设了，但四个条目一条都用不上（0 不许、pending 不许、终态不许、
        // 拼错的列名不许）。
        let raw = "running=0,pending=4,completed=9,runnning=2";
        let (limits, refused) = cog_core::BoardWipLimits::from_spec(raw);
        assert_eq!(limits.iter().count(), 0, "这一档要真的解析不出任何上限");
        assert_eq!(refused.len(), 4);
        let broken = WipLimitsReading::read(Some(raw.into()), &limits, refused);
        assert_eq!(broken.declared.as_deref(), Some(raw));
        assert!(
            broken.basis.contains("一条都用不上") && !broken.basis.contains("没声明"),
            "配坏了不能读成没声明：{}",
            broken.basis
        );
        assert_eq!(
            broken.refused.len(),
            4,
            "被拒的条目要点名，否则「配了、没生效」不可见"
        );

        // 第三档：声明生效。这一档不许带「都用不上」或「没声明」的措辞。
        let (limits, refused) = cog_core::BoardWipLimits::from_spec("running=4");
        let live = WipLimitsReading::read(Some("running=4".into()), &limits, refused);
        assert!(live.basis.contains("1 条生效"), "{}", live.basis);
        assert!(live.refused.is_empty());
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
        let columns = build_board_columns(&[&t], std::slice::from_ref(&t), &limits);
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
        let columns = build_board_columns(&[&a, &b], &[a.clone(), b.clone()], &limits);
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

    /// 卡片要真的在这一列里，而且缺依赖、缺验收标准这两件事必须写在卡片上：
    /// 它们正是「这张卡现在能不能动、能不能给它排期」的判据，界面自己去猜
    /// 就得再读一次任务详情，两处判据迟早分开。
    #[test]
    fn a_column_carries_the_cards_it_counts_with_their_blocking_state() {
        let mut done_dep = task("dep", Some("g1"));
        done_dep.status = cog_core::TaskStatus::Completed;
        let mut a = task("a", Some("g1"));
        a.status = cog_core::TaskStatus::Pending;
        a.blocked_by = vec!["dep".into()];
        a.acceptance = vec!["the tests pass".into()];
        let mut b = task("b", Some("g1"));
        b.status = cog_core::TaskStatus::Pending;
        b.blocked_by = vec!["nobody".into()];
        let all = vec![done_dep, a.clone(), b.clone()];
        let rows: Vec<&Task> = vec![&a, &b];

        let (limits, _) = cog_core::BoardWipLimits::from_spec("");
        let columns = build_board_columns(&rows, &all, &limits);
        let pending = columns.iter().find(|c| c.status == "pending").unwrap();
        assert_eq!(pending.count, 2);
        assert_eq!(pending.cards.len(), 2, "计数是 2 就得有 2 张卡");
        assert_eq!(pending.cards_hidden, 0);

        let card_a = pending.cards.iter().find(|c| c.id == "a").unwrap();
        assert!(!card_a.blocked, "依赖已完成就不是卡住的");
        assert!(card_a.has_acceptance);
        let card_b = pending.cards.iter().find(|c| c.id == "b").unwrap();
        assert!(
            card_b.blocked,
            "依赖不在可见的这一批里也算卡住——看不见的依赖不能当成已完成"
        );
        assert!(!card_b.has_acceptance);
    }

    /// 一列超过上限时截断，并把没带回来的张数说出来。静默截断会让一列看起来
    /// 正好装得下，而「装得下」正是上限这一格要回答的问题。
    #[test]
    fn a_column_over_the_card_cap_says_how_many_it_left_out() {
        let tasks: Vec<Task> = (0..BOARD_CARDS_PER_COLUMN + 5)
            .map(|i| task(&format!("t{i:04}"), Some("g1")))
            .collect();
        let rows: Vec<&Task> = tasks.iter().collect();
        let (limits, _) = cog_core::BoardWipLimits::from_spec("");
        let columns = build_board_columns(&rows, &tasks, &limits);
        let pending = columns.iter().find(|c| c.status == "pending").unwrap();
        assert_eq!(pending.count, BOARD_CARDS_PER_COLUMN + 5);
        assert_eq!(pending.cards.len(), BOARD_CARDS_PER_COLUMN);
        assert_eq!(pending.cards_hidden, 5);
    }

    /// 六列的集合与顺序固定：缺的列要显式出 0，否则「这一列没有任务」与
    /// 「这一列不在读数里」在读面上同形。
    #[test]
    fn board_columns_are_all_six_statuses_in_a_fixed_order() {
        let t = task("a", Some("g1"));
        let (limits, _) = cog_core::BoardWipLimits::from_spec("");
        let columns = build_board_columns(&[&t], std::slice::from_ref(&t), &limits);
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

    /// 基线一条预算都没带时，计划价值与挣值都是「量不出来」，不是 0。
    ///
    /// 基线上的 `budget_tokens = 0` 写的是「这件活没有成本计划」，不是「计划花零个
    /// token」：估算器要等同类任务量出过花销才写得下预算。折成 `measured(0)` 之后，
    /// 「一条都没量出来」与「计划价值真的是零」在屏幕上是一模一样的两个 0，而下一句
    /// 推论就是「一条活都没挣到」。现网 2026-10-26 实测：有基线的项目 2348 个，预算
    /// 全是 0——这两个格子原来就是这么显示的。
    #[test]
    fn baselines_carrying_no_budget_make_the_token_quantities_unmeasurable() {
        let now = chrono::Utc::now();
        let mut done = task("a", Some("g1"));
        done.status = TaskStatus::Completed;
        let window = (
            now - chrono::Duration::minutes(5),
            now + chrono::Duration::minutes(5),
        );
        let plans: BTreeMap<String, cog_core::TaskPlan> =
            [("a".to_string(), plan_between("a", 0, window.0, window.1))]
                .into_iter()
                .collect();
        let spends: BTreeMap<String, u64> = [("a".to_string(), 0u64)].into_iter().collect();

        let reading = build_evm_response(
            "g1".into(),
            &[&done],
            Some(&plans),
            Some(&spends),
            None,
            now,
        );

        assert_eq!(reading.planned_value.value, None, "没有预算就不是 0");
        assert_eq!(reading.earned_value.value, None);
        assert!(
            reading
                .planned_value
                .unmeasurable
                .as_deref()
                .is_some_and(|reason| reason.contains("预算")),
            "要说清为什么量不出来，得到 {:?}",
            reading.planned_value.unmeasurable
        );
        assert!(reading.earned_value.unmeasurable.is_some());
        assert_eq!(reading.schedule_performance_index, None);

        // 花销那一列不受影响：一条都没花过钱是量得出来的 0，而它照样进不了超支表。
        assert_eq!(reading.actual_cost.value, Some(0));
        assert_eq!(reading.cost_performance_index, None);
        assert!(reading.overruns.expect("两张表都读得到").is_empty());
        assert_eq!(reading.attribution.tasks_outside_baseline, Some(0));
    }

    /// 超支与落后是两张逐节点的表。指数只说「整体偏了」，而修法要知道是哪一条：
    /// 超支的那条要重排预算，落后的那条要看依赖。
    ///
    /// 三条边界也钉在这里：花销到线不算超（`>` 不是 `>=`）；没有基线的任务不进
    /// 超支表（没有预算就谈不上超）；过了止期但已经失败的不算落后（那走重规划，
    /// 不是进度问题）。
    #[test]
    fn overruns_and_behind_name_the_nodes_that_are_actually_over() {
        let now = chrono::Utc::now();
        let mut over = task("over", Some("g1"));
        over.status = TaskStatus::Running;
        let mut exact = task("exact", Some("g1"));
        exact.status = TaskStatus::Running;
        let unplanned = task("unplanned", Some("g1"));
        let mut late = task("late", Some("g1"));
        late.status = TaskStatus::Pending;
        let mut failed = task("failed", Some("g1"));
        failed.status = TaskStatus::Failed;

        let window = (
            now - chrono::Duration::minutes(5),
            now + chrono::Duration::minutes(5),
        );
        let plans: BTreeMap<String, cog_core::TaskPlan> = [
            (
                "over".to_string(),
                plan_between("over", 100, window.0, window.1),
            ),
            (
                "exact".to_string(),
                plan_between("exact", 100, window.0, window.1),
            ),
            (
                "late".to_string(),
                plan_between(
                    "late",
                    100,
                    now - chrono::Duration::minutes(10),
                    now - chrono::Duration::minutes(5),
                ),
            ),
            (
                "failed".to_string(),
                plan_between(
                    "failed",
                    100,
                    now - chrono::Duration::minutes(10),
                    now - chrono::Duration::minutes(5),
                ),
            ),
        ]
        .into_iter()
        .collect();
        let spends: BTreeMap<String, u64> = [
            ("over".to_string(), 150u64),
            ("exact".to_string(), 100u64),
            ("unplanned".to_string(), 999_999u64),
        ]
        .into_iter()
        .collect();

        let reading = build_evm_response(
            "g1".into(),
            &[&over, &exact, &unplanned, &late, &failed],
            Some(&plans),
            Some(&spends),
            None,
            now,
        );

        let overruns = reading.overruns.expect("两条账都读到了");
        assert_eq!(overruns.len(), 1, "只报真超了的那一条：{overruns:?}");
        assert_eq!(overruns[0].task_id, "over");
        assert_eq!(overruns[0].budget_tokens, 100);
        assert_eq!(overruns[0].spent_tokens, 150);

        let behind = reading.behind_schedule.expect("基线读到了");
        assert_eq!(behind.len(), 1, "只报过了止期还在等的那一条：{behind:?}");
        assert_eq!(behind[0].task_id, "late");
        assert_eq!(behind[0].status, "pending");
        assert!(behind[0].overdue_secs >= 300, "要带上过了多久");
    }

    /// 读不到的时候两张表都是 `None`。给空表读作「一条也没超、一条也没落后」，
    /// 而真相是这一屏回答不了这个问题。
    #[test]
    fn unreadable_inputs_leave_the_two_node_lists_unmeasured_not_empty() {
        let now = chrono::Utc::now();
        let t = task("a", Some("g1"));
        let blind = build_evm_response("g1".into(), &[&t], None, None, None, now);
        assert!(blind.overruns.is_none());
        assert!(blind.behind_schedule.is_none());

        // 基线读得到、账读不到：超支表量不了，落后表照给。
        let plans: BTreeMap<String, cog_core::TaskPlan> = [(
            "a".to_string(),
            plan_between("a", 100, now - chrono::Duration::minutes(9), now),
        )]
        .into_iter()
        .collect();
        let no_ledger = build_evm_response("g1".into(), &[&t], Some(&plans), None, None, now);
        assert!(no_ledger.overruns.is_none(), "账读不到就不知道谁超了");
        assert!(no_ledger.behind_schedule.is_some(), "止期是本地读到的");
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

    /// 父节点必须在这一棵树上，而且必须在**同一棵树**上：跨项目的父指针会让一个
    /// 项目的范围树伸进另一个项目，而两个项目的行在界面上各画各的，那条边永远
    /// 没人看得见。
    #[test]
    fn a_parent_from_another_project_is_refused() {
        let tasks = vec![task("p-other", Some("g2")), task("a", Some("g1"))];
        let err = check_placement("g1", &tasks, Some("p-other"), &[]).unwrap_err();
        assert_eq!(
            err,
            PlacementError::ParentInAnotherProject {
                parent: "p-other".into(),
                project: "g2".into()
            }
        );
        assert_eq!(err.code(), "parent_in_another_project");
    }

    /// 指着一个不存在的父节点挂上去，这一片活会挂在任何读面都渲染不出来的地方：
    /// 它会被调度、会跑完、会报 Completed，只是从来不在任何一棵树上。
    #[test]
    fn a_parent_that_names_nothing_is_refused() {
        let tasks = vec![task("a", Some("g1"))];
        let err = check_placement("g1", &tasks, Some("ghost"), &[]).unwrap_err();
        assert_eq!(err, PlacementError::UnknownParent("ghost".into()));
    }

    /// 未分组不是容器：它是「没有 goal 的行」那个桶。往桶里放新活，等于造一条
    /// 永远没有 goal 的任务，而它进桶的理由（用户当初没写 goal）根本不成立。
    #[test]
    fn the_ungrouped_bucket_is_not_a_container() {
        let tasks = vec![task("a", Some("g1"))];
        let err = check_placement(UNGROUPED, &tasks, None, &[]).unwrap_err();
        assert_eq!(err, PlacementError::UngroupedIsNotAContainer);
    }

    /// 依赖也必须在同一项目里：指向别处的上游永远不会被当成这一片的上游去推进，
    /// 这条新任务就静静地等到天荒地老。
    #[test]
    fn a_dependency_outside_the_project_is_refused() {
        let tasks = vec![task("dep", Some("g2"))];
        let err = check_placement("g1", &tasks, None, &["dep".into()]).unwrap_err();
        assert_eq!(err, PlacementError::UnknownDependency("dep".into()));
    }

    /// 顶层任务与项目内的父节点都要放行——拒绝不是这一格的默认答案。
    #[test]
    fn a_root_task_and_a_same_project_parent_are_both_allowed() {
        let tasks = vec![task("p", Some("g1")), task("dep", Some("g1"))];
        check_placement("g1", &tasks, None, &[]).expect("顶层任务");
        check_placement("g1", &tasks, Some("p"), &["dep".into()]).expect("项目内的父与依赖");
    }

    /// 新任务继承项目的租户：项目自己的行都写着同一个 workspace 时，跟着它走。
    /// 这条是新任务落在谁家名下唯一的判据——猜错之后没有任何读面会红。
    #[test]
    fn a_new_task_inherits_the_projects_workspace() {
        let mut a = task("a", Some("g1"));
        a.workspace_id = Some("default".into());
        let mut b = task("b", Some("g1"));
        b.workspace_id = Some("default".into());
        let rows = vec![&a, &b];
        assert_eq!(project_workspace(&rows, None).unwrap(), "default");
        assert_eq!(project_workspace(&rows, Some("other")).unwrap(), "other");
    }

    /// 跨了几个 workspace 的项目里不挑一个：挑中的那个会让一半的行看到一条
    /// 不属于自己租户的新任务，而另一半看不到。
    #[test]
    fn an_ambiguous_workspace_is_refused_rather_than_guessed() {
        let mut a = task("a", Some("g1"));
        a.workspace_id = Some("default".into());
        let mut b = task("b", Some("g1"));
        b.workspace_id = Some("other".into());
        let rows = vec![&a, &b];
        let err = project_workspace(&rows, None).unwrap_err();
        assert_eq!(err.code(), "workspace_ambiguous");

        // 一行都没有 workspace 时同样不猜：那是「这个项目不归属于任何人」，
        // 不是「归属默认租户」。
        let bare = task("c", Some("g1"));
        assert_eq!(
            project_workspace(&[&bare], None).unwrap_err().code(),
            "workspace_ambiguous"
        );
    }

    /// 调用方只属于一个 workspace ⇒ 新项目跟着它走，不用每次都说。
    #[test]
    fn a_new_project_inherits_the_only_workspace_of_its_creator() {
        let c = claims(&["acme"]);
        assert_eq!(new_project_workspace(None, Some(&c)).unwrap(), "acme");
    }

    /// 属于多个 workspace 时不挑一个：挑中的那个会让项目出现在一半同事看得到、
    /// 另一半看不到的地方，而创建它的人不会收到任何提示。
    #[test]
    fn a_creator_with_several_workspaces_must_name_one() {
        let c = claims(&["acme", "globex"]);
        let err = new_project_workspace(None, Some(&c)).unwrap_err();
        assert!(
            err.contains("workspace_id"),
            "判词要让读者知道改哪一处: {err}"
        );
        assert_eq!(
            new_project_workspace(Some("globex"), Some(&c)).unwrap(),
            "globex"
        );
    }

    /// 没有 claims（内部调用）也没有显式 workspace 时同样不猜。
    #[test]
    fn a_caller_with_no_workspace_must_name_one() {
        assert!(new_project_workspace(None, None).is_err());
        let empty = claims(&[]);
        assert!(new_project_workspace(None, Some(&empty)).is_err());
    }

    /// 一棵正常的树：父节点带着它的子节点，深度按层递增。
    #[test]
    fn the_wbs_tree_nests_children_under_their_parent() {
        let mut root = task("g1", Some("g1"));
        root.is_executable = false;
        let mut child = task("c1", Some("g1"));
        child.parent_task_id = Some("g1".into());
        let mut grandchild = task("c2", Some("g1"));
        grandchild.parent_task_id = Some("c1".into());
        let rows = vec![&root, &child, &grandchild];

        let wbs = build_wbs_tree("g1".into(), &rows);
        assert_eq!(wbs.total, 3);
        assert_eq!(wbs.roots.len(), 1, "只有没父节点的那一片是根");
        assert!(wbs.orphan_parents.is_empty());
        assert!(wbs.cycles.is_empty());
        let top = &wbs.roots[0];
        assert_eq!(top.id, "g1");
        assert_eq!(top.depth, 0);
        assert_eq!(top.children.len(), 1);
        assert_eq!(top.children[0].id, "c1");
        assert_eq!(top.children[0].depth, 1);
        assert_eq!(top.children[0].children[0].id, "c2");
        assert_eq!(top.children[0].children[0].depth, 2);
    }

    /// 父指针指着项目外的东西：那一片活进 `orphan_parents`，**不进 roots**。
    /// 混进 roots 的话，一棵缺了一块的树会被读成一棵完整的树。
    #[test]
    fn a_task_whose_parent_is_missing_lands_in_orphan_parents_not_in_roots() {
        let mut orphan = task("o1", Some("g1"));
        orphan.parent_task_id = Some("gone".into());
        let root = task("g1", Some("g1"));
        let rows = vec![&root, &orphan];

        let wbs = build_wbs_tree("g1".into(), &rows);
        assert_eq!(wbs.roots.len(), 1);
        assert_eq!(wbs.roots[0].id, "g1");
        assert_eq!(wbs.orphan_parents.len(), 1);
        assert_eq!(wbs.orphan_parents[0].id, "o1");
        assert_eq!(
            wbs.orphan_parents[0].depth, 0,
            "它自己就是这一支的顶，深度从 0 起，但它不是项目的根"
        );
    }

    /// 环里的节点从任何根都走不到。按「递归画子节点」硬走会卡死渲染器，所以
    /// 它们单独成一桶，三个桶加起来必须等于总行数。
    #[test]
    fn a_parent_cycle_lands_in_its_own_bucket_and_nothing_is_dropped() {
        let mut a = task("a", Some("g1"));
        a.parent_task_id = Some("b".into());
        let mut b = task("b", Some("g1"));
        b.parent_task_id = Some("a".into());
        let root = task("g1", Some("g1"));
        let rows = vec![&root, &a, &b];

        let wbs = build_wbs_tree("g1".into(), &rows);
        assert_eq!(wbs.roots.len(), 1);
        assert!(wbs.orphan_parents.is_empty());
        assert_eq!(wbs.cycles.len(), 2, "环里的两片都要被点出来，不是丢掉一片");
        assert_eq!(wbs.total, 3);
        let in_buckets = wbs.roots.len() + wbs.orphan_parents.len() + wbs.cycles.len();
        assert_eq!(in_buckets, 1 + 2);
    }

    /// 自己指着自己是环里最短的一种，不能当成「没有父节点」。
    #[test]
    fn a_self_parent_is_a_cycle() {
        let mut t = task("s", Some("g1"));
        t.parent_task_id = Some("s".into());
        let rows = vec![&t];
        let wbs = build_wbs_tree("g1".into(), &rows);
        assert!(wbs.roots.is_empty());
        assert_eq!(wbs.cycles.len(), 1);
        assert_eq!(wbs.total, 1);
    }

    /// 拆列出来的节点带着人写的验收标准：条数为 0 的任务在树上是「没有做完的
    /// 判据」，这个 0 必须能读出来。
    #[test]
    fn a_node_carries_its_acceptance_count() {
        let mut t = task("a", Some("g1"));
        t.acceptance = vec!["跑通门测试".into(), "读数有消费者".into()];
        let rows = vec![&t];
        let wbs = build_wbs_tree("g1".into(), &rows);
        assert_eq!(wbs.roots[0].acceptance, 2);
    }

    /// 拼出来的那一行要带着人写的东西：名字、父节点、验收标准、依赖、租户。
    /// 少一样，界面上画出来的就是一条和用户写的不一样的活。
    #[test]
    fn the_new_row_carries_what_the_caller_wrote() {
        let req = CreateProjectTaskRequest {
            title: "  拆出第一片  ".into(),
            parent_task_id: Some("p".into()),
            acceptance: vec!["跑通门测试".into()],
            blocked_by: vec!["dep".into()],
            workspace_id: None,
        };
        let t = build_project_task("new".into(), "g1", "default", &req);
        assert_eq!(t.id, "new");
        assert_eq!(t.goal_id.as_deref(), Some("g1"));
        assert_eq!(t.parent_task_id.as_deref(), Some("p"));
        assert_eq!(t.workspace_id.as_deref(), Some("default"));
        assert_eq!(t.acceptance, vec!["跑通门测试".to_string()]);
        assert_eq!(t.blocked_by, vec!["dep".to_string()]);
        assert_eq!(
            t.input["name"],
            serde_json::json!("拆出第一片"),
            "标题两端的空白不该进名字"
        );
        assert_eq!(t.status, TaskStatus::Pending);
        assert!(t.is_executable);
    }
}
