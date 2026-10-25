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
pub const UNGROUPED: &str = "ungrouped";

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
    pub rows: Vec<GanttRow>,
    pub source: Source,
}

/// 任务图的正本表名。项目面自己不带表，读的就是它。
const TASK_TABLE: &str = "cog_dag_tasks";

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

fn project_key(task: &Task) -> &str {
    match task.goal_id.as_deref() {
        Some(g) if !g.is_empty() => g,
        _ => UNGROUPED,
    }
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
fn build_project_rows(tasks: &[Task]) -> Vec<ProjectRow> {
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
                planned: false,
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
    Ok(Json(ProjectsResponse {
        projects: build_project_rows(&tasks),
        total_tasks,
        source: now_source("in-memory task graph"),
    }))
}

fn tasks_of_project<'a>(tasks: &'a [Task], id: &str) -> Vec<&'a Task> {
    tasks.iter().filter(|t| project_key(t) == id).collect()
}

fn build_board_columns(rows: &[&Task]) -> Vec<BoardColumn> {
    status_counts(rows)
        .into_iter()
        .map(|c| BoardColumn {
            status: c.status,
            count: c.count,
            // 列上限要等计划基线（期 2）才有出处。现在留 `None` 而不是填一个默认数：
            // 填了默认数，界面就会说「上限 32，未超」，而实际上没有任何东西在拦派发。
            wip_limit: None,
            over_limit: None,
        })
        .collect()
}

fn build_gantt_rows(rows: &[&Task]) -> Vec<GanttRow> {
    let mut out: Vec<GanttRow> = rows
        .iter()
        .map(|t| GanttRow {
            id: t.id.clone(),
            title: task_title(t),
            status: t.status.as_str().to_string(),
            created_at: t.created_at.to_rfc3339(),
            started_at: t.started_at.map(|d| d.to_rfc3339()),
            updated_at: t.updated_at.to_rfc3339(),
            planned_start: None,
            planned_end: None,
            unplanned: true,
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

    let columns = build_board_columns(&rows);

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

    Ok(Json(GanttResponse {
        project_id: id,
        has_plan: false,
        rows: build_gantt_rows(&rows),
        source: now_source("in-memory task graph"),
    }))
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
        let rows = build_project_rows(&tasks);

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
        let rows = build_project_rows(&tasks);
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

        let rows = build_project_rows(&[satisfied, waiting, dep_done, dep_open, unknown]);
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
        let rows = build_project_rows(&[task("a", Some("g1"))]);
        assert_eq!(rows[0].blocked, 0);
    }

    /// 没有计划基线的行必须标成未排期，且计划两头都是 None。画成一根零长度的
    /// 条会被读成「计划已完成」——空白的形状在这里不能含糊。
    #[test]
    fn gantt_rows_without_a_baseline_are_marked_unplanned_not_zero_length() {
        let t = task("a", Some("g1"));
        let rows = build_gantt_rows(&[&t]);
        assert_eq!(rows.len(), 1);
        assert!(rows[0].unplanned);
        assert_eq!(rows[0].planned_start, None);
        assert_eq!(rows[0].planned_end, None);
    }

    /// 标题取不到就露出 id，不编一个名字：编出来的名字读者会当成真的。
    #[test]
    fn gantt_title_falls_back_to_the_id_instead_of_inventing_one() {
        let mut t = task("task-42", Some("g1"));
        t.input = serde_json::json!({ "unrelated": 1 });
        let rows = build_gantt_rows(&[&t]);
        assert_eq!(rows[0].title, "task-42");
    }

    /// 没配上限就读作「没配」，不能读作「没超」。`wip_limit: Some(默认数)` 会让
    /// 界面宣称一个并不存在的准入控制正在生效。
    #[test]
    fn board_columns_report_no_limit_as_absent_rather_than_as_no_overflow() {
        let t = task("a", Some("g1"));
        let columns = build_board_columns(&[&t]);
        assert!(columns.iter().all(|c| c.wip_limit.is_none()));
        assert!(columns.iter().all(|c| c.over_limit.is_none()));
    }

    /// 六列的集合与顺序固定：缺的列要显式出 0，否则「这一列没有任务」与
    /// 「这一列不在读数里」在读面上同形。
    #[test]
    fn board_columns_are_all_six_statuses_in_a_fixed_order() {
        let t = task("a", Some("g1"));
        let columns = build_board_columns(&[&t]);
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

    /// 两次读同一份数据，行序必须逐行相同：顺序不稳定，界面每次刷新都在跳，
    /// 读者没法比对前后两张图。
    #[test]
    fn project_order_is_stable_across_reads() {
        let tasks = vec![
            task("a", Some("g2")),
            task("b", Some("g1")),
            task("c", Some("g2")),
        ];
        let first = build_project_rows(&tasks);
        let second = build_project_rows(&tasks);
        let ids: Vec<&str> = first.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(ids, vec!["g2", "g1"], "任务多的在前，同数按 id 升序");
        assert_eq!(
            ids,
            second.iter().map(|r| r.id.as_str()).collect::<Vec<_>>()
        );
    }
}
