//! `GET /api/v1/output` —— 这一窗口里回来了什么。
//!
//! 产出不是变更。变更面记的是「谁提了什么、走到哪一步」，那是一条**过程**账；产出面
//! 记的是**离开系统的东西**：完成的活、构建出来的产物、带着提交落地的变更。一条变更可以
//! 走完全程而没有产出（测量说它没落地），一件产出也可以不经过变更账（一个任务跑完就
//! 写下了文件）。所以这一面不读变更账的结算数当主数——那会把「忙了一场」读成「交出了
//! 东西」。
//!
//! **不新建任何存储。** 这张表由已经在跑的三路拼出来：
//!
//! - 完成的活：进程内任务图（`progress::merged_tasks` 的那一份）与库里的面节点；
//! - 构建产物：演进进程的构建跑次（`BuildRunsView`），本进程窗口，重启清零；
//! - 带提交落地的变更：变更账的漏斗（`ChangeLedgerStore::funnel`）里 `landed_with_rev`
//!   那一栏——那些行的落定带着一个真的提交。它与同一个漏斗里的 `landed` 不是一回事：
//!   后者绝大多数是「文档要的，树本来就满足」，一条提交都没送出去，把它读成产出会把
//!   一天五条的交付读成一百条。这一层按 **UTC 天**记账、按**落定时刻**分天——那是账被
//!   关掉的时刻，可能晚于提交本身——窗口边上那一天整天计入，页面上写清楚。它数的不是
//!   提交条数：同一个提交送了几条变更就计几次，一个提交能同时落好几条。
//!
//!   落地流程内部的 `LandingRecord`（`cog-github`）仍然没有发布成读数：它落在部署器
//!   自己的盘上，网关是另一个进程。所以「哪一条提交、什么时刻」这一层答不了，只答
//!   「有几条变更带着提交落了地」。
//!
//! **完成不等于交付。** 任务行走到 `Completed` 只是说这次执行结束了，`result` 才是
//! 交回来的东西。带结果与不带结果分两栏印：把两者合成一个数，读的人会以为每一条完成
//! 都交回了东西。
//!
//! 窗口按任务的 `updated_at` 划。任务没有单独的 `completed_at` 列，`updated_at` 是
//! 最近一次写入；对一条已结束的行，它在完成时刻之后，所以窗口边上的那条可能被多算进
//! 来一条相邻窗口的活，不会被漏掉。这一点写在页面上，不留给读者去猜。

use std::collections::BTreeMap;

use axum::extract::State;
use axum::Json;
use chrono::{DateTime, Duration, Utc};
use serde::Serialize;

use crate::error::ApiError;
use crate::GatewayState;

/// 一层产出这一拍读到了没有。
///
/// 与能力面同一个形状：`measured` 印值，`unwired` 说清缺哪一层，`unread` 说清这一拍
/// 为什么读失败。三态要做的修复各不相同，折成一个数就分不出该修哪儿。
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum Reading<T> {
    Measured { value: T },
    Unwired { because: String },
    Unread { because: String },
}

impl<T> Reading<T> {
    pub fn measured(value: T) -> Self {
        Reading::Measured { value }
    }

    pub fn unwired(because: impl Into<String>) -> Self {
        Reading::Unwired {
            because: because.into(),
        }
    }

    pub fn unread(because: impl Into<String>) -> Self {
        Reading::Unread {
            because: because.into(),
        }
    }

    pub fn value(&self) -> Option<&T> {
        match self {
            Reading::Measured { value } => Some(value),
            _ => None,
        }
    }
}

/// 这一窗口结束的活。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Work {
    /// 结束且成功的条数。主数就是这个。
    pub completed: usize,
    /// 其中带回了 `result` 的条数。它是「交付」，`completed` 减去它是「记账」。
    pub with_result: usize,
    /// 结束且失败的条数。放在同一行，是为了让上面的数有一个分母：只印完成数时，
    /// 一百条里成功三条和一百条里成功九十七条读起来一样。
    pub failed: usize,
    /// 这一层的限定语（窗口在谁的内存里）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

/// 这一窗口构建出来的东西。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Builds {
    /// 结局是 `built` 的跑次：一个可部署的东西真的出来了。
    pub built: usize,
    /// 这一窗口里记下的跑次总数，含失败与半途。
    pub total: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

/// 这一窗口里带着提交落地的变更。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Commits {
    /// 落定时带回了提交的变更条数（[`ChangeFunnelDay::landed_with_rev`] 的和）。
    pub delivered: u64,
    /// 这个数覆盖几个 UTC 天。漏斗按天记账，窗口按小时算时向上取整到天。
    pub days: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

/// 一条完成的活。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Delivery {
    pub id: String,
    /// 它落在哪个项目桶里。没有归属的活也是活，落在 `ungrouped`。
    pub project: String,
    /// 谁做的。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker: Option<String>,
    /// 结束时刻（见模块头：这一列是 `updated_at`）。
    pub finished_at: DateTime<Utc>,
    /// 交回来东西了没有。`false` 的行不是交付，是记账。
    pub carried_result: bool,
    /// 结果的开头一段。整份结果可能很大，这里只放够认出来的那一段。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_excerpt: Option<String>,
}

/// 一个项目这一窗口交了多少。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ProjectOutput {
    pub project: String,
    pub completed: usize,
    pub with_result: usize,
}

/// 这一窗口产出的一张全貌。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Outputs {
    pub generated_at: DateTime<Utc>,
    pub window_hours: i64,
    pub work: Reading<Work>,
    pub builds: Reading<Builds>,
    /// 带着提交落地的变更。没有变更账时是 `unwired`，读失败是 `unread`。
    pub commits: Reading<Commits>,
    /// 最近的完成行，新在前。最多 [`RECENT_ROWS`] 条。
    pub recent: Vec<Delivery>,
    /// 按项目汇总，条数多的在前。
    pub by_project: Vec<ProjectOutput>,
}

/// 这一面一次带多少行明细。
// cap: bound — 界的是单次响应的体积；窗口里有更多完成行时，汇总数照旧是全部
pub const RECENT_ROWS: usize = 50;

/// 结果摘录多少字符。
// cap: bound — 界的是单行体积；结果本身不截断，只在页面上放开头一段
pub const RESULT_EXCERPT_CHARS: usize = 160;

/// 拼这份产出的原料。
///
/// 单独一个结构，是为了让工作台那一格与这一面用**同一次**读算出同一个数：两处各自
/// 读一遍任务图，会在同一次请求里给出两个版本。
pub struct Inputs<'a> {
    pub now: DateTime<Utc>,
    pub window_hours: i64,
    /// 任务图。`None` 是这一拍没读到。
    pub tasks: Option<&'a [cog_core::Task]>,
    /// 构建跑次。外层 `None` 是没有演进管理面。
    pub builds: Option<&'a Result<cog_core::BuildRunsView, String>>,
    /// 变更漏斗的日账。外层 `None` 是没有变更账。
    pub funnel: Option<
        &'a Result<Vec<cog_core::contract::change_ledger::ChangeFunnelDay>, cog_core::SFError>,
    >,
}

/// 一份「什么都没读到」的产出。
///
/// 存在的理由与能力清单的 `Default` 相同：调用方要能说「这一拍没有产出读数」而不
/// 自己编一份。三层各自说自己没读到，所以工作台那几行印的是「缺哪一层」，不是 0。
impl Default for Outputs {
    fn default() -> Self {
        build(Inputs {
            now: Utc::now(),
            window_hours: crate::observability::spend_window_hours(),
            tasks: None,
            builds: None,
            funnel: None,
        })
    }
}

/// 这一层缺席的理由，两种。
///
/// 写成常量而不是散在拼装处：引用它的那一处与页面上的说明是两份文本，改一处而漏另
/// 一处时，页面上会印一句与接口不符的话。
pub const COMMITS_UNWIRED: &str = "这个部署没有变更账：带提交落地的变更这一层读不到";

/// 变更账实现了，但漏斗这一栏没实现。
pub const FUNNEL_UNWIRED: &str = "变更账没有发布漏斗读数（funnel 未实现），这一层缺的是那一栏";

/// 漏斗按 UTC 天记账，窗口按小时算：取几个整天由窗口决定，向上取整。
///
/// 取整方向不是随手定的：少取一天会漏掉窗口开头那一段，多取一天会把窗口外的活算进
/// 来。宁可多算一天并在页面上写清楚「边上那一天整天计入」——漏掉的那一段没人看得出
/// 来，多算的那一段写在注里。
pub fn funnel_days(window_hours: i64) -> u32 {
    ((window_hours.max(1) + 23) / 24) as u32
}

pub fn build(inputs: Inputs<'_>) -> Outputs {
    let window_start = inputs.now - Duration::hours(inputs.window_hours);

    let (work, recent, by_project) = match inputs.tasks {
        None => (
            Reading::unread("这一拍没读到任务图"),
            Vec::new(),
            Vec::new(),
        ),
        Some(tasks) => {
            let mut completed = 0usize;
            let mut with_result = 0usize;
            let mut failed = 0usize;
            let mut per_project: BTreeMap<String, (usize, usize)> = BTreeMap::new();
            let mut finished: Vec<&cog_core::Task> = Vec::new();

            for task in tasks {
                // 窗口外的行不参与任何一栏：一条上周结束的活这一窗没有回来。
                if task.updated_at < window_start {
                    continue;
                }
                match task.status {
                    cog_core::TaskStatus::Completed => {
                        completed += 1;
                        let carried = task.result.is_some();
                        if carried {
                            with_result += 1;
                        }
                        let entry = per_project
                            .entry(task.project_key().to_string())
                            .or_insert((0, 0));
                        entry.0 += 1;
                        if carried {
                            entry.1 += 1;
                        }
                        finished.push(task);
                    }
                    cog_core::TaskStatus::Failed => failed += 1,
                    _ => {}
                }
            }

            // 新在前；同一时刻按 id 兜底，否则同一秒完成的两条每次刷新换顺序。
            finished.sort_by(|a, b| {
                b.updated_at
                    .cmp(&a.updated_at)
                    .then_with(|| a.id.cmp(&b.id))
            });

            let recent: Vec<Delivery> = finished
                .iter()
                .take(RECENT_ROWS)
                .map(|task| Delivery {
                    id: task.id.clone(),
                    project: task.project_key().to_string(),
                    worker: task.agent_id.clone(),
                    finished_at: task.updated_at,
                    carried_result: task.result.is_some(),
                    result_excerpt: task.result.as_ref().map(excerpt),
                })
                .collect();

            let mut per_project: Vec<ProjectOutput> = per_project
                .into_iter()
                .map(|(project, (completed, with_result))| ProjectOutput {
                    project,
                    completed,
                    with_result,
                })
                .collect();
            // 条数多的在前，同数按名字：图表的顺序不能随哈希表的遍历次序变。
            per_project.sort_by(|a, b| {
                b.completed
                    .cmp(&a.completed)
                    .then_with(|| a.project.cmp(&b.project))
            });

            (
                Reading::measured(Work {
                    completed,
                    with_result,
                    failed,
                    note: Some(
                        "任务图活在进程内存里（面节点从库里补齐），重启清零；窗口按 updated_at 划"
                            .into(),
                    ),
                }),
                recent,
                per_project,
            )
        }
    };

    let builds = match inputs.builds {
        None => Reading::unwired("这个部署没有演进管理面"),
        Some(Err(e)) => Reading::unread(format!("读构建跑次失败：{e}")),
        Some(Ok(view)) if !view.attached => {
            Reading::unwired("这个进程没挂构建读数，空名单不是「没构建过」")
        }
        Some(Ok(view)) => {
            let built = view.runs.iter().filter(|r| r.outcome == "built").count();
            Reading::measured(Builds {
                built,
                total: view.runs.len(),
                note: Some("本进程的窗口，重启清零".into()),
            })
        }
    };

    // 带提交落地的变更。`NotImplemented` 与别的读失败要分开：前者是这一栏根本没接通
    // （要接），后者是接通了但这一拍没读到（要查）。两件事的修复方向相反。
    let days = funnel_days(inputs.window_hours);
    let commits = match inputs.funnel {
        None => Reading::unwired(COMMITS_UNWIRED),
        Some(Err(cog_core::SFError::NotImplemented(_))) => Reading::unwired(FUNNEL_UNWIRED),
        Some(Err(e)) => Reading::unread(format!("读变更漏斗失败：{e}")),
        Some(Ok(rows)) => {
            // 一天一行，窗口里没有活动的日子不出现。全都没出现，就是这几天一条
            // 都没送出去——那是一个读数，不是没量到（读不到是上面那两支）。
            let delivered = rows.iter().map(|day| day.landed_with_rev).sum();
            Reading::measured(Commits {
                delivered,
                days,
                note: Some(format!(
                    "按 UTC 天、按落定时刻分天：最近 {days} 天整天计入（窗口边上那一天不按小时切）；\
                     数的是带着提交落地的变更，同一个提交送了几条就计几次；\
                     落定是账被关掉的时刻，可能晚于提交本身"
                )),
            })
        }
    };

    Outputs {
        generated_at: inputs.now,
        window_hours: inputs.window_hours,
        work,
        builds,
        commits,
        recent,
        by_project,
    }
}

/// 结果的一段开头。
///
/// 字符串直接取，其余走 JSON 序列化：一个结果是对象时，读的人要看到的是它的字段，
/// 不是 `"{\"output\":..."` 这样一层转义。
fn excerpt(result: &serde_json::Value) -> String {
    let text = match result {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    };
    let flat = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if flat.chars().count() <= RESULT_EXCERPT_CHARS {
        return flat;
    }
    let head: String = flat.chars().take(RESULT_EXCERPT_CHARS).collect();
    format!("{head}…")
}

pub async fn output_handler(
    State(state): State<std::sync::Arc<GatewayState>>,
) -> Result<Json<Outputs>, ApiError> {
    Ok(Json(read(&state).await))
}

/// 读这一拍的任务图与构建跑次，拼一份产出。
pub async fn read(state: &GatewayState) -> Outputs {
    let now = Utc::now();
    let window_hours = crate::observability::spend_window_hours();
    let tasks = crate::progress::merged_tasks(state).await.ok();
    let builds = match state.evolution_admin.as_ref() {
        None => None,
        Some(admin) => Some(
            admin
                .build_runs(crate::evolution::BUILD_RUN_ROWS_ASKED)
                .await
                .map_err(|e| e.to_string()),
        ),
    };
    // 向漏斗要的天数必须与 `build` 在页面上标的天数同源：读的是一周、标的是三天
    // 时，那个数覆盖的范围与页面上写的不一样，而两个数都不会报错。
    let funnel = match state.change_ledger.as_ref() {
        None => None,
        Some(ledger) => Some(ledger.funnel(funnel_days(window_hours)).await),
    };
    build(Inputs {
        now,
        window_hours,
        tasks: tasks.as_deref(),
        builds: builds.as_ref(),
        funnel: funnel.as_ref(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::{Task, TaskStatus, TaskType};

    fn task(id: &str, status: TaskStatus, minutes_ago: i64, with_result: bool) -> Task {
        let mut task = Task::new(id, TaskType::Custom("x".into()), serde_json::json!({}));
        task.status = status;
        task.updated_at = Utc::now() - Duration::minutes(minutes_ago);
        task.goal_id = Some("proj".into());
        if with_result {
            task.result = Some(serde_json::json!({"output": "done"}));
        }
        task
    }

    fn build_from(tasks: Option<&[Task]>) -> Outputs {
        build(Inputs {
            now: Utc::now(),
            window_hours: 24,
            tasks,
            builds: None,
            funnel: None,
        })
    }

    /// 完成但不带结果的行不是交付：它记了一次结束，没有交回东西。
    #[test]
    fn a_completion_without_a_result_is_counted_but_not_as_a_delivery() {
        let tasks = vec![
            task("a", TaskStatus::Completed, 5, true),
            task("b", TaskStatus::Completed, 5, false),
        ];
        let outputs = build_from(Some(&tasks));
        let work = outputs.work.value().expect("任务图可读时该是 measured");
        assert_eq!(work.completed, 2);
        assert_eq!(work.with_result, 1);
        assert_eq!(outputs.recent.len(), 2);
        // 明细里也要说清楚哪一条没交回东西，否则表上两行看起来一样。
        assert_eq!(
            outputs.recent.iter().filter(|d| d.carried_result).count(),
            1
        );
    }

    /// 窗口外的完成不算这一窗的产出，也不进明细。
    #[test]
    fn work_finished_before_the_window_is_not_counted() {
        let tasks = vec![
            task("inside", TaskStatus::Completed, 30, true),
            task("outside", TaskStatus::Completed, 60 * 48, true),
        ];
        let outputs = build_from(Some(&tasks));
        assert_eq!(outputs.work.value().unwrap().completed, 1);
        assert_eq!(outputs.recent.len(), 1);
        assert_eq!(outputs.recent[0].id, "inside");
    }

    /// 任务图没读到时是 `unread`，不是 0：0 会被读成「这一窗确实什么都没完成」。
    #[test]
    fn an_unread_task_graph_says_so_instead_of_reporting_zero() {
        let outputs = build_from(None);
        assert_eq!(outputs.work, Reading::unread("这一拍没读到任务图"));
        assert!(outputs.recent.is_empty());
    }

    /// 没有变更账时这一层是 `unwired`，且带着理由——不许印成 0。
    #[test]
    fn commits_says_which_layer_is_missing_instead_of_zero() {
        let outputs = build_from(None);
        assert_eq!(outputs.commits, Reading::unwired(COMMITS_UNWIRED));
    }

    fn with_funnel(
        funnel: &Result<Vec<cog_core::contract::change_ledger::ChangeFunnelDay>, cog_core::SFError>,
    ) -> Outputs {
        build(Inputs {
            now: Utc::now(),
            window_hours: 24,
            tasks: None,
            builds: None,
            funnel: Some(funnel),
        })
    }

    fn day(day: &str, landed_with_rev: u64) -> cog_core::contract::change_ledger::ChangeFunnelDay {
        cog_core::contract::change_ledger::ChangeFunnelDay {
            day: day.into(),
            landed_with_rev,
            ..Default::default()
        }
    }

    /// 提交这一层数的是 `landed_with_rev`，不是同一个漏斗里的 `landed`：后者绝大多数
    /// 是「文档要的，树本来就满足」，一条提交都没送出去。两者相加会把一天的交付读成
    /// 一百条，所以这里故意喂一个很大的 `landed`，读数必须只跟带提交的那一栏走。
    #[test]
    fn commits_counts_only_the_closures_that_carried_a_revision() {
        let rows = vec![
            cog_core::contract::change_ledger::ChangeFunnelDay {
                landed: 97,
                ..day("2026-11-09", 3)
            },
            day("2026-11-08", 2),
        ];
        let outputs = with_funnel(&Ok(rows));
        let commits = outputs.commits.value().expect("漏斗读到了就该是 measured");
        assert_eq!(commits.delivered, 5);
        assert_eq!(commits.days, 1);
    }

    /// 窗口里一天活动都没有时是 measured 0，不是 unwired：漏斗对「没活动的一天」不
    /// 产出行，全都没有行就是这几天一条都没送出去——那是一个读数。
    #[test]
    fn an_empty_funnel_is_zero_delivered_not_a_missing_layer() {
        let outputs = with_funnel(&Ok(Vec::new()));
        let commits = outputs.commits.value().expect("空漏斗也是量到了");
        assert_eq!(commits.delivered, 0);
    }

    /// 漏斗这一栏没实现与这一拍读失败要分开：一个去接线，一个去查读失败。
    #[test]
    fn an_unimplemented_funnel_is_unwired_while_a_failed_read_is_unread() {
        let unwired = with_funnel(&Err(cog_core::SFError::NotImplemented("funnel".into())));
        assert_eq!(unwired.commits, Reading::unwired(FUNNEL_UNWIRED));

        let unread = with_funnel(&Err(cog_core::SFError::IO("连不上库".into())));
        match unread.commits {
            Reading::Unread { because } => assert!(because.contains("连不上库")),
            other => panic!("读失败该是 unread，实际是 {other:?}"),
        }
    }

    /// 窗口按小时算、漏斗按天记账：向上取整，且 24 小时正好是一天。
    #[test]
    fn the_funnel_window_rounds_hours_up_to_whole_days() {
        assert_eq!(funnel_days(1), 1);
        assert_eq!(funnel_days(24), 1);
        assert_eq!(funnel_days(25), 2);
        assert_eq!(funnel_days(168), 7);
    }

    /// 明细新在前，条数封顶，且窗口里的汇总数不受封顶影响。
    #[test]
    fn the_table_is_newest_first_and_capped_while_the_counts_stay_whole() {
        let tasks: Vec<Task> = (0..(RECENT_ROWS + 5))
            .map(|i| task(&format!("t{i:03}"), TaskStatus::Completed, i as i64, true))
            .collect();
        let outputs = build_from(Some(&tasks));
        assert_eq!(outputs.recent.len(), RECENT_ROWS);
        assert_eq!(outputs.work.value().unwrap().completed, RECENT_ROWS + 5);
        assert_eq!(outputs.recent[0].id, "t000");
        assert_eq!(
            outputs.recent[RECENT_ROWS - 1].id,
            format!("t{:03}", RECENT_ROWS - 1)
        );
    }

    /// 三层各自的状态在线上就是三个词，且 `measured` 带的是本层自己的值。
    #[test]
    fn each_layer_state_keeps_its_name_on_the_wire() {
        let measured = serde_json::to_value(Reading::measured(Work {
            completed: 3,
            with_result: 2,
            failed: 1,
            note: None,
        }))
        .unwrap();
        assert_eq!(measured["state"], "measured");
        assert_eq!(measured["value"]["completed"], 3);
        assert_eq!(measured["value"]["with_result"], 2);

        let unwired = serde_json::to_value(Reading::<usize>::unwired(COMMITS_UNWIRED)).unwrap();
        assert_eq!(unwired["state"], "unwired");
        assert_eq!(unwired["because"], COMMITS_UNWIRED);
    }

    /// 按项目汇总：没有归属的活落在 `ungrouped`，不与任何项目合并。
    #[test]
    fn project_totals_keep_ungrouped_as_its_own_bucket() {
        let mut loose = task("loose", TaskStatus::Completed, 5, true);
        loose.goal_id = None;
        let mut owned = task("owned", TaskStatus::Completed, 5, true);
        owned.goal_id = Some("alpha".into());
        let tasks = vec![loose, owned];
        let outputs = build_from(Some(&tasks));
        let names: Vec<&str> = outputs
            .by_project
            .iter()
            .map(|p| p.project.as_str())
            .collect();
        assert!(names.contains(&"ungrouped"));
        assert!(names.contains(&"alpha"));
    }

    /// 只取结果的开头一段，且把换行摊平：一行一条，表格里不出现半行空白。
    #[test]
    fn the_excerpt_is_one_flat_line_of_bounded_length() {
        let long = "x".repeat(RESULT_EXCERPT_CHARS * 2);
        let text = excerpt(&serde_json::Value::String(format!("a\nb {long}")));
        assert!(text.starts_with("a b "));
        assert!(text.ends_with('…'));
        assert_eq!(text.chars().count(), RESULT_EXCERPT_CHARS + 1);
    }
}
