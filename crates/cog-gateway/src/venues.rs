//! 场地这一维：同一批任务按「在哪儿做」分开看。
//!
//! 工作台上有三个问题，答案从来不重合：**谁在做**（名册上的一个员工）、
//! **在哪儿做**（memory / wiki / eval …）、**怎么算做完**（任务行上的判据）。
//! 把它们合成一个概念，就会长出「memory 员工」这种身份——同一个通用员工今天
//! 在这个场地、明天在另一个，场地跟着活走，不跟着身份走。
//!
//! 场地本身不是一个对象，没有 id、没有提示词、不花 token：它只是任务行上的
//! 一个标签（[`cog_core::Task::venue`]）。所以这里也没有「场地服务」，只有一次
//! 对任务行的折叠——按标签分组，每组回答三个问题：
//!
//! - **待做**：还没到终态的任务条数。
//! - **判据**：最近一条到过终态的任务，结局是完成还是失败。判据写在任务行上
//!   （`acceptance`），由接活那一方与交活那一方各自读；这一屏只报那一行最后
//!   走到了哪，不替它重新判一遍——重判一遍就成了第二个判据来源。
//! - **写回量**：近一天里这些任务交回的答案有多少字节。场地不花 token，所以
//!   这一列不是花销；它是「这一栏今天往回收了多少东西」，用在读答案的人身上。
//!
//! 折叠是纯函数，读的两个输入（任务行、现在时刻）都从参数进来：一条读数的
//! 形状要能在测试里对着构造出来的行验，而不是只能对着活着的编排器看。

use std::collections::BTreeMap;

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use chrono::{DateTime, Duration, Utc};
use serde::Serialize;

use crate::tasks::visible_to_claims;
use crate::GatewayState;

/// 板子上常驻的三栏：memory 抽取、wiki 蒸馏、评测。
///
/// 这三栏是平台自己的活儿落脚的地方，所以没有活的时候也照样画出来，读数为
/// 零；空板子会被读成「这一页坏了」，而不是「今天还没有活」。这张表说的是
/// **哪些空栏存在**，不是**哪些标签合法**：一个不在这张表上的标签，只要有一
/// 行任务带着它，就照样多出一栏。
pub const STANDING_VENUES: &[&str] = &["memory", "wiki", "eval"];

/// 写回量取多久以内交回的答案。
///
/// 取一整天而不是「全部历史」：场地是每天在动的，从建库第一天起累加出来的
/// 字节数只会单调上涨，涨到后面就不再回答「今天这一栏有多活跃」。
// cap: bound — 场地按天在动；取一整天才能回答「今天这一栏有多活跃」，全部历史只会单调上涨
pub const WRITE_BACK_WINDOW: Duration = Duration::hours(24);

/// 一个场地在这一次读数里的三行。
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct VenueReading {
    /// 场地名，取自任务行上的标签本身，不来自一张注册表：没有人登记过的场地
    /// 也照样出现，只要有一件活落在它上面。
    pub venue: String,
    /// 这个场地挂着多少件还没到终态的活。
    pub open: usize,
    /// 这个场地一共有多少件活（含已终态的）。
    pub tasks: usize,
    /// 最近一条到过终态的任务的结局；一件都没到过终态时是 `None`。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest: Option<VenueVerdict>,
    /// 近 [`WRITE_BACK_WINDOW`] 内这些任务的答案字节数之和。
    pub written_back_bytes: u64,
}

/// 最近一条到过终态的任务。
///
/// 结局只报三种：完成、失败、取消。判据是否通过读的是「到没到完成」，因为
/// 判据本身在执行那一侧就已经判过了——交活的一方读到答案不合判据时会把它判
/// 失败，`Completed` 就是「有人认过这件活做完了」的那一格。
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct VenueVerdict {
    pub task_id: String,
    /// `completed` / `failed` / `cancelled`，与 `TaskStatus::as_str` 同一个拼法。
    pub outcome: String,
    pub at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub struct VenuesResponse {
    pub generated_at: DateTime<Utc>,
    /// 写回量的时间窗，秒。读数离开定义就只是一个数字。
    pub write_back_window_secs: i64,
    pub venues: Vec<VenueReading>,
}

/// 把任务行按场地折成读数。没有场地标签的行不进任何一栏。
///
/// 常驻的三栏（[`STANDING_VENUES`]）先摆上，读数为零；任务行只会往上加，不会
/// 把一栏去掉。所以这一屏在「今天还没有活」和「板子坏了」之间能分开：前者是
/// 三栏零，后者才是一条读数都没有。
pub fn read_venues(tasks: &[cog_core::Task], now: DateTime<Utc>) -> Vec<VenueReading> {
    let since = now - WRITE_BACK_WINDOW;
    let mut by_venue: BTreeMap<&str, VenueReading> = STANDING_VENUES
        .iter()
        .map(|venue| {
            (
                *venue,
                VenueReading {
                    venue: venue.to_string(),
                    open: 0,
                    tasks: 0,
                    latest: None,
                    written_back_bytes: 0,
                },
            )
        })
        .collect();

    for task in tasks {
        let Some(venue) = task.venue_label() else {
            continue;
        };
        let row = by_venue.entry(venue).or_insert_with(|| VenueReading {
            venue: venue.to_string(),
            open: 0,
            tasks: 0,
            latest: None,
            written_back_bytes: 0,
        });
        row.tasks += 1;
        match task.status {
            cog_core::TaskStatus::Completed
            | cog_core::TaskStatus::Failed
            | cog_core::TaskStatus::Cancelled => {
                let newer = row
                    .latest
                    .as_ref()
                    .is_none_or(|held| task.updated_at > held.at);
                if newer {
                    row.latest = Some(VenueVerdict {
                        task_id: task.id.clone(),
                        outcome: task.status.as_str().to_string(),
                        at: task.updated_at,
                    });
                }
                if task.status == cog_core::TaskStatus::Completed && task.updated_at >= since {
                    row.written_back_bytes += written_bytes(task.result.as_ref());
                }
            }
            _ => row.open += 1,
        }
    }

    by_venue.into_values().collect()
}

/// 一次交回的答案有多大。
///
/// 数的是**写回的东西**，不是任务行：`result` 是接活那一方交回的原文，任务行
/// 上的元数据不属于「写回量」。量字节而不是条数，是因为一件活可以一次写回很多
/// 条目、也可以只写回一句结论，条数会把这两种读成一样大。
fn written_bytes(result: Option<&serde_json::Value>) -> u64 {
    match result {
        Some(value) => serde_json::to_string(value)
            .map(|text| text.len() as u64)
            .unwrap_or(0),
        None => 0,
    }
}

/// `GET /api/v1/venues` —— 三个场地读数。
pub async fn venues_handler(
    State(state): State<std::sync::Arc<GatewayState>>,
    claims: Option<Extension<cog_core::Claims>>,
) -> Response {
    let claims = claims.as_ref().map(|Extension(c)| c);
    // 只要带场地标签的行：这张看板按标签折叠，别的行读进来只为扔掉。整图在
    // 2026-11-06 是 9,991 行 / 42 MB 里挑出 4 行，而这张看板每 20 秒被拉一次。
    let tasks: Vec<cog_core::Task> = state
        .orchestrator
        .get_venue_tasks()
        .await
        .into_iter()
        .filter(|t| visible_to_claims(claims, t))
        .collect();
    let now = Utc::now();
    (
        StatusCode::OK,
        Json(VenuesResponse {
            generated_at: now,
            write_back_window_secs: WRITE_BACK_WINDOW.num_seconds(),
            venues: read_venues(&tasks, now),
        }),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::{Task, TaskStatus, TaskType};

    fn row(id: &str, venue: Option<&str>, status: TaskStatus, minutes_ago: i64) -> Task {
        let mut task = Task::new(
            id,
            TaskType::Custom("platform_agent_task".into()),
            serde_json::json!({}),
        );
        task.venue = venue.map(str::to_string);
        task.status = status;
        task.updated_at = Utc::now() - Duration::minutes(minutes_ago);
        task
    }

    /// 三个问题各自回答自己那一格：待做数、最近的判据、写回量。
    #[test]
    fn a_venue_row_answers_three_questions_about_its_own_tasks() {
        let now = Utc::now();
        let mut done = row("m1", Some("memory"), TaskStatus::Completed, 10);
        done.result = Some(serde_json::json!({"entries": ["a", "b"]}));
        let tasks = vec![
            done,
            row("m2", Some("memory"), TaskStatus::Running, 5),
            row("w1", Some("wiki"), TaskStatus::Failed, 20),
            row("no-venue", None, TaskStatus::Running, 3),
        ];

        let venues = read_venues(&tasks, now);
        assert_eq!(
            venues.iter().map(|v| v.venue.as_str()).collect::<Vec<_>>(),
            vec!["eval", "memory", "wiki"],
            "常驻三栏都在，没有场地标签的行不属于任何一栏"
        );
        let by_name = |venue: &str| {
            venues
                .iter()
                .find(|v| v.venue == venue)
                .unwrap_or_else(|| panic!("{venue} 这一栏要在场"))
        };

        let memory = by_name("memory");
        assert_eq!(memory.open, 1);
        assert_eq!(memory.tasks, 2);
        assert_eq!(
            memory.latest.as_ref().map(|v| v.outcome.as_str()),
            Some("completed"),
            "最近一条到终态的是完成，不是那条还在跑的"
        );
        let written = memory.written_back_bytes;
        assert!(written > 0, "交回的答案要算进写回量");

        let wiki = by_name("wiki");
        assert_eq!(wiki.open, 0);
        assert_eq!(
            wiki.latest.as_ref().map(|v| v.outcome.as_str()),
            Some("failed")
        );
        assert_eq!(wiki.written_back_bytes, 0, "失败的活没有写回量");
    }

    /// 写回量按时间窗算：更早交回的答案不算进今天这一格。
    #[test]
    fn write_back_volume_stops_at_the_window() {
        let now = Utc::now();
        let mut old = row("m-old", Some("memory"), TaskStatus::Completed, 60 * 30);
        old.result = Some(serde_json::json!({"entries": ["a"]}));
        let venues = read_venues(&[old], now);
        let memory = venues
            .iter()
            .find(|v| v.venue == "memory")
            .expect("memory 这一栏要在场");
        assert_eq!(memory.written_back_bytes, 0);
        assert_eq!(
            memory.latest.as_ref().map(|v| v.outcome.as_str()),
            Some("completed"),
            "判据那一格不看时间窗：它问的是最后一条到了哪，不是今天的量"
        );
    }

    /// 一条活都没有的时候，板子上仍然摆着三栏，各自读数都是零。
    ///
    /// 这是「今天还没有活」和「这一页没读出来」之间唯一的分界：空数组在前端
    /// 读成后者，读的人会以为板子坏了，而不是以为什么都还没发生。
    #[test]
    fn an_untouched_board_still_shows_the_standing_venues() {
        let venues = read_venues(&[], Utc::now());
        assert_eq!(
            venues.iter().map(|v| v.venue.as_str()).collect::<Vec<_>>(),
            vec!["eval", "memory", "wiki"]
        );
        for venue in &venues {
            assert_eq!(venue.open, 0, "{} 没有等着的活", venue.venue);
            assert_eq!(venue.tasks, 0);
            assert!(venue.latest.is_none(), "{} 还没有跑完过一件", venue.venue);
            assert_eq!(venue.written_back_bytes, 0);
        }
    }

    /// 一个不在常驻表上的标签，照样是多出来的一栏。
    ///
    /// 常驻表说的是「哪些空栏存在」，不是「哪些标签合法」：谁往任务行上写了
    /// 一个新场地，工作台就该多出一栏，而不是把那一行悄悄丢掉。
    #[test]
    fn a_label_outside_the_standing_list_still_gets_a_column() {
        let venues = read_venues(
            &[row("c1", Some("changes"), TaskStatus::Running, 1)],
            Utc::now(),
        );
        assert_eq!(
            venues.iter().map(|v| v.venue.as_str()).collect::<Vec<_>>(),
            vec!["changes", "eval", "memory", "wiki"]
        );
        let changes = venues
            .iter()
            .find(|v| v.venue == "changes")
            .expect("changes 这一栏要在场");
        assert_eq!(changes.open, 1);
    }
}
