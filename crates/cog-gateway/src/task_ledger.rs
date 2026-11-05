//! 任务账这一维：一个数字员工要能自己安排活，先得看得见账。
//!
//! 零凭证那一面上已经有提交口（`POST /api/v1/tasks`），所以集群里每一个智能体都
//! 能往任务图里派活；同一面上却一件读都没有。2026-11-06 在活着的部署上量到的现场：
//! 两个员工与一个规划者分别被问到「账上现在有多少活」，三个人的回答是同一句——
//! 「这个进程里没有读任务账的工具」。能派活、看不见账，规划就只能靠猜。
//!
//! 运营面上那条 `GET /api/v1/tasks/list` 顶不了这一面：它一次吐出九千多行、三十八
//! 兆字节，不分页也不聚合，模型拿到它只会读成噪声。
//!
//! 所以这条读只做一件事：把任务行折成**能据以行动的读数**——在册多少条、各状态
//! 多少条、按场地按人按类型各多少条、最老的那几条、以及其中多少条已经被人接手。
//! 折叠是纯函数，任务行与「现在」都从参数进来，读数的形状因此能在测试里对着构造
//! 出来的行验，而不是只能对着活着的编排器看。
//!
//! 这里**不下判据**：判据写在任务行上（`acceptance`），由接活与交活的两方各自
//! 读。这一面只报账——包括「这一行自己写着多久算超时、它已经跑了多久」这种算术，
//! 但绝不替它宣布这件事过没过。再判一遍就成了第二个判据来源。
//!
//! 代价写在明处：这条读走的是 [`cog_core::OrchestratorControl::get_all_tasks`]，
//! 也就是整张图（2026-11-06 实测 9,991 行 / 42 MB 的 JSON），因为按状态与按类型
//! 计数需要每一行。这与场地那一面不同——那一面按标签筛选后只剩四行，所以它走
//! 了一条窄查询。这里的调用是**按需的**（一个智能体在规划前问一次），不是每二十
//! 秒被轮询一次；哪天它变成轮询的，第一步是给存储层加一条只取这几列的读，而不是
//! 把这条读挪走。

use std::collections::BTreeMap;

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::GatewayState;

/// 一次能带回多少条最老的活。
///
/// 上限存在的理由与聚合存在的理由同一条：这条读是给一个要据此决定下一件事做什么
/// 的智能体用的，不是给一个要把整张账搬到内存里的人用的。超出上限的条数不是被
/// 悄悄截掉——`truncated` 与 `open` 的差就是没带回来的那些。
// cap: bound — 这条读是给智能体据此决定下一件事用的，不是用来搬整张账的；没带回来的那些由 truncated 报出，靠抬这个数补不上
pub const MAX_OLDEST: usize = 50;

/// 没点名时要几条。
// cap: bound — 不点名时的默认页长；要更多的调用方自己点名，所以它不跟着任何一条读数走
pub const DEFAULT_OLDEST: usize = 20;

/// `GET /api/v1/tasks/ledger` 的筛选条件，都可以不给。
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct LedgerQuery {
    /// 只数落在这个场地上的活，精确匹配行上的标签。
    pub venue: Option<String>,
    /// 只数指定给这个人的活：匹配 `owner_unit`（指定谁来做）或 `agent_id`（结果谁交的）。
    pub owner: Option<String>,
    /// 只数这个状态（`pending` / `scheduled` / `running` / `completed` / `failed` / `cancelled`）。
    pub status: Option<String>,
    /// 只数这一类活，拼法同任务行上 [`cog_core::TaskType::kind_key`] 的答案。
    pub kind: Option<String>,
    /// 带回几条最老的活，默认 [`DEFAULT_OLDEST`]，上限 [`MAX_OLDEST`]。
    pub limit: Option<usize>,
}

/// 一条还没到终态的活，读到人能用它决定下一件事的程度。
///
/// 只带这几格，因为其余格子（`input` 的全部、`result`、`acceptance`）是点开这一行
/// 才该看的量级；把它们塞进一张列表，列表本身就没人读了。
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct LedgerRow {
    pub task_id: String,
    /// 第一行读作这一件活的标题，与提交口把首行当标题的约定同一个来源。
    pub title: String,
    pub status: String,
    /// 这一行的类型，拼法同 [`cog_core::TaskType::kind_key`]。
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub venue: Option<String>,
    /// 指定谁来做；没指定时是 `None`，不是空串。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner_unit: Option<String>,
    /// 结果谁交的。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    pub created_at: DateTime<Utc>,
    /// 从建行到「现在」有多少秒。窗口从这里读，而不是让每个调用方自己拿 `now` 减。
    pub age_seconds: i64,
    /// 什么时候被人接手。有值与没值是这个账上最重要的一次分叉：`None` 是排队，
    /// `Some` 是在跑，而这两种行对「还压着多少」的回答不同。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    /// 接手之后跑了多少秒；没接手时为 `None`。
    pub running_seconds: Option<i64>,
    /// 这一行自己写着多久算超时。读数不替它判过没过，只把两个数一起给出来。
    pub timeout_seconds: u64,
    pub retry_count: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub goal_id: Option<String>,
}

/// 一次读数。
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct LedgerReading {
    pub generated_at: DateTime<Utc>,
    /// 满足筛选条件的总行数（含已终态的）。
    pub matched: usize,
    /// 其中还没到终态的条数。
    pub open: usize,
    /// 在册的行里还没有人接手的条数。这就是队列深度。
    pub unclaimed: usize,
    /// 在册的行里已经有人接手的条数。这就是「正在跑的量」——它与 `by_status` 里
    /// 那一格 `running` 不一定相等：接手过又落回排队（重试）的行两个都算。
    pub claimed: usize,
    /// 在册的行里，接手时刻离现在已经超过它自己那条 `timeout_seconds` 的条数。
    ///
    /// 这是算术，不是判据：它只是把行上本来就有的两个数（`started_at`、
    /// `timeout_seconds`）与「现在」比了一次。一条活该不该被收回是回收那一侧的事，
    /// 那一侧有自己的时钟与自己的证据；这一格回答的是「槽位是不是被占着没放手」，
    /// 而不是「该不该收」。
    pub past_its_timeout: usize,
    /// 满足条件的行按状态分。已终态的行留在这里，因为「这几天清掉了多少」与
    /// 「还压着多少」是同一张账的两半。
    pub by_status: BTreeMap<String, usize>,
    /// 满足条件的行按类型再按状态分：`{kind: {status: n}}`。
    ///
    /// 只有状态那一格时，「排队的多」与「在跑的多」看着一样；只有类型那一格时，
    /// 「这一类根本没动」与「这一类已经收完了」也看着一样。两格交叉才回答得了
    /// 「哪一类活卡在哪儿」。
    pub by_kind_status: BTreeMap<String, BTreeMap<String, usize>>,
    /// 还没到终态的行按场地分。没有场地标签的行不进这一格——「没填」不等于
    /// 「属于某个默认场地」，与场地那一面同一条判据。
    pub by_venue: BTreeMap<String, usize>,
    /// 还没到终态的行按人分。`owner_unit` 优先，没指定时退到 `agent_id`；
    /// 两者都没有的行不计入任何一个人。
    pub by_owner: BTreeMap<String, usize>,
    /// 还没到终态的行里最老的若干条，按建行时刻升序。
    pub oldest_open: Vec<LedgerRow>,
    /// `oldest_open` 是不是被 `limit` 截过。截断不说，读的人会把它读成「就这些」。
    pub truncated: bool,
    /// 这次读数用的筛选条件，原样回给调用方：读数离开定义就只是一个数字。
    pub filters: LedgerQuery,
}

/// 把任务行折成读数。
pub fn read_task_ledger(
    tasks: &[cog_core::Task],
    now: DateTime<Utc>,
    query: &LedgerQuery,
) -> LedgerReading {
    let limit = query.limit.unwrap_or(DEFAULT_OLDEST).min(MAX_OLDEST);

    let matched: Vec<&cog_core::Task> = tasks
        .iter()
        .filter(|task| matches_venue(task, query.venue.as_deref()))
        .filter(|task| matches_owner(task, query.owner.as_deref()))
        .filter(|task| matches_kind(task, query.kind.as_deref()))
        .filter(|task| match query.status.as_deref() {
            None => true,
            Some(want) => task.status.as_str() == want.trim(),
        })
        .collect();

    let mut by_status: BTreeMap<String, usize> = BTreeMap::new();
    let mut by_kind_status: BTreeMap<String, BTreeMap<String, usize>> = BTreeMap::new();
    let mut by_venue: BTreeMap<String, usize> = BTreeMap::new();
    let mut by_owner: BTreeMap<String, usize> = BTreeMap::new();
    let mut open_rows: Vec<&cog_core::Task> = Vec::new();
    let mut unclaimed = 0usize;
    let mut claimed = 0usize;
    let mut past_its_timeout = 0usize;

    for task in &matched {
        *by_status
            .entry(task.status.as_str().to_string())
            .or_default() += 1;
        *by_kind_status
            .entry(task.task_type.kind_key())
            .or_default()
            .entry(task.status.as_str().to_string())
            .or_default() += 1;
        if is_terminal(task) {
            continue;
        }
        open_rows.push(task);
        match task.started_at {
            None => unclaimed += 1,
            Some(started) => {
                claimed += 1;
                if (now - started).num_seconds() > task.timeout_seconds as i64 {
                    past_its_timeout += 1;
                }
            }
        }
        if let Some(venue) = labeled(task.venue.as_deref()) {
            *by_venue.entry(venue.to_string()).or_default() += 1;
        }
        if let Some(owner) = owner_of(task) {
            *by_owner.entry(owner.to_string()).or_default() += 1;
        }
    }

    open_rows.sort_by_key(|task| task.created_at);
    let open = open_rows.len();
    let truncated = open > limit;
    let oldest_open = open_rows
        .into_iter()
        .take(limit)
        .map(|task| row_of(task, now))
        .collect();

    LedgerReading {
        generated_at: now,
        matched: matched.len(),
        open,
        unclaimed,
        claimed,
        past_its_timeout,
        by_status,
        by_kind_status,
        by_venue,
        by_owner,
        oldest_open,
        truncated,
        filters: query.clone(),
    }
}

fn is_terminal(task: &cog_core::Task) -> bool {
    matches!(
        task.status,
        cog_core::TaskStatus::Completed
            | cog_core::TaskStatus::Failed
            | cog_core::TaskStatus::Cancelled
    )
}

/// 一个场地名，空白读作没有。
fn labeled(venue: Option<&str>) -> Option<&str> {
    venue.map(str::trim).filter(|v| !v.is_empty())
}

fn matches_venue(task: &cog_core::Task, want: Option<&str>) -> bool {
    match want {
        None => true,
        Some(want) => labeled(task.venue.as_deref()) == Some(want.trim()),
    }
}

fn matches_kind(task: &cog_core::Task, want: Option<&str>) -> bool {
    match want {
        None => true,
        Some(want) => task.task_type.kind_key() == want.trim(),
    }
}

/// 指定先于交付：`owner_unit` 是「指定谁来做」，`agent_id` 是「结果谁交的」。
fn owner_of(task: &cog_core::Task) -> Option<&str> {
    task.owner_unit
        .as_deref()
        .map(str::trim)
        .filter(|o| !o.is_empty())
        .or_else(|| {
            task.agent_id
                .as_deref()
                .map(str::trim)
                .filter(|o| !o.is_empty())
        })
}

fn matches_owner(task: &cog_core::Task, want: Option<&str>) -> bool {
    match want {
        None => true,
        Some(want) => owner_of(task) == Some(want.trim()),
    }
}

/// 一行活的第一句话。
///
/// 任务行上没有单独的标题列：提交口把 `goal` 的第一行读作标题，分解出来的行沿用
/// 同一个约定。这里按同一顺序去找，找不到就退回任务类型——一个空标题与一个没有
/// 标题的任务在图里是不同的东西，读的人要能看出是这个。
fn row_title(task: &cog_core::Task) -> String {
    for key in [
        "goal",
        "title",
        "description",
        "body",
        "prompt",
        "instruction",
    ] {
        if let Some(text) = task.input.get(key).and_then(serde_json::Value::as_str) {
            if let Some(first) = text.lines().map(str::trim).find(|line| !line.is_empty()) {
                return first.to_string();
            }
        }
    }
    task.task_type.kind_key()
}

fn row_of(task: &cog_core::Task, now: DateTime<Utc>) -> LedgerRow {
    LedgerRow {
        task_id: task.id.clone(),
        title: row_title(task),
        status: task.status.as_str().to_string(),
        kind: task.task_type.kind_key(),
        venue: labeled(task.venue.as_deref()).map(str::to_string),
        owner_unit: task
            .owner_unit
            .as_deref()
            .map(str::trim)
            .filter(|o| !o.is_empty())
            .map(str::to_string),
        agent_id: task
            .agent_id
            .as_deref()
            .map(str::trim)
            .filter(|a| !a.is_empty())
            .map(str::to_string),
        created_at: task.created_at,
        age_seconds: (now - task.created_at).num_seconds(),
        started_at: task.started_at,
        running_seconds: task.started_at.map(|started| (now - started).num_seconds()),
        timeout_seconds: task.timeout_seconds,
        retry_count: task.retry_count,
        goal_id: task.goal_id.clone(),
    }
}

/// `GET /api/v1/tasks/ledger` —— 零凭证那一面上的那一面账。
///
/// 与提交口同一个理由挂在零凭证面上：跑在集群里的业务 Pod 本身就是零凭证的，要让
/// 它们带上平台令牌才能看账，等于为一个只读的读数面给每个容器发一个身份。这一面的
/// 边界是网络事实（只有本命名空间的工作负载连得进来），与提交口完全一样。
pub async fn task_ledger_handler(
    State(state): State<std::sync::Arc<GatewayState>>,
    Query(query): Query<LedgerQuery>,
) -> Response {
    let tasks = state.orchestrator.get_all_tasks().await;
    let now = Utc::now();
    (StatusCode::OK, Json(read_task_ledger(&tasks, now, &query))).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use cog_core::{Task, TaskStatus, TaskType};

    fn row(id: &str, status: TaskStatus, minutes_ago: i64) -> Task {
        let mut task = Task::new(
            id,
            TaskType::Custom("platform_agent_task".into()),
            serde_json::json!({"goal": format!("{id} 的第一行\n第二行的事")}),
        );
        task.status = status;
        task.created_at = Utc::now() - Duration::minutes(minutes_ago);
        task
    }

    /// 一条读数要能回答「还压着多少、压在哪个状态、谁手上」。已终态的行进
    /// `by_status`，但不进 `open`，也不进按人按场地的两格——那两格问的是还
    /// 要做的事。
    #[test]
    fn the_reading_separates_open_work_from_what_is_already_done() {
        let now = Utc::now();
        let mut running = row("r1", TaskStatus::Running, 30);
        running.owner_unit = Some("employee:abc".into());
        let mut pending = row("p1", TaskStatus::Pending, 90);
        pending.venue = Some("memory".into());
        let mut done = row("d1", TaskStatus::Completed, 10);
        done.venue = Some("memory".into());

        let reading = read_task_ledger(&[running, pending, done], now, &LedgerQuery::default());

        assert_eq!(reading.matched, 3);
        assert_eq!(reading.open, 2, "终态的行不算待做");
        assert_eq!(reading.by_status.get("completed"), Some(&1));
        assert_eq!(reading.by_status.get("running"), Some(&1));
        assert_eq!(
            reading.by_venue.get("memory"),
            Some(&1),
            "完成的那条不再算进场地那一格"
        );
        assert_eq!(
            reading.by_owner.get("employee:abc"),
            Some(&1),
            "按人分的是还压在他手上的活"
        );
        assert_eq!(
            reading
                .oldest_open
                .iter()
                .map(|r| r.task_id.as_str())
                .collect::<Vec<_>>(),
            vec!["p1", "r1"],
            "最老的排在最前，且只有还没到终态的那些"
        );
        assert_eq!(reading.oldest_open[0].title, "p1 的第一行");
    }

    /// 排队与在跑是两种行：`started_at` 有没有值就是这次分叉。
    ///
    /// 只报「在册多少条」时，一条排了六小时没动过的活与一条真在跑的活读数一样，
    /// 而它们需要的动作正好相反。
    #[test]
    fn a_row_that_was_never_taken_is_not_a_row_that_is_running() {
        let now = Utc::now();
        let queued = row("q1", TaskStatus::Pending, 120);
        let mut in_flight = row("f1", TaskStatus::Running, 120);
        in_flight.started_at = Some(now - Duration::minutes(10));

        let reading = read_task_ledger(&[queued, in_flight], now, &LedgerQuery::default());

        assert_eq!(reading.open, 2);
        assert_eq!(reading.unclaimed, 1);
        assert_eq!(reading.claimed, 1);
        let by_id = |id: &str| {
            reading
                .oldest_open
                .iter()
                .find(|r| r.task_id == id)
                .cloned()
                .expect("这一行在最老的那些里")
        };
        assert_eq!(
            by_id("q1").running_seconds,
            None,
            "没人接手的行没有在跑多久"
        );
        assert!(
            (by_id("q1").age_seconds - 7200).abs() < 120,
            "没接手的行的等待时长就是它的年龄"
        );
        assert_eq!(by_id("f1").running_seconds, Some(600));
    }

    /// 过了自己那条时限要能被数出来，但那是行上两个数的算术，不是这一面下的判据：
    /// 超时窗是行自己写的，读数只把它与「现在」比一次。
    #[test]
    fn a_row_past_its_own_timeout_is_counted_and_not_judged() {
        let now = Utc::now();
        let mut late = row("late", TaskStatus::Running, 300);
        late.started_at = Some(now - Duration::seconds(901));
        late.timeout_seconds = 900;
        let mut on_time = row("on-time", TaskStatus::Running, 300);
        on_time.started_at = Some(now - Duration::seconds(100));
        on_time.timeout_seconds = 900;

        let reading = read_task_ledger(&[late, on_time], now, &LedgerQuery::default());

        assert_eq!(reading.claimed, 2);
        assert_eq!(
            reading.past_its_timeout, 1,
            "只有过了它自己那条时限的那条算"
        );
        assert_eq!(
            reading.by_status.get("running"),
            Some(&2),
            "过了时限的行仍然是 running：这一面不改行，只读行"
        );
    }

    /// 类型与状态交叉那一格要真的交叉：只有状态时「这一类根本没动」与「这一类
    /// 已经收完了」读数相同。
    #[test]
    fn the_kind_by_status_cross_tab_keeps_the_two_axes_apart() {
        let now = Utc::now();
        let mut a = row("a", TaskStatus::Pending, 5);
        a.task_type = TaskType::Custom("memory_extract".into());
        let mut b = row("b", TaskStatus::Running, 6);
        b.task_type = TaskType::Custom("memory_extract".into());
        let mut c = row("c", TaskStatus::Completed, 7);
        c.task_type = TaskType::Custom("memory_extract".into());
        let mut d = row("d", TaskStatus::Pending, 8);
        d.task_type = TaskType::Custom("wiki_distill".into());

        let reading = read_task_ledger(&[a, b, c, d], now, &LedgerQuery::default());

        let memory = reading
            .by_kind_status
            .get("memory_extract")
            .expect("这一类在表上");
        assert_eq!(memory.get("pending"), Some(&1));
        assert_eq!(memory.get("running"), Some(&1));
        assert_eq!(memory.get("completed"), Some(&1));
        assert_eq!(
            reading
                .by_kind_status
                .get("wiki_distill")
                .and_then(|m| m.get("pending")),
            Some(&1)
        );
        assert_eq!(
            reading.by_kind_status.len(),
            2,
            "两类活各占一格，不并成一个 custom"
        );
    }

    /// 上限是上限，被截断这件事要能被读出来：截断不说，读的人会把它读成「就这些」。
    #[test]
    fn a_truncated_page_says_it_was_truncated() {
        let now = Utc::now();
        let tasks: Vec<Task> = (0..5)
            .map(|i| row(&format!("t{i}"), TaskStatus::Pending, 100 - i))
            .collect();
        let reading = read_task_ledger(
            &tasks,
            now,
            &LedgerQuery {
                limit: Some(2),
                ..Default::default()
            },
        );
        assert_eq!(reading.oldest_open.len(), 2);
        assert_eq!(reading.open, 5);
        assert!(reading.truncated);
    }

    /// 超过上限的点名不许静默通过：`limit: 10_000` 要落在 [`MAX_OLDEST`] 上，
    /// 否则一条读就变成把整张账搬进上下文的那条路。
    #[test]
    fn the_limit_cannot_be_raised_past_the_page() {
        let now = Utc::now();
        let tasks: Vec<Task> = (0..MAX_OLDEST + 5)
            .map(|i| row(&format!("t{i}"), TaskStatus::Pending, 1000 - i as i64))
            .collect();
        let reading = read_task_ledger(
            &tasks,
            now,
            &LedgerQuery {
                limit: Some(10_000),
                ..Default::default()
            },
        );
        assert_eq!(reading.oldest_open.len(), MAX_OLDEST);
        assert!(reading.truncated);
    }

    /// 筛选条件各筛各的，且 `owner` 两条来源都认：指定谁来做，或结果谁交的。
    #[test]
    fn the_filters_narrow_the_reading_without_inventing_rows() {
        let now = Utc::now();
        let mut a = row("a", TaskStatus::Pending, 5);
        a.venue = Some("wiki".into());
        a.owner_unit = Some("employee:1".into());
        let mut b = row("b", TaskStatus::Running, 6);
        b.venue = Some("memory".into());
        b.agent_id = Some("employee:1".into());
        let c = row("c", TaskStatus::Pending, 7);

        let all = [a, b, c].to_vec();
        let by_venue = read_task_ledger(
            &all,
            now,
            &LedgerQuery {
                venue: Some("memory".into()),
                ..Default::default()
            },
        );
        assert_eq!(by_venue.matched, 1);
        assert_eq!(by_venue.oldest_open[0].task_id, "b");

        let by_owner = read_task_ledger(
            &all,
            now,
            &LedgerQuery {
                owner: Some("employee:1".into()),
                ..Default::default()
            },
        );
        assert_eq!(
            by_owner.matched, 2,
            "指定给他做的与结果是他交的，都算在他名下"
        );
    }

    /// 空白场地不是场地：与场地那一面同一条判据，否则「没填」会被读成一个叫
    /// 空字符串的场地。
    #[test]
    fn a_blank_label_is_not_a_venue() {
        let now = Utc::now();
        let mut task = row("x", TaskStatus::Pending, 1);
        task.venue = Some("   ".into());
        let reading = read_task_ledger(&[task], now, &LedgerQuery::default());
        assert!(reading.by_venue.is_empty());
    }
}
