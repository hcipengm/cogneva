//! 三本账的一拍进度：任务、变更、原则各自在哪儿，按责任人摊开。
//!
//! 三本账各有一个读面（`/api/v1/tasks/*`、`/api/v1/governance/changes`、
//! `/api/v1/governance/principles`），但没有一处回答「现在进展到哪儿了」。
//! 问的人只能把三个响应拼起来，而三次请求各有各的时刻——拼出来的那份东西
//! 不属于任何一个瞬间，谁也没法说它是哪一刻的读数。这条读数在同一个时刻取
//! 三份账，所以它说得出「这一刻」。
//!
//! 任务这一段按 `owner_unit` 摊开，并且**读出责任人还在不在**。账上有归属、
//! 人已经不在了，是「责任到人」唯一会失败的方式，而只数行数的读面会把它读成
//! 一片安静：那一列填着名字，看起来有人。在岗与否只对真的持有未结活的 unit
//! 去问，所以这条读数的代价随「有多少个在干活的 unit」增长，不随账的体量增长。
//!
//! 没有归属的未结活单独计数，不摊进任何一个 unit，也不折进「其他」。折进去
//! 会让一条没人认领的活和一条有人认领的活看起来一样，而这两件事要的动作相反。
//!
//! 三本账缺一本时那一段是 `null` 而不是一串零：零会被读成「没有欠账」，与
//! 「这一本没接上」差着一次完全不同的处置。

use axum::{extract::State, Json};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::{error::ApiError, GatewayState};

/// 一个 unit 还答不答话。
///
/// 三档而不是两档：在岗账读不出来的时候，「不在」与「读不到」会指往两个相反
/// 的方向——前者要派人接手，后者要去看在岗账本身。把两者压成一档，一条读不动
/// 的在岗账会让所有面主都看着像走了。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Presence {
    Present,
    Absent,
    /// 这个部署没有在岗账，或者读它的时候失败了。
    Unreadable,
}

impl Presence {
    /// 与 [`Self`] 的序列化拼写一致的那一个词，给人读的表格用。
    pub fn as_str(&self) -> &'static str {
        match self {
            Presence::Present => "present",
            Presence::Absent => "absent",
            Presence::Unreadable => "unreadable",
        }
    }
}

/// 一个状态下的行数。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StateCount {
    pub state: String,
    /// 这个状态是不是「还开着」。开着的行是活，关掉的行是历史。
    pub open: bool,
    pub rows: u64,
}

/// 一个责任人的名下有什么。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OwnerProgress {
    pub unit: String,
    pub presence: Presence,
    /// 还开着的活（pending + scheduled + running）。
    pub open: u64,
    pub completed: u64,
    pub failed: u64,
    pub cancelled: u64,
    /// 名下最老的一条未结活是什么时候建的。一条挂了很多天的活与一条今天才
    /// 派下去的活在总数上一样，在这里差着几天的年龄。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oldest_open_at: Option<DateTime<Utc>>,
}

/// 任务图那一段。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TaskProgress {
    pub rows: u64,
    pub open: u64,
    /// 开着的、行上没有 `owner_unit`、而且本来该有人领的活。它们不在任何一个
    /// `owners` 条目里。
    pub unowned_open: u64,
    /// 开着、没有归属、但本来就不该被谁领的行：目标的占位行。它们是没闭上的
    /// 目标，不是没人认领的活——两件事要的处置不同，所以不并进上面那一格。
    pub unowned_goals: u64,
    /// 状态还是 pending、行上却带着一段错的活。
    ///
    /// 一条活只有在出过错之后才带着错；它还是 pending 又说得出错，说明它没有
    /// 结束，是被放回了队里。这个数大，说明有那么一批活在被反复调度、从未开工——
    /// 而按状态看，它们与排得好好的活逐字同形。
    pub returned_to_line: u64,
    pub by_state: Vec<StateCount>,
    pub owners: Vec<OwnerProgress>,
}

/// 变更账那一段。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ChangeProgress {
    pub rows: u64,
    /// 还开着（欠着）的行。
    pub outstanding: u64,
    /// 已经落地或已经退役的行。
    pub settled: u64,
    pub by_state: Vec<StateCount>,
}

/// 原则台账那一段。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PrincipleProgress {
    pub rows: u64,
    /// 有门的条数。它数的是「有没有人声明过一道门」，不是「规则有没有被遵守」——
    /// 这里不减那个区别，减了会让读数看起来比它能证明的更小。
    pub enforced: u64,
    pub unenforced: u64,
    pub unmeasured: u64,
    pub retired: u64,
}

/// `GET /api/v1/governance/progress` 的响应体，也是 CLI 印出来的那份东西。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Progress {
    pub generated_at: DateTime<Utc>,
    pub tasks: TaskProgress,
    /// 这个部署没有变更账时为 `None`。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub changes: Option<ChangeProgress>,
    /// 这个部署没有原则台账时为 `None`。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub principles: Option<PrincipleProgress>,
}

/// 把一个 unit 名下的一摞行数成 [`OwnerProgress`]。
///
/// 只有 `open`、`completed`、`failed`、`cancelled` 四列：`pending`、`scheduled`、
/// `running` 三者对「谁该动手」是同一个答案（这个人名下的活还没完），拆开只会让
/// 一张表多两列没人看的数。
fn owner_progress(
    unit: &str,
    presence: Presence,
    rows: impl Iterator<Item = (cog_core::TaskStatus, DateTime<Utc>)>,
) -> OwnerProgress {
    use cog_core::TaskStatus;
    let mut out = OwnerProgress {
        unit: unit.to_string(),
        presence,
        open: 0,
        completed: 0,
        failed: 0,
        cancelled: 0,
        oldest_open_at: None,
    };
    for (status, created_at) in rows {
        if status.is_terminal() {
            match status {
                TaskStatus::Completed => out.completed += 1,
                TaskStatus::Failed => out.failed += 1,
                TaskStatus::Cancelled => out.cancelled += 1,
                // `is_terminal` 覆盖的就是这三个；另三个走下面那条分支。
                TaskStatus::Pending | TaskStatus::Scheduled | TaskStatus::Running => {}
            }
            continue;
        }
        out.open += 1;
        out.oldest_open_at = Some(match out.oldest_open_at {
            Some(oldest) => oldest.min(created_at),
            None => created_at,
        });
    }
    out
}

/// 从任务行算出任务那一段。
///
/// 纯函数：给它行与在岗读数，它给出一份摊好的账。之所以不在这里去读在岗账，
/// 是因为在岗账是每个 unit 一次往返，而「哪些 unit 值得问」正是这段算出来的
/// 东西——先算再问，问的次数就跟着活的分布走，不跟着账的体量走。
///
/// `presence` 里没有的 unit 记 [`Presence::Unreadable`]，不记 `Absent`。
/// 「没问过」与「问过、答案是没人」是两件事，只有后者能支持「派人接手」。
pub fn task_progress(
    tasks: &[cog_core::Task],
    presence: &BTreeMap<String, Presence>,
) -> TaskProgress {
    let mut by_status: BTreeMap<&'static str, u64> = BTreeMap::new();
    let mut open = 0u64;
    let mut unowned_open = 0u64;
    let mut unowned_goals = 0u64;
    let mut returned_to_line = 0u64;
    let mut owned: BTreeMap<String, Vec<(cog_core::TaskStatus, DateTime<Utc>)>> = BTreeMap::new();

    for task in tasks {
        *by_status.entry(task.status.as_str()).or_default() += 1;
        if !task.status.is_terminal() {
            open += 1;
            if task.status == cog_core::TaskStatus::Pending && task.error.is_some() {
                returned_to_line += 1;
            }
        }
        match task.owner_unit.as_deref() {
            Some(unit) => owned
                .entry(unit.to_string())
                .or_default()
                .push((task.status, task.created_at)),
            None => {
                if !task.status.is_terminal() {
                    if task.is_executable {
                        unowned_open += 1;
                    } else {
                        unowned_goals += 1;
                    }
                }
            }
        }
    }

    let by_state = cog_core::TaskStatus::ALL
        .iter()
        .map(|status| StateCount {
            state: status.as_str().to_string(),
            open: !status.is_terminal(),
            rows: by_status.get(status.as_str()).copied().unwrap_or(0),
        })
        .collect();

    // 只在真的持有未结活时才问在岗，也只把这样的 unit 列出来：一个已经交完活的
    // unit 不在不在岗都不影响谁该动手，把它列进来只会把名单撑成一张历史。
    let owners = owned
        .into_iter()
        .filter(|(_, rows)| rows.iter().any(|(status, _)| !status.is_terminal()))
        .map(|(unit, rows)| {
            let presence = presence.get(&unit).copied().unwrap_or(Presence::Unreadable);
            owner_progress(&unit, presence, rows.into_iter())
        })
        .collect();

    TaskProgress {
        rows: tasks.len() as u64,
        open,
        unowned_open,
        unowned_goals,
        returned_to_line,
        by_state,
        owners,
    }
}

/// 变更账的行 → 那一段读数。
pub fn change_progress(counted: &[(cog_core::ChangeState, u64)]) -> ChangeProgress {
    let lookup: BTreeMap<&'static str, u64> = counted
        .iter()
        .map(|(state, rows)| (state.as_str(), *rows))
        .collect();
    let by_state: Vec<StateCount> = cog_core::ChangeState::ALL
        .iter()
        .map(|state| StateCount {
            state: state.as_str().to_string(),
            open: state.is_outstanding(),
            rows: lookup.get(state.as_str()).copied().unwrap_or(0),
        })
        .collect();
    let rows = by_state.iter().map(|cell| cell.rows).sum();
    let outstanding = by_state
        .iter()
        .filter(|cell| cell.open)
        .map(|cell| cell.rows)
        .sum();
    ChangeProgress {
        rows,
        outstanding,
        settled: rows - outstanding,
        by_state,
    }
}

/// 原则台账的普查 → 那一段读数。
pub fn principle_progress(census: &cog_core::PrincipleCensus) -> PrincipleProgress {
    PrincipleProgress {
        rows: census.rows(),
        enforced: census.enforced,
        unenforced: census.unenforced,
        unmeasured: census.unmeasured,
        retired: census.retired,
    }
}

/// 这一拍要读的三本账与一张在岗账。任何一本缺席都是一种可读的状态，不是错误。
#[derive(Clone, Copy)]
pub struct Sources<'a> {
    pub governance: Option<&'a dyn cog_core::GovernanceStore>,
    pub change_ledger: Option<&'a dyn cog_core::ChangeLedgerStore>,
    pub principle_ledger: Option<&'a dyn cog_core::PrincipleLedgerStore>,
    pub registry: Option<&'a dyn cog_core::AgentRegistry>,
}

/// 问每个持有未结活的 unit 还在不在。
///
/// 只问 [`task_progress`] 会列出来的那些 unit，所以这张表的键是「有活的 unit」，
/// 不是「所有登记过的 unit」。
async fn read_presence(
    registry: Option<&dyn cog_core::AgentRegistry>,
    tasks: &[cog_core::Task],
) -> BTreeMap<String, Presence> {
    let mut units: Vec<&str> = tasks
        .iter()
        .filter(|task| !task.status.is_terminal())
        .filter_map(|task| task.owner_unit.as_deref())
        .collect();
    units.sort_unstable();
    units.dedup();
    let mut out = BTreeMap::new();
    let Some(registry) = registry else {
        // 没有在岗账不是「谁都不在」：那是这个部署答不了这个问题。
        for unit in units {
            out.insert(unit.to_string(), Presence::Unreadable);
        }
        return out;
    };
    for unit in units {
        let presence = match registry.get(unit).await {
            Ok(Some(_)) => Presence::Present,
            Ok(None) => Presence::Absent,
            Err(_) => Presence::Unreadable,
        };
        out.insert(unit.to_string(), presence);
    }
    out
}

/// 三本账各读一次，算出这一刻的进度。
///
/// 任务行取两处并去重：调用方手里的那一片，和治理工作区里的面节点。一个进程读得到
/// 哪一片取决于它在哪个工作区，而「进度」问的是整个平台——只读一面会把另一面的
/// 未结活读成不存在。
pub async fn collect(
    tasks: Vec<cog_core::Task>,
    sources: Sources<'_>,
) -> Result<Progress, cog_core::SFError> {
    let now = Utc::now();
    let mut merged: BTreeMap<String, cog_core::Task> = tasks
        .into_iter()
        .map(|task| (task.id.clone(), task))
        .collect();
    if let Some(store) = sources.governance {
        for task in store.face_nodes().await? {
            merged.entry(task.id.clone()).or_insert(task);
        }
    }
    let tasks: Vec<cog_core::Task> = merged.into_values().collect();

    let presence = read_presence(sources.registry, &tasks).await;
    let changes = match sources.change_ledger {
        Some(ledger) => Some(change_progress(&ledger.census().await?)),
        None => None,
    };
    let principles = match sources.principle_ledger {
        Some(ledger) => Some(principle_progress(&ledger.census().await?)),
        None => None,
    };

    Ok(Progress {
        generated_at: now,
        tasks: task_progress(&tasks, &presence),
        changes,
        principles,
    })
}

pub async fn read_progress(state: &GatewayState) -> Result<Progress, ApiError> {
    let tasks = state.orchestrator.get_all_tasks().await;
    Ok(collect(
        tasks,
        Sources {
            governance: state.governance_store.as_deref(),
            change_ledger: state.change_ledger.as_deref(),
            principle_ledger: state.principle_ledger.as_deref(),
            registry: state.agent_registry.as_deref(),
        },
    )
    .await?)
}

pub async fn progress_handler(
    State(state): State<Arc<GatewayState>>,
) -> Result<Json<Progress>, ApiError> {
    Ok(Json(read_progress(&state).await?))
}
