//! 项目控制回路：把挣值面的越界回注成任务图里的活。
//!
//! 挣值这套读数只有在这里算得出来——任务图在编排侧、逐笔 token 账在网关自己手里，
//! 而 `/projects/{id}/evm` 已经把两边拼过一次。所以「越界了没有」在这一侧判，
//! 「要不要造活、造哪一条」交给编排侧：那两个判断要读基线与任务图，判据留在那一侧
//! 才不会分裂成两套。
//!
//! 一个读面不能顺手造任务：`GET` 是有人来问才发生的，而越界是一个**与提问无关的
//! 事实**——没人打开那个项目，落后就照样在发生。控制回路因此是一条自己的循环，
//! 按自己的节拍跑，不挂在任何一次请求上。
//!
//! 判据只有 1.0 一条线，见 [`cog_core::PerformanceIndex::breaching`]：这条回路不
//! 替「落后多少算落后」挑容差，噪声由去重那一侧管（一版基线一条重规划）。

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use cog_core::{BreachReport, PerformanceIndex, ReplanMinter, ReplanOutcome, Task};
use tracing::{debug, info, warn};

use crate::projects::{build_evm_response, readable_plans, EvmResponse, UNGROUPED};
use crate::GatewayState;

/// 循环名，报在 `cogneva_loop_*` 的 `loop` 标签上。
pub const PROJECT_CONTROL_LOOP: &str = "gateway_project_control";

/// 一轮之间隔多久。
///
/// 与挣值本身的粒度同一量级：计划价值按「已过时间 / 计划总时长」连续折算，但一条
/// 重规划改的是一整版基线，它不会因为早问了两分钟而不同。间隔只决定「这一版基线
/// 多快被看到」，不决定造几条——去重键是基线版本。
const CADENCE: Duration = Duration::from_secs(600);

/// 起这条回路。`minter` 缺席时不跑：没有编排侧就没有图可以把活放进，跑起来也只
/// 能攒下一串判词。
pub fn spawn_project_control(
    state: Arc<GatewayState>,
    minter: Arc<dyn ReplanMinter>,
    shutdown: cog_core::ShutdownSignal,
) -> tokio::task::JoinHandle<()> {
    let stop = shutdown.clone();
    cog_core::loop_health::spawn(
        PROJECT_CONTROL_LOOP,
        cog_core::loop_health::Cadence::Periodic(CADENCE),
        shutdown,
        move |beat| {
            let state = Arc::clone(&state);
            let minter = Arc::clone(&minter);
            let stop = stop.clone();
            async move {
                let mut interval = tokio::time::interval(CADENCE);
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    // 每一轮都盖章：没有项目越界是常态，不是这一环停了。
                    beat.beat();
                    tokio::select! {
                        biased;
                        _ = stop.wait() => {
                            info!("project control loop shutting down");
                            return;
                        }
                        _ = interval.tick() => {}
                    }
                    run_once(&state, minter.as_ref()).await;
                }
            }
        },
    )
}

/// 一轮：判出所有越界，把它们一起交给编排侧。
pub async fn run_once(state: &Arc<GatewayState>, minter: &dyn ReplanMinter) {
    let readings = readings();
    let (breaches, blind) = breaches_this_pass(state).await;
    for reason in blind {
        readings.record_blind(reason);
    }
    dispatch(&breaches, minter).await;
}

/// 把这一轮判出的越界交给造任务的那一侧，按结局各说一句。
///
/// 与判定分开是为了它验得了：判词要真被交出去，「交出去的是几条、结局是什么」
/// 才有人看得见。没有越界时一次也不调用——空着一批去问，会让造任务那一侧每一轮
/// 都扫一遍图。
pub async fn dispatch(breaches: &[BreachReport], minter: &dyn ReplanMinter) {
    if breaches.is_empty() {
        return;
    }
    match minter.mint_replans(breaches).await {
        Ok(outcomes) => {
            // 交出去几条、回来几个结局：少回来的那几条没有任何判词，日志上不点名
            // 就永远不知道是哪一处越界没人管。
            if outcomes.len() != breaches.len() {
                warn!(
                    breaches = breaches.len(),
                    outcomes = outcomes.len(),
                    "the minting side answered fewer outcomes than the breaches it was given"
                );
            }
            for minting in outcomes {
                match minting.outcome {
                    ReplanOutcome::Minted { task_id } => {
                        readings().record(Outcome::Minted);
                        info!(
                            project = %minting.project,
                            index = minting.index.code(),
                            task_id = %task_id,
                            "performance breach turned into a task on the graph"
                        );
                    }
                    ReplanOutcome::AlreadyMinted { task_id, open } => {
                        readings().record(if open {
                            Outcome::AlreadyOpen
                        } else {
                            Outcome::AlreadyClosed
                        });
                        debug!(
                            project = %minting.project,
                            index = minting.index.code(),
                            task_id = %task_id,
                            open,
                            "this baseline revision already has a replan task"
                        );
                    }
                    ReplanOutcome::Refused { reason } => {
                        readings().record(Outcome::Refused);
                        warn!(
                            project = %minting.project,
                            index = minting.index.code(),
                            reason = %reason,
                            "a performance breach was not turned into a task"
                        );
                    }
                }
            }
        }
        // 回注失败不是「没有越界」：这一轮什么都没落定，下一轮还会再判一次。
        Err(e) => {
            readings().record(Outcome::DispatchFailed);
            warn!(
                breaches = breaches.len(),
                error = %e,
                "performance breaches were measured but not turned into tasks"
            );
        }
    }
}

/// 这一轮有哪些项目越界，以及这一轮有哪些量**没量成**。
async fn breaches_this_pass(state: &Arc<GatewayState>) -> (Vec<BreachReport>, Vec<Blind>) {
    // 基线读不到时一个项目也判不了：三个量全靠它。空着回去，而不是把每个项目都
    // 报成不越界——那是两件事。
    let Some(plans) = readable_plans(&state.orchestrator).await else {
        return (Vec::new(), vec![Blind::NoBaseline]);
    };
    let tasks = state.orchestrator.get_all_tasks().await;
    // 一轮问一次账，不是每个项目问一次：账里已经有 task_id，按项目切是本地的事。
    let spends = spends_of(state, &tasks).await;
    if spends.is_none() {
        return (
            breaches_of(&tasks, &plans, None, Utc::now()),
            vec![Blind::CostUnreadable],
        );
    }
    (
        breaches_of(&tasks, &plans, spends.as_ref(), Utc::now()),
        Vec::new(),
    )
}

/// 一轮的全部花销，按 task_id 聚合。账读不到时 `None`——与「一条都没花过」同形，
/// 而成本绩效会因此量不出来。那正是实话。
async fn spends_of(state: &Arc<GatewayState>, tasks: &[Task]) -> Option<BTreeMap<String, u64>> {
    let Some(store) = state.llm_usage.as_ref() else {
        warn!("project control: no token ledger in this process; cost is never measured");
        return None;
    };
    let ids: Vec<String> = tasks.iter().map(|t| t.id.clone()).collect();
    match store.spend_by_task(&ids).await {
        Ok(rows) => Some(rows.into_iter().collect()),
        Err(e) => {
            warn!(error = %e, "project control: task spend is not readable from the ledger");
            None
        }
    }
}

// ─── 这条回路自己的读数 ────────────────────────────────────────────────────

/// 这一次判词最后落成了什么。
///
/// 与日志同一个封闭词表，但它们是两种载体：日志回答「那一刻发生了什么」，计数器
/// 回答「到现在为止成了几件」。一条回路只留日志的时候，「这个部署从没造出过一条
/// 重规划」与「这条回路没在跑」在读数面上同形——而这两件事的修法完全相反。
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(usize)]
pub enum Outcome {
    /// 越界变成了一条图上的活。
    Minted = 0,
    /// 这一版基线已经有一条还在跑的，等它。
    AlreadyOpen = 1,
    /// 这一版基线的那一条已经走完了：它的结论去读，不再造。
    AlreadyClosed = 2,
    /// 生成处拒绝了，判词在日志里。
    Refused = 3,
    /// 交给造任务那一侧时整体失败：这一轮什么都没落定。
    DispatchFailed = 4,
}

impl Outcome {
    /// 每一个取值，按计数器下标排序。顺序由 [`Self::label`] 自己核，不由调用方记。
    pub const ALL: [Self; 5] = [
        Self::Minted,
        Self::AlreadyOpen,
        Self::AlreadyClosed,
        Self::Refused,
        Self::DispatchFailed,
    ];

    /// `outcome` 标签上的值。
    pub const fn label(self) -> &'static str {
        match self {
            Self::Minted => "minted",
            Self::AlreadyOpen => "already_open",
            Self::AlreadyClosed => "already_closed",
            Self::Refused => "refused",
            Self::DispatchFailed => "dispatch_failed",
        }
    }
}

/// 这一轮里没能量出来的那几件。
///
/// 三格各自把读者送去不同的地方：没有基线是计划面没写，账读不到是 token 账，
/// 账没接上是这个进程的装配。合成一格「没量成」会让这三种都去查同一处。
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(usize)]
pub enum Blind {
    /// 计划基线读不出来：挣值的三个量一个都算不了。
    NoBaseline = 0,
    /// 进程里没有 token 账：成本这一项永远量不出来。
    NoLedger = 1,
    /// 账在，这一下读不出来。
    CostUnreadable = 2,
}

impl Blind {
    pub const ALL: [Self; 3] = [Self::NoBaseline, Self::NoLedger, Self::CostUnreadable];

    pub const fn label(self) -> &'static str {
        match self {
            Self::NoBaseline => "no_baseline",
            Self::NoLedger => "no_ledger",
            Self::CostUnreadable => "cost_unreadable",
        }
    }
}

const OUTCOME_LABEL: &str = "outcome";
const BLIND_LABEL: &str = "reading";

/// 这条回路从本进程起来到现在，落成了多少条、有多少轮没量成。
///
/// 进程级累计，随生产者一起消失：它是「这条回路在这一版部署上做了什么事」，
/// 不是平台的历史，所以不进库、没有需要退役的行。
#[derive(Default)]
pub struct Readings {
    outcomes: [std::sync::atomic::AtomicU64; Outcome::ALL.len()],
    blindsight: [std::sync::atomic::AtomicU64; Blind::ALL.len()],
}

impl Readings {
    pub fn record(&self, outcome: Outcome) {
        self.outcomes[outcome as usize].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn record_blind(&self, blind: Blind) {
        self.blindsight[blind as usize].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn counts(&self) -> [(Outcome, u64); Outcome::ALL.len()] {
        Outcome::ALL.map(|o| {
            (
                o,
                self.outcomes[o as usize].load(std::sync::atomic::Ordering::Relaxed),
            )
        })
    }

    pub fn blind_counts(&self) -> [(Blind, u64); Blind::ALL.len()] {
        Blind::ALL.map(|b| {
            (
                b,
                self.blindsight[b as usize].load(std::sync::atomic::Ordering::Relaxed),
            )
        })
    }
}

/// 本进程的那一份。做成进程级而不是挂在状态上的一个字段，与后台循环的注册表
/// 同一个理由：它是这个进程自己的累计读数，没有第二个读者会拿它去判别的进程。
pub fn readings() -> Arc<Readings> {
    static READINGS: std::sync::OnceLock<Arc<Readings>> = std::sync::OnceLock::new();
    Arc::clone(READINGS.get_or_init(|| Arc::new(Readings::default())))
}

pub const OUTCOMES_TOTAL: &str = "cogneva_project_control_outcomes_total";
pub const UNMEASURED_TOTAL: &str = "cogneva_project_control_unmeasured_total";

#[async_trait::async_trait]
impl cog_core::Observable for Readings {
    async fn collect_metrics(
        &self,
        _dimension: &str,
    ) -> cog_core::SFResult<Vec<cog_core::observability::RawMetric>> {
        // 零值也发：一格还没发生过是这一版部署的读数，缺一格则什么都不是。
        let mut readings = Vec::with_capacity(Outcome::ALL.len() + Blind::ALL.len());
        for (outcome, count) in self.counts() {
            readings.push(
                cog_core::observability::RawMetric::new(OUTCOMES_TOTAL, count as f64)
                    .with_label(OUTCOME_LABEL, outcome.label()),
            );
        }
        for (blind, count) in self.blind_counts() {
            readings.push(
                cog_core::observability::RawMetric::new(UNMEASURED_TOTAL, count as f64)
                    .with_label(BLIND_LABEL, blind.label()),
            );
        }
        Ok(readings)
    }

    async fn collect_trace(
        &self,
        _task_id: &str,
    ) -> cog_core::SFResult<Vec<cog_core::observability::TraceFragment>> {
        Ok(Vec::new())
    }

    /// 进程级累计，哪个维度问都是同一个数。
    fn available_dimensions(&self) -> Vec<cog_core::observability::DimensionSpec> {
        Vec::new()
    }
}

/// 按项目切图，逐个项目读出越界。
///
/// 一个项目一份挣值响应，而不是把整个部署拼成一份：越界要挂到某个项目根下，
/// 合并之后「哪一条落后」就说不出来了。
fn breaches_of(
    tasks: &[Task],
    plans: &BTreeMap<String, cog_core::TaskPlan>,
    spends: Option<&BTreeMap<String, u64>>,
    now: chrono::DateTime<chrono::Utc>,
) -> Vec<BreachReport> {
    let mut by_project: BTreeMap<&str, Vec<&Task>> = BTreeMap::new();
    for task in tasks {
        by_project.entry(task.project_key()).or_default().push(task);
    }

    let mut found = Vec::new();
    for (project, rows) in by_project {
        // 未分组不是项目：它没有根，一条重规划挂在谁下面都说不清。
        if project == UNGROUPED {
            continue;
        }
        // 一条基线行都没有的项目，三个量量不出来，不必去算。
        if !rows.iter().any(|t| plans.contains_key(&t.id)) {
            continue;
        }
        let evm = build_evm_response(
            project.to_string(),
            &rows,
            Some(plans),
            spends,
            // 无归因用量与越界判定无关：它不属于任何项目，也就不进任何一个项目的
            // 成本绩效。这里不读它，省掉每轮一次全窗扫描。
            None,
            now,
        );
        found.extend(breaches_in(&evm));
    }
    found
}

/// 一份挣值读数里的越界。
///
/// 三个量任一量不出来就没有越界这回事：`None` 是「没量过」，不是零——把量不出来
/// 读成零会造出一条「差好多」的重规划，而那个差没有任何一项测量支持。
fn breaches_in(evm: &EvmResponse) -> Vec<BreachReport> {
    let Some(earned) = evm.earned_value.value else {
        return Vec::new();
    };
    let mut found = Vec::new();
    for (index, measured, against) in [
        (
            PerformanceIndex::Schedule,
            evm.schedule_performance_index,
            evm.planned_value.value,
        ),
        (
            PerformanceIndex::Cost,
            evm.cost_performance_index,
            evm.baselined_actual_cost.value,
        ),
    ] {
        let (Some(measured), Some(against)) = (measured, against) else {
            continue;
        };
        if index.breaching(measured) {
            found.push(BreachReport {
                project: evm.project_id.clone(),
                index,
                measured,
                earned,
                against,
            });
        }
    }
    found
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::TaskStatus;

    fn task(id: &str, goal: &str, status: TaskStatus) -> Task {
        let mut t = Task::new(id, cog_core::TaskType::Generator, serde_json::json!({}));
        t.goal_id = Some(goal.to_string());
        t.status = status;
        t
    }

    /// 一条计划了 `tokens` token、从 `from` 分钟前到 `to` 分钟后的任务。
    fn plan(
        task_id: &str,
        tokens: u64,
        from: i64,
        to: i64,
        now: chrono::DateTime<chrono::Utc>,
    ) -> cog_core::TaskPlan {
        cog_core::TaskPlan {
            workspace_id: "default".into(),
            task_id: task_id.into(),
            planned_start: Some(now + chrono::Duration::minutes(from)),
            planned_end: Some(now + chrono::Duration::minutes(to)),
            budget_tokens: tokens,
            baseline_rev: "rev-1".into(),
            updated_at: now,
        }
    }

    fn evm(
        rows: &[Task],
        plans: &[cog_core::TaskPlan],
        spends: &[(&str, u64)],
        now: chrono::DateTime<chrono::Utc>,
    ) -> EvmResponse {
        let plans: BTreeMap<String, cog_core::TaskPlan> = plans
            .iter()
            .map(|p| (p.task_id.clone(), p.clone()))
            .collect();
        let spends: BTreeMap<String, u64> = spends
            .iter()
            .map(|(id, tokens)| ((*id).to_string(), *tokens))
            .collect();
        let rows: Vec<&Task> = rows.iter().collect();
        build_evm_response("g1".into(), &rows, Some(&plans), Some(&spends), None, now)
    }

    /// 按计划走、按预算花：两个指数都刚好 1.0，一条活也不造。
    #[test]
    fn a_project_on_plan_breaches_on_neither_index() {
        let now = chrono::Utc::now();
        // 两条都在计划窗口结束时交付、各自挣满 1000：计划价值 2000、挣值 2000。
        let a = task("a", "g1", TaskStatus::Completed);
        let b = task("b", "g1", TaskStatus::Completed);
        let reading = evm(
            &[a, b],
            &[
                plan("a", 1000, -20, -10, now),
                plan("b", 1000, -20, -10, now),
            ],
            // 花在预算上：成本绩效 1.0。
            &[("a", 1000), ("b", 1000)],
            now,
        );
        assert_eq!(reading.planned_value.value, Some(2000));
        assert_eq!(reading.earned_value.value, Some(2000));
        assert_eq!(reading.schedule_performance_index, Some(1.0));
        assert_eq!(reading.cost_performance_index, Some(1.0));
        assert_eq!(breaches_in(&reading), Vec::new());
    }

    /// 该做完的没做完：只有进度越界，成本没有超——两件事分得开。
    #[test]
    fn a_project_behind_schedule_names_the_schedule_index_and_the_gap() {
        let now = chrono::Utc::now();
        let done = task("done", "g1", TaskStatus::Completed);
        let late = task("late", "g1", TaskStatus::Running);
        let reading = evm(
            &[done, late],
            &[
                plan("done", 1000, -20, -10, now),
                plan("late", 1000, -20, -10, now),
            ],
            &[("done", 1000)],
            now,
        );
        // 计划价值 2000、挣值 1000、花 1000。
        let found = breaches_in(&reading);
        assert_eq!(found.len(), 1, "{found:?}");
        assert_eq!(found[0].index, PerformanceIndex::Schedule);
        assert_eq!(found[0].project, "g1");
        assert_eq!(found[0].measured, 0.5);
        assert_eq!(found[0].earned, 1000);
        assert_eq!(found[0].against, 2000, "分母是计划价值");
        assert_eq!(found[0].gap_tokens(), 1000, "差多少要能折成 token");
    }

    /// 两条都越界时是两条，不是一条：成本与进度要分开处置。
    #[test]
    fn a_cost_overrun_is_a_second_breach_not_the_same_one() {
        let now = chrono::Utc::now();
        let done = task("done", "g1", TaskStatus::Completed);
        let late = task("late", "g1", TaskStatus::Running);
        let reading = evm(
            &[done, late],
            &[
                plan("done", 1000, -20, -10, now),
                plan("late", 1000, -20, -10, now),
            ],
            // 挣 1000、花 4000：成本绩效 0.25。
            &[("done", 4000)],
            now,
        );
        let found = breaches_in(&reading);
        assert_eq!(found.len(), 2, "{found:?}");
        let cost = found
            .iter()
            .find(|b| b.index == PerformanceIndex::Cost)
            .expect("成本越界");
        assert_eq!(cost.measured, 0.25);
        assert_eq!(cost.earned, 1000);
        assert_eq!(cost.against, 4000, "分母是同批任务的实际成本");
        assert_eq!(cost.gap_tokens(), 3000, "差多少要能折成 token");
        let schedule = found
            .iter()
            .find(|b| b.index == PerformanceIndex::Schedule)
            .expect("进度越界");
        assert_eq!(schedule.against, 2000, "分母是计划价值");
        assert_eq!(schedule.gap_tokens(), 1000);
    }

    /// 量不出来的指数不造活：`None` 不是零，也不是「一切正常」。
    #[test]
    fn an_index_that_could_not_be_measured_never_breaches() {
        let now = chrono::Utc::now();
        let done = task("done", "g1", TaskStatus::Completed);
        // 没有基线：三个量全部量不出来。
        let blind = build_evm_response("g1".into(), &[&done], None, None, None, now);
        assert_eq!(breaches_in(&blind), Vec::new());

        // 基线读到了、账上一条花销都没有：成本的分母为零，成本这条量不出来，
        // 只有进度能判。
        let reading = evm(&[done], &[plan("done", 1000, -20, -10, now)], &[], now);
        assert_eq!(reading.cost_performance_index, None);
        assert_eq!(reading.schedule_performance_index, Some(1.0));
        assert!(breaches_in(&reading)
            .iter()
            .all(|b| b.index != PerformanceIndex::Cost));
    }

    /// 未分组不是项目，没有基线的项目也判不出来：两种都一条活也不造。
    #[test]
    fn only_projects_that_have_a_baseline_are_judged() {
        let now = chrono::Utc::now();
        let ungrouped = task("u", "", TaskStatus::Running);
        assert_eq!(ungrouped.project_key(), UNGROUPED, "空 goal 就是未分组桶");
        let unplanned = task("p", "g1", TaskStatus::Completed);
        let mut plans: BTreeMap<String, cog_core::TaskPlan> = BTreeMap::new();
        // 基线全在未分组那条上：g1 一条都没有。
        plans.insert("u".into(), plan("u", 1000, -20, -10, now));
        let found = breaches_of(&[ungrouped, unplanned], &plans, Some(&BTreeMap::new()), now);
        assert_eq!(found, Vec::new(), "两个项目都没有可比的基线");
    }

    /// 账读不到时进度照判、成本不判：一条腿断不代表另一条腿也断。
    #[test]
    fn an_unreadable_ledger_leaves_the_schedule_reading_standing() {
        let now = chrono::Utc::now();
        let done = task("done", "g1", TaskStatus::Completed);
        let late = task("late", "g1", TaskStatus::Running);
        let plans: BTreeMap<String, cog_core::TaskPlan> = [
            plan("done", 1000, -20, -10, now),
            plan("late", 1000, -20, -10, now),
        ]
        .into_iter()
        .map(|p| (p.task_id.clone(), p))
        .collect();

        let found = breaches_of(&[done, late], &plans, None, now);
        assert_eq!(found.len(), 1, "{found:?}");
        assert_eq!(found[0].index, PerformanceIndex::Schedule);
    }

    /// 判出来的越界真的交到造任务那一侧：几条就交几条，没越界就一次都不交。
    #[test]
    fn the_breaches_reach_the_side_that_can_mint_tasks() {
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let minter = Recorder {
            seen: Arc::clone(&seen),
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        runtime.block_on(dispatch(&[], &minter));
        assert!(seen.lock().unwrap().is_empty(), "空的一批不去问");

        let batch = vec![
            BreachReport {
                project: "g1".into(),
                index: PerformanceIndex::Schedule,
                measured: 0.5,
                earned: 1,
                against: 2,
            },
            BreachReport {
                project: "g1".into(),
                index: PerformanceIndex::Cost,
                measured: 0.25,
                earned: 1,
                against: 4,
            },
        ];
        runtime.block_on(dispatch(&batch, &minter));
        assert_eq!(seen.lock().unwrap().len(), 1, "一轮一批，不是一条一调");
        assert_eq!(seen.lock().unwrap()[0], batch);
    }

    /// 一个把结局照脚本交回来的造任务侧，用来验这条回路自己的读数。
    struct Answering {
        outcome: ReplanOutcome,
        fails: bool,
    }

    #[async_trait::async_trait]
    impl ReplanMinter for Answering {
        async fn mint_replans(
            &self,
            breaches: &[BreachReport],
        ) -> cog_core::SFResult<Vec<cog_core::ReplanMinting>> {
            if self.fails {
                return Err(cog_core::SFError::Agent("minting side is down".into()));
            }
            Ok(breaches
                .iter()
                .map(|b| cog_core::ReplanMinting {
                    project: b.project.clone(),
                    index: b.index,
                    outcome: self.outcome.clone(),
                })
                .collect())
        }
    }

    fn one_breach() -> BreachReport {
        BreachReport {
            project: "g1".into(),
            index: PerformanceIndex::Schedule,
            measured: 0.5,
            earned: 1,
            against: 2,
        }
    }

    fn outcome_count(outcome: Outcome) -> u64 {
        readings()
            .counts()
            .into_iter()
            .find(|(o, _)| *o == outcome)
            .map(|(_, n)| n)
            .unwrap_or(0)
    }

    /// 这条回路自己的读数：每种结局各自一格，回注整体失败单独一格——它既不是
    /// 「没有越界」也不是任何一条判词，读成前两者都会让人以为事情有结论。
    ///
    /// 区分在 `dispatch_failed` 那格上：只记日志的话，造任务侧挂掉与这一轮没
    /// 有越界在读数面上同形。
    #[test]
    fn every_dispatch_outcome_is_counted_under_its_own_word() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let cases = [
            (
                ReplanOutcome::Minted {
                    task_id: "replan-a".into(),
                },
                Outcome::Minted,
            ),
            (
                ReplanOutcome::AlreadyMinted {
                    task_id: "replan-b".into(),
                    open: true,
                },
                Outcome::AlreadyOpen,
            ),
            (
                ReplanOutcome::AlreadyMinted {
                    task_id: "replan-c".into(),
                    open: false,
                },
                Outcome::AlreadyClosed,
            ),
            (
                ReplanOutcome::Refused {
                    reason: "no root row".into(),
                },
                Outcome::Refused,
            ),
        ];
        for (outcome, counted) in cases {
            let before = outcome_count(counted);
            let minter = Answering {
                outcome: outcome.clone(),
                fails: false,
            };
            runtime.block_on(dispatch(&[one_breach()], &minter));
            assert_eq!(outcome_count(counted), before + 1, "{outcome:?} 这一格没动");
        }

        let before = outcome_count(Outcome::DispatchFailed);
        let minter = Answering {
            outcome: ReplanOutcome::Minted {
                task_id: "unused".into(),
            },
            fails: true,
        };
        runtime.block_on(dispatch(&[one_breach()], &minter));
        assert_eq!(outcome_count(Outcome::DispatchFailed), before + 1);
    }

    /// 每一格都发，零值也发：一格还没发生过是这一版部署的读数，缺一格则什么都
    /// 不是——少一格会让「从没造出过」与「这一版不认识这个结局」同形。
    #[test]
    fn the_reading_publishes_every_cell_including_the_zeros() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let readings = Readings::default();
        let published = runtime
            .block_on(cog_core::Observable::collect_metrics(&readings, ""))
            .unwrap();
        for outcome in Outcome::ALL {
            assert!(
                published.iter().any(|m| m.name == OUTCOMES_TOTAL
                    && m.labels.get(OUTCOME_LABEL).map(String::as_str) == Some(outcome.label())),
                "少了 {} 这一格：{published:?}",
                outcome.label()
            );
        }
        for blind in Blind::ALL {
            assert!(
                published.iter().any(|m| m.name == UNMEASURED_TOTAL
                    && m.labels.get(BLIND_LABEL).map(String::as_str) == Some(blind.label())),
                "少了 {} 这一格：{published:?}",
                blind.label()
            );
        }
        assert!(published.iter().all(|m| m.value == 0.0), "新起的一份全是零");
    }

    /// 量不出来与量出来是零是两件事：账没接上时成本绩效永远算不出来，这时
    /// 「没有成本越界」不能悄悄成立——它要被记进 `reading=no_ledger` 那一格。
    #[test]
    fn a_pass_that_could_not_measure_its_cost_says_so_in_the_reading() {
        let readings = Readings::default();
        readings.record_blind(Blind::NoLedger);
        let counts = readings.blind_counts();
        assert_eq!(
            counts
                .into_iter()
                .find(|(b, _)| *b == Blind::NoLedger)
                .map(|(_, n)| n),
            Some(1)
        );
        assert_eq!(Blind::NoLedger.label(), "no_ledger");
    }

    /// 一个只记下被问了什么的造任务侧。
    #[derive(Default)]
    struct Recorder {
        seen: Arc<std::sync::Mutex<Vec<Vec<BreachReport>>>>,
    }

    #[async_trait::async_trait]
    impl ReplanMinter for Recorder {
        async fn mint_replans(
            &self,
            breaches: &[BreachReport],
        ) -> cog_core::SFResult<Vec<cog_core::ReplanMinting>> {
            self.seen.lock().unwrap().push(breaches.to_vec());
            Ok(Vec::new())
        }
    }

    /// 计划价值为零的项目没有进度可落后：分母为零时比值没有定义，不是零。
    #[test]
    fn a_project_whose_plan_has_not_started_has_no_schedule_breach() {
        let now = chrono::Utc::now();
        let open = task("open", "g1", TaskStatus::Scheduled);
        let reading = evm(&[open], &[plan("open", 1000, 10, 20, now)], &[], now);
        assert_eq!(reading.planned_value.value, Some(0));
        assert_eq!(reading.schedule_performance_index, None, "分母为零");
        assert_eq!(breaches_in(&reading), Vec::new());
    }
}
