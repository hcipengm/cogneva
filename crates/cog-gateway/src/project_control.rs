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
// cap: bound — 它只决定多快被看到，不决定造几条（去重键是基线版本）
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
    let (breaches, blind, coverage) = breaches_this_pass(state).await;
    for reason in blind {
        readings.record_blind(reason);
    }
    // 覆盖只在真的量成了的那一轮才盖出去：没量成时保留上一份与它自己的时刻，
    // 由读那一面按时刻判它新不新——把「这一轮没量成」写成 0 覆盖，与「图上真的
    // 一行基线都没有」同形，而后者是一个需要动手的事实。
    if let Some(coverage) = coverage {
        readings.record_coverage(coverage, Utc::now().timestamp());
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

/// 这一轮有哪些项目越界、有哪些量**没量成**，以及这一轮量成的那份覆盖。
///
/// 覆盖与判词同一次遍历出：覆盖率是「这一轮到底看见了多大的图」，它只有在与判词
/// 同一时刻读出来的前提下才说明得了「那几条越界是在多大的图上判的」。
async fn breaches_this_pass(
    state: &Arc<GatewayState>,
) -> (Vec<BreachReport>, Vec<Blind>, Option<Coverage>) {
    // 基线读不到时一个项目也判不了：三个量全靠它。空着回去，而不是把每个项目都
    // 报成不越界——那是两件事。
    let Some(plans) = readable_plans(&state.orchestrator).await else {
        return (Vec::new(), vec![Blind::NoBaseline], None);
    };
    let tasks = state.orchestrator.get_all_tasks().await;
    // 一轮问一次账，不是每个项目问一次：账里已经有 task_id，按项目切是本地的事。
    match spends_of(state.llm_usage.as_ref(), &tasks).await {
        Ok(spends) => {
            let (breaches, coverage) = breaches_of(&tasks, &plans, Some(&spends), Utc::now());
            (breaches, Vec::new(), Some(coverage))
        }
        // 账读不到时覆盖照样算得出来：挣值那一侧的分母是基线，不是花销。
        Err(blind) => {
            let (breaches, coverage) = breaches_of(&tasks, &plans, None, Utc::now());
            (breaches, vec![blind], Some(coverage))
        }
    }
}

/// 一轮的全部花销，按 task_id 聚合。
///
/// 两种量不成要分开报：进程里根本没有账，与账在但这一下读不出来。它们的修法不同
/// （前一个是装配，后一个是存储），而读数上合成一格会让第二种盖住第一种——「没接
/// 账」是这个进程永久的状态，恰恰需要一眼看见。
async fn spends_of(
    ledger: Option<&Arc<dyn cog_core::LlmUsageLedger>>,
    tasks: &[Task],
) -> Result<BTreeMap<String, u64>, Blind> {
    let Some(store) = ledger else {
        warn!("project control: no token ledger in this process; cost is never measured");
        return Err(Blind::NoLedger);
    };
    let ids: Vec<String> = tasks.iter().map(|t| t.id.clone()).collect();
    match store.spend_by_task(&ids).await {
        Ok(rows) => Ok(rows.into_iter().collect()),
        Err(e) => {
            warn!(error = %e, "project control: task spend is not readable from the ledger");
            Err(Blind::CostUnreadable)
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
    ///
    /// 它的斜率不是故障：一条越界在它那条重规划走完之前，每一轮都会在这里加一，
    /// 所以一块停滞的项目每 600 秒稳定贡献一条。按「非零」给它定红会天天响——这是
    /// 回路正常工作的样子。
    AlreadyOpen = 1,
    /// 图里已经有一条同 id 的行，而它已经走完：结论去读它，不再造。
    ///
    /// 只剩一条路能到这里：造任务那一侧算出的 id 与图里某一行相同、那一行已终态，
    /// 于是入图被当重复行跳过。基线版本上的去重是它之前的一道门，走到这里说明这一
    /// 行的 id 不是从当前版本算出来的，读它自己的状态比猜它的来历可靠。
    AlreadyClosed = 2,
    /// 生成处拒绝了，判词在日志里。
    ///
    /// **只能当存在性读，不能当斜率读**：拒绝的理由有六种，其中「这一版已经试满三
    /// 次」是终局——一条越界一旦走到那里，它此后每一轮都贡献一条拒绝。`rate()` 上
    /// 它是一条常开的线，任何按斜率定的阈值都会恒红。理由要分得开，得让造任务那一
    /// 侧把闭集理由交出来，目前它交的是一段给人读的句子。
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

const OUTCOME_LABEL: &str = crate::LABEL_OUTCOME;
const BLIND_LABEL: &str = "reading";

/// 这一轮在图上看见了多少行，其中有多少行量得出来。
///
/// 为什么分子与分母是同一族的两格、而不是一个比例：一个孤立的「覆盖率 61%」在 3 行
/// 的图上与在 3917 行的图上读起来一样，而它们的修法相反——前者是刚建的项目、还不该
/// 有基线，后者是装配漏了、每天都少判一片。人口下限必须跟着数一起出去，而不是让读
/// 那个比例的人自己猜。
///
/// 两种覆盖各有自己的分母：任务行的分母是图上全部任务，项目行的分母是图上全部项目
/// （未分组不算项目，它没有根）。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Coverage {
    /// 图上全部任务行，与其中有一行基线的行。
    pub tasks: u64,
    pub tasks_baselined: u64,
    /// 已经开工或已经收尾的任务行，与其中有一行基线的行。
    ///
    /// 这一格把「还没轮到」摘出去：待定与已排期还没轮到自己有基线。剩下的差额是
    /// 真判不出来的行，但它有两条来路——算不出来（那个种类攒不够样本）与漏了写，
    /// 两件的修法不同，而这三格本身分不开它们。
    pub started: u64,
    pub started_baselined: u64,
    /// 挂在别人下面的任务行，与带验收标准的行。
    ///
    /// 两格的分母都是 `tasks`：看板与甘特要的是「有多少行进得了树、有多少行做完
    /// 有判据」，两件事在同一个图上数才有意义。现网这两个数都很低，原因不在判据
    /// 上——图上大多数行不是分解出来的，是各个回路直接把一件通知当成任务写进来的，
    /// 那种行天然没有父、也没有验收标准。读数把它们摆出来，不等于它们是漏写。
    pub parented: u64,
    pub accepted: u64,
    /// 图上的项目数（不含未分组），与挣值挣得出来的项目数。
    pub projects: u64,
    pub projects_measurable: u64,
}

/// 已经开工或已经收尾的任务：它本该有一行基线。
///
/// 已取消的不算：它此后不再贡献任何挣值，补一行基线改变不了任何读数，把它记进缺陷
/// 人口只会让那个数永远降不到零。
fn baseline_due(status: cog_core::TaskStatus) -> bool {
    use cog_core::TaskStatus::{Completed, Failed, Running};
    matches!(status, Running | Completed | Failed)
}

/// 这条回路从本进程起来到现在，落成了多少条、有多少轮没量成。
///
/// 进程级累计，随生产者一起消失：它是「这条回路在这一版部署上做了什么事」，
/// 不是平台的历史，所以不进库、没有需要退役的行。
#[derive(Default)]
pub struct Readings {
    outcomes: [std::sync::atomic::AtomicU64; Outcome::ALL.len()],
    blindsight: [std::sync::atomic::AtomicU64; Blind::ALL.len()],
    /// 最近一轮量成的那份覆盖，连同量成它的时刻（unix 秒）。
    ///
    /// 带时刻而不是只带值，是因为它是个**会被冻住的读数**：这一轮没量成时不更新它，
    /// 光看值分不出「刚量过」与「两小时前量过一次」。读的那一面拿 `time() - 时刻`
    /// 判新旧，停下来的量器不会把一份旧覆盖继续冒充成现在的。
    coverage: std::sync::Mutex<Option<(Coverage, i64)>>,
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

    /// 盖出这一轮量成的覆盖。没量成的那一轮不许调它——留着上一份与它自己的时刻，
    /// 比写一份零更像真话。
    pub fn record_coverage(&self, coverage: Coverage, measured_at_unix: i64) {
        if let Ok(mut slot) = self.coverage.lock() {
            *slot = Some((coverage, measured_at_unix));
        }
    }

    pub fn coverage(&self) -> Option<(Coverage, i64)> {
        self.coverage.lock().ok().and_then(|slot| *slot)
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

/// 某一类行在图上总共有多少（分母）。
///
/// **这三格目前只有读数，没有判据**，而有意的：现网量过 52 条「已经开工却缺基线」的行，
/// 30 条 `handover` 根本没有 `started_at`、其余落在 `self_signal`／`platform_intent_assess`
/// 这些样本不足 5 条的种类上——它们是**冷启动人口**，不是有人漏写。修法是给那个种类定
/// 一份计划或等它攒够样本，两件都不发生在「把行补上」这个动作里；而按电平给它们定红，
/// 得到的是一条永远红、谁也灭不掉的告警，读的人三天之内就会把它关掉，那时真的回归也
/// 没人看得见。
///
/// 它该有的判据是「这一刻钟里有多少条活是因为**没样本**而不是因为**没算**而没排上期」，
/// 那件事只有估算器知道（它在 [`crate::project_control`] 看不见的那一侧按种类跳过）。
/// 在那条计数器落地之前，这里只发读数：覆盖率看得见，判词留白，白是写在纸上的白。
pub const COVERABLE_ROWS: &str = "cogneva_project_control_coverable_rows";
/// 其中有多少量得出来（分子）。
pub const COVERED_ROWS: &str = "cogneva_project_control_covered_rows";
/// 上一次量成覆盖的时刻（unix 秒）；0 表示这一版还没量成过。
pub const COVERAGE_MEASURED_UNIX: &str = "cogneva_project_control_coverage_measured_unix";

/// 任务行有没有一行基线（`task_plan`）：没有基线的行三个挣值量一个都算不了。
const KIND_TASK_BASELINE: &str = "task_baseline";
/// 同上，人口收窄到「已经开工或已经收尾」的行。
///
/// 这一格的差额**不全是缺陷**：现网 52 条里，2/3 落在没有任何时长样本的种类上，它们缺
/// 基线是谁也没法算，不是漏了。读它的时候要把「还没轮到」（待定／已排期）与「算不出来」
/// 分开看，两种缺法对应两种修法。
const KIND_STARTED_TASKS: &str = "started_tasks";
/// 任务行有没有父指针：没有父指针的行进不了 WBS 树，甘特与看板只能把它当散行画。
const KIND_TASK_PARENTED: &str = "task_parented";
/// 任务行有没有验收标准：没有的行做完了没有判据，看板只能看状态不能判交付。
const KIND_TASK_ACCEPTED: &str = "task_accepted";
/// 项目能不能算出挣值：`earned_value` 为 `None` 的项目一条越界都判不出来。
const KIND_PROJECT_EVM: &str = "project_evm";

/// 一行覆盖率读数怎么取：种类名，加上一份「把覆盖率拆成（该有、已经有）」的函数。
///
/// 单写一个别名不是为了少打几个字，是因为这张表的元素类型两头都要写在纸上——一边是
/// 名字，一边是取值函数——不加名字时每次读这张表都得先在心里把 `fn(Coverage) -> (u64, u64)`
/// 解一遍才知道两格分别是什么。
type CoverageRowRead = fn(Coverage) -> (u64, u64);

const COVERAGE_KINDS: [(&str, CoverageRowRead); 5] = [
    (KIND_TASK_BASELINE, |c| (c.tasks, c.tasks_baselined)),
    (KIND_TASK_PARENTED, |c| (c.tasks, c.parented)),
    (KIND_TASK_ACCEPTED, |c| (c.tasks, c.accepted)),
    (KIND_STARTED_TASKS, |c| (c.started, c.started_baselined)),
    (KIND_PROJECT_EVM, |c| (c.projects, c.projects_measurable)),
];

#[async_trait::async_trait]
impl cog_core::Observable for Readings {
    async fn collect_metrics(
        &self,
        _dimension: &str,
    ) -> cog_core::SFResult<Vec<cog_core::observability::RawMetric>> {
        // 零值也发：一格还没发生过是这一版部署的读数，缺一格则什么都不是。
        let mut readings = Vec::with_capacity(
            Outcome::ALL.len() + Blind::ALL.len() + 2 * COVERAGE_KINDS.len() + 1,
        );
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
        // 覆盖也照发，包括「还没量成过」那一份全零：缺一格会让「这一版没量过」与
        // 「这一版不认识这个读数」同形。时刻那一格是它的保鲜标记。
        let (coverage, measured_at) = self.coverage().unwrap_or_default();
        for (kind, cells) in COVERAGE_KINDS {
            let (coverable, covered) = cells(coverage);
            readings.push(
                cog_core::observability::RawMetric::new(COVERABLE_ROWS, coverable as f64)
                    .with_label(crate::LABEL_KIND, kind),
            );
            readings.push(
                cog_core::observability::RawMetric::new(COVERED_ROWS, covered as f64)
                    .with_label(crate::LABEL_KIND, kind),
            );
        }
        readings.push(cog_core::observability::RawMetric::new(
            COVERAGE_MEASURED_UNIX,
            measured_at as f64,
        ));
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
) -> (Vec<BreachReport>, Coverage) {
    let mut by_project: BTreeMap<&str, Vec<&Task>> = BTreeMap::new();
    for task in tasks {
        by_project.entry(task.project_key()).or_default().push(task);
    }

    // 与判词同一次遍历数出来：覆盖说的是「这一轮判词是在多大的图上得的」，分成
    // 两趟数就会在图上正在变的时候给出一份与判词不相干的覆盖。
    let mut coverage = Coverage {
        tasks: tasks.len() as u64,
        tasks_baselined: tasks.iter().filter(|t| plans.contains_key(&t.id)).count() as u64,
        started: tasks.iter().filter(|t| baseline_due(t.status)).count() as u64,
        started_baselined: tasks
            .iter()
            .filter(|t| baseline_due(t.status) && plans.contains_key(&t.id))
            .count() as u64,
        parented: tasks.iter().filter(|t| t.parent_task_id.is_some()).count() as u64,
        accepted: tasks.iter().filter(|t| !t.acceptance.is_empty()).count() as u64,
        ..Coverage::default()
    };

    let mut found = Vec::new();
    for (project, rows) in by_project {
        // 未分组不是项目：它没有根，一条重规划挂在谁下面都说不清。它也因此不进
        // 项目级的分母——它是桶，不是项目。
        if project == UNGROUPED {
            continue;
        }
        coverage.projects += 1;
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
        // 「挣值算得出来」判的是挣值本身，不是两个指数：指数各有自己的分母
        // （计划价值、实际成本），把「指数为 `None`」记进覆盖会让刚起步的项目
        // 与真的算不出挣值的项目混成一格。
        if evm.earned_value.value.is_some() {
            coverage.projects_measurable += 1;
        }
        found.extend(breaches_in(&evm));
    }
    (found, coverage)
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
        let (found, coverage) =
            breaches_of(&[ungrouped, unplanned], &plans, Some(&BTreeMap::new()), now);
        assert_eq!(found, Vec::new(), "两个项目都没有可比的基线");
        // 分母仍然是两个项目：判不出来不等于图里没有它，那正是要看见的缺口。
        assert_eq!(coverage.projects, 1, "未分组不进项目级分母");
        assert_eq!(coverage.projects_measurable, 0);
        assert_eq!(coverage.tasks, 2);
        assert_eq!(coverage.tasks_baselined, 1, "基线全在未分组那条上");
        // 两条都已经开工或收尾，一条有基线：差额 1 就是这条回路的缺陷人口。
        assert_eq!(coverage.started, 2);
        assert_eq!(coverage.started_baselined, 1);
    }

    /// 「还没轮到」与「已经开工」分得开：待定、已排期、已取消缺基线不进这一格。
    ///
    /// 合成一格会让那个数永远降不到零——每建一条新任务它就加一，而新任务刚建出来
    /// 本来就不该有基线。这一格只回答「有多少已经开工的活在挣值面上是看不见的」，
    /// 它自己不是判据（其中一条来路是那个种类攒不够样本，补行也补不出来）。
    #[test]
    fn only_tasks_that_have_already_started_count_as_missing_a_baseline() {
        let now = chrono::Utc::now();
        let rows = [
            task("p", "g1", TaskStatus::Pending),
            task("s", "g1", TaskStatus::Scheduled),
            task("r", "g1", TaskStatus::Running),
            task("c", "g1", TaskStatus::Completed),
            task("f", "g1", TaskStatus::Failed),
            task("x", "g1", TaskStatus::Cancelled),
        ];
        // 一行基线也没有。
        let (found, coverage) = breaches_of(&rows, &BTreeMap::new(), None, now);
        assert_eq!(found, Vec::new());
        assert_eq!(coverage.tasks, 6);
        assert_eq!(coverage.tasks_baselined, 0);
        assert_eq!(coverage.started, 3, "开工与收尾的三条才算缺陷人口");
        assert_eq!(coverage.started_baselined, 0);
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

        let (found, coverage) = breaches_of(&[done, late], &plans, None, now);
        assert_eq!(found.len(), 1, "{found:?}");
        assert_eq!(found[0].index, PerformanceIndex::Schedule);
        // 账读不到不拖累覆盖：挣值那一侧的分母是基线，不是花销。
        assert_eq!(coverage.tasks, 2);
        assert_eq!(coverage.tasks_baselined, 2);
        assert_eq!(coverage.projects, 1);
        assert_eq!(coverage.projects_measurable, 1);
        assert_eq!(coverage.started, 2);
        assert_eq!(coverage.started_baselined, 2);
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

    /// 覆盖发的是**分子与分母两格**，另带量成它的时刻。
    ///
    /// 只发一个比例的话，「图上只有 3 行、其中 0 行有基线」与「3917 行里 2385 行有
    /// 基线」在读数面上同形，而前者该等、后者该修。只发值不发时刻的话，一份两小时
    /// 前量成的覆盖会继续冒充现在的——这一格恰恰是会在量不成时被冻住的。
    #[test]
    fn the_coverage_ships_its_denominator_and_the_moment_it_was_measured() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let readings = Readings::default();
        let published = runtime
            .block_on(cog_core::Observable::collect_metrics(&readings, ""))
            .unwrap();
        assert_eq!(
            cell(&published, COVERAGE_MEASURED_UNIX, None),
            Some(0.0),
            "还没量过是 0，不是「刚量过一份全零」"
        );

        readings.record_coverage(
            Coverage {
                tasks: 3917,
                tasks_baselined: 2385,
                started: 600,
                started_baselined: 540,
                parented: 37,
                accepted: 32,
                projects: 10,
                projects_measurable: 7,
            },
            1_790_000_000,
        );
        let published = runtime
            .block_on(cog_core::Observable::collect_metrics(&readings, ""))
            .unwrap();
        assert_eq!(
            cell(&published, COVERABLE_ROWS, Some(KIND_TASK_BASELINE)),
            Some(3917.0)
        );
        assert_eq!(
            cell(&published, COVERED_ROWS, Some(KIND_TASK_BASELINE)),
            Some(2385.0)
        );
        assert_eq!(
            cell(&published, COVERABLE_ROWS, Some(KIND_STARTED_TASKS)),
            Some(600.0)
        );
        assert_eq!(
            cell(&published, COVERED_ROWS, Some(KIND_STARTED_TASKS)),
            Some(540.0)
        );
        assert_eq!(
            cell(&published, COVERABLE_ROWS, Some(KIND_TASK_PARENTED)),
            Some(3917.0),
            "父指针与验收标准的分母与基线那格相同：同一张图上数才有意义"
        );
        assert_eq!(
            cell(&published, COVERED_ROWS, Some(KIND_TASK_PARENTED)),
            Some(37.0)
        );
        assert_eq!(
            cell(&published, COVERED_ROWS, Some(KIND_TASK_ACCEPTED)),
            Some(32.0)
        );
        assert_eq!(
            cell(&published, COVERABLE_ROWS, Some(KIND_PROJECT_EVM)),
            Some(10.0)
        );
        assert_eq!(
            cell(&published, COVERED_ROWS, Some(KIND_PROJECT_EVM)),
            Some(7.0)
        );
        assert_eq!(
            cell(&published, COVERAGE_MEASURED_UNIX, None),
            Some(1_790_000_000.0)
        );
    }

    /// 一格里那个值：按指标名取，带 `kind` 的再按 `kind` 取。
    fn cell(
        published: &[cog_core::observability::RawMetric],
        name: &str,
        kind: Option<&str>,
    ) -> Option<f64> {
        published
            .iter()
            .find(|m| m.name == name && m.labels.get(crate::LABEL_KIND).map(String::as_str) == kind)
            .map(|m| m.value)
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

    /// 「这个进程没接账」这一格必须由**真实的那条路**产生，不是由测试自己写进去。
    ///
    /// 合成一格「成本读不到」时，这一格恒零：装配错了的部署看起来与账读坏了的一模
    /// 一样，而这正是它唯一该说话的时刻。
    #[tokio::test]
    async fn a_process_with_no_ledger_says_which_of_the_two_failures_it_hit() {
        let tasks = vec![task("t1", "p", TaskStatus::Completed)];
        assert_eq!(
            spends_of(None, &tasks).await.unwrap_err(),
            Blind::NoLedger,
            "no ledger in this process is its own reading, not an unreadable one"
        );
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
