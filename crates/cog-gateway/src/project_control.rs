//! 项目控制回路：量图上还有多少行没有计划基线。
//!
//! 这条回路原来做的是另一件事——判出挣值越界，把一条重规划任务塞进任务图，让模型
//! 重新排期。那条路删掉了：它跑完三十条、交回零个变更，而它要交的「新一版基线」
//! 本来就是估算器每轮自己写的。越界的指数本身也不再算：计划里写着「此刻开工」，
//! 而任务在队列里排着几小时，指数必然是 0——它量的是计划与现实的距离，不是哪条活
//! 干得好不好，那个距离也没有哪一处能靠「再造一条活」缩小。
//!
//! 留下的是这一半：排期这把尺子在图上有多少行真的用上了，以及这一轮量不出来的
//! 原因。这个问题有判据，也有修法——修的是估算器的种类样本，不是任务行。

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use cog_core::Task;
use tracing::info;

use crate::projects::{build_evm_response, readable_plans, UNGROUPED};
use crate::GatewayState;

/// 循环名，报在 `cogneva_loop_*` 的 `loop` 标签上。
pub const PROJECT_CONTROL_LOOP: &str = "gateway_project_control";

/// 一轮之间隔多久。
///
/// 覆盖率不是在窗口上发生的事件，是一个会被冻住的读数：一轮只决定它多快跟上图的
/// 变化。十分钟量一次，图上多了一千行也还看得见它是旧的（这一格带着量成时刻）。
// cap: bound — 只决定读数多快跟上图的变化，不决定任何动作
const CADENCE: Duration = Duration::from_secs(600);

/// 起这条回路。
pub fn spawn_project_control(
    state: Arc<GatewayState>,
    shutdown: cog_core::ShutdownSignal,
) -> tokio::task::JoinHandle<()> {
    let stop = shutdown.clone();
    cog_core::loop_health::spawn(
        PROJECT_CONTROL_LOOP,
        cog_core::loop_health::Cadence::Periodic(CADENCE),
        shutdown,
        move |beat| {
            let state = Arc::clone(&state);
            let stop = stop.clone();
            async move {
                let mut interval = tokio::time::interval(CADENCE);
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    // 每一轮都盖章：量得出来是常态，量不出来才是这一环的问题。
                    beat.beat();
                    tokio::select! {
                        biased;
                        _ = stop.wait() => {
                            info!("project control loop shutting down");
                            return;
                        }
                        _ = interval.tick() => {}
                    }
                    run_once(&state).await;
                }
            }
        },
    )
}

/// 一轮：量这一遍图上的覆盖率，量不出来时记一条具名原因。
pub async fn run_once(state: &Arc<GatewayState>) {
    let readings = readings();
    let (blind, coverage) = coverage_this_pass(state).await;
    for reason in blind {
        readings.record_blind(reason);
    }
    // 覆盖只在真的量成了的那一轮才盖出去：没量成时保留上一份与它自己的时刻，
    // 由读那一面按时刻判它新不新——把「这一轮没量成」写成 0 覆盖，与「图上真的
    // 一行基线都没有」同形，而后者是一个需要动手的事实。
    if let Some(coverage) = coverage {
        readings.record_coverage(coverage, Utc::now().timestamp());
    }
}

/// 这一轮量出来的覆盖，与量不出来的原因。
///
/// 基线读不到时一行也数不了：五格覆盖率里有三格的分母靠它，「没有基线」与
/// 「没有行」在读数面上同形。空着回去，而不是盖一份全零。
async fn coverage_this_pass(state: &Arc<GatewayState>) -> (Vec<Blind>, Option<Coverage>) {
    let Some(plans) = readable_plans(&state.orchestrator).await else {
        return (vec![Blind::NoBaseline], None);
    };
    let tasks = state.orchestrator.get_all_tasks().await;
    let coverage = coverage_of(&tasks, &plans, Utc::now());
    (Vec::new(), Some(coverage))
}

// ─── 这条回路自己的读数 ────────────────────────────────────────────────────

/// 这一轮里没能量出来的那几件。
///
/// 只剩一格：基线整张表读不出来。它曾经还有两格说的是 token 账（没接上、这一下
/// 读不到），那两格随成本指数一起去掉了——账读不到只影响成本绩效，而成本绩效
/// 不再有任何读者。
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(usize)]
pub enum Blind {
    /// 计划基线读不出来：五格覆盖率里三格的分母一个都算不了。
    NoBaseline = 0,
}

impl Blind {
    pub const ALL: [Self; 1] = [Self::NoBaseline];

    pub const fn label(self) -> &'static str {
        match self {
            Self::NoBaseline => "no_baseline",
        }
    }
}

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
/// 已取消的不算：它此后不再贡献任何读数，补一行基线改变不了什么，把它记进缺陷
/// 人口只会让那个数永远降不到零。
fn baseline_due(status: cog_core::TaskStatus) -> bool {
    use cog_core::TaskStatus::{Completed, Failed, Running};
    matches!(status, Running | Completed | Failed)
}

/// 这条回路从本进程起来到现在，有多少轮没量成。
///
/// 进程级累计，随生产者一起消失：它是「这条回路在这一版部署上遇到了什么」，
/// 不是平台的历史，所以不进库、没有需要退役的行。
#[derive(Default)]
pub struct Readings {
    blindsight: [std::sync::atomic::AtomicU64; Blind::ALL.len()],
    /// 最近一轮量成的那份覆盖，连同量成它的时刻（unix 秒）。
    ///
    /// 带时刻而不是只带值，是因为它是个**会被冻住的读数**：这一轮没量成时不更新它，
    /// 光看值分不出「刚量过」与「两小时前量过一次」。读的那一面拿 `time() - 时刻`
    /// 判新旧，停下来的量器不会把一份旧覆盖继续冒充成现在的。
    coverage: std::sync::Mutex<Option<(Coverage, i64)>>,
}

impl Readings {
    pub fn record_blind(&self, blind: Blind) {
        self.blindsight[blind as usize].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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

pub const UNMEASURED_TOTAL: &str = "cogneva_project_control_unmeasured_total";

/// 某一类行在图上总共有多少（分母）。
///
/// **这几格只有读数，没有判据**，而且是有意的：现网量过 52 条「已经开工却缺基线」的行，
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

/// 任务行有没有一行基线（`task_plan`）。
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
/// 项目能不能算出挣值：`earned_value` 为 `None` 的项目连挣值都摆不出来。
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
        let mut readings = Vec::with_capacity(Blind::ALL.len() + 2 * COVERAGE_KINDS.len() + 1);
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

/// 按项目切图，数这一轮看见了多少、量得出来多少。
///
/// 一个项目一条挣值读数，而不是把整个部署拼成一条：项目能不能算出挣值是逐项目的事，
/// 合并之后「哪个项目没有基线可算」就说不出来了。
fn coverage_of(
    tasks: &[Task],
    plans: &BTreeMap<String, cog_core::TaskPlan>,
    now: chrono::DateTime<chrono::Utc>,
) -> Coverage {
    let mut by_project: BTreeMap<&str, Vec<&Task>> = BTreeMap::new();
    for task in tasks {
        by_project.entry(task.project_key()).or_default().push(task);
    }

    // 与项目那一格同一次遍历数出来：覆盖说的是「这一轮是在多大的图上得的」，分成
    // 两趟数就会在图上正在变的时候给出一份不相干的覆盖。
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

    for (project, rows) in by_project {
        // 未分组不是项目：它没有根。它也因此不进项目级的分母——它是桶，不是项目。
        if project == UNGROUPED {
            continue;
        }
        coverage.projects += 1;
        // 一条基线行都没有的项目算不出挣值，不必去算。
        if !rows.iter().any(|t| plans.contains_key(&t.id)) {
            continue;
        }
        let evm = build_evm_response(
            project.to_string(),
            &rows,
            Some(plans),
            // 花销不读：成本绩效已经没有读者，而这一格只问挣值算不算得出来。
            // 每轮按两万个任务 id 去扫一遍账，买不到一个会被人看的数。
            None,
            None,
            now,
        );
        if evm.earned_value.value.is_some() {
            coverage.projects_measurable += 1;
        }
    }
    coverage
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

    /// 没有基线的项目不进项目级分子，但仍在分母里。
    #[test]
    fn only_projects_that_have_a_baseline_are_measurable() {
        let now = chrono::Utc::now();
        let ungrouped = task("u", "", TaskStatus::Running);
        assert_eq!(ungrouped.project_key(), UNGROUPED, "空 goal 就是未分组桶");
        let unplanned = task("p", "g1", TaskStatus::Completed);
        let mut plans: BTreeMap<String, cog_core::TaskPlan> = BTreeMap::new();
        // 基线全在未分组那条上：g1 一条都没有。
        plans.insert("u".into(), plan("u", 1000, -20, -10, now));
        let coverage = coverage_of(&[ungrouped, unplanned], &plans, now);
        // 分母仍然是项目：算不出来不等于图里没有它，那正是要看见的缺口。
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
    /// 本来就不该有基线。这一格只回答「有多少已经开工的活在排期面上是看不见的」，
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
        let coverage = coverage_of(&rows, &BTreeMap::new(), now);
        assert_eq!(coverage.tasks, 6);
        assert_eq!(coverage.tasks_baselined, 0);
        assert_eq!(coverage.started, 3, "开工与收尾的三条才算缺陷人口");
        assert_eq!(coverage.started_baselined, 0);
    }

    /// 发出去的是**分子与分母两格**，另带量成它的时刻。
    ///
    /// 只发一个比例的话，「图上只有 3 行、其中 0 行有基线」与「3917 行里 2385 行有
    /// 基线」在读数面上同形，而前者该等、后者该修。
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
        for blind in Blind::ALL {
            assert!(
                published.iter().any(|m| m.name == UNMEASURED_TOTAL
                    && m.labels.get(BLIND_LABEL).map(String::as_str) == Some(blind.label())),
                "少了 {} 这一格：{published:?}",
                blind.label()
            );
        }
        for (kind, _) in COVERAGE_KINDS {
            assert_eq!(
                cell(&published, COVERABLE_ROWS, Some(kind)),
                Some(0.0),
                "{kind} 的分母格不见了"
            );
            assert_eq!(
                cell(&published, COVERED_ROWS, Some(kind)),
                Some(0.0),
                "{kind} 的分子格不见了"
            );
        }
        assert_eq!(cell(&published, COVERAGE_MEASURED_UNIX, None), Some(0.0));
        assert!(published.iter().all(|m| m.value == 0.0), "新起的一份全是零");
    }

    /// 覆盖发的是**分子与分母两格**，另带量成它的时刻。
    ///
    /// 只发值不发时刻的话，一份两小时前量成的覆盖会继续冒充现在的——这一格恰恰
    /// 是会在量不成时被冻住的。
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
}
