//! Observable implementation for cog-collaboration.
//! Exposes D8 (Multi-Agent Collaboration) raw metrics.

use crate::squad::classify;
use async_trait::async_trait;
use cog_core::observability::{DimensionSpec, Observable, RawMetric, TraceFragment};
use cog_core::SFResult;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

/// The chart rule that reads the routing series this module publishes.
///
/// Named here, next to the producer, so the gate that checks the rule selects on
/// tiers this build actually writes has one place to find the name — a rule
/// renamed in the chart and not here would silently stop being checked.
pub const ROUTING_DECLARATION_FACE_GONE_RULE: &str = "routing_declaration_face_gone";

static GLOBAL: OnceLock<Arc<CollaborationObservable>> = OnceLock::new();

/// 自审的判定取值域。三个值都是结局，不是过程：`failed` 这一格存在的理由与
/// 另外两格相同——只按成功发布结局的族没有失败面，一次都答不上来的自审会
/// 与「没有自审」在读数上完全同形。
pub const SELF_REVIEW_VERDICT_PASS: &str = "pass";
pub const SELF_REVIEW_VERDICT_NEED_REVISION: &str = "need_revision";
pub const SELF_REVIEW_VERDICT_FAILED: &str = "failed";

/// 判据面：自审这次比对有没有外部判据（配置里的 spec、best_practices，或调用点
/// 交上来的任务说明）。`absent` 不是错误态，是「这次打分的输入只有它自己上一步
/// 写出来的文本」——分数能不能被反驳，全看这一格。
pub const SELF_REVIEW_CRITERIA_DECLARED: &str = "declared";
pub const SELF_REVIEW_CRITERIA_ABSENT: &str = "absent";

/// 自审被跳过的原因取值域。与判定面分开一处：跳过不是一次判定，读「门说了不」
/// 的那一格不能把「门根本没开」算进去。
///
/// `upstream_unavailable`：actor 自己都没到上游，输出是占位符——省下的是两次
/// 注定无效的调用（判据：输出来自 fallback 分支）。`disabled`：这段输出整个
/// 不属于自审覆盖的面（配置没开）。`self_evolution`：自进化产出按记录在案的
/// 理由不审（推理型模型会吐自然语言，改写步会挂满超时）。
pub const SELF_REVIEW_SKIP_UPSTREAM_UNAVAILABLE: &str = "upstream_unavailable";
pub const SELF_REVIEW_SKIP_DISABLED: &str = "disabled";
pub const SELF_REVIEW_SKIP_SELF_EVOLUTION: &str = "self_evolution";

/// The two ends of a revision step inside a self-review that did run. Both
/// cells answer a question the verdict cannot: a review that ended in
/// `need_revision` with the text unchanged is a review the loop could stop at,
/// and the pair (a revision that rewrote the text, one that rewrote nothing)
/// is what separates "the loop stops when rewriting buys nothing" from "the
/// loop never reached its second iteration".
pub const SELF_REVIEW_REVISION_CHANGED: &str = "changed";
pub const SELF_REVIEW_REVISION_UNCHANGED: &str = "unchanged";

/// How the pipeline's independent-review gate ended for one authoring verdict.
///
/// The gate is a second evaluator call asked to confirm a `Pass`, and it is the
/// fattest prompt in the chain. Two of these cells are verdicts and two are
/// savings, and all four are needed together: a `rejected` count on its own says
/// nothing about how often the second call was not spent, and a series with only
/// the two verdicts cannot be told from a deleted call site, because a call that
/// never happens writes no cell at all.
///
/// `not_asked_no_prior_verdict` is the saving: the gate asks a fresh judge to
/// confirm a verdict the authoring evaluator reached while holding no history at
/// all, and in that case the reviewer's request would be byte-identical to the
/// author's — same task, plan, generation, criteria and (empty) history. A
/// verdict cannot be independent of a question that was never asked differently.
/// `not_asked_disabled` is the same skip for the other reason: the gate is
/// configured off in this deployment.
pub const INDEPENDENT_REVIEW_AGREED: &str = "agreed";
pub const INDEPENDENT_REVIEW_REJECTED: &str = "rejected";
pub const INDEPENDENT_REVIEW_NOT_ASKED_NO_PRIOR_VERDICT: &str = "not_asked_no_prior_verdict";
pub const INDEPENDENT_REVIEW_NOT_ASKED_DISABLED: &str = "not_asked_disabled";

/// Every cell of the independent-review outcome, published at zero as well. See
/// [`INDEPENDENT_REVIEW_AGREED`].
pub const INDEPENDENT_REVIEW_OUTCOMES: [&str; 4] = [
    INDEPENDENT_REVIEW_AGREED,
    INDEPENDENT_REVIEW_REJECTED,
    INDEPENDENT_REVIEW_NOT_ASKED_NO_PRIOR_VERDICT,
    INDEPENDENT_REVIEW_NOT_ASKED_DISABLED,
];

/// The two ends of the history a failure analysis is shown, summed in bytes of
/// the serialized prompt section. `dropped` is what the bound removed: a
/// zero there means the bound never bound (not that it is broken), while a
/// zero on `fed` would mean the classifier was asked to classify with no
/// history at all.
pub const RALPH_HISTORY_FED: &str = "fed";
pub const RALPH_HISTORY_DROPPED: &str = "dropped";

/// Both ends of the failure-analysis history, published even at zero. See
/// [`RALPH_HISTORY_FED`].
pub const RALPH_HISTORY_PARTS: [&str; 2] = [RALPH_HISTORY_FED, RALPH_HISTORY_DROPPED];

/// How a decomposition left the boundary gate.
///
/// Three cells rather than two, because "the rules were read and the plan
/// stayed inside them" and "there was no hard rule to read" are different
/// facts that a pass/fail pair renders as one. The second is the shape a
/// configuration mistake takes: a rule set that is empty, disabled or soft-only
/// leaves nothing to violate, and on a counter that only reports violations it
/// looks exactly like a plan that was checked and cleared.
///
/// `violated` is the refusal itself, so the cell that matters when reading
/// whether a declared budget is doing anything is the one that must be able to
/// stay at zero without the gate being dead: a zero here next to a rising
/// `passed` says the rules were read and the plans fitted; the same zero next
/// to a rising `no_rules` says the rules were never reached.
pub const BOUNDARY_NO_HARD_RULES: &str = "no_hard_rules";
pub const BOUNDARY_PASSED: &str = "passed";
pub const BOUNDARY_VIOLATED: &str = "violated";

/// Every cell of the boundary verdict, published at zero as well. See
/// [`BOUNDARY_NO_HARD_RULES`].
pub const BOUNDARY_OUTCOMES: [&str; 3] =
    [BOUNDARY_NO_HARD_RULES, BOUNDARY_PASSED, BOUNDARY_VIOLATED];

/// The dimensions a hard rule can name.
///
/// A closed set on purpose: the evaluator produces a violation only for the
/// names it has a check for, and a rule declaring anything else is skipped, so
/// the label's value domain is these five whatever a configuration says. The
/// cells are published at zero too, which is what keeps "this dimension never
/// fired" apart from "this dimension was renamed out from under the reader".
pub const BOUNDARY_DIMENSIONS: [&str; 5] = [
    "TokenBudget",
    "SkillBoundary",
    "TimeBoundary",
    "StateBoundary",
    "DataBoundary",
];

pub fn global_observable() -> Arc<CollaborationObservable> {
    GLOBAL
        .get_or_init(|| Arc::new(CollaborationObservable::new()))
        .clone()
}

use tokio::sync::Mutex;

/// 自审判定的单元：(stage, criteria, verdict)。三段都取自闭集，所以单元数有界。
type SelfReviewCell = (String, String, String);

/// 自审跳过的单元：(stage, reason)。
type SelfReviewSkipCell = (String, String);

/// Collaboration-layer observable state.
#[derive(Default)]
pub struct CollaborationObservable {
    agent_message_count: AtomicU64,
    agent_turnaround_ms: Arc<Mutex<HashMap<String, Vec<u64>>>>,
    round_count: AtomicU64,
    /// Ralph Loop 终止计数，按终止原因分类（stagnated / budget_exhausted）。
    /// 不收敛链被有界止损是核心健康信号，必须可观测。
    /// 同步锁而非 `try_lock`：这是分类可达性自查的记录端，一次丢失会被
    /// 读成「这个分类从没被记录过」而报出并不存在的分叉。
    ralph_terminations: Arc<std::sync::Mutex<HashMap<String, u64>>>,
    /// 自进化任务的结果计数，按结局分类（submitted / no_artifacts /
    /// submit_failed / no_sink）。一个跑完却没有产出变更的自进化任务在
    /// 此之前与成功完全无法区分：squad 报 success、输出 JSON 里只是没有
    /// change_ids，既没有日志也没有指标，于是"生成侧不出货"能沉默地持续
    /// 下去。落地通道有没有货必须可数。
    change_yields: Arc<Mutex<HashMap<String, u64>>>,
    /// 分类声明的计数（产生端：写出带前缀 reason/feedback 时记一次）。
    /// 与 [`Self::ralph_terminations`]（记录端）构成分类可达性自查的两端。
    /// 用同步锁而非 `try_lock` 丢弃：这一端是「有没有声明」的证据本身，
    /// 计数漏记只会让矛盾看不见——自查建在会丢证据的计数上就没有意义。
    announced_classes: Arc<std::sync::Mutex<HashMap<String, u64>>>,
    /// 已报过的不可达分类。日志只在不成立→成立的那一刻报一次（矛盾是状态，
    /// 不是事件），gauge 则每轮照常打——序列恒 0 的事实必须一直看得见。
    unreachable_reported: Arc<Mutex<HashSet<String>>>,
    /// Route decisions, keyed by (stage, mode). The stage says which rule
    /// decided: the deterministic scale verdict, a keyword, the complexity
    /// score, the Agent, or the fallback. Without the stage, "the shortcut is
    /// running" and "nothing declares a scope" are the same reading — both
    /// leave the `direct` cell flat.
    route_decisions: Arc<std::sync::Mutex<HashMap<(String, String), u64>>>,
    /// Declared scales, over the closed set of tier names (unknown / none /
    /// shortcut / deep). This face answers whether the verdict still has an
    /// input: tiering reads the files and the diff the request declares itself,
    /// so a day when no request declares anything retires the whole rule — and
    /// it looks exactly like a day with no small changes.
    declared_scales: Arc<std::sync::Mutex<HashMap<String, u64>>>,
    /// Which declarations the routed requests actually carried. The tier face
    /// says what was decided; this says which inputs could have decided it, so
    /// a declaration no caller sends is visible as a cell that never moves
    /// rather than as an inference from reading the tiering's source.
    declaration_inputs: Arc<std::sync::Mutex<HashMap<String, u64>>>,
    /// Where the class a decomposition was retrieved under came from (carried
    /// with the goal, or the host task's own type). The two are the same string
    /// only as long as every caller that re-hosts a goal carries the class
    /// along; when one stops, the rows still come back and only this face says
    /// they now name the run instead of the work.
    goal_class_sources: Arc<std::sync::Mutex<HashMap<String, u64>>>,
    /// Self-review verdicts, keyed by (stage, criteria, verdict).
    ///
    /// The three faces answer three different questions a review's log line
    /// cannot: which stages review at all, how often the gate said no (the
    /// count of iterations, and therefore of LLM calls, follows from it), and
    /// whether the comparison had anything outside the review to compare
    /// against. That last one is the only way to tell a reviewing stage from a
    /// stage that reviews its own text with no standard — the two look
    /// identical from the score alone, which is why the score alone was never
    /// enough to decide whether the gate was doing anything.
    self_review_verdicts: Arc<std::sync::Mutex<HashMap<SelfReviewCell, u64>>>,
    /// Self-review skips, keyed by (stage, reason).
    ///
    /// A review that is not run is a saving, and an unread saving is not one:
    /// two calls per skipped review are the difference between the review
    /// surface costing what it is supposed to and costing double while it
    /// closes nothing. Kept apart from the verdict cells so "the gate objected"
    /// and "the gate never opened" never add up to the same number.
    self_review_skips: Arc<std::sync::Mutex<HashMap<SelfReviewSkipCell, u64>>>,
    /// The two ends of a self-review's revision step, keyed by (stage, outcome).
    ///
    /// See [`SELF_REVIEW_REVISION_CHANGED`]: the saving this face measures is the
    /// rest of the loop — a review whose revision rewrote nothing has nothing
    /// left to ask, and the iterations it does not run are two calls each.
    self_review_revisions: Arc<std::sync::Mutex<HashMap<(String, String), u64>>>,
    /// Bytes of serialized history handed to the failure-analysis classifier,
    /// keyed by [`RALPH_HISTORY_PARTS`].
    ///
    /// A prompt the loop pays for by the byte: without this face the bound that
    /// keeps it from growing with the run is indistinguishable from the bound
    /// never being reached, and both look like a classifier that works.
    ralph_history_bytes: Arc<std::sync::Mutex<HashMap<&'static str, u64>>>,
    /// Resume outcomes, over the closed set in [`crate::resume::RESUME_OUTCOMES`].
    /// The chain whose absence this measures is silent by construction: a task
    /// whose progress was never resumed simply starts again, which is what a
    /// first run looks like. Both cells are published so "no resume ever
    /// happened" and "nothing needed resuming" stay different readings.
    resume_outcomes: Arc<std::sync::Mutex<HashMap<String, u64>>>,
    /// How each decomposition left the boundary gate, over
    /// [`BOUNDARY_OUTCOMES`].
    ///
    /// The configured hard rules are the only place a task-size budget is
    /// declared, and until this face existed nothing said whether they were
    /// ever read: a rule set nobody consults and a plan that never broke one
    /// both left the same trace, which was none. Keyed by a `&'static str` so
    /// the three cells are the constants above and a typo cannot open a fourth.
    boundary_evaluations: Arc<std::sync::Mutex<HashMap<&'static str, u64>>>,
    /// Boundary violations by dimension, over [`BOUNDARY_DIMENSIONS`].
    ///
    /// Separate from the verdict above because the verdict says the gate
    /// refused and this says what it refused over — the difference between one
    /// dimension misconfigured and every plan coming in oversized.
    boundary_violations: Arc<std::sync::Mutex<HashMap<String, u64>>>,
    /// How the pipeline's independent-review gate ended, over
    /// [`INDEPENDENT_REVIEW_OUTCOMES`].
    ///
    /// The gate spends a whole evaluator call on a second opinion about a
    /// verdict already reached, and the two things a reader has to tell apart
    /// are a gate that buys independence from one that re-samples the same
    /// question. Neither the accepted verdict nor the log line separates them;
    /// only these cells and their zeros do. Keyed by a `&'static str` so the
    /// cells are the constants above and a typo cannot open a fifth.
    independent_reviews: Arc<std::sync::Mutex<HashMap<&'static str, u64>>>,
}

impl CollaborationObservable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_message(&self) {
        self.agent_message_count.fetch_add(1, Ordering::Relaxed);
    }

    pub async fn record_turnaround(&self, agent_id: impl Into<String>, ms: u64) {
        self.agent_turnaround_ms
            .lock()
            .await
            .entry(agent_id.into())
            .or_default()
            .push(ms);
    }

    pub fn record_round(&self) {
        self.round_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_ralph_termination(&self, reason: &str) {
        let mut map = self
            .ralph_terminations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *map.entry(reason.to_string()).or_insert(0) += 1;
    }

    pub fn record_change_yield(&self, outcome: &str) {
        if let Ok(mut map) = self.change_yields.try_lock() {
            *map.entry(outcome.to_string()).or_insert(0) += 1;
        }
    }

    /// Record one self-review verdict.
    ///
    /// A synchronous lock: this counter is the evidence of how often the quality
    /// gate said no — and a dropped count would read as a gate that never
    /// objected, which is precisely the state the counter exists to rule out.
    pub fn record_self_review_verdict(
        &self,
        stage: &str,
        config: &cog_core::SelfReviewConfig,
        verdict: &str,
    ) {
        let criteria = if config.has_external_criterion() {
            SELF_REVIEW_CRITERIA_DECLARED
        } else {
            SELF_REVIEW_CRITERIA_ABSENT
        };
        let mut map = self
            .self_review_verdicts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *map.entry((stage.to_string(), criteria.to_string(), verdict.to_string()))
            .or_insert(0) += 1;
    }

    /// Record one self-review that was not run, and why.
    ///
    /// Synchronous for the same reason as the verdict counter: a dropped count
    /// would read as reviews that were run, and the calls they cost, coming
    /// back.
    pub fn record_self_review_skip(&self, stage: &str, reason: &str) {
        let mut map = self
            .self_review_skips
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *map.entry((stage.to_string(), reason.to_string()))
            .or_insert(0) += 1;
    }

    /// Record how one self-review's revision step ended. Synchronous for the
    /// same reason as the verdict counter: the count is the evidence that the
    /// loop stopped where it did, and a dropped count reads as a loop that kept
    /// iterating.
    pub fn record_self_review_revision(&self, stage: &str, outcome: &str) {
        let mut map = self
            .self_review_revisions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *map.entry((stage.to_string(), outcome.to_string()))
            .or_insert(0) += 1;
    }

    /// Record how the independent-review gate ended for one authoring verdict.
    ///
    /// Synchronous for the same reason as the review counters above: a dropped
    /// count reads as a gate that was never asked, and the saving this family
    /// exists to show would then be indistinguishable from the gate being gone.
    pub fn record_independent_review(&self, outcome: &'static str) {
        let mut map = self
            .independent_reviews
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *map.entry(outcome).or_insert(0) += 1;
    }

    /// Record the size of the history a failure analysis was shown: the bytes
    /// fed, and the bytes the bound kept out.
    pub fn record_failure_history_bytes(&self, fed: usize, dropped: usize) {
        let mut map = self
            .ralph_history_bytes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *map.entry(RALPH_HISTORY_FED).or_insert(0) += fed as u64;
        *map.entry(RALPH_HISTORY_DROPPED).or_insert(0) += dropped as u64;
    }

    /// Record one route decision. A synchronous lock, because the cell is the
    /// evidence that a rule decided anything at all: a dropped count makes a
    /// live rule look like one that never decided.
    pub fn record_route_decision(&self, stage: &str, mode: &str) {
        let mut map = self
            .route_decisions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *map.entry((stage.to_string(), mode.to_string()))
            .or_insert(0) += 1;
    }

    /// Record how one decomposition left the boundary gate. Takes one of
    /// [`BOUNDARY_OUTCOMES`] by type, so the reading cannot grow a cell that is
    /// not one of the three.
    pub fn record_boundary_evaluation(&self, outcome: &'static str) {
        let mut map = self
            .boundary_evaluations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *map.entry(outcome).or_insert(0) += 1;
    }

    /// Record one boundary violation under the dimension that produced it. The
    /// name is the rule's own, which is why the published set is the closed
    /// [`BOUNDARY_DIMENSIONS`] rather than whatever a configuration declared.
    pub fn record_boundary_violation(&self, dimension: &str) {
        let mut map = self
            .boundary_violations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *map.entry(dimension.to_string()).or_insert(0) += 1;
    }

    /// Record one resume outcome. A synchronous lock: this counter is the only
    /// evidence that a resumed task was resumed at all, so a dropped count
    /// would read as a chain that never ran.
    pub fn record_resume(&self, outcome: &str) {
        let mut map = self
            .resume_outcomes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *map.entry(outcome.to_string()).or_insert(0) += 1;
    }

    /// Record one declared scale, counted by tier name. The tiers published for
    /// it are the ones a `DeclaredScale` can be labelled with, never a name
    /// taken from the caller.
    pub fn record_declared_scale(&self, tier: &str) {
        let mut map = self
            .declared_scales
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *map.entry(tier.to_string()).or_insert(0) += 1;
    }

    /// Record one declaration a routed request carried. Counted per request, so
    /// a request that both names files and attaches a diff counts on both
    /// cells: the reading is "how many requests carried this input", which is
    /// the question asked of it.
    pub fn record_declaration_input(&self, input: &str) {
        let mut map = self
            .declaration_inputs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *map.entry(input.to_string()).or_insert(0) += 1;
    }

    /// Record where one planner's goal class came from.
    pub fn record_goal_class_source(&self, source: &str) {
        let mut map = self
            .goal_class_sources
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *map.entry(source.to_string()).or_insert(0) += 1;
    }

    /// 记一次分类声明（产生端调用）。临界区只有一次 map 插入，同步加锁
    /// 换取不丢证据。
    pub fn announce_class(&self, class: &str) {
        let mut map = self
            .announced_classes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *map.entry(class.to_string()).or_insert(0) += 1;
    }
}

#[async_trait]
impl Observable for CollaborationObservable {
    async fn collect_metrics(&self, dimension: &str) -> SFResult<Vec<RawMetric>> {
        let mut metrics = Vec::new();
        if dimension == "D8" {
            metrics.push(RawMetric::new(
                "collab_message_count",
                self.agent_message_count.load(Ordering::Relaxed) as f64,
            ));
            metrics.push(RawMetric::new(
                "collab_round_count",
                self.round_count.load(Ordering::Relaxed) as f64,
            ));
            let turnarounds = self.agent_turnaround_ms.lock().await;
            for (agent_id, latencies) in turnarounds.iter() {
                if !latencies.is_empty() {
                    let avg = latencies.iter().sum::<u64>() as f64 / latencies.len() as f64;
                    metrics.push(
                        RawMetric::new("collab_agent_turnaround_ms", avg)
                            .with_label("agent_id", agent_id),
                    );
                }
            }
            // 同步锁取一份快照即放：临时 guard 在语句结束就释放，不会跨
            // `.await` 持有，也不会把记录端挡在门外。
            let recorded = self
                .ralph_terminations
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone();
            for (reason, count) in recorded.iter() {
                metrics.push(
                    RawMetric::new("ralph_terminations_total", *count as f64)
                        .with_label("reason", reason),
                );
            }
            let yields = self.change_yields.lock().await;
            for (outcome, count) in yields.iter() {
                metrics.push(
                    RawMetric::new("self_evolution_change_yield_total", *count as f64)
                        .with_label("outcome", outcome),
                );
            }
            let reviews = self
                .self_review_verdicts
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone();
            for ((stage, criteria, verdict), count) in reviews.iter() {
                metrics.push(
                    RawMetric::new("self_review_verdict_total", *count as f64)
                        .with_label("stage", stage)
                        .with_label("criteria", criteria)
                        .with_label("verdict", verdict),
                );
            }
            let skips = self
                .self_review_skips
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone();
            for ((stage, reason), count) in skips.iter() {
                metrics.push(
                    RawMetric::new("self_review_skipped_total", *count as f64)
                        .with_label("stage", stage)
                        .with_label("reason", reason),
                );
            }

            let revisions = self
                .self_review_revisions
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone();
            for ((stage, outcome), count) in revisions.iter() {
                metrics.push(
                    RawMetric::new("self_review_revision_total", *count as f64)
                        .with_label("stage", stage)
                        .with_label("outcome", outcome),
                );
            }

            // The independent-review gate's face, every cell published. The two
            // savings cells are the reason: "the gate was never asked" and "the
            // call site that asks it is gone" write the same (no) series, and
            // the second is exactly what a deleted guard looks like.
            let independent = self
                .independent_reviews
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone();
            for outcome in INDEPENDENT_REVIEW_OUTCOMES {
                let count = independent.get(outcome).copied().unwrap_or(0);
                metrics.push(
                    RawMetric::new("pge_independent_review_total", count as f64)
                        .with_label("outcome", outcome),
                );
            }

            // The failure-analysis prompt's own size, both ends published.
            let history_bytes = self
                .ralph_history_bytes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone();
            for part in RALPH_HISTORY_PARTS {
                let count = history_bytes.get(part).copied().unwrap_or(0);
                metrics.push(
                    RawMetric::new("ralph_failure_prompt_bytes_total", count as f64)
                        .with_label("part", part),
                );
            }

            // The routing face: every cell of the (stage, mode) cross product is
            // published, zeros included. A missing series and "this stage never
            // decided" are the same thing to a scraper, and a missing series is
            // what a de-wired recorder looks like — so that has to read as a
            // cell stuck at zero, never as a series that does not exist.
            let decisions = self
                .route_decisions
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone();
            for stage in crate::profile::RouteStage::ALL {
                for mode in crate::profile::PgeMode::ALL {
                    let key = (stage.as_str().to_string(), mode.as_str().to_string());
                    let count = decisions.get(&key).copied().unwrap_or(0);
                    metrics.push(
                        RawMetric::new("collab_route_decisions_total", count as f64)
                            .with_label("stage", stage.as_str())
                            .with_label("mode", mode.as_str()),
                    );
                }
            }

            // The tier face. Tiering reads what the request declares itself, so
            // "the declaration face is gone" and "no small changes today" have
            // to be separable readings: the first is the change that retires the
            // whole rule, and its only symptom would be the shortcut cell going
            // flat.
            let scales = self
                .declared_scales
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone();
            for label in crate::profile::SCALE_LABELS {
                let count = scales.get(label).copied().unwrap_or(0);
                metrics.push(
                    RawMetric::new("collab_declared_scale_total", count as f64)
                        .with_label("tier", label),
                );
            }

            // And which declarations those scales were read from. Without this
            // the tier face cannot tell "nobody declares any more" from "one
            // declaration has no producer": both leave the tier cells that only
            // that declaration feeds sitting at zero, and telling them apart
            // otherwise means reading the tiering's source.
            let inputs = self
                .declaration_inputs
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone();
            for input in crate::profile::DeclarationInput::ALL {
                let count = inputs.get(input.as_str()).copied().unwrap_or(0);
                metrics.push(
                    RawMetric::new("collab_declaration_inputs_total", count as f64)
                        .with_label("input", input.as_str()),
                );
            }

            // Which of the two readings the planner's goal class came from.
            // Both cells are published: a class always carried and a class
            // never carried have to look different, and the second is what a
            // forgotten producer of the field looks like.
            let sources = self
                .goal_class_sources
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone();
            for source in cog_core::GoalClassSource::ALL {
                let count = sources.get(source.as_str()).copied().unwrap_or(0);
                metrics.push(
                    RawMetric::new("collab_goal_class_source_total", count as f64)
                        .with_label("source", source.as_str()),
                );
            }

            // The boundary gate's verdict and the dimensions it refused over.
            // Both faces are published in full, zeros included: reporting only
            // the violations leaves "the rules were read and the plan fitted"
            // and "there was no hard rule to read" as the same number, and the
            // second is what a misconfigured rule set looks like.
            let verdicts = self
                .boundary_evaluations
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone();
            for outcome in BOUNDARY_OUTCOMES {
                let count = verdicts.get(outcome).copied().unwrap_or(0);
                metrics.push(
                    RawMetric::new("collab_boundary_evaluation_total", count as f64)
                        .with_label("outcome", outcome),
                );
            }
            let violations = self
                .boundary_violations
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone();
            for dimension in BOUNDARY_DIMENSIONS {
                let count = violations.get(dimension).copied().unwrap_or(0);
                metrics.push(
                    RawMetric::new("collab_boundary_violation_total", count as f64)
                        .with_label("dimension", dimension),
                );
            }

            // 续跑链的恢复端：每个结局都发布，零也发布。
            let resumes = self
                .resume_outcomes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone();
            for outcome in crate::resume::RESUME_OUTCOMES {
                let count = resumes.get(outcome).copied().unwrap_or(0);
                metrics.push(
                    RawMetric::new("collab_task_resume_total", count as f64)
                        .with_label("outcome", outcome),
                );
            }

            // 分类可达性自查：某个已声明的分类在产生端声明过，却在记录端从未
            // 落成终止——事件在日志里不断发生而指标序列恒为 0，说明中间边界
            // 把分类丢了。这一层必须自己说出来，不能等外部拿日志和指标对账。
            let announced = self
                .announced_classes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone();
            for class in classify::unreachable_classes(&announced, &recorded) {
                let mut reported = self.unreachable_reported.lock().await;
                if reported.insert(class.to_string()) {
                    tracing::warn!(
                        class,
                        "declared failure class was announced but never recorded; \
                         its series stays at zero while the events keep happening"
                    );
                }
                metrics.push(
                    RawMetric::new("collab_classification_unreachable", 1.0)
                        .with_label("class", class),
                );
            }
        }
        Ok(metrics)
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    fn available_dimensions(&self) -> Vec<DimensionSpec> {
        vec![DimensionSpec::bounded("D8")]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::observability::Observable;

    fn metric(metrics: &[RawMetric], name: &str) -> Option<RawMetric> {
        metrics.iter().find(|m| m.name == name).cloned()
    }

    /// 判据面与判定面一起发布。一次「有外部判据」的自审和一次「只有自己上一步
    /// 写出来的文本」的自审，在分数、日志、调用次数上完全同形；只有这一格能把
    /// 它们分开，而这一步是不是在给东西打分，全看它。
    #[tokio::test]
    async fn a_review_is_counted_by_the_standard_it_was_held_to() {
        let obs = CollaborationObservable::new();
        let held_to_a_standard = cog_core::SelfReviewConfig {
            spec: Some("the task's own words".into()),
            ..Default::default()
        };
        obs.record_self_review_verdict("planner", &held_to_a_standard, SELF_REVIEW_VERDICT_PASS);
        obs.record_self_review_verdict(
            "moderator",
            &cog_core::SelfReviewConfig::default(),
            SELF_REVIEW_VERDICT_NEED_REVISION,
        );

        let metrics = obs.collect_metrics("D8").await.unwrap();
        let reviews: Vec<&RawMetric> = metrics
            .iter()
            .filter(|m| m.name == "self_review_verdict_total")
            .collect();
        assert_eq!(reviews.len(), 2);
        let cell = |stage: &str, criteria: &str, verdict: &str| {
            reviews
                .iter()
                .find(|m| {
                    m.labels.get("stage").map(String::as_str) == Some(stage)
                        && m.labels.get("criteria").map(String::as_str) == Some(criteria)
                        && m.labels.get("verdict").map(String::as_str) == Some(verdict)
                })
                .map(|m| m.value)
                .unwrap_or_else(|| panic!("{stage}/{criteria}/{verdict} is reported"))
        };
        assert_eq!(cell("planner", SELF_REVIEW_CRITERIA_DECLARED, "pass"), 1.0);
        assert_eq!(
            cell("moderator", SELF_REVIEW_CRITERIA_ABSENT, "need_revision"),
            1.0
        );
    }

    /// 失败分析提示词的两端按字节发布，零也发布。
    #[tokio::test]
    async fn both_ends_of_the_failure_prompt_history_are_published() {
        let obs = CollaborationObservable::new();
        obs.record_failure_history_bytes(120, 0);

        let metrics = obs.collect_metrics("D8").await.unwrap();
        let bytes = |part: &str| {
            metrics
                .iter()
                .find(|m| {
                    m.name == "ralph_failure_prompt_bytes_total"
                        && m.labels.get("part").map(String::as_str) == Some(part)
                })
                .map(|m| m.value)
                .unwrap_or_else(|| panic!("{part} 这一格在没裁过时也要在"))
        };
        assert_eq!(bytes(RALPH_HISTORY_FED), 120.0);
        assert_eq!(bytes(RALPH_HISTORY_DROPPED), 0.0);
    }

    /// 改写这一步的两种结局各自成格：改写买到了新文本、改写什么都没改。
    /// 「循环从没走到改写」与「每次改写都重写了文本」不能读成同一件事。
    #[tokio::test]
    async fn a_revision_is_counted_by_what_it_rewrote() {
        let obs = CollaborationObservable::new();
        obs.record_self_review_revision("evaluator", SELF_REVIEW_REVISION_UNCHANGED);
        obs.record_self_review_revision("planner", SELF_REVIEW_REVISION_CHANGED);

        let metrics = obs.collect_metrics("D8").await.unwrap();
        let revisions: Vec<&RawMetric> = metrics
            .iter()
            .filter(|m| m.name == "self_review_revision_total")
            .collect();
        let cell = |stage: &str, outcome: &str| {
            revisions
                .iter()
                .find(|m| {
                    m.labels.get("stage").map(String::as_str) == Some(stage)
                        && m.labels.get("outcome").map(String::as_str) == Some(outcome)
                })
                .map(|m| m.value)
                .unwrap_or_else(|| panic!("{stage}/{outcome} 这一格要在"))
        };
        assert_eq!(cell("evaluator", SELF_REVIEW_REVISION_UNCHANGED), 1.0);
        assert_eq!(cell("planner", SELF_REVIEW_REVISION_CHANGED), 1.0);
    }

    #[tokio::test]
    async fn change_yield_is_counted_per_outcome() {
        let obs = CollaborationObservable::new();
        obs.record_change_yield("no_artifacts");
        obs.record_change_yield("no_artifacts");
        obs.record_change_yield("submitted");

        let metrics = obs.collect_metrics("D8").await.unwrap();
        let yields: Vec<&RawMetric> = metrics
            .iter()
            .filter(|m| m.name == "self_evolution_change_yield_total")
            .collect();
        assert_eq!(yields.len(), 2);
        let no_artifacts = yields
            .iter()
            .find(|m| m.labels.get("outcome").map(String::as_str) == Some("no_artifacts"))
            .expect("no_artifacts outcome is reported");
        assert_eq!(no_artifacts.value, 2.0);
        let submitted = yields
            .iter()
            .find(|m| m.labels.get("outcome").map(String::as_str) == Some("submitted"))
            .expect("submitted outcome is reported");
        assert_eq!(submitted.value, 1.0);
    }

    #[tokio::test]
    async fn a_run_that_never_yielded_reports_nothing() {
        // Absence of the series is the honest state: no self-evolution run has
        // finished yet. A zero-valued series would claim a measurement that
        // was never taken.
        let obs = CollaborationObservable::new();
        let metrics = obs.collect_metrics("D8").await.unwrap();
        assert!(metric(&metrics, "self_evolution_change_yield_total").is_none());
    }

    fn unreachable(metrics: &[RawMetric]) -> Vec<String> {
        metrics
            .iter()
            .filter(|m| m.name == "collab_classification_unreachable")
            .filter_map(|m| m.labels.get("class").cloned())
            .collect()
    }

    #[tokio::test]
    async fn a_declared_class_that_was_never_recorded_is_reported() {
        let obs = CollaborationObservable::new();
        obs.announce_class(classify::DEGENERATE_LOOP_CLASS);
        obs.record_ralph_termination(classify::UNCLASSIFIED_CLASS);

        let metrics = obs.collect_metrics("D8").await.unwrap();
        assert_eq!(
            unreachable(&metrics),
            vec![classify::DEGENERATE_LOOP_CLASS.to_string()],
            "a class declared but never recorded is the contradiction the plane must surface"
        );
    }

    #[tokio::test]
    async fn a_class_recorded_after_being_declared_clears_the_contradiction() {
        let obs = CollaborationObservable::new();
        obs.announce_class(classify::DEGENERATE_LOOP_CLASS);
        let metrics = obs.collect_metrics("D8").await.unwrap();
        assert_eq!(unreachable(&metrics).len(), 1);

        obs.record_ralph_termination(classify::DEGENERATE_LOOP_CLASS);
        let metrics = obs.collect_metrics("D8").await.unwrap();
        assert!(unreachable(&metrics).is_empty());
    }

    #[tokio::test]
    async fn nothing_declared_reports_no_unreachable_class() {
        let obs = CollaborationObservable::new();
        obs.record_ralph_termination("stagnated");
        let metrics = obs.collect_metrics("D8").await.unwrap();
        assert!(unreachable(&metrics).is_empty());
    }

    fn route_cells(metrics: &[RawMetric]) -> Vec<(String, String, f64)> {
        metrics
            .iter()
            .filter(|m| m.name == "collab_route_decisions_total")
            .map(|m| {
                (
                    m.labels.get("stage").cloned().unwrap_or_default(),
                    m.labels.get("mode").cloned().unwrap_or_default(),
                    m.value,
                )
            })
            .collect()
    }

    fn scale_cells(metrics: &[RawMetric]) -> Vec<(String, f64)> {
        metrics
            .iter()
            .filter(|m| m.name == "collab_declared_scale_total")
            .map(|m| (m.labels.get("tier").cloned().unwrap_or_default(), m.value))
            .collect()
    }

    fn input_cells(metrics: &[RawMetric]) -> Vec<(String, f64)> {
        metrics
            .iter()
            .filter(|m| m.name == "collab_declaration_inputs_total")
            .map(|m| (m.labels.get("input").cloned().unwrap_or_default(), m.value))
            .collect()
    }

    /// Counting by stage is the point: the mode alone cannot say whether the
    /// shortcut is still being taken or whether every request now falls through
    /// to the default.
    #[tokio::test]
    async fn route_decisions_are_counted_per_stage_and_mode() {
        let obs = CollaborationObservable::new();
        obs.record_route_decision("declared_scale", "direct");
        obs.record_route_decision("declared_scale", "direct");
        obs.record_route_decision("default", "roundtable");

        let metrics = obs.collect_metrics("D8").await.unwrap();
        let cells = route_cells(&metrics);
        let find = |stage: &str, mode: &str| {
            cells
                .iter()
                .find(|(s, m, _)| s == stage && m == mode)
                .map(|(_, _, v)| *v)
                .unwrap_or_default()
        };
        assert_eq!(find("declared_scale", "direct"), 2.0);
        assert_eq!(find("default", "roundtable"), 1.0);
        assert_eq!(find("keyword", "pipeline"), 0.0);
    }

    /// The whole cross product is published, zeros included. A stage that never
    /// fired and a stage whose instrumentation was removed would otherwise be
    /// the same reading — an absent series.
    #[tokio::test]
    async fn every_route_cell_exists_before_anything_is_routed() {
        let obs = CollaborationObservable::new();
        let metrics = obs.collect_metrics("D8").await.unwrap();
        let cells = route_cells(&metrics);
        assert_eq!(
            cells.len(),
            crate::profile::RouteStage::ALL.len() * crate::profile::PgeMode::ALL.len(),
            "one cell per (stage, mode): {cells:?}"
        );
        assert!(cells.iter().all(|(_, _, v)| *v == 0.0), "{cells:?}");
    }

    /// The tiering reads what a request declares, so "no request declared
    /// anything" has to be readable. If the declaration face went away, every
    /// cell would be zero — and that is exactly the state this series makes
    /// distinguishable from "no prose-only request arrived", which is also all
    /// zeros in the shortcut cell.
    #[tokio::test]
    async fn the_declaration_face_is_published_before_anything_declares() {
        let obs = CollaborationObservable::new();
        obs.record_declared_scale("unknown");
        let metrics = obs.collect_metrics("D8").await.unwrap();
        let cells = scale_cells(&metrics);
        assert_eq!(cells.len(), crate::profile::SCALE_LABELS.len(), "{cells:?}");
        let unknown = cells
            .iter()
            .find(|(tier, _)| tier == "unknown")
            .expect("unknown is published");
        assert_eq!(unknown.1, 1.0);
        let shortcut = cells
            .iter()
            .find(|(tier, _)| tier == "shortcut")
            .expect("the shortcut cell is published even at zero");
        assert_eq!(shortcut.1, 0.0);
    }

    /// Each declaration the tiering reads has a cell before any traffic.
    ///
    /// This is the reading that separates "no request declares any more" from
    /// "one declaration has no producer". Both leave the tier cells that only
    /// that declaration feeds at zero; only this series says which input went
    /// missing, and a cell that is absent instead of zero reads as a declaration
    /// that never existed.
    #[tokio::test]
    async fn every_declaration_the_tiering_reads_has_a_cell() {
        let obs = CollaborationObservable::new();
        obs.record_declaration_input(crate::profile::DeclarationInput::Diff.as_str());
        let metrics = obs.collect_metrics("D8").await.unwrap();
        let cells = input_cells(&metrics);
        assert_eq!(
            cells.len(),
            crate::profile::DeclarationInput::ALL.len(),
            "{cells:?}"
        );
        let diff = cells
            .iter()
            .find(|(input, _)| input == "diff")
            .expect("diff is published");
        assert_eq!(diff.1, 1.0);
        let goal_paths = cells
            .iter()
            .find(|(input, _)| input == "goal_paths")
            .expect("an input nothing carried is published as a zero cell");
        assert_eq!(goal_paths.1, 0.0);
    }

    /// The chart's rule selects on label values this build produces.
    ///
    /// A rule that names a tier nothing publishes never fires and never says so,
    /// which is the silent half of the defect it was written to catch. Read from
    /// the chart rather than from a list kept here: the two sides are maintained
    /// in different files, and only one of them is compiled.
    #[test]
    fn the_routing_rule_selects_on_tiers_this_build_publishes() {
        const RULE: &str = ROUTING_DECLARATION_FACE_GONE_RULE;
        const METRIC: &str = "collab_declared_scale_total";

        let config = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../deploy/helm/cogneva/files/cogneva.json"),
        )
        .expect("read the chart's cogneva.json");
        let parsed: serde_json::Value = serde_json::from_str(&config).expect("chart JSON parses");
        let rule = parsed["observability"]["infra_watch"]["rules"]
            .as_array()
            .expect("rules array")
            .iter()
            .find(|r| r["name"].as_str() == Some(RULE))
            .unwrap_or_else(|| panic!("the chart is missing rule {RULE}"));

        let promql = rule["promql"].as_str().unwrap_or_default();
        assert!(
            promql.contains(METRIC),
            "the rule must read the series this crate publishes: {promql}"
        );
        let tiers = tier_label_values(promql);
        assert!(
            !tiers.is_empty(),
            "the rule must select tiers, not the whole series: {promql}"
        );
        for tier in &tiers {
            assert!(
                crate::profile::SCALE_LABELS.contains(&tier.as_str()),
                "rule {RULE} selects tier=\"{tier}\", which nothing publishes: {:?}",
                crate::profile::SCALE_LABELS
            );
        }
        // The rule is about the tiering's input face, so it must select the
        // measured tiers. Selecting `unknown` instead would read "requests
        // declared nothing", which is also true of a deployment where the
        // selector was never handed a profile — a different fault that the
        // routing count already covers.
        for tiers_measured in ["shortcut", "deep", "none"] {
            assert!(
                tiers.contains(&tiers_measured.to_string()),
                "rule {RULE} must select tier=\"{tiers_measured}\": {tiers:?}"
            );
        }
    }

    /// Every `tier="x"` and `tier=~"x|y"` value in a promql expression.
    fn tier_label_values(promql: &str) -> Vec<String> {
        let mut values = Vec::new();
        let mut rest = promql;
        while let Some(at) = rest.find("tier=") {
            rest = &rest[at + "tier=".len()..];
            rest = rest.strip_prefix('~').unwrap_or(rest);
            let Some(quoted) = rest.strip_prefix('"') else {
                continue;
            };
            let Some(end) = quoted.find('"') else { break };
            for value in quoted[..end].split('|') {
                if !value.is_empty() {
                    values.push(value.to_string());
                }
            }
            rest = &quoted[end..];
        }
        values
    }

    /// The boundary gate's three verdicts and its five dimensions are all on
    /// the wire before anything has happened, and each cell counts only what
    /// was recorded into it. The zero cells are the point: `no_rules` has to be
    /// readable as a cell, because a rule set that was never reached is a
    /// configuration fault that a violations-only counter reports as silence.
    #[tokio::test]
    async fn every_boundary_cell_is_published_and_counted_apart() {
        let obs = CollaborationObservable::new();
        let metrics = obs.collect_metrics("D8").await.unwrap();
        let cell = |metrics: &[RawMetric], name: &str, label: &str, value: &str| {
            metrics
                .iter()
                .find(|m| {
                    m.name == name
                        && m.labels.get(label).map(String::as_str) == Some(value)
                })
                .map(|m| m.value)
                .unwrap_or_else(|| {
                    panic!("{name}{{{label}=\"{value}\"}} must be published, at 0 when nothing was recorded")
                })
        };

        for outcome in BOUNDARY_OUTCOMES {
            assert_eq!(
                cell(
                    &metrics,
                    "collab_boundary_evaluation_total",
                    "outcome",
                    outcome
                ),
                0.0,
                "{outcome} must be published even with nothing recorded into it"
            );
        }
        for dimension in BOUNDARY_DIMENSIONS {
            assert_eq!(
                cell(
                    &metrics,
                    "collab_boundary_violation_total",
                    "dimension",
                    dimension
                ),
                0.0
            );
        }

        obs.record_boundary_evaluation(BOUNDARY_NO_HARD_RULES);
        obs.record_boundary_evaluation(BOUNDARY_PASSED);
        obs.record_boundary_evaluation(BOUNDARY_VIOLATED);
        obs.record_boundary_violation("TokenBudget");
        obs.record_boundary_violation("TokenBudget");
        obs.record_boundary_violation("DataBoundary");

        let metrics = obs.collect_metrics("D8").await.unwrap();
        assert_eq!(
            cell(
                &metrics,
                "collab_boundary_evaluation_total",
                "outcome",
                BOUNDARY_NO_HARD_RULES
            ),
            1.0
        );
        assert_eq!(
            cell(
                &metrics,
                "collab_boundary_evaluation_total",
                "outcome",
                BOUNDARY_PASSED
            ),
            1.0
        );
        assert_eq!(
            cell(
                &metrics,
                "collab_boundary_evaluation_total",
                "outcome",
                BOUNDARY_VIOLATED
            ),
            1.0
        );
        assert_eq!(
            cell(
                &metrics,
                "collab_boundary_violation_total",
                "dimension",
                "TokenBudget"
            ),
            2.0
        );
        assert_eq!(
            cell(
                &metrics,
                "collab_boundary_violation_total",
                "dimension",
                "DataBoundary"
            ),
            1.0
        );
        assert_eq!(
            cell(
                &metrics,
                "collab_boundary_violation_total",
                "dimension",
                "TimeBoundary"
            ),
            0.0,
            "counting one dimension twice must not move another"
        );
    }

    /// All four independent-review cells are on the wire before the gate has
    /// been asked anything, and each counts only what was recorded into it.
    ///
    /// The two `not_asked_*` cells are the reason the family exists at all: they
    /// are the saving (one evaluator call, the fattest prompt in the chain) and
    /// they are written by a branch that spends nothing, so on a series that
    /// only counts what it bought they would be silence — the same silence a
    /// deleted call site leaves.
    #[tokio::test]
    async fn every_independent_review_cell_is_published_and_counted_apart() {
        let obs = CollaborationObservable::new();
        let cell = |metrics: &[RawMetric], outcome: &str| {
            metrics
                .iter()
                .find(|m| {
                    m.name == "pge_independent_review_total"
                        && m.labels.get("outcome").map(String::as_str) == Some(outcome)
                })
                .map(|m| m.value)
                .unwrap_or_else(|| {
                    panic!("pge_independent_review_total{{outcome=\"{outcome}\"}} must be published, at 0 when nothing was recorded")
                })
        };

        let metrics = obs.collect_metrics("D8").await.unwrap();
        for outcome in INDEPENDENT_REVIEW_OUTCOMES {
            assert_eq!(
                cell(&metrics, outcome),
                0.0,
                "{outcome} must be published even with nothing recorded into it"
            );
        }

        obs.record_independent_review(INDEPENDENT_REVIEW_NOT_ASKED_NO_PRIOR_VERDICT);
        obs.record_independent_review(INDEPENDENT_REVIEW_NOT_ASKED_NO_PRIOR_VERDICT);
        obs.record_independent_review(INDEPENDENT_REVIEW_AGREED);
        obs.record_independent_review(INDEPENDENT_REVIEW_REJECTED);

        let metrics = obs.collect_metrics("D8").await.unwrap();
        assert_eq!(
            cell(&metrics, INDEPENDENT_REVIEW_NOT_ASKED_NO_PRIOR_VERDICT),
            2.0
        );
        assert_eq!(cell(&metrics, INDEPENDENT_REVIEW_AGREED), 1.0);
        assert_eq!(cell(&metrics, INDEPENDENT_REVIEW_REJECTED), 1.0);
        assert_eq!(
            cell(&metrics, INDEPENDENT_REVIEW_NOT_ASKED_DISABLED),
            0.0,
            "a saving for one reason must not be counted as a saving for another"
        );
    }

    /// The extractor reads both spellings the chart uses, so a rule that moves
    /// from an equality to a regex does not silently stop being checked.
    #[test]
    fn tier_values_are_read_from_both_matcher_spellings() {
        let mut one = tier_label_values(r#"x{tier="unknown"}"#);
        one.sort();
        assert_eq!(one, vec!["unknown".to_string()]);
        let mut many = tier_label_values(r#"x{tier=~"shortcut|deep|none"} y{tier="unknown"}"#);
        many.sort();
        assert_eq!(
            many,
            vec![
                "deep".to_string(),
                "none".to_string(),
                "shortcut".to_string(),
                "unknown".to_string()
            ]
        );
    }
}
