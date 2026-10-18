//! Observable implementation for cog-agent.
//! Exposes the agent's cumulative run counters.

use async_trait::async_trait;
use cog_core::observability::{DimensionSpec, Observable, RawMetric, TraceFragment};
use cog_core::SFResult;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

static GLOBAL: OnceLock<Arc<AgentObservable>> = OnceLock::new();

pub fn global_observable() -> Arc<AgentObservable> {
    GLOBAL
        .get_or_init(|| Arc::new(AgentObservable::new()))
        .clone()
}

/// Agent-level observable state.
/// Tracks how the agent's runs ended, so the iteration ceiling can be derived
/// from what this role has actually delivered.
#[derive(Default)]
pub struct AgentObservable {
    run_count: AtomicU64,
    success_count: AtomicU64,
    budget_exhausted_count: AtomicU64,
    total_steps: AtomicU64,
    total_tool_calls: AtomicU64,
    /// Per-role reads of how many iterations a run actually took, keyed by role.
    ///
    /// The key is a role, which is a closed set the code names — not a task id
    /// or a path, which would grow without bound. The sample itself is a recent
    /// window (see [`BUDGET_SAMPLE_WINDOW`]), so the map stays bounded too.
    role_calibrations: Arc<Mutex<HashMap<String, RoleCalibration>>>,
    /// The agents the manager currently holds, one entry per live object.
    ///
    /// Published by the manager on every change and read at scrape time, so the
    /// count is the roster as it stands rather than the last event that touched
    /// it. The age is computed here for the same reason: an age written at the
    /// moment an agent was created stops growing the moment nothing else
    /// happens, and "nothing else happened" is exactly the state a leaked agent
    /// is in.
    ///
    /// The roster exists beside `role_calibrations` rather than inside it
    /// because they answer different questions about the same role: calibration
    /// is what a role's finished runs needed, this is how many of its objects
    /// are still alive. A leak shows up as the count and the age rising while
    /// the runs behind the calibration stop arriving.
    roster: Mutex<Vec<RosterEntry>>,
    /// Every role the roster has ever held.
    ///
    /// A gauge has no delete: a role whose last agent goes away simply stops
    /// being written, and the last nonzero value stays in the store reading as
    /// "still N alive". So the scrape writes an explicit zero for every role
    /// it has seen, and needs this set to know which those are.
    roster_roles: Mutex<HashMap<String, bool>>,
}

/// One live agent as the manager holds it.
#[derive(Clone)]
struct RosterEntry {
    role: String,
    created_at: std::time::Instant,
    /// Whether this object is scoped to a task or lives as long as the process.
    ///
    /// Carried into the exposition because the age reading is only judgeable
    /// within one of the two: one process-lifetime agent makes the age of the
    /// oldest object climb to the process uptime forever, and every forgotten
    /// task-scoped agent hides behind it.
    task_scoped: bool,
}

/// How many of a role's most recent completed runs feed its budget.
///
/// A window rather than a lifetime archive on purpose: the ceiling is meant to
/// track the workload as it is now, and a run from a month ago under a
/// different model says nothing about how much work a run needs today.
const BUDGET_SAMPLE_WINDOW: usize = 64;

/// What a role's own finished runs say about how many iterations it needs.
///
/// The iteration budget used to be one literal for every role, which can only
/// be right by accident: set too low, a run that was still working is cut off
/// mid-exploration and returns nothing (measured: 6 of 8 generator runs stopped
/// at exactly the ceiling with tool calls still pending, against a 25% delivery
/// rate); set too high, every run pays for slack it does not use. How much work
/// a role needs is observable from the runs it completes, so the ceiling is
/// derived from that instead of being a number someone has to guess and keep
/// guessing.
#[derive(Default)]
struct RoleCalibration {
    /// Iterations consumed by runs of this role that finished with an answer it
    /// reached on its own. A run cut off at the ceiling is left out even when
    /// the forced ask salvaged an answer: it stopped where the ceiling said, so
    /// its length measures the ceiling, not the work. Feeding it back lets every
    /// rescue raise the next ceiling, which makes the next rescue more likely —
    /// a ceiling climbing on its own record of having been hit.
    delivered_iterations: VecDeque<u32>,
    /// Runs of this role that were cut off at the ceiling. A run that stops
    /// this way is direct evidence the ceiling was binding, which is what an
    /// operator needs to see when the derived value is not moving.
    exhausted_runs: u64,
    /// Of those, the ones that ended with no answer at all — the run spent its
    /// whole budget and the forced final ask did not produce one either. Kept
    /// apart from `exhausted_runs` because that series also carries the runs the
    /// forced ask salvaged (`RunOutcome::DeliveredAfterExhaustion`), and the two
    /// endings cost the same and mean different things: one bought nothing, the
    /// other bought an answer out of a budget it had already spent. A reader
    /// told only the total cannot tell a role whose ceiling is too low from one
    /// whose work is simply not answering.
    exhausted_no_deliverable_runs: u64,
}

/// How an agent run ended.
///
/// A run that spent its whole iteration budget still reading and testing has
/// bought nothing: it hands back a sentinel, not an answer. Counting it as a
/// success hides the ending that costs the most tokens, so the endings are
/// distinguished here and counted apart — including the one that delivers
/// anyway, because "it delivered" and "it delivered out of a budget it had
/// already spent" cost the same and mean different things.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunOutcome {
    /// The model stopped calling tools and produced an answer.
    Delivered,
    /// The iteration budget ran out, and the forced ask still produced an
    /// answer. The run handed something back, but how long it *needed* is
    /// unmeasured — it was cut off — so this ending may not feed the
    /// calibration that decides how long this role's runs may be.
    DeliveredAfterExhaustion,
    /// The iteration budget ran out before an answer was written.
    BudgetExhausted,
}

impl RunOutcome {
    /// Whether this ending handed an answer back.
    ///
    /// Two readers judge the same endings the same way — the per-role success
    /// counter and whatever scores the skill the role was bound to — so the
    /// rule sits here rather than being restated at each of them. A run cut off
    /// at the ceiling that still answered counts, because it did hand something
    /// back; one that spent the budget without an answer does not.
    pub fn delivered(self) -> bool {
        matches!(self, Self::Delivered | Self::DeliveredAfterExhaustion)
    }
}

impl AgentObservable {
    pub fn new() -> Self {
        Self::default()
    }

    /// Replace the published roster with `entries` (role, creation instant).
    ///
    /// Replaced wholesale rather than incremented on create and on retire: a
    /// reader comparing a count that was incremented and decremented on two
    /// separate paths can only ask whether the two balanced, and a path that
    /// failed to decrement looks exactly like a path that never ran. Publishing
    /// the list makes the manager the single owner of the truth and the scrape
    /// a plain view of it.
    pub fn publish_roster(
        &self,
        entries: impl IntoIterator<Item = (String, std::time::Instant, bool)>,
    ) {
        let next: Vec<RosterEntry> = entries
            .into_iter()
            .map(|(role, created_at, task_scoped)| RosterEntry {
                role,
                created_at,
                task_scoped,
            })
            .collect();
        {
            let mut roles = self
                .roster_roles
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            for entry in &next {
                roles.insert(entry.role.clone(), entry.task_scoped);
            }
        }
        *self
            .roster
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = next;
    }

    /// Record a high-level agent run (called by AgentRuntime::run).
    ///
    /// `role` is what makes the reading usable at all: the iteration ceiling is
    /// derived per role, and a run counted without its role cannot inform any
    /// decision. `iterations` is in the same unit the ceiling is expressed in —
    /// not `steps`, which counts assistant turns and would silently rescale
    /// every reading it fed.
    pub fn record_run(
        &self,
        role: &str,
        outcome: RunOutcome,
        iterations: u32,
        steps: usize,
        tool_calls: usize,
    ) {
        self.run_count.fetch_add(1, Ordering::Relaxed);
        if outcome.delivered() {
            self.success_count.fetch_add(1, Ordering::Relaxed);
        }
        self.total_steps.fetch_add(steps as u64, Ordering::Relaxed);
        self.total_tool_calls
            .fetch_add(tool_calls as u64, Ordering::Relaxed);

        let mut calibrations = self
            .role_calibrations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let calibration = calibrations.entry(role.to_string()).or_default();
        if outcome == RunOutcome::Delivered {
            if calibration.delivered_iterations.len() == BUDGET_SAMPLE_WINDOW {
                calibration.delivered_iterations.pop_front();
            }
            calibration.delivered_iterations.push_back(iterations);
        }
        // Counted here, at the run's end, and not at the moment the ceiling is
        // judged spent (`note_budget_exhausted`): only the ending says whether
        // the forced ask produced an answer. The judgement-time counter cannot
        // tell the two apart, which is exactly the gap this one fills.
        if outcome == RunOutcome::BudgetExhausted {
            calibration.exhausted_no_deliverable_runs += 1;
        }
    }

    /// Note that a run's iteration budget was spent, at the moment the budget is
    /// judged spent rather than at the end of the run.
    ///
    /// What a run cost and what it finally handed back are two facts about two
    /// moments. Recorded only at the end, a run that spent everything and was
    /// then salvaged by the forced ask would read exactly like one that never
    /// came near the ceiling: the ending that costs the most tokens, reported as
    /// the healthy one. Recorded here, it is counted whatever ending the run
    /// lands on — and counted even if it never lands on one.
    pub fn note_budget_exhausted(&self, role: &str) {
        self.budget_exhausted_count.fetch_add(1, Ordering::Relaxed);
        let mut calibrations = self
            .role_calibrations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        calibrations
            .entry(role.to_string())
            .or_default()
            .exhausted_runs += 1;
    }

    /// The iteration ceiling to run this role under, given the value a caller
    /// would use with no evidence behind it (`seed`: the operator's config, or
    /// the role's skill).
    ///
    /// With nothing observed about this role the seed stands — moving a number
    /// without evidence would be guesswork with extra steps. Once runs have
    /// completed, the ceiling is the longest run this role needed plus one more
    /// typical run's worth of room:
    ///
    /// - the longest completed run is the floor of experience. A ceiling that
    ///   will not admit a run of a shape this role is known to have needed is
    ///   not a safety limit, it is a defect;
    /// - the headroom is what keeps the ceiling from being a claim about the
    ///   past. A ceiling set exactly to the historical maximum cuts off the
    ///   first run that is slightly harder than anything seen, and that cut is
    ///   total: the run returns nothing and has already spent everything. One
    ///   typical work unit (the mean) of slack absorbs that variance.
    ///
    /// Both terms are observed, so the ceiling follows the workload instead of
    /// needing to be retuned by hand whenever the workload changes.
    pub fn iteration_budget_for(&self, role: &str, seed: u32) -> u32 {
        let calibrations = self
            .role_calibrations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(calibration) = calibrations.get(role) else {
            return seed;
        };
        if calibration.delivered_iterations.is_empty() {
            return seed;
        }
        let longest = *calibration
            .delivered_iterations
            .iter()
            .max()
            .expect("sample is not empty");
        let sum: u32 = calibration.delivered_iterations.iter().sum();
        let typical = sum.div_ceil(calibration.delivered_iterations.len() as u32);
        seed.max(longest.saturating_add(typical))
    }
}

#[async_trait]
impl Observable for AgentObservable {
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        let mut metrics = Vec::new();

        // High-level counters (available in all dimensions)
        let runs = self.run_count.load(Ordering::Relaxed);
        metrics.push(RawMetric::new("agent_run_count", runs as f64));
        metrics.push(RawMetric::new(
            "agent_success_count",
            self.success_count.load(Ordering::Relaxed) as f64,
        ));
        metrics.push(RawMetric::new(
            "agent_budget_exhausted_count",
            self.budget_exhausted_count.load(Ordering::Relaxed) as f64,
        ));
        metrics.push(RawMetric::new(
            "agent_total_steps",
            self.total_steps.load(Ordering::Relaxed) as f64,
        ));
        metrics.push(RawMetric::new(
            "agent_total_tool_calls",
            self.total_tool_calls.load(Ordering::Relaxed) as f64,
        ));

        // The iteration ceiling is decided per role, so the reading of it has
        // to be per role too: a single process-wide number here could not tell
        // an operator which role is being cut off, which is the one question
        // this pair of series exists to answer.
        for (role, calibration) in self
            .role_calibrations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
        {
            metrics.push(
                RawMetric::new(
                    "agent_iteration_budget_exhausted",
                    calibration.exhausted_runs as f64,
                )
                .with_label("role", role),
            );
            metrics.push(
                RawMetric::new(
                    "agent_iteration_budget_exhausted_no_deliverable",
                    calibration.exhausted_no_deliverable_runs as f64,
                )
                .with_label("role", role),
            );
        }

        // The manager's roster, published on every change and read here. Two
        // series per role because either one alone can be read as healthy: a
        // count that stopped falling says nothing about how long the oldest
        // object has been there, and one agent created at boot looks exactly
        // like a busy role on the count alone.
        {
            let roster = self
                .roster
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let mut by_role: HashMap<&str, (u64, f64)> = HashMap::new();
            for entry in roster.iter() {
                let age = entry.created_at.elapsed().as_secs_f64();
                let slot = by_role.entry(entry.role.as_str()).or_insert((0, 0.0));
                slot.0 += 1;
                if age > slot.1 {
                    slot.1 = age;
                }
            }
            let roles = self
                .roster_roles
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            for (role, task_scoped) in roles.iter() {
                let lifetime = if *task_scoped { "task" } else { "process" };
                let (count, oldest_age) =
                    by_role.get(role.as_str()).copied().unwrap_or((0, f64::NAN));
                metrics.push(
                    RawMetric::new("agent_roster", count as f64)
                        .with_label("role", role)
                        .with_label("lifetime", lifetime),
                );
                // Only while the role still holds something. An age with no
                // object behind it would be a number left over from the last
                // scrape, and a reader who takes it as the age of the oldest
                // live agent reads a leak where there is none. The count is the
                // gate: zero there means the age of this role is not a reading.
                if count > 0 {
                    metrics.push(
                        RawMetric::new("agent_roster_oldest_age_seconds", oldest_age)
                            .with_label("role", role)
                            .with_label("lifetime", lifetime),
                    );
                }
            }
        }

        Ok(metrics)
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        // No per-step records are kept. The series this used to serve were keyed
        // by task id, so their cardinality grew with every task, and nothing read
        // them: the scrape never asks for them and no rule or panel named them.
        Ok(Vec::new())
    }

    /// 空集：这个 observable 的读数不随维度变。它只报累计计数器，采集侧因此
    /// "没有可采维度时采一次"，而不是按维度重复问同一份读数回来。
    fn available_dimensions(&self) -> Vec<DimensionSpec> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The collector asks an observable that declares no dimensions once, with
    /// an empty name; that is the only call this observable ever answers.
    async fn readings(o: &AgentObservable) -> (f64, f64, f64) {
        let metrics = o.collect_metrics("").await.expect("collect_metrics");
        let read = |name: &str| {
            metrics
                .iter()
                .find(|m| m.name == name)
                .map(|m| m.value)
                .unwrap_or(-1.0)
        };
        (
            read("agent_run_count"),
            read("agent_success_count"),
            read("agent_budget_exhausted_count"),
        )
    }

    /// A role whose last agent was removed must still be written, as zero.
    ///
    /// A gauge has no delete. If the empty role is simply left out of the
    /// scrape, the store keeps the last nonzero value and every reader of it
    /// sees a role that still holds agents, forever.
    #[tokio::test]
    async fn an_emptied_role_is_written_as_zero_rather_than_left_out() {
        let o = AgentObservable::new();
        let now = std::time::Instant::now();
        o.publish_roster(vec![("planner".to_string(), now, true)]);
        o.publish_roster(Vec::<(String, std::time::Instant, bool)>::new());

        let metrics = o.collect_metrics("").await.expect("collect_metrics");
        let count = metrics
            .iter()
            .find(|m| {
                m.name == "agent_roster"
                    && m.labels.get("role").map(String::as_str) == Some("planner")
            })
            .expect("the role is still reported after it emptied");
        assert_eq!(count.value, 0.0);
        assert_eq!(
            count.labels.get("lifetime").map(String::as_str),
            Some("task")
        );
        assert!(
            !metrics
                .iter()
                .any(|m| m.name == "agent_roster_oldest_age_seconds"),
            "an age with nothing behind it would be the previous scrape's number"
        );
    }

    /// The process-lifetime agent must be separable, or the age reading cannot
    /// be judged: it alone would hold the oldest age at the process uptime and
    /// hide every task agent that was never retired.
    #[tokio::test]
    async fn a_process_lifetime_agent_is_labelled_apart_from_task_agents() {
        let o = AgentObservable::new();
        let now = std::time::Instant::now();
        o.publish_roster(vec![
            ("planner".to_string(), now, true),
            ("mode_selector".to_string(), now, false),
        ]);

        let metrics = o.collect_metrics("").await.expect("collect_metrics");
        let lifetime_of = |role: &str| {
            metrics
                .iter()
                .find(|m| {
                    m.name == "agent_roster"
                        && m.labels.get("role").map(String::as_str) == Some(role)
                })
                .and_then(|m| m.labels.get("lifetime").map(String::as_str))
        };
        assert_eq!(lifetime_of("planner"), Some("task"));
        assert_eq!(lifetime_of("mode_selector"), Some("process"));
    }

    #[tokio::test]
    async fn a_run_that_spent_its_budget_is_counted_as_an_exhaustion_not_a_success() {
        let o = AgentObservable::new();
        o.note_budget_exhausted("generator");
        o.record_run("generator", RunOutcome::BudgetExhausted, 10, 21, 12);

        let (runs, successes, exhausted) = readings(&o).await;
        assert_eq!(runs, 1.0);
        assert_eq!(
            successes, 0.0,
            "a run that wrote no answer must not be counted as a success"
        );
        assert_eq!(exhausted, 1.0);
    }

    /// 抢救回来的那一轮照样把预算花光了。读数必须两件都说：它是一次交付，
    /// 也是一次烧空。只在终局记一次的话，这种结尾会读成完全健康的一轮——
    /// 而它恰恰是最贵的那种。
    #[tokio::test]
    async fn a_run_salvaged_after_spending_its_budget_reads_as_both() {
        let o = AgentObservable::new();
        o.note_budget_exhausted("generator");
        o.record_run(
            "generator",
            RunOutcome::DeliveredAfterExhaustion,
            11,
            21,
            12,
        );

        let (runs, successes, exhausted) = readings(&o).await;
        assert_eq!(runs, 1.0);
        assert_eq!(successes, 1.0, "它确实把东西交出去了");
        assert_eq!(exhausted, 1.0, "而它确实把预算花光了才交出去");
    }

    /// 被截断的运行不能反过来给上限当证据：它停在上限指定的地方，量到的是
    /// 上限本身，不是工作量。拿它抬墙就是「越抢救、墙越高、越容易再撞」。
    #[test]
    fn a_salvaged_run_does_not_raise_the_ceiling_it_stopped_at() {
        let o = AgentObservable::new();
        o.note_budget_exhausted("generator");
        o.record_run(
            "generator",
            RunOutcome::DeliveredAfterExhaustion,
            11,
            21,
            12,
        );
        assert_eq!(
            o.iteration_budget_for("generator", 10),
            10,
            "在上限处被截断的运行，不能是「上限太低」的证据"
        );
    }

    #[tokio::test]
    async fn a_run_that_delivered_an_answer_is_counted_as_a_success() {
        let o = AgentObservable::new();
        o.record_run("generator", RunOutcome::Delivered, 3, 7, 2);

        let (runs, successes, exhausted) = readings(&o).await;
        assert_eq!(runs, 1.0);
        assert_eq!(successes, 1.0);
        assert_eq!(exhausted, 0.0);
    }

    /// 没有任何观测就该原样用调用方给的种子：凭一个没看见过的事实改数字，
    /// 只是把猜数换了个地方猜。
    #[test]
    fn a_role_with_no_completed_run_keeps_the_seed_budget() {
        let o = AgentObservable::new();
        assert_eq!(o.iteration_budget_for("generator", 10), 10);
        // 只有被截断的运行也不是"跑完过"的证据：它没交卷，也就没说需要多少轮。
        o.record_run("generator", RunOutcome::BudgetExhausted, 10, 21, 12);
        assert_eq!(o.iteration_budget_for("generator", 10), 10);
    }

    /// 上限必须容得下这个角色**已经交付过**的轮数，否则它不是安全线而是缺陷：
    /// 一个已知形状的运行会被砍在半路，而且砍掉是全额的——什么都没产出，
    /// 花掉的却全花了。
    #[test]
    fn the_ceiling_admits_the_longest_run_this_role_has_delivered() {
        let o = AgentObservable::new();
        for iterations in [6, 8] {
            o.record_run("generator", RunOutcome::Delivered, iterations, 20, 5);
        }
        let budget = o.iteration_budget_for("generator", 10);
        assert!(
            budget > 8,
            "the ceiling must clear the longest completed run: {budget}"
        );
        assert!(
            budget > 10,
            "two delivered runs of 6 and 8 say the seed was binding, not that 10 is right: {budget}"
        );
    }

    /// 读数按角色分开：另外一条角色的运行说不了这个角色需要多少轮。
    #[test]
    fn one_roles_runs_do_not_set_another_roles_ceiling() {
        let o = AgentObservable::new();
        for _ in 0..4 {
            o.record_run("generator", RunOutcome::Delivered, 20, 40, 8);
        }
        assert_eq!(o.iteration_budget_for("evaluator", 5), 5);
        assert!(o.iteration_budget_for("generator", 10) > 10);
    }

    /// 窗口是有界的：上限只跟着最近一轮的样本走，不会把整个历史累加进内存，
    /// 也不会被很久以前的运行永远按住。
    #[test]
    fn the_sample_window_stays_bounded_and_recent() {
        let o = AgentObservable::new();
        for _ in 0..(BUDGET_SAMPLE_WINDOW + 10) {
            o.record_run("generator", RunOutcome::Delivered, 40, 80, 8);
        }
        // 40 * (window + 10) 如果全留下会把均值抬到 40，上限就成 80 了。
        assert_eq!(
            o.iteration_budget_for("generator", 1),
            40 + 40,
            "the ceiling must be derived from the retained window, not the whole history"
        );
    }

    /// 上限只能被**交卷**的运行推高：被截断的运行没交付，它证明不了需要多少轮，
    /// 拿它去加预算等于按一个从没看见过的数字结账。
    #[test]
    fn only_completed_runs_raise_the_ceiling() {
        let o = AgentObservable::new();
        for _ in 0..50 {
            o.record_run("generator", RunOutcome::BudgetExhausted, 10, 21, 12);
        }
        assert_eq!(o.iteration_budget_for("generator", 10), 10);
    }

    /// 每个角色的截断次数要能被读到：上限是不是在绑住某条路径，是这次改动
    /// 存在的原因本身，看不见就等于没改。
    #[tokio::test]
    async fn exhausted_runs_are_readable_per_role() {
        let o = AgentObservable::new();
        // 撞墙记在判定的那一刻，终局记在 run 结束时——按真实顺序两件都走一遍。
        for _ in 0..2 {
            o.note_budget_exhausted("generator");
            o.record_run("generator", RunOutcome::BudgetExhausted, 10, 21, 12);
        }
        o.record_run("evaluator", RunOutcome::Delivered, 3, 7, 2);

        let metrics = o.collect_metrics("D1").await.expect("collect_metrics");
        let exhausted = |role: &str| {
            metrics
                .iter()
                .find(|m| {
                    m.name == "agent_iteration_budget_exhausted"
                        && m.labels.get("role").map(String::as_str) == Some(role)
                })
                .map(|m| m.value)
                .unwrap_or(-1.0)
        };
        assert_eq!(exhausted("generator"), 2.0);
        assert_eq!(
            exhausted("evaluator"),
            0.0,
            "a role that never exhausted must read zero, not be missing"
        );
    }

    /// 「跑到顶且没交卷」与「跑到顶、被强制一问救回来」是两种结局，在
    /// `exhausted_runs` 那条总线上读不出差别。这条钉住只数前者的那条序列：
    /// 总额把两者都算上，无交付那条只算真买了空的一个。
    #[tokio::test]
    async fn a_no_deliverable_ending_is_counted_apart_from_a_salvaged_one() {
        let o = AgentObservable::new();
        for _ in 0..2 {
            o.note_budget_exhausted("generator");
            o.record_run("generator", RunOutcome::BudgetExhausted, 10, 21, 12);
        }
        o.note_budget_exhausted("generator");
        o.record_run(
            "generator",
            RunOutcome::DeliveredAfterExhaustion,
            10,
            21,
            12,
        );

        let metrics = o.collect_metrics("D1").await.expect("collect_metrics");
        let read = |name: &str| {
            metrics
                .iter()
                .find(|m| {
                    m.name == name && m.labels.get("role").map(String::as_str) == Some("generator")
                })
                .map(|m| m.value)
                .unwrap_or(-1.0)
        };
        assert_eq!(read("agent_iteration_budget_exhausted"), 3.0);
        assert_eq!(
            read("agent_iteration_budget_exhausted_no_deliverable"),
            2.0,
            "the salvaged run belongs on the total but not on the no-deliverable series"
        );
    }

    /// 这一组就是这个 observable 的全部产出。多一个名字就多一条没人读的序列，
    /// 少一个名字就是一条静默消失的读数，两种都要在这里响。
    #[tokio::test]
    async fn the_published_set_is_exactly_the_cumulative_counters() {
        let o = AgentObservable::new();
        o.record_run("generator", RunOutcome::Delivered, 3, 7, 2);
        o.record_run("evaluator", RunOutcome::BudgetExhausted, 10, 21, 12);

        let mut names: Vec<String> = o
            .collect_metrics("")
            .await
            .expect("collect_metrics")
            .into_iter()
            .map(|m| m.name)
            .collect();
        names.sort();
        names.dedup();
        assert_eq!(
            names,
            vec![
                "agent_budget_exhausted_count",
                "agent_iteration_budget_exhausted",
                "agent_iteration_budget_exhausted_no_deliverable",
                "agent_run_count",
                "agent_success_count",
                "agent_total_steps",
                "agent_total_tool_calls",
            ]
        );
    }

    /// 声明为空集不是遗漏：它让采集侧只问一次。一旦重新声明某个维度，采集侧
    /// 会按维度把同一份不随维度变的读数问回来，Prometheus 会把重复样本丢掉。
    #[test]
    fn no_dimension_is_declared_because_the_readings_do_not_vary_by_one() {
        assert!(AgentObservable::new().available_dimensions().is_empty());
    }
}
