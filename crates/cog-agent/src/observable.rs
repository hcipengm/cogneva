//! Observable implementation for cog-agent.
//! Exposes D1 (Outcome), D2 (Planning), and D3 (Tool Use) raw metrics.

use async_trait::async_trait;
use cog_core::observability::{Observable, RawMetric, TraceFragment};
use cog_core::SFResult;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use tokio::sync::Mutex as AsyncMutex;

static GLOBAL: OnceLock<Arc<AgentObservable>> = OnceLock::new();

pub fn global_observable() -> Arc<AgentObservable> {
    GLOBAL
        .get_or_init(|| Arc::new(AgentObservable::new()))
        .clone()
}

/// Agent-level observable state.
/// Tracks step-level execution data that can be flushed as raw metrics
/// for downstream eval consumption.
#[derive(Default)]
pub struct AgentObservable {
    step_records: Arc<AsyncMutex<Vec<StepRecord>>>,
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
    /// Iterations consumed by runs of this role that finished with an answer.
    delivered_iterations: VecDeque<u32>,
    /// Runs of this role that were cut off at the ceiling. A run that stops
    /// this way is direct evidence the ceiling was binding, which is what an
    /// operator needs to see when the derived value is not moving.
    exhausted_runs: u64,
}

/// How an agent run ended.
///
/// A run that spent its whole iteration budget still reading and testing has
/// bought nothing: it hands back a sentinel, not an answer. Counting it as a
/// success hides the ending that costs the most tokens, so the two are
/// distinguished here and counted apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunOutcome {
    /// The model stopped calling tools and produced an answer.
    Delivered,
    /// The iteration budget ran out before an answer was written.
    BudgetExhausted,
}

#[derive(Debug, Clone)]
struct StepRecord {
    task_id: String,
    step_index: usize,
    action_type: String,
    success: bool,
    duration_ms: u64,
    tool_calls: usize,
    tool_errors: usize,
}

impl AgentObservable {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a step execution internally (called by the agent runtime).
    #[allow(clippy::too_many_arguments)]
    pub async fn record_step(
        &self,
        task_id: impl Into<String>,
        step_index: usize,
        action_type: impl Into<String>,
        success: bool,
        duration_ms: u64,
        tool_calls: usize,
        tool_errors: usize,
    ) {
        self.step_records.lock().await.push(StepRecord {
            task_id: task_id.into(),
            step_index,
            action_type: action_type.into(),
            success,
            duration_ms,
            tool_calls,
            tool_errors,
        });
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
        match outcome {
            RunOutcome::Delivered => {
                self.success_count.fetch_add(1, Ordering::Relaxed);
            }
            RunOutcome::BudgetExhausted => {
                self.budget_exhausted_count.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.total_steps.fetch_add(steps as u64, Ordering::Relaxed);
        self.total_tool_calls
            .fetch_add(tool_calls as u64, Ordering::Relaxed);

        let mut calibrations = self
            .role_calibrations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let calibration = calibrations.entry(role.to_string()).or_default();
        match outcome {
            RunOutcome::Delivered => {
                if calibration.delivered_iterations.len() == BUDGET_SAMPLE_WINDOW {
                    calibration.delivered_iterations.pop_front();
                }
                calibration.delivered_iterations.push_back(iterations);
            }
            RunOutcome::BudgetExhausted => calibration.exhausted_runs += 1,
        }
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

    /// Clear records for a given task.
    pub async fn clear_task(&self, task_id: &str) {
        let mut recs = self.step_records.lock().await;
        recs.retain(|r| r.task_id != task_id);
    }
}

#[async_trait]
impl Observable for AgentObservable {
    async fn collect_metrics(&self, dimension: &str) -> SFResult<Vec<RawMetric>> {
        let recs = self.step_records.lock().await;
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
        }

        match dimension {
            "D1" => {
                for r in recs.iter() {
                    metrics.push(
                        RawMetric::new("agent_step_duration_ms", r.duration_ms as f64)
                            .with_label("task_id", &r.task_id)
                            .with_label("step_index", r.step_index.to_string()),
                    );
                    metrics.push(
                        RawMetric::new("agent_step_success", if r.success { 1.0 } else { 0.0 })
                            .with_label("task_id", &r.task_id),
                    );
                }
            }
            "D2" => {
                for r in recs.iter() {
                    metrics.push(
                        RawMetric::new("agent_plan_step_count", r.step_index as f64 + 1.0)
                            .with_label("task_id", &r.task_id),
                    );
                }
            }
            "D3" => {
                for r in recs.iter() {
                    metrics.push(
                        RawMetric::new("agent_tool_calls", r.tool_calls as f64)
                            .with_label("task_id", &r.task_id),
                    );
                    metrics.push(
                        RawMetric::new("agent_tool_errors", r.tool_errors as f64)
                            .with_label("task_id", &r.task_id),
                    );
                }
            }
            _ => {}
        }

        Ok(metrics)
    }

    async fn collect_trace(&self, task_id: &str) -> SFResult<Vec<TraceFragment>> {
        let recs = self.step_records.lock().await;
        let fragments: Vec<TraceFragment> = recs
            .iter()
            .filter(|r| r.task_id == task_id)
            .map(|r| TraceFragment {
                step_index: r.step_index,
                action_type: r.action_type.clone(),
                action_params: serde_json::Value::Null,
                thought: None,
                screenshot_hash: None,
                ui_state: None,
                tool_calls: Vec::new(),
                duration_ms: r.duration_ms,
                success: r.success,
                error: None,
            })
            .collect();
        Ok(fragments)
    }

    fn available_dimensions(&self) -> Vec<String> {
        vec!["D1".into(), "D2".into(), "D3".into()]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn readings(o: &AgentObservable) -> (f64, f64, f64) {
        let metrics = o.collect_metrics("D1").await.expect("collect_metrics");
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

    #[tokio::test]
    async fn a_run_that_spent_its_budget_is_counted_as_an_exhaustion_not_a_success() {
        let o = AgentObservable::new();
        o.record_run("generator", RunOutcome::BudgetExhausted, 10, 21, 12);

        let (runs, successes, exhausted) = readings(&o).await;
        assert_eq!(runs, 1.0);
        assert_eq!(
            successes, 0.0,
            "a run that wrote no answer must not be counted as a success"
        );
        assert_eq!(exhausted, 1.0);
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
        o.record_run("generator", RunOutcome::BudgetExhausted, 10, 21, 12);
        o.record_run("generator", RunOutcome::BudgetExhausted, 10, 21, 12);
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
}
