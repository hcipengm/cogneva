//! Observable implementation for cog-agent.
//! Exposes D1 (Outcome), D2 (Planning), and D3 (Tool Use) raw metrics.

use async_trait::async_trait;
use cog_core::observability::{Observable, RawMetric, TraceFragment};
use cog_core::SFResult;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use tokio::sync::Mutex;

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
    step_records: Arc<Mutex<Vec<StepRecord>>>,
    run_count: AtomicU64,
    success_count: AtomicU64,
    total_steps: AtomicU64,
    total_tool_calls: AtomicU64,
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
    pub fn record_run(&self, success: bool, steps: usize, tool_calls: usize) {
        self.run_count.fetch_add(1, Ordering::Relaxed);
        if success {
            self.success_count.fetch_add(1, Ordering::Relaxed);
        }
        self.total_steps.fetch_add(steps as u64, Ordering::Relaxed);
        self.total_tool_calls
            .fetch_add(tool_calls as u64, Ordering::Relaxed);
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
            "agent_total_steps",
            self.total_steps.load(Ordering::Relaxed) as f64,
        ));
        metrics.push(RawMetric::new(
            "agent_total_tool_calls",
            self.total_tool_calls.load(Ordering::Relaxed) as f64,
        ));

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
