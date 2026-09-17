//! Observable implementation for cog-collaboration.
//! Exposes D8 (Multi-Agent Collaboration) raw metrics.

use async_trait::async_trait;
use cog_core::observability::{Observable, RawMetric, TraceFragment};
use cog_core::SFResult;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

static GLOBAL: OnceLock<Arc<CollaborationObservable>> = OnceLock::new();

pub fn global_observable() -> Arc<CollaborationObservable> {
    GLOBAL
        .get_or_init(|| Arc::new(CollaborationObservable::new()))
        .clone()
}

use tokio::sync::Mutex;

/// Collaboration-layer observable state.
#[derive(Default)]
pub struct CollaborationObservable {
    agent_message_count: AtomicU64,
    agent_turnaround_ms: Arc<Mutex<HashMap<String, Vec<u64>>>>,
    round_count: AtomicU64,
    /// Ralph Loop 终止计数，按终止原因分类（stagnated / budget_exhausted）。
    /// 不收敛链被有界止损是核心健康信号，必须可观测。
    ralph_terminations: Arc<Mutex<HashMap<String, u64>>>,
    /// 自进化任务的结果计数，按结局分类（submitted / no_artifacts /
    /// submit_failed / no_sink）。一个跑完却没有产出变更的自进化任务在
    /// 此之前与成功完全无法区分：squad 报 success、输出 JSON 里只是没有
    /// change_ids，既没有日志也没有指标，于是"生成侧不出货"能沉默地持续
    /// 下去。落地通道有没有货必须可数。
    change_yields: Arc<Mutex<HashMap<String, u64>>>,
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
        if let Ok(mut map) = self.ralph_terminations.try_lock() {
            *map.entry(reason.to_string()).or_insert(0) += 1;
        }
    }

    pub fn record_change_yield(&self, outcome: &str) {
        if let Ok(mut map) = self.change_yields.try_lock() {
            *map.entry(outcome.to_string()).or_insert(0) += 1;
        }
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
            let terminations = self.ralph_terminations.lock().await;
            for (reason, count) in terminations.iter() {
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
        }
        Ok(metrics)
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    fn available_dimensions(&self) -> Vec<String> {
        vec!["D8".into()]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::observability::Observable;

    fn metric(metrics: &[RawMetric], name: &str) -> Option<RawMetric> {
        metrics.iter().find(|m| m.name == name).cloned()
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
}
