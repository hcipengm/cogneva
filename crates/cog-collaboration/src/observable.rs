//! Observable implementation for cog-collaboration.
//! Exposes D8 (Multi-Agent Collaboration) raw metrics.

use crate::squad::classify;
use async_trait::async_trait;
use cog_core::observability::{DimensionSpec, Observable, RawMetric, TraceFragment};
use cog_core::SFResult;
use std::collections::{HashMap, HashSet};
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
}
