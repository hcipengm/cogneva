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
