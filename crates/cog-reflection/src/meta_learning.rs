//! Meta-learning engine for predictive decision recommendations.
//! Generalised to track per-category success rates for arbitrary decisions
//! (PGE mode, reset strategy, retry policy, self-review threshold, etc.)
//! and recommend the better-performing option.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::types::{DecisionStatistics, ModeDecisionRecord, ModeStatistics};
use crate::LearningRecorder;
use crate::{DecisionCategory, DecisionOutcome};
use cog_core::SFResult;

use cog_core::{ModeRecommendation, TaskFeatures};

/// Lightweight meta-learning engine that tracks per-category success rates
/// for arbitrary decisions and recommends the better-performing option.
pub struct MetaLearningEngine {
    /// Legacy PgeMode statistics (retained for backward compatibility).
    stats: Arc<RwLock<HashMap<String, ModeStatistics>>>,
    /// Generic decision statistics keyed by (category, feature_key).
    decision_stats: Arc<RwLock<HashMap<(DecisionCategory, String), DecisionStatistics>>>,
    recorder: Arc<dyn LearningRecorder>,
    /// Minimum samples per decision before making a recommendation.
    min_samples: u32,
    /// Success-rate margin required to prefer one decision over another.
    margin: f64,
    /// 产物级进化策略源（§14.3 热替换）：配置后推荐参数以策略产物 active
    /// 版本为准，self.min_samples/self.margin 仅作兜底。
    policy: Option<(crate::PolicyStore, String)>,
}

impl std::fmt::Debug for MetaLearningEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetaLearningEngine")
            .field("min_samples", &self.min_samples)
            .field("margin", &self.margin)
            .finish()
    }
}

impl MetaLearningEngine {
    pub fn new(recorder: Arc<dyn LearningRecorder>) -> Self {
        Self {
            stats: Arc::new(RwLock::new(HashMap::new())),
            decision_stats: Arc::new(RwLock::new(HashMap::new())),
            recorder,
            min_samples: 3,
            margin: 0.15,
            policy: None,
        }
    }

    /// Configure the minimum samples required per mode before recommending.
    pub fn with_min_samples(mut self, n: u32) -> Self {
        self.min_samples = n;
        self
    }

    /// Configure the success-rate margin (default 0.15 = 15 %).
    pub fn with_margin(mut self, m: f64) -> Self {
        self.margin = m;
        self
    }

    /// 接入产物级进化策略源：推荐参数（min_samples/margin）从策略产物
    /// active 版本读取，`activate` 热替换后下一次推荐即生效。
    pub fn with_policy_store(mut self, store: crate::PolicyStore, policy_name: &str) -> Self {
        self.policy = Some((store, policy_name.to_string()));
        self
    }

    /// 有效调参：策略产物 active 版本优先，缺失字段回退到构造参数。
    /// 公开：参数搜索要拿它当基线，自己另推一份就会与推荐路径实际用的
    /// 参数悄悄分叉。
    pub async fn tuned_params(&self) -> (u32, f64) {
        if let Some((store, name)) = &self.policy {
            if let Ok(Some(artifact)) = store.load_active(name).await {
                let min_samples = artifact
                    .payload
                    .get("min_samples")
                    .and_then(|v| v.as_u64())
                    .map(|v| v as u32)
                    .unwrap_or(self.min_samples);
                let margin = artifact
                    .payload
                    .get("margin")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(self.margin);
                return (min_samples, margin);
            }
        }
        (self.min_samples, self.margin)
    }

    /// Snapshot of every observed decision group, for offline parameter
    /// search. Grouping is dropped: the tuning parameters apply to the rule
    /// itself, so the search pools the groups the rule is applied to.
    pub async fn decision_groups(&self) -> Vec<HashMap<String, (u32, u32)>> {
        self.decision_stats
            .read()
            .await
            .values()
            .map(|s| s.counts.clone())
            .collect()
    }

    /// Build a lookup key from task features.
    fn key(features: &TaskFeatures) -> String {
        // Use task_type + first domain tag as the primary key.
        // This keeps the stat space bounded while still being useful.
        let domain = features.domain_tags.first().cloned().unwrap_or_default();
        format!("{}:{}", features.task_type, domain)
    }

    // ========================================================================
    // Generic API
    // ========================================================================

    /// Record the outcome of a generic decision so the model can learn.
    pub async fn record(
        &self,
        category: DecisionCategory,
        features: &TaskFeatures,
        decision: &str,
        outcome: DecisionOutcome,
    ) -> SFResult<()> {
        let key = Self::key(features);
        {
            let mut guard = self.decision_stats.write().await;
            let stats = guard.entry((category, key.clone())).or_default();
            let entry = stats.counts.entry(decision.to_string()).or_insert((0, 0));
            entry.0 += 1;
            if outcome == DecisionOutcome::Success {
                entry.1 += 1;
            }
        }

        let learning = cog_core::Learning::new(
            cog_core::LearningCategory::Insight,
            cog_core::Priority::Medium,
            cog_core::Area::Config,
            format!("Decision {:?} for {}", category, key),
            serde_json::json!({
                "category": category,
                "key": key,
                "decision": decision,
                "outcome": outcome,
            })
            .to_string(),
            "Track decision outcomes",
            cog_core::LearningSource::SelfReview,
        );
        self.recorder.record_learning(learning).await?;

        info!(
            category = ?category,
            key = %key,
            decision = %decision,
            outcome = ?outcome,
            "Recorded generic decision outcome"
        );
        Ok(())
    }

    /// Recommend the best decision for a given category and task features.
    /// Returns `Some(decision)` when historical data strongly favours one
    /// option, or `None` when there is insufficient data or the options are
    /// within the configured margin.
    pub async fn recommend(
        &self,
        category: DecisionCategory,
        features: &TaskFeatures,
    ) -> Option<String> {
        let key = Self::key(features);
        let guard = self.decision_stats.read().await;
        let stats = guard
            .get(&(category, key.clone()))
            .cloned()
            .unwrap_or_default();
        drop(guard);

        if stats.counts.is_empty() {
            debug!(
                category = ?category,
                key = %key,
                "Meta-learning cold start — no data"
            );
            return None;
        }

        let (min_samples, margin) = self.tuned_params().await;
        let decision = select_decision(&stats.counts, min_samples, margin);
        if decision.is_none() {
            debug!(
                category = ?category,
                key = %key,
                min_samples,
                margin,
                "Meta-learning: no decision clears the configured margin"
            );
        }
        decision
    }

    // ========================================================================
    // Backward-compatible PgeMode API
    // ========================================================================

    /// Recommend a mode based on historical data for this task category.
    pub async fn recommend_mode(&self, features: &TaskFeatures) -> ModeRecommendation {
        match self.recommend(DecisionCategory::PgeMode, features).await {
            Some(ref d) if d.eq_ignore_ascii_case("pipeline") => ModeRecommendation::Pipeline,
            Some(ref d) if d.eq_ignore_ascii_case("roundtable") => ModeRecommendation::Roundtable,
            _ => {
                // Fall back to legacy stats table.
                let key = Self::key(features);
                let guard = self.stats.read().await;
                let stats = guard.get(&key).cloned().unwrap_or_default();
                drop(guard);

                let (min_samples, margin) = self.tuned_params().await;
                if stats.pipeline_attempts < min_samples && stats.roundtable_attempts < min_samples
                {
                    debug!(
                        key = %key,
                        pipeline = stats.pipeline_attempts,
                        roundtable = stats.roundtable_attempts,
                        "Meta-learning cold start — falling back to default profile"
                    );
                    return ModeRecommendation::UseDefault;
                }

                let pipeline_rate =
                    stats.pipeline_successes as f64 / stats.pipeline_attempts.max(1) as f64;
                let roundtable_rate =
                    stats.roundtable_successes as f64 / stats.roundtable_attempts.max(1) as f64;

                info!(
                    key = %key,
                    pipeline_rate = %format!("{:.2}", pipeline_rate),
                    roundtable_rate = %format!("{:.2}", roundtable_rate),
                    "Meta-learning mode recommendation (legacy path)"
                );

                if pipeline_rate > roundtable_rate + margin {
                    ModeRecommendation::Pipeline
                } else if roundtable_rate > pipeline_rate + margin {
                    ModeRecommendation::Roundtable
                } else {
                    ModeRecommendation::UseDefault
                }
            }
        }
    }

    /// Record the actual outcome of a mode decision so the model can learn.
    pub async fn record_outcome(
        &self,
        features: &TaskFeatures,
        selected_mode: &str,
        success: bool,
        score: f32,
        latency_ms: u64,
    ) -> SFResult<()> {
        let outcome = if success {
            DecisionOutcome::Success
        } else {
            DecisionOutcome::Failed
        };
        self.record(DecisionCategory::PgeMode, features, selected_mode, outcome)
            .await?;

        // Also update legacy stats table for backward compatibility.
        let key = Self::key(features);
        {
            let mut guard = self.stats.write().await;
            let stats = guard.entry(key.clone()).or_default();
            match selected_mode.to_lowercase().as_str() {
                "pipeline" => {
                    stats.pipeline_attempts += 1;
                    if success {
                        stats.pipeline_successes += 1;
                    }
                }
                "roundtable" => {
                    stats.roundtable_attempts += 1;
                    if success {
                        stats.roundtable_successes += 1;
                    }
                }
                other => {
                    warn!(mode = %other, "Unknown mode in meta-learning outcome");
                }
            }
        }

        // Persist decision record.
        let record = ModeDecisionRecord {
            task_features: features.clone(),
            selected_mode: selected_mode.to_string(),
            actual_success: success,
            actual_score: score,
            actual_latency_ms: latency_ms,
            timestamp: Utc::now(),
        };

        let learning = cog_core::Learning::new(
            cog_core::LearningCategory::Insight,
            cog_core::Priority::Medium,
            cog_core::Area::Config,
            format!("Mode decision {}", key),
            serde_json::to_string(&record).unwrap_or_default(),
            "Track mode decision outcomes",
            cog_core::LearningSource::SelfReview,
        );
        self.recorder.record_learning(learning).await?;

        info!(
            key = %key,
            mode = %selected_mode,
            success = success,
            "Recorded mode decision outcome"
        );
        Ok(())
    }

    /// Load aggregated stats from a set of persisted `ModeDecisionRecord`s.
    /// Useful for restoring state after restart.
    pub async fn load_from_records(&self, records: Vec<ModeDecisionRecord>) {
        let mut guard = self.stats.write().await;
        for r in records {
            let key = Self::key(&r.task_features);
            let stats = guard.entry(key).or_default();
            match r.selected_mode.to_lowercase().as_str() {
                "pipeline" => {
                    stats.pipeline_attempts += 1;
                    if r.actual_success {
                        stats.pipeline_successes += 1;
                    }
                }
                "roundtable" => {
                    stats.roundtable_attempts += 1;
                    if r.actual_success {
                        stats.roundtable_successes += 1;
                    }
                }
                _ => {}
            }
        }
    }
}

#[async_trait::async_trait]
impl cog_core::MetaLearning for MetaLearningEngine {
    async fn recommend_mode(&self, features: &TaskFeatures) -> ModeRecommendation {
        MetaLearningEngine::recommend_mode(self, features).await
    }

    async fn record_outcome(
        &self,
        features: &TaskFeatures,
        selected_mode: &str,
        success: bool,
        score: f32,
        latency_ms: u64,
    ) -> SFResult<()> {
        MetaLearningEngine::record_outcome(
            self,
            features,
            selected_mode,
            success,
            score,
            latency_ms,
        )
        .await
    }

    async fn recommend(
        &self,
        category: DecisionCategory,
        features: &TaskFeatures,
    ) -> Option<String> {
        MetaLearningEngine::recommend(self, category, features)
            .await
            .map(|s| s.to_string())
    }

    async fn record(
        &self,
        category: DecisionCategory,
        features: &TaskFeatures,
        decision: &str,
        outcome: DecisionOutcome,
    ) -> SFResult<()> {
        MetaLearningEngine::record(self, category, features, decision, outcome).await
    }
}

/// The decision-selection rule, as a pure function of the observations.
///
/// Shared by the live recommendation path and the offline replay that scores
/// candidate tuning parameters: a replay that models the rule differently from
/// the way the rule actually runs would compare parameter sets against a
/// system that does not exist. Iteration order is fixed (best rate first,
/// ties by decision name) so the outcome does not depend on hash ordering.
///
/// Returns `None` while no decision has been observed `min_samples` times, and
/// when the best of them does not lead the runner-up by more than `margin` —
/// a margin that never suppressed anything would make the parameter nothing
/// but a number in a file.
pub fn select_decision(
    counts: &HashMap<String, (u32, u32)>,
    min_samples: u32,
    margin: f64,
) -> Option<String> {
    let floor = min_samples.max(1);
    let mut eligible: Vec<(&String, f64, u32)> = counts
        .iter()
        .filter(|(_, (attempts, _))| *attempts >= floor)
        .map(|(decision, (attempts, successes))| {
            (decision, *successes as f64 / *attempts as f64, *attempts)
        })
        .collect();
    eligible.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(b.0))
    });

    let (best, best_rate, _) = eligible.first()?;
    if let Some((_, runner_up_rate, _)) = eligible.get(1) {
        if *best_rate <= runner_up_rate + margin {
            return None;
        }
    }
    Some(best.to_string())
}

/// Replay the recommendation path over a set of observed decision groups.
///
/// For every group the rule would have decided on, the resulting arm holds
/// that decision's observed trials. Groups the rule leaves undecided
/// contribute nothing: the live path falls back to its own default profile
/// there, and a replay has no trials for the decision it never took.
///
/// Both arms resample the same recorded trials, so they are not independent
/// while the two-proportion test assumes they are. That makes the test
/// conservative — the gate only lets a candidate through when it clears
/// significance, so a conservative test errs toward the current version.
pub fn replay_outcomes(
    groups: &[HashMap<String, (u32, u32)>],
    min_samples: u32,
    margin: f64,
) -> Vec<bool> {
    let mut outcomes = Vec::new();
    for counts in groups {
        let Some(decision) = select_decision(counts, min_samples, margin) else {
            continue;
        };
        let Some(&(attempts, successes)) = counts.get(&decision) else {
            continue;
        };
        outcomes.extend(std::iter::repeat_n(true, successes as usize));
        outcomes.extend(std::iter::repeat_n(false, (attempts - successes) as usize));
    }
    outcomes
}

#[cfg(test)]
mod tests {
    use super::*;

    fn counts(spec: &[(&str, u32, u32)]) -> HashMap<String, (u32, u32)> {
        spec.iter()
            .map(|(d, a, s)| (d.to_string(), (*a, *s)))
            .collect()
    }

    #[test]
    fn selection_is_independent_of_iteration_order() {
        // The rates differ by more than the margin, so exactly one decision
        // qualifies — which one must not depend on hash order.
        let c = counts(&[("pipeline", 10, 9), ("roundtable", 10, 1)]);
        assert_eq!(select_decision(&c, 3, 0.15).as_deref(), Some("pipeline"));
        let c = counts(&[("roundtable", 10, 9), ("pipeline", 10, 1)]);
        assert_eq!(select_decision(&c, 3, 0.15).as_deref(), Some("roundtable"));
    }

    #[test]
    fn margin_suppresses_a_close_lead() {
        let c = counts(&[("pipeline", 10, 6), ("roundtable", 10, 5)]);
        assert_eq!(select_decision(&c, 3, 0.15), None);
        assert_eq!(select_decision(&c, 3, 0.05).as_deref(), Some("pipeline"));
    }

    #[test]
    fn thin_decisions_are_ineligible() {
        let c = counts(&[("pipeline", 10, 5), ("roundtable", 2, 2)]);
        // roundtable is perfect but has not been observed min_samples times.
        assert_eq!(select_decision(&c, 3, 0.15).as_deref(), Some("pipeline"));
        // Lowering the floor alone still leaves a single eligible decision.
        assert_eq!(select_decision(&c, 2, 0.15).as_deref(), Some("roundtable"));
    }

    #[test]
    fn no_eligible_decision_yields_none() {
        let c = counts(&[("pipeline", 1, 1)]);
        assert_eq!(select_decision(&c, 3, 0.15), None);
        assert_eq!(select_decision(&HashMap::new(), 3, 0.15), None);
    }

    #[test]
    fn replay_returns_the_trials_of_the_selected_decision() {
        let groups = vec![
            counts(&[("pipeline", 4, 4), ("roundtable", 4, 0)]),
            counts(&[("pipeline", 4, 0), ("roundtable", 4, 4)]),
        ];
        let outcomes = replay_outcomes(&groups, 3, 0.15);
        assert_eq!(outcomes.len(), 8);
        assert!(outcomes.iter().all(|&o| o));
        // A margin that exceeds the lead leaves both groups undecided.
        assert!(replay_outcomes(&groups, 3, 1.5).is_empty());
    }
}
