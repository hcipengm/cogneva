//! Meta-learning engine for predictive decision recommendations.
//! Generalised to track per-category success rates for arbitrary decisions
//! (PGE mode, reset strategy, retry policy, self-review threshold, etc.)
//! and recommend the better-performing option.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::types::{DecisionStatistics, ModeDecisionRecord};
use crate::LearningRecorder;
use crate::{DecisionCategory, DecisionOutcome};
use cog_core::{SFError, SFResult};

use cog_core::{ModeRecommendation, TaskFeatures};

/// 决策聚合的耐久快照。
///
/// `decision_stats` 是进程内的聚合，重启即空；而推荐路径与调参驱动都读它，
/// 于是「攒够证据才能调参」这件事会被每次重启清零——在每次换版都重建 Pod 的
/// 部署上，`min_trials` 永远攒不满，参数搜索结构性地跑不起来。
///
/// 快照把这聚合落在本进程自己的数据卷上：键空间有界（每个决策类别只有少数
/// 几个分组，键取自不随对象变化的字段）、体积小、只被本进程读写，因此既不
/// 需要共享存储，也不依赖 memory 后端。写走「临时文件 + rename」，写到一半
/// 被杀不会留下半个 JSON。
#[derive(Debug, Clone)]
pub struct DecisionStatsSnapshot {
    path: PathBuf,
}

impl DecisionStatsSnapshot {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &std::path::Path {
        &self.path
    }

    /// 读回上次的快照。文件不存在（首次启动）返回 `Ok(None)`；读不出来或
    /// 解析不了返回 `Err`——调用方据此告警并从空开始，而不是把启动卡死。
    pub async fn load(
        &self,
    ) -> SFResult<Option<HashMap<(DecisionCategory, String), DecisionStatistics>>> {
        let text = match tokio::fs::read_to_string(&self.path).await {
            Ok(t) => t,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(SFError::IO(format!("read {}: {}", self.path.display(), e))),
        };
        let file: SnapshotFile = serde_json::from_str(&text).map_err(SFError::Serialization)?;
        Ok(Some(file.into_map()))
    }

    /// 覆盖写整份快照。
    pub async fn save(
        &self,
        stats: &HashMap<(DecisionCategory, String), DecisionStatistics>,
    ) -> SFResult<()> {
        if let Some(parent) = self.path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| SFError::IO(format!("create {}: {}", parent.display(), e)))?;
        }
        let body = serde_json::to_string(&SnapshotFile::from_map(stats))
            .map_err(SFError::Serialization)?;
        let tmp = self.path.with_extension("json.tmp");
        tokio::fs::write(&tmp, body.as_bytes())
            .await
            .map_err(|e| SFError::IO(format!("write {}: {}", tmp.display(), e)))?;
        tokio::fs::rename(&tmp, &self.path)
            .await
            .map_err(|e| SFError::IO(format!("rename to {}: {}", self.path.display(), e)))?;
        Ok(())
    }
}

/// 快照的文件形态。`decision_stats` 的键是 `(DecisionCategory, String)`，
/// 而 JSON 的对象键只能是字符串，所以分组落成数组。
#[derive(Debug, Serialize, Deserialize)]
struct SnapshotFile {
    version: u32,
    groups: Vec<SnapshotGroup>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SnapshotGroup {
    category: DecisionCategory,
    key: String,
    counts: HashMap<String, (u32, u32)>,
}

impl SnapshotFile {
    const VERSION: u32 = 1;

    fn from_map(stats: &HashMap<(DecisionCategory, String), DecisionStatistics>) -> Self {
        let mut groups: Vec<SnapshotGroup> = stats
            .iter()
            .map(|((category, key), s)| SnapshotGroup {
                category: *category,
                key: key.clone(),
                counts: s.counts.clone(),
            })
            .collect();
        // 同一份状态必须写出同一个文件：顺序随哈希序变，排障时会把
        // "什么都没变"读成"状态变了"。
        groups.sort_by(|a, b| {
            a.key
                .cmp(&b.key)
                .then_with(|| format!("{:?}", a.category).cmp(&format!("{:?}", b.category)))
        });
        Self {
            version: Self::VERSION,
            groups,
        }
    }

    fn into_map(self) -> HashMap<(DecisionCategory, String), DecisionStatistics> {
        self.groups
            .into_iter()
            .map(|g| ((g.category, g.key), DecisionStatistics { counts: g.counts }))
            .collect()
    }
}

/// Lightweight meta-learning engine that tracks per-category success rates
/// for arbitrary decisions and recommends the better-performing option.
pub struct MetaLearningEngine {
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
    /// 决策聚合的耐久快照：配置后每次记录落盘、启动时读回。不配则与从前
    /// 一样只在进程内攒。
    snapshot: Option<DecisionStatsSnapshot>,
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
            decision_stats: Arc::new(RwLock::new(HashMap::new())),
            recorder,
            min_samples: 3,
            margin: 0.15,
            policy: None,
            snapshot: None,
        }
    }

    /// 接入耐久快照：重启后聚合能被读回来，而不是从零开始攒证据。
    pub fn with_state_snapshot(mut self, snapshot: DecisionStatsSnapshot) -> Self {
        self.snapshot = Some(snapshot);
        self
    }

    /// 启动时把上次的聚合读回来，返回（恢复的分组数, 恢复的试次数）。
    /// 调用方必须把这行读出来——「重启之后还剩多少证据」是这条链唯一的带内读数。
    pub async fn hydrate(&self) -> SFResult<(usize, u64)> {
        let Some(snapshot) = &self.snapshot else {
            return Ok((0, 0));
        };
        let Some(restored) = snapshot.load().await? else {
            return Ok((0, 0));
        };
        let trials: u64 = restored
            .values()
            .flat_map(|s| s.counts.values())
            .map(|(attempts, _)| u64::from(*attempts))
            .sum();
        let groups = restored.len();
        *self.decision_stats.write().await = restored;
        Ok((groups, trials))
    }

    /// 决策一记进内存就落盘。写失败只告警：快照是耐久化那一步，它失败不该
    /// 把已经记下的决策整条回滚，也不该让调用方以为决策没记上。
    async fn persist_state(&self) {
        let Some(snapshot) = &self.snapshot else {
            return;
        };
        let stats = self.decision_stats.read().await.clone();
        if let Err(e) = snapshot.save(&stats).await {
            warn!(
                path = %snapshot.path().display(),
                error = %e,
                "meta-learning state snapshot write failed"
            );
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
        self.persist_state().await;

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
    // PgeMode API
    // ========================================================================

    /// Recommend a mode based on historical data for this task category.
    ///
    /// This is the generic rule applied to `PgeMode`, and nothing else: keeping
    /// a second statistics table with its own copy of the rule for this one
    /// category meant two definitions of the same decision that could drift
    /// apart, one of which also had no durable copy.
    pub async fn recommend_mode(&self, features: &TaskFeatures) -> ModeRecommendation {
        match self.recommend(DecisionCategory::PgeMode, features).await {
            Some(ref d) if d.eq_ignore_ascii_case("pipeline") => ModeRecommendation::Pipeline,
            Some(ref d) if d.eq_ignore_ascii_case("roundtable") => ModeRecommendation::Roundtable,
            _ => ModeRecommendation::UseDefault,
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

        // Persist decision record.
        let key = Self::key(features);
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

    // ------------------------------------------------------------------
    // Durable state snapshot
    // ------------------------------------------------------------------

    fn features(task_type: &str, domain: &str) -> TaskFeatures {
        TaskFeatures {
            task_type: task_type.to_string(),
            domain_tags: vec![domain.to_string()],
            estimated_complexity: 0.5,
            has_external_dependencies: false,
            historical_success_rate: 0.5,
            required_skills: Vec::new(),
        }
    }

    fn engine_with_snapshot(path: &std::path::Path) -> MetaLearningEngine {
        let recorder: Arc<dyn LearningRecorder> = Arc::new(crate::InMemoryRecorder::new());
        MetaLearningEngine::new(recorder).with_state_snapshot(DecisionStatsSnapshot::new(path))
    }

    #[tokio::test]
    async fn hydration_restores_what_the_previous_process_decided() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta_learning/decision_stats.json");
        let f = features("squad", "self-signal");

        // First process: accumulate evidence, then go away.
        let first = engine_with_snapshot(&path);
        for _ in 0..6 {
            first
                .record(
                    DecisionCategory::PgeMode,
                    &f,
                    "pipeline",
                    DecisionOutcome::Success,
                )
                .await
                .unwrap();
        }
        for _ in 0..6 {
            first
                .record(
                    DecisionCategory::PgeMode,
                    &f,
                    "roundtable",
                    DecisionOutcome::Failed,
                )
                .await
                .unwrap();
        }
        let before = first.decision_groups().await;
        assert_eq!(before.len(), 1);
        assert_eq!(
            select_decision(&before[0], 3, 0.15).as_deref(),
            Some("pipeline")
        );

        // Second process: a fresh engine over the same path starts blind.
        let second = engine_with_snapshot(&path);
        assert!(second.decision_groups().await.is_empty());

        let (groups, trials) = second.hydrate().await.unwrap();
        assert_eq!(groups, 1);
        assert_eq!(trials, 12);

        // The restored aggregate must drive the same recommendation the live
        // one did, otherwise "we kept the evidence" is not the claim.
        let after = second.decision_groups().await;
        assert_eq!(after, before);
        assert_eq!(
            second.recommend_mode(&f).await,
            ModeRecommendation::Pipeline
        );
    }

    #[tokio::test]
    async fn hydration_is_empty_when_there_is_nothing_to_restore() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta_learning/decision_stats.json");

        let absent = engine_with_snapshot(&path);
        assert_eq!(absent.hydrate().await.unwrap(), (0, 0));
        assert!(absent.decision_groups().await.is_empty());

        // No snapshot configured at all is the same reading, not an error.
        let recorder: Arc<dyn LearningRecorder> = Arc::new(crate::InMemoryRecorder::new());
        let configured = MetaLearningEngine::new(recorder);
        assert_eq!(configured.hydrate().await.unwrap(), (0, 0));
    }

    #[tokio::test]
    async fn hydration_reports_a_corrupt_snapshot_instead_of_masking_it() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta_learning/decision_stats.json");
        tokio::fs::create_dir_all(path.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&path, b"{not json").await.unwrap();

        let engine = engine_with_snapshot(&path);
        assert!(engine.hydrate().await.is_err());
        assert!(engine.decision_groups().await.is_empty());
    }

    #[tokio::test]
    async fn snapshot_write_is_atomic_and_leaves_no_temp_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta_learning/decision_stats.json");
        let engine = engine_with_snapshot(&path);
        engine
            .record(
                DecisionCategory::PgeMode,
                &features("squad", "self-signal"),
                "pipeline",
                DecisionOutcome::Success,
            )
            .await
            .unwrap();

        assert!(path.exists());
        assert!(!path.with_extension("json.tmp").exists());
    }

    #[tokio::test]
    async fn an_unwritable_snapshot_does_not_discard_the_decision() {
        let dir = tempfile::tempdir().unwrap();
        // A regular file where the snapshot's parent directory should be:
        // create_dir_all cannot succeed, so the write path is exercised
        // without taking the process down with it.
        let blocker = dir.path().join("meta_learning");
        tokio::fs::write(&blocker, b"not a directory")
            .await
            .unwrap();
        let path = blocker.join("decision_stats.json");

        let engine = engine_with_snapshot(&path);
        engine
            .record(
                DecisionCategory::PgeMode,
                &features("squad", "self-signal"),
                "pipeline",
                DecisionOutcome::Success,
            )
            .await
            .unwrap();
        assert_eq!(engine.decision_groups().await.len(), 1);
    }

    #[tokio::test]
    async fn a_later_decision_overwrites_the_snapshot_rather_than_appending() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta_learning/decision_stats.json");
        let f = features("squad", "self-signal");
        let engine = engine_with_snapshot(&path);
        engine
            .record(
                DecisionCategory::PgeMode,
                &f,
                "pipeline",
                DecisionOutcome::Success,
            )
            .await
            .unwrap();
        engine
            .record(
                DecisionCategory::PgeMode,
                &f,
                "pipeline",
                DecisionOutcome::Success,
            )
            .await
            .unwrap();

        // Reloading twice must not double-count: the file is state, not a log.
        let reloaded = engine_with_snapshot(&path);
        assert_eq!(reloaded.hydrate().await.unwrap(), (1, 2));
    }
}
