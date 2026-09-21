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

use cog_core::{DecisionGroupKey, ModeRecommendation, TaskFeatures};

/// 决策聚合的耐久快照。
///
/// `decision_stats` 是进程内的聚合，重启即空；而推荐路径与调参驱动都读它，
/// 于是「攒够证据才能调参」这件事会被每次重启清零——在每次换版都重建 Pod 的
/// 部署上，`min_trials` 永远攒不满，参数搜索结构性地跑不起来。
///
/// 快照把这聚合落在本进程自己的数据卷上：键空间有界（每个决策类别只有少数
/// 几个分组，分组判据是 [`DecisionGroupKey`]，值域由任务类别组成）、体积小、
/// 只被本进程读写，因此既不需要共享存储，也不依赖 memory 后端。写走
/// 「临时文件 + rename」，写到一半被杀不会留下半个 JSON。
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
    ) -> SFResult<Option<HashMap<(DecisionCategory, DecisionGroupKey), DecisionStatistics>>> {
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
        stats: &HashMap<(DecisionCategory, DecisionGroupKey), DecisionStatistics>,
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

/// 快照的文件形态。`decision_stats` 的键是 `(DecisionCategory, DecisionGroupKey)`，
/// 而 JSON 的对象键只能是字符串，所以分组落成数组。
#[derive(Debug, Serialize, Deserialize)]
struct SnapshotFile {
    version: u32,
    groups: Vec<SnapshotGroup>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SnapshotGroup {
    category: DecisionCategory,
    key: DecisionGroupKey,
    counts: HashMap<String, (u32, u32)>,
}

impl SnapshotFile {
    const VERSION: u32 = 1;

    fn from_map(stats: &HashMap<(DecisionCategory, DecisionGroupKey), DecisionStatistics>) -> Self {
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
                .as_str()
                .cmp(b.key.as_str())
                .then_with(|| format!("{:?}", a.category).cmp(&format!("{:?}", b.category)))
        });
        Self {
            version: Self::VERSION,
            groups,
        }
    }

    fn into_map(self) -> HashMap<(DecisionCategory, DecisionGroupKey), DecisionStatistics> {
        self.groups
            .into_iter()
            .map(|g| ((g.category, g.key), DecisionStatistics { counts: g.counts }))
            .collect()
    }
}

/// Lightweight meta-learning engine that tracks per-category success rates
/// for arbitrary decisions and recommends the better-performing option.
pub struct MetaLearningEngine {
    /// Generic decision statistics keyed by (category, group).
    decision_stats: Arc<RwLock<HashMap<(DecisionCategory, DecisionGroupKey), DecisionStatistics>>>,
    recorder: Arc<dyn LearningRecorder>,
    /// Minimum samples per decision before making a recommendation.
    min_samples: u32,
    /// Success-rate margin required to prefer one decision over another.
    margin: f64,
    /// 产物级进化策略源（§14.3 热替换）：配置后推荐参数以策略产物 active
    /// 版本为准，self.min_samples/self.margin 仅作兜底。
    policy: Option<(crate::PolicyStore, String)>,
    /// 决策聚合的耐久快照：每次记录落盘、构造时读回。None 只出现在测试构造
    /// 的引擎上——非测试构建里没有构造无快照引擎的入口。
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
    fn base(recorder: Arc<dyn LearningRecorder>) -> Self {
        Self {
            decision_stats: Arc::new(RwLock::new(HashMap::new())),
            recorder,
            min_samples: 3,
            margin: 0.15,
            policy: None,
            snapshot: None,
        }
    }

    /// Engine without durable state: everything it learns is gone when the
    /// process is. Test-only on purpose — a deployment's engine carries a
    /// snapshot, and keeping this out of non-test builds means "built an engine
    /// that nothing can restore into" is not something the code can express.
    #[cfg(test)]
    pub(crate) fn new_ephemeral(recorder: Arc<dyn LearningRecorder>) -> Self {
        Self::base(recorder)
    }

    /// The single construction path for a deployment: attach the durable
    /// snapshot and read it back immediately, reporting what came back.
    ///
    /// Attaching and reading are one step on purpose. Split in two, "attached
    /// the snapshot but forgot to read it" has no observation surface at all:
    /// the symptom is evidence reset on restart, and that looks exactly like
    /// "nothing had been accumulated yet" except for one number, which at that
    /// moment nobody is printing.
    pub async fn with_durable_state(
        recorder: Arc<dyn LearningRecorder>,
        snapshot: DecisionStatsSnapshot,
    ) -> Self {
        let mut engine = Self::base(recorder);
        engine.snapshot = Some(snapshot);
        engine.hydrate().await;
        engine
    }

    /// 启动时把上次的聚合读回来，并把读数打出来。
    /// 「重启之后还剩多少证据」是这条链唯一的带内读数，所以它不可能是一次
    /// 静默的内部动作；读不出来（文件损坏、权限不对）只告警、不失败——一份
    /// 坏快照不该拦住启动，但它必须留下痕迹，否则"从空开始"会被读成"本来
    /// 就是空的"。
    async fn hydrate(&self) {
        let Some(snapshot) = &self.snapshot else {
            return;
        };
        let path = snapshot.path().display().to_string();
        match snapshot.load().await {
            Ok(Some(restored)) => {
                let trials: u64 = restored
                    .values()
                    .flat_map(|s| s.counts.values())
                    .map(|(attempts, _)| u64::from(*attempts))
                    .sum();
                let groups = restored.len();
                *self.decision_stats.write().await = restored;
                if groups == 0 {
                    info!(
                        path = %path,
                        "meta-learning state: nothing to restore (first start, or no decision recorded yet)"
                    );
                } else {
                    info!(
                        path = %path,
                        groups,
                        trials,
                        "meta-learning state restored from the durable snapshot"
                    );
                }
            }
            Ok(None) => info!(
                path = %path,
                "meta-learning state: nothing to restore (no snapshot yet)"
            ),
            Err(e) => warn!(
                path = %path,
                error = %e,
                "meta-learning state snapshot unreadable; starting from empty"
            ),
        }
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

    // ========================================================================
    // Generic API
    // ========================================================================

    /// Record the outcome of a generic decision so the model can learn.
    pub async fn record(
        &self,
        category: DecisionCategory,
        group: &DecisionGroupKey,
        decision: &str,
        outcome: DecisionOutcome,
    ) -> SFResult<()> {
        {
            let mut guard = self.decision_stats.write().await;
            let stats = guard.entry((category, group.clone())).or_default();
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
            format!("Decision {:?} for {}", category, group),
            serde_json::json!({
                "category": category,
                "key": group.as_str(),
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
            key = %group,
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
        group: &DecisionGroupKey,
    ) -> Option<String> {
        let guard = self.decision_stats.read().await;
        let stats = guard
            .get(&(category, group.clone()))
            .cloned()
            .unwrap_or_default();
        drop(guard);

        if stats.counts.is_empty() {
            debug!(
                category = ?category,
                key = %group,
                "Meta-learning cold start — no data"
            );
            return None;
        }

        let (min_samples, margin) = self.tuned_params().await;
        let decision = select_decision(&stats.counts, min_samples, margin);
        if decision.is_none() {
            debug!(
                category = ?category,
                key = %group,
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

    /// Recommend a mode for the given decision group.
    ///
    /// This is the generic rule applied to `PgeMode`, and nothing else: keeping
    /// a second statistics table with its own copy of the rule for this one
    /// category meant two definitions of the same decision that could drift
    /// apart, one of which also had no durable copy.
    pub async fn recommend_mode(&self, group: &DecisionGroupKey) -> ModeRecommendation {
        match self.recommend(DecisionCategory::PgeMode, group).await {
            Some(ref d) if d.eq_ignore_ascii_case("pipeline") => ModeRecommendation::Pipeline,
            Some(ref d) if d.eq_ignore_ascii_case("roundtable") => ModeRecommendation::Roundtable,
            _ => ModeRecommendation::UseDefault,
        }
    }

    /// Record the actual outcome of a mode decision so the model can learn.
    /// `features` are the recorded context of the decision, not its grouping.
    pub async fn record_outcome(
        &self,
        group: &DecisionGroupKey,
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
        self.record(DecisionCategory::PgeMode, group, selected_mode, outcome)
            .await?;

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
            format!("Mode decision {}", group),
            serde_json::to_string(&record).unwrap_or_default(),
            "Track mode decision outcomes",
            cog_core::LearningSource::SelfReview,
        );
        self.recorder.record_learning(learning).await?;

        info!(
            key = %group,
            mode = %selected_mode,
            success = success,
            "Recorded mode decision outcome"
        );
        Ok(())
    }
}

#[async_trait::async_trait]
impl cog_core::MetaLearning for MetaLearningEngine {
    async fn recommend_mode(&self, group: &DecisionGroupKey) -> ModeRecommendation {
        MetaLearningEngine::recommend_mode(self, group).await
    }

    async fn record_outcome(
        &self,
        group: &DecisionGroupKey,
        features: &TaskFeatures,
        selected_mode: &str,
        success: bool,
        score: f32,
        latency_ms: u64,
    ) -> SFResult<()> {
        MetaLearningEngine::record_outcome(
            self,
            group,
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
        group: &DecisionGroupKey,
    ) -> Option<String> {
        MetaLearningEngine::recommend(self, category, group)
            .await
            .map(|s| s.to_string())
    }

    async fn record(
        &self,
        category: DecisionCategory,
        group: &DecisionGroupKey,
        decision: &str,
        outcome: DecisionOutcome,
    ) -> SFResult<()> {
        MetaLearningEngine::record(self, category, group, decision, outcome).await
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
///
/// When two arms both clear the floor and neither leads by the margin, the
/// rule does not abstain: it spends the decision on the arm with the fewest
/// observations. Abstaining hands the choice back to the caller's default, and
/// the default is what produced the counts in the first place — so a mode that
/// the evidence favours but that has only been tried a handful of times can
/// never be tried enough to become the leader, and the comparison stays
/// undecided forever, whatever the threshold. Feeding the thin arm is what
/// lets the margin test reach a verdict at all, and it costs nothing while a
/// leader is decisive: an arm that leads by the margin is still the one chosen.
/// A tie on observations means no arm is thin, so there is nothing to feed and
/// the answer stays `None`.
///
/// The arm fed is the thinnest, not the best-rated among the thin ones: the
/// floor above already says a rate computed from too few trials is not worth
/// acting on, and trusting that rate here would contradict it at exactly the
/// moment it is least reliable.
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

    // Nothing has cleared the floor, so there is no comparison to explore
    // from — the caller's default stands.
    let (best, best_rate, _) = eligible.first()?;
    let leads = match eligible.get(1) {
        Some((_, runner_up_rate, _)) => *best_rate > runner_up_rate + margin,
        // A sole eligible arm has no competitor to be measured against.
        None => true,
    };
    if leads {
        return Some(best.to_string());
    }
    least_observed(counts)
}

/// The decision observed the fewest times, or `None` when the counts tie.
///
/// Ties are broken by name so the choice never depends on hash order, the same
/// way the margin comparison does not.
fn least_observed(counts: &HashMap<String, (u32, u32)>) -> Option<String> {
    let min = counts.values().map(|(attempts, _)| *attempts).min()?;
    let max = counts.values().map(|(attempts, _)| *attempts).max()?;
    if min == max {
        return None;
    }
    counts
        .iter()
        .filter(|(_, (attempts, _))| *attempts == min)
        .map(|(decision, _)| decision.clone())
        .min()
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

    /// 两个模式都没跑出差距时，规则必须去喂样本少的那一个。若在这里弃权，
    /// 选择权落回调用方的默认模式——而计数正是默认模式跑出来的，于是"证据
    /// 偏向但很少被试"的模式永远攒不够样本翻盘，比较永远停在未决。
    #[test]
    fn an_undecided_rule_explores_the_least_observed_arm() {
        // 实测分布：默认模式被跑了 91 次、挑战者只有 3 次，两者都是 0 成功。
        let c = counts(&[("pipeline", 91, 0), ("roundtable", 3, 0)]);
        assert_eq!(
            select_decision(&c, 3, 0.15).as_deref(),
            Some("roundtable"),
            "the arm starving for trials is the one the decision must feed"
        );
    }

    /// 探索是有界的：两边样本齐平就没有"薄"的一边可喂，答案回到弃权，默认
    /// 模式接管。否则这条规则会一直摆动而不是收敛。
    #[test]
    fn exploration_stops_once_the_arms_are_equally_observed() {
        let c = counts(&[("pipeline", 91, 0), ("roundtable", 91, 0)]);
        assert_eq!(select_decision(&c, 3, 0.15), None);
    }

    /// 探索只在未决时发生：一边已经按 margin 领先，就不该为了凑样本把决策
    /// 让给明显更差的那一边。
    #[test]
    fn a_clear_leader_is_never_traded_away_for_samples() {
        let c = counts(&[("pipeline", 91, 50), ("roundtable", 3, 0)]);
        assert_eq!(select_decision(&c, 3, 0.15).as_deref(), Some("pipeline"));
    }

    /// 探索的取值也不依赖哈希序：样本数最少的若不止一个，按名字定，像
    /// margin 比较那样给出唯一答案。
    #[test]
    fn exploration_picks_deterministically_among_equally_thin_arms() {
        let c = counts(&[("pipeline", 20, 0), ("hybrid", 4, 0), ("roundtable", 4, 0)]);
        assert_eq!(select_decision(&c, 3, 0.15).as_deref(), Some("hybrid"));
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

    fn features(task_type: &str) -> TaskFeatures {
        TaskFeatures {
            task_type: task_type.to_string(),
            domain_tags: Vec::new(),
            estimated_complexity: 0.5,
            has_external_dependencies: false,
            historical_success_rate: 0.5,
            required_skills: Vec::new(),
        }
    }

    fn group(task_type: &str) -> DecisionGroupKey {
        DecisionGroupKey::by_task_type(task_type)
    }

    async fn engine_with_snapshot(path: &std::path::Path) -> MetaLearningEngine {
        let recorder: Arc<dyn LearningRecorder> = Arc::new(crate::InMemoryRecorder::new());
        MetaLearningEngine::with_durable_state(recorder, DecisionStatsSnapshot::new(path)).await
    }

    #[tokio::test]
    async fn recording_an_outcome_lands_in_the_group_the_reader_looks_up() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta_learning/decision_stats.json");
        let recorder = Arc::new(crate::InMemoryRecorder::new());
        let engine = MetaLearningEngine::with_durable_state(
            recorder.clone(),
            DecisionStatsSnapshot::new(&path),
        )
        .await;

        let g = group("squad");
        for _ in 0..3 {
            engine
                .record_outcome(&g, &features("squad"), "roundtable", true, 1.0, 5)
                .await
                .unwrap();
        }

        // What the write side recorded is what the read side finds: both went
        // through the same group value, not through two matching derivations.
        assert_eq!(
            engine.recommend_mode(&g).await,
            ModeRecommendation::Roundtable
        );
        assert_eq!(
            engine.recommend_mode(&group("other")).await,
            ModeRecommendation::UseDefault
        );

        // The decision is also a learning record: the outcome plus the context
        // of the decision, which is what the offline reading consumes.
        let learnings = recorder.list_learnings(None).await.unwrap();
        let decision = learnings
            .iter()
            .find(|l| l.summary == "Mode decision squad")
            .expect("the outcome must be recorded under the group it was taken in");
        assert!(decision
            .details
            .contains("\"selected_mode\":\"roundtable\""));
        assert!(decision.details.contains("\"task_type\":\"squad\""));
    }

    #[tokio::test]
    async fn hydration_restores_what_the_previous_process_decided() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta_learning/decision_stats.json");
        let g = group("squad");

        // First process: accumulate evidence, then go away.
        let first = engine_with_snapshot(&path).await;
        for _ in 0..6 {
            first
                .record(
                    DecisionCategory::PgeMode,
                    &g,
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
                    &g,
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

        // Second process: construction *is* the read-back, so a fresh engine
        // over the same path already holds the evidence — nothing to remember
        // to call afterwards.
        let second = engine_with_snapshot(&path).await;
        let restored = second.decision_groups().await;
        assert_eq!(restored, before);
        let trials: u32 = restored[0].values().map(|(attempts, _)| attempts).sum();
        assert_eq!(trials, 12);

        // The restored aggregate must drive the same recommendation the live
        // one did, otherwise "we kept the evidence" is not the claim.
        assert_eq!(
            second.recommend_mode(&g).await,
            ModeRecommendation::Pipeline
        );
    }

    #[tokio::test]
    async fn a_fresh_engine_starts_empty_when_there_is_nothing_to_restore() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta_learning/decision_stats.json");

        let absent = engine_with_snapshot(&path).await;
        assert!(absent.decision_groups().await.is_empty());

        // An engine with no snapshot at all is the same reading, not an error,
        // and it never touches the filesystem.
        let recorder: Arc<dyn LearningRecorder> = Arc::new(crate::InMemoryRecorder::new());
        let ephemeral = MetaLearningEngine::new_ephemeral(recorder);
        assert!(ephemeral.decision_groups().await.is_empty());
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn a_corrupt_snapshot_starts_empty_without_being_overwritten() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta_learning/decision_stats.json");
        tokio::fs::create_dir_all(path.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&path, b"{not json").await.unwrap();

        let engine = engine_with_snapshot(&path).await;
        assert!(engine.decision_groups().await.is_empty());
        // The unreadable file stays where it is: a boot that silently rewrote
        // it would destroy the only evidence of what went wrong.
        assert_eq!(tokio::fs::read(&path).await.unwrap(), b"{not json");
    }

    #[tokio::test]
    async fn snapshot_write_is_atomic_and_leaves_no_temp_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta_learning/decision_stats.json");
        let engine = engine_with_snapshot(&path).await;
        engine
            .record(
                DecisionCategory::PgeMode,
                &group("squad"),
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

        let engine = engine_with_snapshot(&path).await;
        engine
            .record(
                DecisionCategory::PgeMode,
                &group("squad"),
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
        let g = group("squad");
        let engine = engine_with_snapshot(&path).await;
        engine
            .record(
                DecisionCategory::PgeMode,
                &g,
                "pipeline",
                DecisionOutcome::Success,
            )
            .await
            .unwrap();
        engine
            .record(
                DecisionCategory::PgeMode,
                &g,
                "pipeline",
                DecisionOutcome::Success,
            )
            .await
            .unwrap();

        // Reloading must not double-count: the file is state, not a log.
        let reloaded = engine_with_snapshot(&path).await;
        let groups = reloaded.decision_groups().await;
        assert_eq!(groups.len(), 1);
        assert_eq!(
            groups[0]
                .values()
                .map(|(attempts, _)| attempts)
                .sum::<u32>(),
            2
        );
    }
}
