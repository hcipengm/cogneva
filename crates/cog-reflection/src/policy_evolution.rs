//! 产物级进化的自主触发者。
//!
//! 策略产物的写入侧原本只有两个 admin HTTP 端点，没有任何自主调用方——
//! 读侧（推荐路径每次读 active 版本）是活的，写侧只能靠人敲，于是整条
//! 产物级通道是一条半环。本模块补上写侧的自主触发者。
//!
//! 一轮做的事：把已记录的决策结果按候选参数重放一遍，用两比例 z 检验
//! 挑出显著更优的那组参数，交给 `ArtifactEvolution::evolve` 保存为新版本
//! 并热替换。重放走的是推荐路径同一个选择函数，所以候选参数是在"系统
//! 真实会怎么选"上比较的，不是在一个平行实现上比较。
//!
//! 这一轮运算本身不碰 LLM、不碰网络，但它的**输入**是 LLM 驱动的 squad
//! 执行留下的决策结果。上游断供时循环照常醒、照常报结论，只是每次都在
//! 报证据不足——"能跑"不等于"有东西可学"，别把这行 INFO 读成链在路上跑。

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use cog_core::{SFError, SFResult};

use crate::meta_learning::replay_outcomes;
use crate::policy_store::PolicyCandidate;
use crate::{ArtifactEvolution, MetaLearningEngine};

/// 自主参数搜索的配置面。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PolicyEvolutionConfig {
    /// 总开关。默认开：这条循环的输入全部是本进程已记录的结果，副作用只
    /// 落在本进程自己的策略目录里，不需要按部署指定属主。
    pub enabled: bool,
    /// 相邻两轮之间的间隔（秒），下限 60。
    pub interval_secs: u64,
    /// 演化的策略名。参数语义由该策略的读取方决定，换名字等于换策略。
    pub policy_name: String,
    /// 一条参数只有在能落到这么多条已观测结果上时才参与比较。低于这个
    /// 量，两比例检验不可能显著，硬比只会让搜索被噪声主导。
    pub min_trials: usize,
    /// `min_samples` 候选取值。
    pub min_samples_grid: Vec<u32>,
    /// `margin` 候选取值。
    pub margin_grid: Vec<f64>,
}

impl Default for PolicyEvolutionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_secs: 3600,
            policy_name: "meta_learning.mode".into(),
            min_trials: 40,
            min_samples_grid: vec![2, 3, 5, 8],
            margin_grid: vec![0.0, 0.05, 0.1, 0.15, 0.2, 0.3],
        }
    }
}

impl PolicyEvolutionConfig {
    /// 从 cogneva.json 的 `self_evolution.artifact_evolution` 段加载，再
    /// 叠加 env 覆盖。文件或段缺失时返回 Default；段存在但解析失败、或
    /// env 值非法时返回 Err——配置写错必须响亮失败。
    pub fn load() -> SFResult<Self> {
        let path = std::env::var("COGNEVA_CONFIG_PATH")
            .unwrap_or_else(|_| "/etc/cogneva/cogneva.json".into());
        let mut cfg = match std::fs::read_to_string(&path) {
            Ok(text) => {
                let root: serde_json::Value = serde_json::from_str(&text)
                    .map_err(|e| SFError::Config(format!("{path}: {e}")))?;
                match root.pointer("/self_evolution/artifact_evolution") {
                    Some(section) => serde_json::from_value(section.clone()).map_err(|e| {
                        SFError::Config(format!("{path} self_evolution.artifact_evolution: {e}"))
                    })?,
                    None => Self::default(),
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Self::default(),
            Err(e) => return Err(SFError::Config(format!("{path}: {e}"))),
        };
        if let Ok(v) = std::env::var("COGNEVA_POLICY_EVOLUTION_ENABLED") {
            cfg.enabled = matches!(v.as_str(), "1" | "true" | "yes" | "on");
        }
        if let Ok(v) = std::env::var("COGNEVA_POLICY_EVOLUTION_INTERVAL_SECS") {
            cfg.interval_secs = v.parse().map_err(|_| {
                SFError::Config(format!("COGNEVA_POLICY_EVOLUTION_INTERVAL_SECS: {v}"))
            })?;
        }
        Ok(cfg)
    }
}

/// 一轮的结论。
#[derive(Debug, Clone, PartialEq)]
pub enum PolicyEvolutionOutcome {
    /// 当前参数能落到的可比较结果太少，比较没有意义。
    InsufficientEvidence {
        /// 重放出来的规模：每组只放**被选中的那一个决策**的尝试。
        replayed_trials: usize,
        /// 耐久快照里记下的全部尝试数。与 `replayed_trials` 分开报，否则
        /// 运维面上分不出「压根没数据」和「有数据但当前参数几乎选不中」。
        recorded_trials: usize,
    },
    /// 比较过了，没有任何候选显著更优。
    NoImprovement {
        considered: usize,
        baseline_trials: usize,
        recorded_trials: usize,
    },
    /// 已保存为新版本并热替换。
    Adopted {
        version: u64,
        min_samples: u32,
        margin: f64,
        z: f64,
        uplift: f64,
    },
}

/// 自主参数搜索：读历史结果 → 重放候选 → 显著才升级。
pub struct PolicyEvolutionDriver {
    config: PolicyEvolutionConfig,
    engine: Arc<MetaLearningEngine>,
    evolution: Arc<ArtifactEvolution>,
}

impl PolicyEvolutionDriver {
    pub fn new(
        config: PolicyEvolutionConfig,
        engine: Arc<MetaLearningEngine>,
        evolution: Arc<ArtifactEvolution>,
    ) -> Self {
        Self {
            config,
            engine,
            evolution,
        }
    }

    /// 跑一轮。没有可执行的升级时也返回结论，调用方据此留痕。
    pub async fn run_once(&self) -> SFResult<PolicyEvolutionOutcome> {
        let groups = self.engine.decision_groups().await;
        let recorded_trials: usize = groups
            .iter()
            .flat_map(|g| g.values())
            .map(|(attempts, _)| *attempts as usize)
            .sum();
        // 基线就是推荐路径此刻实际会用的参数——同一份推导，不另推一遍。
        let (base_min_samples, base_margin) = self.engine.tuned_params().await;
        let baseline = replay_outcomes(&groups, base_min_samples, base_margin);
        if baseline.len() < self.config.min_trials {
            return Ok(PolicyEvolutionOutcome::InsufficientEvidence {
                replayed_trials: baseline.len(),
                recorded_trials,
            });
        }
        let s_base = baseline.iter().filter(|&&o| o).count();

        let mut best: Option<(f64, u32, f64, Vec<bool>)> = None;
        let mut considered = 0usize;
        for &min_samples in &self.config.min_samples_grid {
            for &margin in &self.config.margin_grid {
                if min_samples == base_min_samples && (margin - base_margin).abs() < f64::EPSILON {
                    continue;
                }
                let outcomes = replay_outcomes(&groups, min_samples, margin);
                if outcomes.len() < self.config.min_trials {
                    continue;
                }
                considered += 1;
                let successes = outcomes.iter().filter(|&&o| o).count();
                let (z, significant) = crate::eval_harness::two_proportion_z_test(
                    s_base,
                    baseline.len(),
                    successes,
                    outcomes.len(),
                );
                // z > 0 才是候选更好；显著更差与不显著一样不动手。
                if !significant || z <= 0.0 {
                    continue;
                }
                if best.as_ref().is_none_or(|b| z > b.0) {
                    best = Some((z, min_samples, margin, outcomes));
                }
            }
        }

        let Some((z, min_samples, margin, outcomes)) = best else {
            return Ok(PolicyEvolutionOutcome::NoImprovement {
                considered,
                baseline_trials: baseline.len(),
                recorded_trials,
            });
        };

        let uplift = outcomes.iter().filter(|&&o| o).count() as f64 / outcomes.len() as f64
            - s_base as f64 / baseline.len() as f64;
        let payload = serde_json::json!({
            "min_samples": min_samples,
            "margin": margin,
        });
        let candidate = PolicyCandidate {
            payload: payload.clone(),
            outcomes,
            reason: format!(
                "autonomous parameter search: min_samples={min_samples}, margin={margin:.2}, \
                 replayed over {} observed decision groups",
                groups.len()
            ),
        };
        let (artifact, verdict) = self
            .evolution
            .evolve(&self.config.policy_name, &baseline, &candidate)
            .await?;
        if verdict != crate::EvalVerdict::Adopt {
            // 门禁归 evolve 所有（管理端点走同一道门），所以判定以它为准。
            // 同一组切片喂进去现在必然得出同一个判定，这一支不是靠"两次调用
            // 之间基线变了"能触发的；它留着是为了 evolve 的判据将来加严时，
            // 驱动与门禁之间还有一处对账——少了它，驱动会拿一个没被采纳的
            // 候选去报 Adopted。
            warn!(
                policy = %self.config.policy_name,
                ?verdict,
                "policy evolution: candidate no longer clears the gate; leaving active version alone"
            );
            return Ok(PolicyEvolutionOutcome::NoImprovement {
                considered,
                baseline_trials: baseline.len(),
                recorded_trials,
            });
        }
        info!(
            policy = %self.config.policy_name,
            version = artifact.version,
            min_samples,
            margin,
            z = format!("{z:.2}"),
            uplift = format!("{:+.1}%", uplift * 100.0),
            "artifact-level evolution: policy version hot-swapped"
        );
        Ok(PolicyEvolutionOutcome::Adopted {
            version: artifact.version,
            min_samples,
            margin,
            z,
            uplift,
        })
    }
}

/// This loop's name in the liveness census.
pub const POLICY_EVOLUTION_LOOP: &str = "policy_evolution";

/// 周期性跑一轮。间隔下限 60s——一轮是纯本地计算，没有需要保护的外部
/// 依赖，但也不该比反思周期更密。
pub async fn run_policy_evolution_loop(
    driver: Arc<PolicyEvolutionDriver>,
    shutdown: cog_core::ShutdownSignal,
) {
    let interval = Duration::from_secs(driver.config.interval_secs.max(60));
    info!(
        interval_secs = interval.as_secs(),
        policy = %driver.config.policy_name,
        min_trials = driver.config.min_trials,
        "artifact-level evolution driver started"
    );
    let mut ticker = tokio::time::interval(interval);
    // Two of the three outcomes of a round change nothing, so the loop's own
    // output cannot say whether it is running; its liveness comes from here.
    let beat = cog_core::loop_health::register(
        POLICY_EVOLUTION_LOOP,
        cog_core::loop_health::Cadence::Periodic(interval),
    );
    let _mortality = beat.watch_death(shutdown.clone());
    loop {
        beat.beat();
        tokio::select! {
            biased;
            _ = shutdown.wait() => break,
            // 每轮结果必须落在默认可见级别。这一环的三种结论里两种都不动手，
            // 若只在 debug 留痕，"没有更优候选"与"循环根本没跑"在运维面上就是
            // 同一片空白；而 `baseline_trials` 正是"决策结果是否在积累"的唯一
            // 带内证据，代价是每小时一行。
            _ = ticker.tick() => match driver.run_once().await {
                Ok(outcome) => info!(?outcome, "artifact-level evolution round"),
                Err(e) => warn!(error = %e, "artifact-level evolution round failed"),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::{DecisionCategory, DecisionOutcome};

    /// Record outcomes through the same entry point the live system uses, so
    /// the driver is exercised against the grouping the engine really builds.
    async fn record_counts(engine: &MetaLearningEngine, counts: &[(&str, u32, u32)]) {
        let group = cog_core::DecisionGroupKey::by_task_type("squad");
        for (decision, attempts, successes) in counts {
            for i in 0..*attempts {
                engine
                    .record(
                        DecisionCategory::PgeMode,
                        &group,
                        decision,
                        if i < *successes {
                            DecisionOutcome::Success
                        } else {
                            DecisionOutcome::Failed
                        },
                    )
                    .await
                    .unwrap();
            }
        }
    }

    async fn engine_with(counts: &[(&str, u32, u32)]) -> Arc<MetaLearningEngine> {
        let engine = Arc::new(MetaLearningEngine::new_ephemeral(Arc::new(
            crate::InMemoryRecorder::new(),
        )));
        record_counts(&engine, counts).await;
        engine
    }

    /// Same, but with the engine bound to the policy store it will tune — the
    /// baseline has to be the parameters the recommendation path actually
    /// reads, not the constructor defaults it falls back to.
    async fn engine_with_store(
        counts: &[(&str, u32, u32)],
        store: crate::PolicyStore,
        policy_name: &str,
    ) -> Arc<MetaLearningEngine> {
        let engine = Arc::new(
            MetaLearningEngine::new_ephemeral(Arc::new(crate::InMemoryRecorder::new()))
                .with_policy_store(store, policy_name),
        );
        record_counts(&engine, counts).await;
        engine
    }

    #[tokio::test]
    async fn insufficient_evidence_leaves_the_store_untouched() {
        let tmp = tempfile::tempdir().unwrap();
        let evolution = Arc::new(ArtifactEvolution::new(crate::PolicyStore::new(tmp.path())));
        let d = PolicyEvolutionDriver::new(
            PolicyEvolutionConfig::default(),
            engine_with(&[]).await,
            evolution.clone(),
        );
        assert_eq!(
            d.run_once().await.unwrap(),
            PolicyEvolutionOutcome::InsufficientEvidence {
                replayed_trials: 0,
                recorded_trials: 0,
            }
        );
        assert!(
            evolution
                .store()
                .list_versions("meta_learning.mode")
                .await
                .unwrap()
                .is_empty(),
            "证据不足时不得写出任何版本"
        );
    }

    #[tokio::test]
    async fn insufficient_evidence_separates_recorded_from_replayed() {
        // 同一份计数：记下 40 次尝试，但重放每组只放被选中的那一个决策，
        // 本轮选中的 roundtable 只有 30 次。两个数都必须报出来——只报一个
        // 的话，「压根没数据」与「有数据但当前参数几乎选不中」在运维面上
        // 长得一模一样，而两者的处置完全不同。
        let tmp = tempfile::tempdir().unwrap();
        let evolution = Arc::new(ArtifactEvolution::new(crate::PolicyStore::new(tmp.path())));
        let d = PolicyEvolutionDriver::new(
            PolicyEvolutionConfig {
                min_trials: 100,
                ..PolicyEvolutionConfig::default()
            },
            engine_with(&[("pipeline", 10, 0), ("roundtable", 30, 30)]).await,
            evolution,
        );
        match d.run_once().await.unwrap() {
            PolicyEvolutionOutcome::InsufficientEvidence {
                replayed_trials,
                recorded_trials,
            } => {
                assert_eq!(replayed_trials, 30, "重放规模只含被选中的那个决策");
                assert_eq!(recorded_trials, 40, "耐久计数含全部决策");
            }
            other => panic!("expected InsufficientEvidence, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn an_already_best_baseline_is_left_alone() {
        let tmp = tempfile::tempdir().unwrap();
        // 默认参数就会选中 roundtable（1.0 对 0.0，领先远超 margin），
        // 基线臂已经是它能拿到的最好一组，任何候选都只能与它相同。
        let evolution = Arc::new(ArtifactEvolution::new(crate::PolicyStore::new(tmp.path())));
        let d = PolicyEvolutionDriver::new(
            PolicyEvolutionConfig {
                min_trials: 20,
                ..PolicyEvolutionConfig::default()
            },
            engine_with(&[("pipeline", 10, 0), ("roundtable", 30, 30)]).await,
            evolution.clone(),
        );
        match d.run_once().await.unwrap() {
            PolicyEvolutionOutcome::NoImprovement {
                baseline_trials, ..
            } => {
                assert_eq!(baseline_trials, 30);
            }
            other => panic!("expected NoImprovement, got {other:?}"),
        }
        assert!(
            evolution
                .store()
                .list_versions("meta_learning.mode")
                .await
                .unwrap()
                .is_empty(),
            "没有更优候选时不得写出任何版本"
        );
    }

    #[tokio::test]
    async fn a_significantly_better_candidate_is_adopted_and_hot_swapped() {
        let tmp = tempfile::tempdir().unwrap();
        let evolution = Arc::new(ArtifactEvolution::new(crate::PolicyStore::new(tmp.path())));
        // active 版本把 min_samples 抬到 30，roundtable（20 次全成）因此
        // 进不了候选，选择规则只能退到 pipeline（0.5）；放宽门槛后
        // roundtable 胜出，重放臂变成 20 次全成——两组显著可分。
        evolution
            .store()
            .save_new_version(
                "meta_learning.mode",
                serde_json::json!({"min_samples": 30, "margin": 0.15}),
                "seed",
            )
            .await
            .unwrap();
        let d = PolicyEvolutionDriver::new(
            PolicyEvolutionConfig {
                min_trials: 5,
                ..PolicyEvolutionConfig::default()
            },
            engine_with_store(
                &[("pipeline", 40, 20), ("roundtable", 20, 20)],
                crate::PolicyStore::new(tmp.path()),
                "meta_learning.mode",
            )
            .await,
            evolution.clone(),
        );

        match d.run_once().await.unwrap() {
            PolicyEvolutionOutcome::Adopted {
                version,
                min_samples,
                margin,
                z,
                ..
            } => {
                assert_eq!(version, 2, "新版本追加在 seed 之后");
                assert!(z > 1.96);
                // 采用的参数必须真的能选中 roundtable（1.0 vs 0.5）。
                assert!(
                    min_samples <= 20,
                    "min_samples={min_samples} 会让候选臂取不到 roundtable"
                );
                assert!(margin < 0.5, "margin={margin} 会压掉这个领先幅度");
                let active = evolution
                    .store()
                    .load_active("meta_learning.mode")
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(active.version, 2, "adopt 必须热替换");
                assert_eq!(
                    active.payload["min_samples"],
                    serde_json::json!(min_samples)
                );
                assert!(active.reason.contains("autonomous"));
                assert!(evolution
                    .store()
                    .verify_chain("meta_learning.mode")
                    .await
                    .unwrap());
            }
            other => panic!("expected Adopted, got {other:?}"),
        }
    }

    #[test]
    fn config_rejects_a_malformed_section() {
        let parsed: Result<PolicyEvolutionConfig, _> =
            serde_json::from_str("{\"min_trials\": \"x\"}");
        assert!(parsed.is_err(), "配置写错必须响亮失败");
    }
}
