//! 消融实验编排器。
//! 对若干消融组（Baseline / +SelfReview / +PGE / +Ralph / +Memory）在同一数据集上
//! 分别评估，计算各组相对 Baseline 的增量（D12 指标来源），并给出显著性检验。

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::comparator::StatisticalTest;
use crate::dataset::EvalDataset;
use crate::metric::EvalResult;

/// 可消融的系统组件。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Component {
    SelfReview,
    Pge,
    Ralph,
    Memory,
}

/// 一个消融组：启用特定组件子集的运行配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AblationGroup {
    pub name: String,
    pub components_enabled: HashSet<Component>,
    #[serde(default)]
    pub llm_model: Option<String>,
    /// 每组重复次数（取均值）。
    #[serde(default = "default_runs")]
    pub runs: u32,
}

fn default_runs() -> u32 {
    1
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AblationConfig {
    pub groups: Vec<AblationGroup>,
}

impl AblationConfig {
    /// 标准五组消融矩阵（Baseline → +SelfReview → +PGE → +Ralph → +Memory）。
    pub fn standard_matrix() -> Self {
        let group = |name: &str, comps: &[Component]| AblationGroup {
            name: name.into(),
            components_enabled: comps.iter().copied().collect(),
            llm_model: None,
            runs: 1,
        };
        Self {
            groups: vec![
                group("Baseline", &[]),
                group("+SelfReview", &[Component::SelfReview]),
                group("+PGE", &[Component::SelfReview, Component::Pge]),
                group(
                    "+Ralph",
                    &[Component::SelfReview, Component::Pge, Component::Ralph],
                ),
                group(
                    "+Memory",
                    &[
                        Component::SelfReview,
                        Component::Pge,
                        Component::Ralph,
                        Component::Memory,
                    ],
                ),
            ],
        }
    }
}

/// 单组评估汇总。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupReport {
    pub group_name: String,
    pub pass_rate: f64,
    /// 指标名 → 均值。
    pub metric_means: HashMap<String, f64>,
    /// 主分数（pass_rate）逐 run 采样，用于显著性检验。
    pub run_scores: Vec<f64>,
}

/// 单组相对 Baseline 的增量。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AblationDelta {
    pub group_name: String,
    pub pass_rate_delta: f64,
    /// Welch t-test (t 统计量, p 值近似自由度比)，样本不足时为 None。
    pub significance: Option<(f64, f64)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AblationReport {
    pub dataset: String,
    pub groups: Vec<GroupReport>,
    /// 第一组视为 Baseline，其余组给出增量。
    pub deltas: Vec<AblationDelta>,
}

impl AblationReport {
    /// 堆叠柱状图数据：每层消融相对前一层（累计式）的增量贡献。
    /// 返回 [(group_name, marginal_delta)]，Baseline 的 marginal delta 为其自身 pass_rate。
    pub fn delta_stacked_bar(&self) -> Vec<(String, f64)> {
        let mut out = Vec::new();
        let mut prev = 0.0;
        for g in &self.groups {
            out.push((g.group_name.clone(), g.pass_rate - prev));
            prev = g.pass_rate;
        }
        out
    }
}

/// 执行单个消融组评估的抽象（由调用方注入，负责按组配置 agent 运行时）。
#[async_trait::async_trait]
pub trait AblationExecutor: Send + Sync {
    async fn run_group(&self, group: &AblationGroup, dataset: &EvalDataset) -> Vec<EvalResult>;
}

pub struct AblationRunner {
    config: AblationConfig,
    executor: Arc<dyn AblationExecutor>,
}

impl AblationRunner {
    pub fn new(config: AblationConfig, executor: Arc<dyn AblationExecutor>) -> Self {
        Self { config, executor }
    }

    pub async fn run_all(&self, dataset: &EvalDataset) -> AblationReport {
        let mut groups = Vec::new();
        for group in &self.config.groups {
            let mut all_results = Vec::new();
            let mut run_scores = Vec::new();
            for _ in 0..group.runs.max(1) {
                let results = self.executor.run_group(group, dataset).await;
                let pass_rate = pass_rate_of(&results);
                run_scores.push(pass_rate);
                all_results.extend(results);
            }
            groups.push(GroupReport {
                group_name: group.name.clone(),
                pass_rate: pass_rate_of(&all_results),
                metric_means: metric_means_of(&all_results),
                run_scores,
            });
        }

        let mut deltas = Vec::new();
        if let Some(baseline) = groups.first() {
            for g in groups.iter().skip(1) {
                let significance = if g.run_scores.len() >= 2 && baseline.run_scores.len() >= 2 {
                    Some(StatisticalTest::welch_t_test(
                        &baseline.run_scores,
                        &g.run_scores,
                    ))
                } else {
                    None
                };
                deltas.push(AblationDelta {
                    group_name: g.group_name.clone(),
                    pass_rate_delta: g.pass_rate - baseline.pass_rate,
                    significance,
                });
            }
        }

        AblationReport {
            dataset: dataset.name.clone(),
            groups,
            deltas,
        }
    }
}

fn pass_rate_of(results: &[EvalResult]) -> f64 {
    if results.is_empty() {
        return 0.0;
    }
    let passed = results.iter().filter(|r| r.passed).count();
    passed as f64 / results.len() as f64
}

fn metric_means_of(results: &[EvalResult]) -> HashMap<String, f64> {
    let mut sums: HashMap<String, (f64, usize)> = HashMap::new();
    for r in results {
        for m in &r.metrics {
            let e = sums.entry(m.metric.clone()).or_default();
            e.0 += m.value;
            e.1 += 1;
        }
    }
    sums.into_iter()
        .map(|(k, (s, n))| (k, s / n as f64))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metric::MetricValue;

    struct FakeExecutor;

    #[async_trait::async_trait]
    impl AblationExecutor for FakeExecutor {
        async fn run_group(&self, group: &AblationGroup, dataset: &EvalDataset) -> Vec<EvalResult> {
            // 模拟：启用 PGE 的组通过 2/2，Baseline 通过 1/2
            let pass_all = group.components_enabled.contains(&Component::Pge);
            dataset
                .cases
                .iter()
                .enumerate()
                .map(|(i, c)| EvalResult {
                    case_id: c.id.clone(),
                    passed: pass_all || i == 0,
                    metrics: vec![MetricValue {
                        metric: "task_success_rate".into(),
                        value: if pass_all { 1.0 } else { 0.5 },
                        passed: pass_all,
                        threshold: Some(0.5),
                    }],
                    agent_output: serde_json::Value::Null,
                    duration_ms: 0,
                    token_usage: 0,
                    cost: 0.0,
                    error: None,
                    steps: vec![],
                    trace_json: None,
                })
                .collect()
        }
    }

    fn two_case_dataset() -> EvalDataset {
        let mut d = EvalDataset::new("toy");
        for id in ["c1", "c2"] {
            d.add_case(crate::dataset::EvalCase {
                id: id.into(),
                name: id.into(),
                input: serde_json::Value::Null,
                expected_output: None,
                expected_tools: None,
                metrics: vec![],
                tags: vec![],
            });
        }
        d
    }

    #[tokio::test]
    async fn computes_deltas_against_baseline() {
        let config = AblationConfig {
            groups: vec![
                AblationGroup {
                    name: "Baseline".into(),
                    components_enabled: HashSet::new(),
                    llm_model: None,
                    runs: 1,
                },
                AblationGroup {
                    name: "+PGE".into(),
                    components_enabled: [Component::Pge].into_iter().collect(),
                    llm_model: None,
                    runs: 1,
                },
            ],
        };
        let runner = AblationRunner::new(config, Arc::new(FakeExecutor));
        let report = runner.run_all(&two_case_dataset()).await;

        assert_eq!(report.groups[0].pass_rate, 0.5);
        assert_eq!(report.groups[1].pass_rate, 1.0);
        assert_eq!(report.deltas.len(), 1);
        assert!((report.deltas[0].pass_rate_delta - 0.5).abs() < 1e-9);

        let stacked = report.delta_stacked_bar();
        assert_eq!(stacked[0], ("Baseline".to_string(), 0.5));
        assert_eq!(stacked[1], ("+PGE".to_string(), 0.5));
    }
}
