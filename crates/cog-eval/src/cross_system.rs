//! 跨系统对比框架。
//! 核心实验：方法论（PGE 层）可移植性验证 —— 给外部系统（AutoGPT/MetaGPT 等）
//! 注入方法论前后在同一数据集上对比性能 delta，输出跨系统对比报告。

use std::collections::HashSet;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::ablation::Component;
use crate::comparator::StatisticalTest;
use crate::dataset::{EvalCase, EvalDataset};
use crate::metric::EvalResult;

/// 注入外部系统的方法论配置（如 PGE 层）。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MethodologyConfig {
    pub name: String,
    /// 注入的组件集合（复用消融组件语义）。
    #[serde(default)]
    pub components: HashSet<Component>,
    /// 可选系统提示词前导（外部系统注入点）。
    #[serde(default)]
    pub prompt_preamble: Option<String>,
}

/// 可被对比的外部系统（AutoGPT / MetaGPT / 另一 LLM 上的 Cogneva 等）。
#[async_trait::async_trait]
pub trait ExternalSystem: Send + Sync {
    fn name(&self) -> &str;
    async fn setup(&self) -> Result<(), String>;
    /// methodology 为 Some 时按注入配置运行（系统自行决定注入点）。
    async fn run_task(
        &self,
        task: &EvalCase,
        methodology: Option<&MethodologyConfig>,
    ) -> EvalResult;
    async fn teardown(&self) -> Result<(), String>;
}

pub struct CrossSystemBenchmark {
    pub systems: Vec<Arc<dyn ExternalSystem>>,
    pub dataset: EvalDataset,
}

/// 单个系统的对比结果。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemScore {
    pub system: String,
    pub baseline_pass_rate: f64,
    pub with_methodology_pass_rate: f64,
    pub delta: f64,
    /// Welch t-test (t, p)，样本足够时给出。
    pub significance: Option<(f64, f64)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossSystemReport {
    pub methodology: String,
    pub dataset: String,
    pub scores: Vec<SystemScore>,
    /// 多数系统 delta > 0 视为方法论可移植。
    pub transferable: bool,
}

impl CrossSystemReport {
    /// 渲染 markdown 对比矩阵（行 = 系统，列 = baseline/注入后/delta/显著性）。
    pub fn to_markdown(&self) -> String {
        let mut md = String::new();
        md.push_str(&format!(
            "# 跨系统可移植性报告 — 方法论 {} @ {}\n\n",
            self.methodology, self.dataset
        ));
        md.push_str("| 系统 | Baseline | +方法论 | Δ | 显著性 |\n|---|---|---|---|---|\n");
        for s in &self.scores {
            let sig = match s.significance {
                Some((t, p)) => format!("t={t:.2}, p={p:.3}{}", if p < 0.05 { " *" } else { "" }),
                None => "n/a".into(),
            };
            md.push_str(&format!(
                "| {} | {:.3} | {:.3} | {:+.3} | {} |\n",
                s.system, s.baseline_pass_rate, s.with_methodology_pass_rate, s.delta, sig
            ));
        }
        md.push_str(&format!(
            "\n**可移植性结论**：{}\n",
            if self.transferable {
                "方法论在多数系统上带来正增量，可移植 ✅"
            } else {
                "正增量不占多数，暂不可证可移植 ⚠️"
            }
        ));
        md
    }
}

impl CrossSystemBenchmark {
    pub fn new(systems: Vec<Arc<dyn ExternalSystem>>, dataset: EvalDataset) -> Self {
        Self { systems, dataset }
    }

    /// 验证方法论可移植性：每个系统先后跑 baseline 与注入后两组，计算 delta。
    pub async fn verify_portability(&self, methodology: &MethodologyConfig) -> CrossSystemReport {
        let mut scores = Vec::new();
        for system in &self.systems {
            if let Err(e) = system.setup().await {
                tracing::warn!(system = system.name(), error = %e, "跨系统对比：setup 失败，跳过");
                continue;
            }
            let mut baseline_flags = Vec::new();
            let mut injected_flags = Vec::new();
            let mut baseline_passed = 0usize;
            let mut injected_passed = 0usize;
            let n = self.dataset.cases.len().max(1) as f64;
            for case in &self.dataset.cases {
                let base = system.run_task(case, None).await;
                baseline_flags.push(if base.passed { 1.0 } else { 0.0 });
                baseline_passed += base.passed as usize;
                let inj = system.run_task(case, Some(methodology)).await;
                injected_flags.push(if inj.passed { 1.0 } else { 0.0 });
                injected_passed += inj.passed as usize;
            }
            if let Err(e) = system.teardown().await {
                tracing::warn!(system = system.name(), error = %e, "跨系统对比：teardown 失败");
            }
            let baseline_rate = baseline_passed as f64 / n;
            let injected_rate = injected_passed as f64 / n;
            let significance = if baseline_flags.len() >= 2 {
                Some(StatisticalTest::welch_t_test(
                    &baseline_flags,
                    &injected_flags,
                ))
            } else {
                None
            };
            scores.push(SystemScore {
                system: system.name().to_string(),
                baseline_pass_rate: baseline_rate,
                with_methodology_pass_rate: injected_rate,
                delta: injected_rate - baseline_rate,
                significance,
            });
        }
        let positive = scores.iter().filter(|s| s.delta > 0.0).count();
        let transferable = !scores.is_empty() && positive * 2 > scores.len();
        CrossSystemReport {
            methodology: methodology.name.clone(),
            dataset: self.dataset.name.clone(),
            scores,
            transferable,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metric::MetricValue;

    struct FakeSystem {
        name: String,
        /// 注入方法论后额外通过的 case 数。
        boost: usize,
    }

    #[async_trait::async_trait]
    impl ExternalSystem for FakeSystem {
        fn name(&self) -> &str {
            &self.name
        }
        async fn setup(&self) -> Result<(), String> {
            Ok(())
        }
        async fn run_task(
            &self,
            task: &EvalCase,
            methodology: Option<&MethodologyConfig>,
        ) -> EvalResult {
            let idx: usize = task.id.trim_start_matches('c').parse().unwrap_or(0);
            let passed = idx == 0 || (methodology.is_some() && idx <= self.boost);
            EvalResult {
                case_id: task.id.clone(),
                passed,
                metrics: vec![MetricValue {
                    metric: "task_success_rate".into(),
                    value: passed as u8 as f64,
                    passed,
                    threshold: None,
                }],
                agent_output: serde_json::Value::Null,
                duration_ms: 0,
                token_usage: 0,
                cost: 0.0,
                error: None,
                steps: vec![],
                trace_json: None,
            }
        }
        async fn teardown(&self) -> Result<(), String> {
            Ok(())
        }
    }

    fn dataset(n: usize) -> EvalDataset {
        let mut d = EvalDataset::new("toy");
        for i in 0..n {
            d.add_case(EvalCase {
                id: format!("c{i}"),
                name: format!("c{i}"),
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
    async fn portability_report_computes_deltas() {
        let bench = CrossSystemBenchmark::new(
            vec![
                Arc::new(FakeSystem {
                    name: "SysA".into(),
                    boost: 3,
                }),
                Arc::new(FakeSystem {
                    name: "SysB".into(),
                    boost: 0,
                }),
            ],
            dataset(4),
        );
        let m = MethodologyConfig {
            name: "PGE".into(),
            ..Default::default()
        };
        let report = bench.verify_portability(&m).await;
        assert_eq!(report.scores.len(), 2);
        let a = &report.scores[0];
        assert!((a.baseline_pass_rate - 0.25).abs() < 1e-9);
        assert!((a.with_methodology_pass_rate - 1.0).abs() < 1e-9);
        assert!(a.delta > 0.0);
        let b = &report.scores[1];
        assert!(b.delta.abs() < 1e-9);
        // 2 个系统里 1 个正增量 → 不占多数 → 不可移植
        assert!(!report.transferable);
        let md = report.to_markdown();
        assert!(md.contains("SysA"));
        assert!(md.contains("PGE"));
    }
}
