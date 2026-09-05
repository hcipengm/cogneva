//! 进化效果评估 harness（审计 4.2）与 benchmark 报告框架（审计 4.5 代码侧）。
//!
//! 用固定任务集衡量 change 前后的成功率、延迟、成本：
//! - 成功率差异用两比例 z 检验（α=0.05）判断统计显著性；
//! - 仅当「显著更好」时给出 Adopt，显著更差 Reject，其余 Inconclusive ——
//!   配合「拒绝无统计显著的改动」原则，只有 Adopt 应被部署流水线接受；
//! - `BenchReport` 产出可复现的 JSON / Markdown 报告工件。

use std::future::Future;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// 固定评估任务。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalTask {
    pub id: String,
    pub input: serde_json::Value,
}

/// 单次任务执行结果。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalOutcome {
    pub task_id: String,
    pub success: bool,
    pub latency_ms: u64,
    pub cost_tokens: u64,
}

/// 一组执行的聚合摘要。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalSummary {
    pub total: usize,
    pub succeeded: usize,
    pub success_rate: f64,
    pub mean_latency_ms: f64,
    pub total_cost_tokens: u64,
}

impl EvalSummary {
    pub fn from_outcomes(outcomes: &[EvalOutcome]) -> Self {
        let total = outcomes.len();
        let succeeded = outcomes.iter().filter(|o| o.success).count();
        let success_rate = if total == 0 {
            0.0
        } else {
            succeeded as f64 / total as f64
        };
        let mean_latency_ms = if total == 0 {
            0.0
        } else {
            outcomes.iter().map(|o| o.latency_ms as f64).sum::<f64>() / total as f64
        };
        let total_cost_tokens = outcomes.iter().map(|o| o.cost_tokens).sum();
        Self {
            total,
            succeeded,
            success_rate,
            mean_latency_ms,
            total_cost_tokens,
        }
    }
}

/// change 前后对比结论。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvalVerdict {
    /// 成功率统计显著提升 —— 唯一应被部署的结论。
    Adopt,
    /// 成功率统计显著下降。
    Reject,
    /// 无统计显著差异（按设计原则同样拒绝部署）。
    Inconclusive,
}

/// 对比报告。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalComparison {
    pub before: EvalSummary,
    pub after: EvalSummary,
    /// 两比例 z 统计量。
    pub z_score: f64,
    pub significant: bool,
    pub verdict: EvalVerdict,
}

/// 两比例 z 检验：H0 = 两组成功率相同。返回 (z, |z|>1.96)。
pub fn two_proportion_z_test(s1: usize, n1: usize, s2: usize, n2: usize) -> (f64, bool) {
    if n1 == 0 || n2 == 0 {
        return (0.0, false);
    }
    let p1 = s1 as f64 / n1 as f64;
    let p2 = s2 as f64 / n2 as f64;
    let pooled = (s1 + s2) as f64 / (n1 + n2) as f64;
    let se = (pooled * (1.0 - pooled) * (1.0 / n1 as f64 + 1.0 / n2 as f64)).sqrt();
    if se == 0.0 {
        // 两组成功率同为 0 或 1：无方差，无法区分。
        return (0.0, false);
    }
    let z = (p2 - p1) / se;
    (z, z.abs() > 1.96)
}

/// 对比 change 前后两组执行结果。
pub fn compare(before: &[EvalOutcome], after: &[EvalOutcome]) -> EvalComparison {
    let b = EvalSummary::from_outcomes(before);
    let a = EvalSummary::from_outcomes(after);
    let (z, significant) = two_proportion_z_test(b.succeeded, b.total, a.succeeded, a.total);
    let verdict = if !significant {
        EvalVerdict::Inconclusive
    } else if z > 0.0 {
        EvalVerdict::Adopt
    } else {
        EvalVerdict::Reject
    };
    EvalComparison {
        before: b,
        after: a,
        z_score: z,
        significant,
        verdict,
    }
}

/// 对固定任务集执行 runner 并收集结果。
pub async fn evaluate<F, Fut>(tasks: &[EvalTask], runner: F) -> Vec<EvalOutcome>
where
    F: Fn(&EvalTask) -> Fut,
    Fut: Future<Output = EvalOutcome>,
{
    let mut outcomes = Vec::with_capacity(tasks.len());
    for task in tasks {
        outcomes.push(runner(task).await);
    }
    outcomes
}

// ─── Benchmark 报告框架（4.5 代码侧） ─────────────────────────────────

/// 一次可复现 benchmark 运行的完整报告。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchReport {
    pub suite: String,
    pub version: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub summary: EvalSummary,
    pub outcomes: Vec<EvalOutcome>,
}

impl BenchReport {
    pub fn new(
        suite: impl Into<String>,
        version: impl Into<String>,
        started_at: DateTime<Utc>,
        outcomes: Vec<EvalOutcome>,
    ) -> Self {
        Self {
            suite: suite.into(),
            version: version.into(),
            started_at,
            finished_at: Utc::now(),
            summary: EvalSummary::from_outcomes(&outcomes),
            outcomes,
        }
    }

    /// 渲染为 Markdown 报告（可直接发布）。
    pub fn to_markdown(&self) -> String {
        let mut md = format!(
            "# Benchmark 报告：{} ({})\n\n- 开始：{}\n- 结束：{}\n- 任务数：{}\n- 成功率：{:.1}%（{}/{}）\n- 平均延迟：{:.1} ms\n- 总成本：{} tokens\n\n| 任务 | 成功 | 延迟 (ms) | 成本 (tokens) |\n|---|---|---|---|\n",
            self.suite,
            self.version,
            self.started_at.to_rfc3339(),
            self.finished_at.to_rfc3339(),
            self.summary.total,
            self.summary.success_rate * 100.0,
            self.summary.succeeded,
            self.summary.total,
            self.summary.mean_latency_ms,
            self.summary.total_cost_tokens,
        );
        for o in &self.outcomes {
            md.push_str(&format!(
                "| {} | {} | {} | {} |\n",
                o.task_id, o.success, o.latency_ms, o.cost_tokens
            ));
        }
        md
    }

    /// 写 JSON + Markdown 双工件到指定目录，返回 (json_path, md_path)。
    pub async fn write_artifacts(
        &self,
        dir: &std::path::Path,
    ) -> cog_core::SFResult<(std::path::PathBuf, std::path::PathBuf)> {
        tokio::fs::create_dir_all(dir).await.map_err(|e| {
            cog_core::SFError::IO(format!("failed to create {}: {}", dir.display(), e))
        })?;
        let stamp = self.started_at.format("%Y%m%dT%H%M%SZ");
        let json_path = dir.join(format!("bench-{}-{}.json", self.suite, stamp));
        let md_path = dir.join(format!("bench-{}-{}.md", self.suite, stamp));
        let json = serde_json::to_string_pretty(self).map_err(cog_core::SFError::Serialization)?;
        tokio::fs::write(&json_path, json)
            .await
            .map_err(|e| cog_core::SFError::IO(format!("write json report: {}", e)))?;
        tokio::fs::write(&md_path, self.to_markdown())
            .await
            .map_err(|e| cog_core::SFError::IO(format!("write md report: {}", e)))?;
        Ok((json_path, md_path))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn outcomes(spec: &[(bool, u64)]) -> Vec<EvalOutcome> {
        spec.iter()
            .enumerate()
            .map(|(i, (ok, lat))| EvalOutcome {
                task_id: format!("t{}", i),
                success: *ok,
                latency_ms: *lat,
                cost_tokens: 10,
            })
            .collect()
    }

    #[test]
    fn summary_aggregates_correctly() {
        let s = EvalSummary::from_outcomes(&outcomes(&[(true, 100), (false, 300)]));
        assert_eq!(s.total, 2);
        assert_eq!(s.succeeded, 1);
        assert!((s.success_rate - 0.5).abs() < 1e-9);
        assert!((s.mean_latency_ms - 200.0).abs() < 1e-9);
        assert_eq!(s.total_cost_tokens, 20);
    }

    #[test]
    fn significant_improvement_adopts() {
        // 50% → 100%，n=40 每组：z ≈ 3.42 > 1.96
        let before: Vec<_> = (0..40).map(|i| (i % 2 == 0, 100)).collect();
        let after: Vec<_> = (0..40).map(|_| (true, 100)).collect();
        let cmp = compare(&outcomes(&before), &outcomes(&after));
        assert!(cmp.significant);
        assert_eq!(cmp.verdict, EvalVerdict::Adopt);
    }

    #[test]
    fn significant_regression_rejects() {
        let before: Vec<_> = (0..40).map(|_| (true, 100)).collect();
        let after: Vec<_> = (0..40).map(|i| (i % 2 == 0, 100)).collect();
        let cmp = compare(&outcomes(&before), &outcomes(&after));
        assert!(cmp.significant);
        assert_eq!(cmp.verdict, EvalVerdict::Reject);
    }

    #[test]
    fn small_sample_difference_is_inconclusive() {
        // 3/4 vs 4/4：差异大但样本太小，统计不显著 → 拒绝部署（Inconclusive）。
        let cmp = compare(
            &outcomes(&[(true, 100), (true, 100), (true, 100), (false, 100)]),
            &outcomes(&[(true, 100), (true, 100), (true, 100), (true, 100)]),
        );
        assert_eq!(cmp.verdict, EvalVerdict::Inconclusive);
    }

    #[test]
    fn empty_groups_are_inconclusive() {
        let cmp = compare(&[], &outcomes(&[(true, 100)]));
        assert!(!cmp.significant);
        assert_eq!(cmp.verdict, EvalVerdict::Inconclusive);
    }

    #[tokio::test]
    async fn bench_report_writes_json_and_markdown() {
        let tmp = tempfile::tempdir().unwrap();
        let report = BenchReport::new(
            "agentbench-mini",
            "0.1.20",
            Utc::now(),
            outcomes(&[(true, 120), (false, 250)]),
        );
        let (json_path, md_path) = report.write_artifacts(tmp.path()).await.unwrap();
        let json = std::fs::read_to_string(json_path).unwrap();
        assert!(json.contains("\"suite\": \"agentbench-mini\""));
        let md = std::fs::read_to_string(md_path).unwrap();
        assert!(md.contains("成功率：50.0%"));
        assert!(md.contains("| t0 | true | 120 | 10 |"));
    }
}
