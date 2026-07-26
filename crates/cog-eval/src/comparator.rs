//! A/B 对比分析 — 统计显著性检验。

use crate::metric::EvalResult;
use serde::{Deserialize, Serialize};
use statrs::distribution::ContinuousCDF;
use std::collections::HashMap;

/// 单指标对比结果。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricComparison {
    pub metric: String,
    pub baseline_mean: f64,
    pub challenger_mean: f64,
    pub delta: f64,
    pub delta_percent: f64,
    pub p_value: f64,
    pub significant: bool,
    pub winner: String,
}

/// 对比报告。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComparisonReport {
    pub baseline_name: String,
    pub challenger_name: String,
    pub baseline_pass_rate: f64,
    pub challenger_pass_rate: f64,
    pub delta: f64,
    pub delta_percent: f64,
    pub statistically_significant: bool,
    pub p_value: f64,
    pub winner: String,
    pub sample_size: usize,
    pub effect_size: f64,
    pub per_metric_comparison: Vec<MetricComparison>,
    pub recommendation: String,
}

/// A/B 对比器。
pub struct AbComparator;

impl Default for AbComparator {
    fn default() -> Self {
        Self::new()
    }
}

impl AbComparator {
    pub fn new() -> Self {
        Self
    }

    /// 基础对比：基于综合通过率的 z-test。
    pub fn compare(
        &self,
        baseline: &[EvalResult],
        challenger: &[EvalResult],
        baseline_name: &str,
        challenger_name: &str,
    ) -> ComparisonReport {
        self.compare_detailed(baseline, challenger, baseline_name, challenger_name)
    }

    /// 详细对比：综合通过率 + 按指标 Welch t-test + Cohen's d。
    pub fn compare_detailed(
        &self,
        baseline: &[EvalResult],
        challenger: &[EvalResult],
        baseline_name: &str,
        challenger_name: &str,
    ) -> ComparisonReport {
        let baseline_pass = baseline.iter().filter(|r| r.passed).count() as f64;
        let baseline_rate = baseline_pass / baseline.len().max(1) as f64;

        let challenger_pass = challenger.iter().filter(|r| r.passed).count() as f64;
        let challenger_rate = challenger_pass / challenger.len().max(1) as f64;

        let delta = challenger_rate - baseline_rate;
        let delta_percent = if baseline_rate > 0.0 {
            (delta / baseline_rate) * 100.0
        } else {
            0.0
        };

        // Two-proportion z-test
        let n1 = baseline.len() as f64;
        let n2 = challenger.len() as f64;
        let p1 = baseline_rate;
        let p2 = challenger_rate;
        let p_pool = (baseline_pass + challenger_pass) / (n1 + n2);
        let se = (p_pool * (1.0 - p_pool) * (1.0 / n1 + 1.0 / n2)).sqrt();

        let z = if se > 0.0 { (p2 - p1) / se } else { 0.0 };
        let p_value = 2.0
            * (1.0
                - statrs::distribution::Normal::new(0.0, 1.0)
                    .unwrap()
                    .cdf(z.abs()));

        let significant = p_value < 0.05;
        let winner = if significant {
            if challenger_rate > baseline_rate {
                challenger_name.to_string()
            } else {
                baseline_name.to_string()
            }
        } else {
            "inconclusive".to_string()
        };

        // Per-metric comparison
        let per_metric =
            self.compare_per_metric(baseline, challenger, baseline_name, challenger_name);

        // Cohen's d for overall pass rate (treating as proportions)
        let effect_size = cohens_d_for_proportions(p1, p2, n1, n2);

        let recommendation = self.recommend(
            significant,
            delta,
            effect_size,
            challenger_rate,
            baseline_rate,
        );

        ComparisonReport {
            baseline_name: baseline_name.into(),
            challenger_name: challenger_name.into(),
            baseline_pass_rate: baseline_rate,
            challenger_pass_rate: challenger_rate,
            delta,
            delta_percent,
            statistically_significant: significant,
            p_value,
            winner,
            sample_size: baseline.len(),
            effect_size,
            per_metric_comparison: per_metric,
            recommendation,
        }
    }

    fn compare_per_metric(
        &self,
        baseline: &[EvalResult],
        challenger: &[EvalResult],
        baseline_name: &str,
        challenger_name: &str,
    ) -> Vec<MetricComparison> {
        let mut baseline_by_metric: HashMap<String, Vec<f64>> = HashMap::new();
        let mut challenger_by_metric: HashMap<String, Vec<f64>> = HashMap::new();

        for r in baseline {
            for m in &r.metrics {
                baseline_by_metric
                    .entry(m.metric.clone())
                    .or_default()
                    .push(m.value);
            }
        }
        for r in challenger {
            for m in &r.metrics {
                challenger_by_metric
                    .entry(m.metric.clone())
                    .or_default()
                    .push(m.value);
            }
        }

        let mut comparisons = vec![];
        for metric in baseline_by_metric.keys() {
            let b = baseline_by_metric.get(metric).unwrap();
            let c = challenger_by_metric.get(metric);
            if c.is_none() {
                continue;
            }
            let c = c.unwrap();
            if b.len() < 2 || c.len() < 2 {
                continue;
            }

            let b_mean = mean(b);
            let c_mean = mean(c);
            let delta = c_mean - b_mean;
            let delta_pct = if b_mean.abs() > 1e-9 {
                (delta / b_mean) * 100.0
            } else {
                0.0
            };

            let (_t, p) = StatisticalTest::welch_t_test(b, c);
            let sig = p < 0.05;
            let winner = if sig {
                if c_mean > b_mean {
                    challenger_name.to_string()
                } else {
                    baseline_name.to_string()
                }
            } else {
                "inconclusive".to_string()
            };

            comparisons.push(MetricComparison {
                metric: metric.clone(),
                baseline_mean: b_mean,
                challenger_mean: c_mean,
                delta,
                delta_percent: delta_pct,
                p_value: p,
                significant: sig,
                winner,
            });
        }

        comparisons.sort_by(|a, b| a.metric.cmp(&b.metric));
        comparisons
    }

    fn recommend(
        &self,
        significant: bool,
        delta: f64,
        effect_size: f64,
        challenger_rate: f64,
        baseline_rate: f64,
    ) -> String {
        if !significant {
            if challenger_rate >= baseline_rate {
                return "Inconclusive — challenger shows improvement but not statistically significant. Consider increasing sample size.".into();
            } else {
                return "Inconclusive — no meaningful difference detected.".into();
            }
        }

        if delta > 0.0 {
            if effect_size >= 0.8 {
                "Strong recommendation: Adopt challenger. Large effect size with statistical significance.".into()
            } else if effect_size >= 0.5 {
                "Moderate recommendation: Adopt challenger. Medium effect size, statistically significant.".into()
            } else {
                "Weak recommendation: Adopt challenger. Small but statistically significant improvement.".into()
            }
        } else {
            if effect_size <= -0.8 {
                "Strong recommendation: Keep baseline. Challenger shows large degradation.".into()
            } else if effect_size <= -0.5 {
                "Moderate recommendation: Keep baseline. Challenger shows medium degradation."
                    .into()
            } else {
                "Weak recommendation: Keep baseline. Small but statistically significant degradation.".into()
            }
        }
    }
}

/// 统计检验。
pub struct StatisticalTest;

impl StatisticalTest {
    /// Welch's t-test for comparing means.
    pub fn welch_t_test(a: &[f64], b: &[f64]) -> (f64, f64) {
        let n1 = a.len() as f64;
        let n2 = b.len() as f64;
        let m1 = a.iter().sum::<f64>() / n1;
        let m2 = b.iter().sum::<f64>() / n2;
        let v1 = a.iter().map(|x| (x - m1).powi(2)).sum::<f64>() / (n1 - 1.0);
        let v2 = b.iter().map(|x| (x - m2).powi(2)).sum::<f64>() / (n2 - 1.0);

        let se = (v1 / n1 + v2 / n2).sqrt();
        let t = if se > 0.0 { (m1 - m2) / se } else { 0.0 };

        let df = (v1 / n1 + v2 / n2).powi(2)
            / ((v1 / n1).powi(2) / (n1 - 1.0) + (v2 / n2).powi(2) / (n2 - 1.0));

        let p = if df > 0.0 {
            2.0 * (1.0
                - statrs::distribution::StudentsT::new(0.0, 1.0, df)
                    .unwrap()
                    .cdf(t.abs()))
        } else {
            1.0
        };

        (t, p)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn mean(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.iter().sum::<f64>() / values.len() as f64
}

/// Cohen's d for two independent proportions (using arcsine transformation approximation).
fn cohens_d_for_proportions(p1: f64, p2: f64, n1: f64, n2: f64) -> f64 {
    let h1 = 2.0 * p1.sqrt().asin();
    let h2 = 2.0 * p2.sqrt().asin();
    let pooled_sd = (1.0 / n1 + 1.0 / n2).sqrt();
    if pooled_sd > 0.0 {
        (h2 - h1) / pooled_sd
    } else {
        0.0
    }
}
