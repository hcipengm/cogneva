//!Eval service trait and gateway DTOs.
//!Keeps `cog-gateway` decoupled from `cog-eval` concrete types.

use serde::{Deserialize, Serialize};

/// 两比例 z 检验：H0 = 两组成功率相同。返回 `(z, |z| > 1.96)`。
///
/// 这是全工作区**唯一**一处「差异是不是真的」的公式。它在契约层而不是在
/// `cog-eval` 里，因为读它的有两个 crate——eval 的 A/B 门，和自进化那条把
/// 结果映射成 [`crate::contract::promotion::EvalVerdict`] 的判定——而判定必须
/// 落在同一把尺子上。原先它住在 `cog-eval`：`cog-reflection` 为了拿到它而
/// 直接依赖 `cog-eval`，一条纯函数就成了两个 crate 之间的整条依赖边。
///
/// 它不是一个服务：没有状态、没有 I/O、不 await。把它挂到某个 trait 上会要求
/// 每个调用点先拿到一份服务句柄，而判定点（自进化网格里逐格算显著性）本来就
/// 离装配层很远，中间会多出一条纯为拿尺子而存在的管线。
pub fn two_proportion_z_test(s1: usize, n1: usize, s2: usize, n2: usize) -> (f64, bool) {
    if n1 == 0 || n2 == 0 {
        return (0.0, false);
    }
    let p1 = s1 as f64 / n1 as f64;
    let p2 = s2 as f64 / n2 as f64;
    let pooled = (s1 + s2) as f64 / (n1 + n2) as f64;
    let se = (pooled * (1.0 - pooled) * (1.0 / n1 as f64 + 1.0 / n2 as f64)).sqrt();
    if se == 0.0 {
        // 两组都是 0 或都是 1：没有方差，两者不可分。
        return (0.0, false);
    }
    let z = (p2 - p1) / se;
    (z, z.abs() > 1.96)
}

/// Dataset metadata returned by list endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalDatasetInfo {
    pub name: String,
    pub path: String,
    pub case_count: usize,
    pub tags: Vec<String>,
}

/// Response for `GET /api/v1/eval/datasets`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalListDatasetsResponse {
    pub datasets: Vec<EvalDatasetInfo>,
}

/// Response for `GET /api/v1/eval/report/:report_id`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalGetReportResponse {
    pub run_id: String,
    pub dataset_name: String,
    pub report_markdown: String,
    pub report_json: String,
}

/// Trait abstracting the eval framework so the gateway never depends on `cog-eval`.
#[async_trait::async_trait]
pub trait EvalService: Send + Sync {
    /// Run an eval dataset and return the report as JSON.
    async fn run_eval(&self, dataset_path: &str) -> crate::SFResult<serde_json::Value>;

    /// Compare two result arrays (baseline vs challenger) and return comparison report as JSON.
    async fn compare_eval(
        &self,
        baseline: serde_json::Value,
        challenger: serde_json::Value,
        baseline_name: &str,
        challenger_name: &str,
    ) -> crate::SFResult<serde_json::Value>;

    /// Render a report JSON string into the requested format (`markdown`, `json`, `html`).
    async fn render_report(&self, report_json: &str, format: &str) -> crate::SFResult<String>;
}

#[cfg(test)]
mod tests {
    use super::*;

    // The cut is |z| > 1.96; a large, clean difference must clear it and a
    // same-rate pair must not. Pinning both directions keeps the significance
    // boundary from drifting when the formula is touched. It lives with the
    // formula, at the definition, so both consumers are covered by one pin.
    #[test]
    fn the_z_test_separates_a_clear_gap_from_no_gap() {
        let (z, significant) = two_proportion_z_test(90, 100, 50, 100);
        assert!(
            z.abs() > 1.96,
            "a 90% vs 50% gap over 100 each is significant: z={z}"
        );
        assert!(significant);

        let (z, significant) = two_proportion_z_test(50, 100, 51, 100);
        assert!(
            !significant,
            "a 1-point gap over 100 each is not significant: z={z}"
        );
    }

    // Zero samples and the degenerate all-pass / all-fail pair have no variance
    // to separate; the test must say "cannot tell" rather than divide by zero.
    #[test]
    fn a_degenerate_pair_is_not_called_significant() {
        assert_eq!(two_proportion_z_test(0, 0, 5, 10), (0.0, false));
        assert_eq!(two_proportion_z_test(0, 0, 0, 0), (0.0, false));
        assert_eq!(two_proportion_z_test(10, 10, 10, 10), (0.0, false));
        assert_eq!(two_proportion_z_test(0, 10, 0, 10), (0.0, false));
    }
}
