//! D11 — 代码质量与架构指标计算（软工赛道 ASE/ICSE）。
//! 测量约定与 D10 相同：静态分析 harness 把结果写入 `agent_output[metric_name]`。

use crate::dataset::EvalCase;
use crate::metric::{EvalMetric, EvalResult};

fn measured(result: &EvalResult, key: &str) -> Option<f64> {
    result.agent_output.get(key).and_then(|v| v.as_f64())
}

fn lower_better(result: &EvalResult, key: &str, threshold: f64) -> (f64, bool) {
    match measured(result, key) {
        Some(v) => (v, v <= threshold),
        None => (0.0, true),
    }
}

fn higher_better(result: &EvalResult, key: &str, threshold: f64) -> (f64, bool) {
    match measured(result, key) {
        Some(v) => (v, v >= threshold),
        None => (0.0, true),
    }
}

pub fn compute(metric: &EvalMetric, _case: &EvalCase, result: &EvalResult) -> (f64, bool) {
    match metric {
        EvalMetric::ClippyWarnCount { threshold } => {
            lower_better(result, "clippy_warn_count", *threshold)
        }
        EvalMetric::CargoTestPassRate { threshold } => {
            higher_better(result, "cargo_test_pass_rate", *threshold)
        }
        EvalMetric::RegressionRate { threshold } => {
            lower_better(result, "regression_rate", *threshold)
        }
        EvalMetric::CargoDenySecurityIssues { threshold } => {
            lower_better(result, "cargo_deny_security_issues", *threshold)
        }
        EvalMetric::CrateDependencyCount { threshold } => {
            lower_better(result, "crate_dependency_count", *threshold)
        }
        EvalMetric::FaultIsolationSurvivalRate { threshold } => {
            higher_better(result, "fault_isolation_survival_rate", *threshold)
        }
        EvalMetric::PluginHotplugDowntime { threshold_ms } => {
            lower_better(result, "plugin_hotplug_downtime_ms", *threshold_ms as f64)
        }
        EvalMetric::ArchitectureDriftIndex { threshold } => {
            lower_better(result, "architecture_drift_index", *threshold)
        }
        EvalMetric::NewPluginIntegrationCost { threshold } => {
            lower_better(result, "new_plugin_integration_cost", *threshold)
        }
        _ => (0.0, true),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{mk_case, mk_result_for_output};

    #[test]
    fn test_pass_rate_higher_better() {
        let result = mk_result_for_output(serde_json::json!({"cargo_test_pass_rate": 0.97}));
        let metric = EvalMetric::CargoTestPassRate { threshold: 0.95 };
        let (v, p) = compute(&metric, &mk_case(), &result);
        assert_eq!(v, 0.97);
        assert!(p);
    }

    #[test]
    fn clippy_warn_lower_better() {
        let result = mk_result_for_output(serde_json::json!({"clippy_warn_count": 12}));
        let metric = EvalMetric::ClippyWarnCount { threshold: 10.0 };
        let (v, p) = compute(&metric, &mk_case(), &result);
        assert_eq!(v, 12.0);
        assert!(!p);
    }
}
