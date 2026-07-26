//! D10 — 系统部署与运维指标计算（docs/2026-06-29_16-00 cog-eval 增强方案 §2.1）。
//! 测量约定：SystemEvalHarness 把测量值写入 agent_output[metric_name]，
//! 本模块读取该值并与阈值比较；缺失时返回占位 (0.0, true)。

use crate::dataset::EvalCase;
use crate::metric::{EvalMetric, EvalResult};

fn measured(result: &EvalResult, key: &str) -> Option<f64> {
    result.agent_output.get(key).and_then(|v| v.as_f64())
}

/// 越低越好：value <= threshold 通过。
fn lower_better(result: &EvalResult, key: &str, threshold: f64) -> (f64, bool) {
    match measured(result, key) {
        Some(v) => (v, v <= threshold),
        None => (0.0, true),
    }
}

/// 越高越好：value >= threshold 通过。
fn higher_better(result: &EvalResult, key: &str, threshold: f64) -> (f64, bool) {
    match measured(result, key) {
        Some(v) => (v, v >= threshold),
        None => (0.0, true),
    }
}

pub fn compute(metric: &EvalMetric, _case: &EvalCase, result: &EvalResult) -> (f64, bool) {
    match metric {
        EvalMetric::DeploymentTime { threshold_ms } => {
            lower_better(result, "deployment_time_ms", *threshold_ms as f64)
        }
        EvalMetric::DeploymentStepCount { threshold } => {
            lower_better(result, "deployment_step_count", *threshold)
        }
        EvalMetric::FirstTimeSuccessRate { threshold } => {
            higher_better(result, "first_time_success_rate", *threshold)
        }
        EvalMetric::MttrPodCrash { threshold_ms } => {
            lower_better(result, "mttr_pod_crash_ms", *threshold_ms as f64)
        }
        EvalMetric::MttrNodeOffline { threshold_ms } => {
            lower_better(result, "mttr_node_offline_ms", *threshold_ms as f64)
        }
        EvalMetric::MttrDiskFull { threshold_ms } => {
            lower_better(result, "mttr_disk_full_ms", *threshold_ms as f64)
        }
        EvalMetric::ScaleElasticityP50 { threshold_ms } => {
            lower_better(result, "scale_elasticity_p50_ms", *threshold_ms as f64)
        }
        EvalMetric::ScaleElasticityP99 { threshold_ms } => {
            lower_better(result, "scale_elasticity_p99_ms", *threshold_ms as f64)
        }
        EvalMetric::ResourceOverheadCpu { threshold } => {
            lower_better(result, "resource_overhead_cpu", *threshold)
        }
        EvalMetric::ResourceOverheadMemory { threshold } => {
            lower_better(result, "resource_overhead_memory", *threshold)
        }
        EvalMetric::GatewayLatencyP50 { threshold_ms } => {
            lower_better(result, "gateway_latency_p50_ms", *threshold_ms as f64)
        }
        EvalMetric::GatewayLatencyP99 { threshold_ms } => {
            lower_better(result, "gateway_latency_p99_ms", *threshold_ms as f64)
        }
        EvalMetric::StabilitySla { threshold } => {
            higher_better(result, "stability_sla", *threshold)
        }
        _ => (0.0, true),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_result(output: serde_json::Value) -> EvalResult {
        EvalResult {
            case_id: "t1".into(),
            passed: true,
            metrics: vec![],
            agent_output: output,
            duration_ms: 0,
            token_usage: 0,
            cost: 0.0,
            error: None,
            steps: vec![],
            trace_json: None,
        }
    }

    fn mk_case() -> EvalCase {
        EvalCase {
            id: "t1".into(),
            name: "test".into(),
            input: serde_json::Value::Null,
            expected_output: None,
            expected_tools: None,
            metrics: vec![],
            tags: vec![],
        }
    }

    #[test]
    fn deployment_time_lower_better() {
        let result = mk_result(serde_json::json!({"deployment_time_ms": 42000}));
        let metric = EvalMetric::DeploymentTime {
            threshold_ms: 60000,
        };
        let (v, p) = compute(&metric, &mk_case(), &result);
        assert_eq!(v, 42000.0);
        assert!(p);
    }

    #[test]
    fn sla_higher_better() {
        let result = mk_result(serde_json::json!({"stability_sla": 0.995}));
        let metric = EvalMetric::StabilitySla { threshold: 0.999 };
        let (v, p) = compute(&metric, &mk_case(), &result);
        assert_eq!(v, 0.995);
        assert!(!p);
    }

    #[test]
    fn missing_measurement_is_placeholder() {
        let result = mk_result(serde_json::Value::Null);
        let metric = EvalMetric::MttrPodCrash { threshold_ms: 5000 };
        assert_eq!(compute(&metric, &mk_case(), &result), (0.0, true));
    }
}
