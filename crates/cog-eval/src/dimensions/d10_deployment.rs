//! D10 — 系统部署与运维指标计算。
//! 测量约定：SystemEvalHarness 把测量值写入 `agent_output[metric_name]`，
//! 本模块读取该值并与阈值比较。
//!
//! 缺失时返回 `(0.0, false)`，不是 `(0.0, true)`：**没量到不是通过**。返回通过的那
//! 一版让「生产者没接线」与「量到了并且达标」在判词上同形——2026-11-24 实测，
//! `SystemEvalHarness` 全树没有一处实例化，D10 的每一项都走的是这一支。

use crate::dataset::EvalCase;
use crate::metric::{EvalMetric, EvalResult};

fn measured(result: &EvalResult, key: &str) -> Option<f64> {
    result.agent_output.get(key).and_then(|v| v.as_f64())
}

/// 越低越好：value <= threshold 通过。
fn lower_better(result: &EvalResult, key: &str, threshold: f64) -> (f64, bool) {
    match measured(result, key) {
        Some(v) => (v, v <= threshold),
        // 这一维属于 D10、但没有读数：判「不通过」，判词留给上层说「没量到」。
        None => (0.0, false),
    }
}

/// 越高越好：value >= threshold 通过。
fn higher_better(result: &EvalResult, key: &str, threshold: f64) -> (f64, bool) {
    match measured(result, key) {
        Some(v) => (v, v >= threshold),
        // 同 `lower_better`：缺键是「没量到」，不是「达标」。
        None => (0.0, false),
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
    use crate::test_support::{mk_case, mk_result_for_output};

    /// 缺键不许读成通过：`SystemEvalHarness` 今天一处也没实例化，D10 的每一项都
    /// 走这一支，而它此前返回 `(0.0, true)`——「生产者没接线」被记成「达标」。
    #[test]
    fn d10_missing_measurement_is_not_a_pass() {
        let result = mk_result_for_output(serde_json::json!({}));
        let (v, p) = compute(
            &EvalMetric::DeploymentTime { threshold_ms: 60000 },
            &mk_case(),
            &result,
        );
        assert_eq!(v, 0.0);
        assert!(!p, "没有读数的一维被判成了通过");
        // 有读数时照旧按阈值判：低于阈值通过、高于阈值不通过。
        let slow = mk_result_for_output(serde_json::json!({"deployment_time_ms": 90000}));
        assert!(!compute(
            &EvalMetric::DeploymentTime { threshold_ms: 60000 },
            &mk_case(),
            &slow
        )
        .1);
    }

    #[test]
    fn deployment_time_lower_better() {
        let result = mk_result_for_output(serde_json::json!({"deployment_time_ms": 42000}));
        let metric = EvalMetric::DeploymentTime {
            threshold_ms: 60000,
        };
        let (v, p) = compute(&metric, &mk_case(), &result);
        assert_eq!(v, 42000.0);
        assert!(p);
    }

    #[test]
    fn sla_higher_better() {
        let result = mk_result_for_output(serde_json::json!({"stability_sla": 0.995}));
        let metric = EvalMetric::StabilitySla { threshold: 0.999 };
        let (v, p) = compute(&metric, &mk_case(), &result);
        assert_eq!(v, 0.995);
        assert!(!p);
    }

    /// 旧的契约在这一条上写的是 `(0.0, true)`——「没有读数」记成「通过」。
    /// 契约改了：缺键是「没量到」，判不通过（这一条与上面的
    /// `d10_missing_measurement_is_not_a_pass` 是同一个判据的两种输入形状：
    /// 空对象与 Null）。
    #[test]
    fn a_missing_measurement_is_not_a_placeholder_pass() {
        let result = mk_result_for_output(serde_json::Value::Null);
        let metric = EvalMetric::MttrPodCrash { threshold_ms: 5000 };
        assert_eq!(compute(&metric, &mk_case(), &result), (0.0, false));
    }
}
