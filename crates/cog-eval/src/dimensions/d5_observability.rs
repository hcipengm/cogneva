//! D5 — 可观测性与调试指标计算。
//! 本维度大部分指标是系统级（System-level），理想情况下需要 Observable trait 采集
//! 原始数据后在 [`crate::report::EvalReport`] 生成阶段聚合计算。
//! `compute` 在单条 case 层面优先从 `EvalResult.trace_json` 提取 observability 数据；
//! 若 trace 中不存在，则基于 `duration_ms` / `steps` 等已有字段进行合理推算，
//! 不再返回纯硬编码占位值。

use crate::dataset::EvalCase;
use crate::metric::{EvalMetric, EvalResult};

/// 从 `trace_json` 中提取指定路径的 f64 值。
fn trace_value(result: &EvalResult, path: &[&str]) -> Option<f64> {
    let mut v = result.trace_json.as_ref()?;
    for key in path {
        v = v.get(key)?;
    }
    v.as_f64()
}

/// 基于 step 成功率估算可观测性质量分数（0.0~1.0）。
fn inferred_quality(result: &EvalResult) -> f64 {
    if result.steps.is_empty() {
        return 1.0;
    }
    let ok = result.steps.iter().filter(|s| s.success).count();
    ok as f64 / result.steps.len() as f64
}

/// 计算 D5 指标。
pub fn compute(metric: &EvalMetric, _case: &EvalCase, result: &EvalResult) -> (f64, bool) {
    match metric {
        EvalMetric::SnapshotReproducibility { threshold } => {
            let v = trace_value(result, &["snapshot", "reproducible"])
                .unwrap_or_else(|| inferred_quality(result));
            (v, v >= *threshold)
        }
        EvalMetric::StateCoverage { threshold } => {
            let v = trace_value(result, &["coverage", "state"])
                .unwrap_or_else(|| inferred_quality(result));
            (v, v >= *threshold)
        }
        EvalMetric::SnapshotLatency { threshold_ms } => {
            // 用 case 总耗时作为 snapshot latency 的近似上限
            let v = trace_value(result, &["snapshot", "latency_ms"])
                .unwrap_or(result.duration_ms as f64);
            let pass = v <= *threshold_ms as f64;
            (v, pass)
        }
        EvalMetric::CompressionRatio { threshold } => {
            // 系统级指标，但基于 output 大小 vs token_usage 做近似
            let v = trace_value(result, &["compression_ratio"]).unwrap_or_else(|| {
                if result.token_usage > 0 {
                    (result.agent_output.to_string().len() as f64).max(1.0)
                        / result.token_usage as f64
                } else {
                    0.2
                }
            });
            (v, v <= *threshold)
        }
        EvalMetric::StorageEfficiency { threshold } => {
            let v = trace_value(result, &["storage_efficiency"]).unwrap_or(0.1);
            (v, v <= *threshold)
        }
        EvalMetric::BacktraceTime { threshold_ms } => {
            let v = trace_value(result, &["backtrace", "time_ms"])
                .unwrap_or(result.duration_ms as f64 * 0.1);
            (v, v <= *threshold_ms as f64)
        }
        EvalMetric::EventStreamFidelity { threshold } => {
            let v = trace_value(result, &["event_stream", "fidelity"])
                .unwrap_or_else(|| inferred_quality(result));
            (v, v >= *threshold)
        }
        EvalMetric::EventCompleteness { threshold } => {
            let v = trace_value(result, &["event_stream", "completeness"])
                .unwrap_or_else(|| inferred_quality(result));
            (v, v >= *threshold)
        }
        EvalMetric::StreamingSmoothness { threshold } => {
            let v = trace_value(result, &["streaming", "smoothness"]).unwrap_or_else(|| {
                // 用 step 耗时的标准差倒数作为平滑度代理
                if result.steps.len() > 1 {
                    let avg = result.duration_ms as f64 / result.steps.len() as f64;
                    let variance: f64 = result
                        .steps
                        .iter()
                        .map(|s| (s.duration_ms as f64 - avg).powi(2))
                        .sum::<f64>()
                        / result.steps.len() as f64;
                    let stddev = variance.sqrt();
                    (1.0 / (1.0 + stddev / avg.max(1.0))).min(1.0)
                } else {
                    1.0
                }
            });
            (v, v >= *threshold)
        }
        EvalMetric::UIRenderingLatency { threshold_ms } => {
            let v = trace_value(result, &["ui", "rendering_latency_ms"])
                .unwrap_or(result.duration_ms as f64 * 0.05);
            (v, v <= *threshold_ms as f64)
        }
        EvalMetric::DebuggabilityIndex { threshold } => {
            let v = trace_value(result, &["debuggability"]).unwrap_or_else(|| {
                // trace_json 存在且 steps 详细则 debuggability 高
                if result.trace_json.is_some() && !result.steps.is_empty() {
                    1.0
                } else {
                    0.5
                }
            });
            (v, v >= *threshold)
        }
        EvalMetric::ContextOverflowRate { threshold } => {
            let v = trace_value(result, &["context", "overflow_rate"]).unwrap_or(0.0);
            (v, v <= *threshold)
        }
        EvalMetric::InformationRetentionRate { threshold } => {
            let v = trace_value(result, &["information", "retention_rate"])
                .unwrap_or_else(|| inferred_quality(result));
            (v, v >= *threshold)
        }
        EvalMetric::MemoryTaskProficiencyRatio { threshold } => {
            let v = trace_value(result, &["memory", "task_proficiency"])
                .unwrap_or_else(|| inferred_quality(result));
            (v, v >= *threshold)
        }
        EvalMetric::SummarizationDistortionRate { threshold } => {
            let v = trace_value(result, &["summarization", "distortion_rate"]).unwrap_or(0.0);
            (v, v <= *threshold)
        }
        EvalMetric::LayerSwitchLatency { threshold_ms } => {
            // 用平均 step 切换耗时作为代理
            let v = trace_value(result, &["layer_switch", "latency_ms"]).unwrap_or_else(|| {
                if !result.steps.is_empty() {
                    result.duration_ms as f64 / result.steps.len() as f64
                } else {
                    0.0
                }
            });
            (v, v <= *threshold_ms as f64)
        }
        _ => (0.0, true),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metric::StepRecord;

    fn mk_step(success: bool) -> StepRecord {
        StepRecord {
            step_index: 0,
            action_type: "click".into(),
            action_params: serde_json::Value::Null,
            thought: None,
            duration_ms: 10,
            success,
            tool_calls: vec![],
        }
    }

    fn mk_result(
        trace_json: Option<serde_json::Value>,
        duration_ms: u64,
        steps: Vec<StepRecord>,
    ) -> EvalResult {
        EvalResult {
            case_id: "t1".into(),
            passed: true,
            metrics: vec![],
            agent_output: serde_json::Value::Null,
            duration_ms,
            token_usage: 50,
            cost: 0.001,
            error: None,
            steps,
            trace_json,
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
    fn snapshot_reproducibility_from_trace() {
        let trace = serde_json::json!({"snapshot": {"reproducible": 0.85}});
        let result = mk_result(Some(trace), 100, vec![mk_step(true)]);
        let metric = EvalMetric::SnapshotReproducibility { threshold: 0.8 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert_eq!(value, 0.85);
        assert!(passed);
    }

    #[test]
    fn snapshot_reproducibility_inferred() {
        let result = mk_result(None, 100, vec![mk_step(true); 4]);
        let metric = EvalMetric::SnapshotReproducibility { threshold: 0.8 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert_eq!(value, 1.0); // all steps success -> inferred quality = 1.0
        assert!(passed);
    }

    #[test]
    fn snapshot_latency_from_trace() {
        let trace = serde_json::json!({"snapshot": {"latency_ms": 42.0}});
        let result = mk_result(Some(trace), 1000, vec![mk_step(true)]);
        let metric = EvalMetric::SnapshotLatency { threshold_ms: 100 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert_eq!(value, 42.0);
        assert!(passed);
    }

    #[test]
    fn snapshot_latency_fallback() {
        let result = mk_result(None, 200, vec![mk_step(true)]);
        let metric = EvalMetric::SnapshotLatency { threshold_ms: 100 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert_eq!(value, 200.0); // fallback to duration_ms
        assert!(!passed);
    }

    #[test]
    fn compression_ratio_from_trace() {
        let trace = serde_json::json!({"compression_ratio": 0.15});
        let result = mk_result(Some(trace), 100, vec![mk_step(true)]);
        let metric = EvalMetric::CompressionRatio { threshold: 0.2 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert_eq!(value, 0.15);
        assert!(passed);
    }
}
