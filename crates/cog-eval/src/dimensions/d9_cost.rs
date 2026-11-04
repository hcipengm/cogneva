//! D9 — 成本与资源效率指标计算。

use crate::dataset::EvalCase;
use crate::metric::{EvalMetric, EvalResult};

/// 计算 D9 指标。
pub fn compute(metric: &EvalMetric, _case: &EvalCase, result: &EvalResult) -> (f64, bool) {
    match metric {
        // 这两条只在花销/用量真的量到时才有意义。调用方（runner）在读数缺失时
        // 已经跳过整条指标；这里再挡一次，是因为 `dimensions::compute` 还能从
        // 别处被直接调用，而 `Some` 之外的分支在这里等于「没量到」。
        EvalMetric::CostPerStep { threshold } => match result.cost {
            Some(cost) => {
                let steps = result.steps.len().max(1) as f64;
                let cost_per_step = cost / steps;
                (cost_per_step, cost_per_step <= *threshold)
            }
            None => (0.0, true),
        },
        EvalMetric::TokenPerStep { threshold } => match result.token_usage {
            Some(tokens) => {
                let steps = result.steps.len().max(1) as f64;
                let token_per_step = tokens as f64 / steps;
                (token_per_step, token_per_step <= *threshold)
            }
            None => (0.0, true),
        },
        EvalMetric::InferenceLatency { threshold_ms } => {
            let latency = result.duration_ms as f64;
            (latency, latency <= *threshold_ms as f64)
        }
        EvalMetric::TimeToFirstToken { threshold_ms } => {
            // 简化：用首步耗时近似 TTFT
            let ttft = result
                .steps
                .first()
                .map(|s| s.duration_ms)
                .unwrap_or(result.duration_ms) as f64;
            (ttft, ttft <= *threshold_ms as f64)
        }
        _ => (0.0, true),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metric::StepRecord;
    use crate::test_support::mk_case;

    fn mk_result(
        steps: Vec<StepRecord>,
        duration_ms: u64,
        token_usage: Option<u64>,
        cost: Option<f64>,
    ) -> EvalResult {
        EvalResult {
            case_id: "t1".into(),
            passed: true,
            metrics: vec![],
            agent_output: serde_json::Value::Null,
            duration_ms,
            token_usage,
            cost,
            error: None,
            steps,
            trace_json: None,
        }
    }

    #[test]
    fn cost_per_step() {
        let result = mk_result(
            vec![
                StepRecord {
                    step_index: 0,
                    action_type: "click".into(),
                    action_params: serde_json::Value::Null,
                    thought: None,
                    duration_ms: 10,
                    success: true,
                    tool_calls: vec![]
                };
                4
            ],
            100,
            Some(50),
            Some(0.004),
        );
        let metric = EvalMetric::CostPerStep { threshold: 0.002 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert_eq!(value, 0.001); // 0.004 / 4
        assert!(passed);
    }

    #[test]
    fn token_per_step() {
        let result = mk_result(
            vec![
                StepRecord {
                    step_index: 0,
                    action_type: "click".into(),
                    action_params: serde_json::Value::Null,
                    thought: None,
                    duration_ms: 10,
                    success: true,
                    tool_calls: vec![]
                };
                5
            ],
            100,
            Some(100),
            Some(0.001),
        );
        let metric = EvalMetric::TokenPerStep { threshold: 25.0 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert_eq!(value, 20.0); // 100 / 5
        assert!(passed);
    }

    #[test]
    fn inference_latency_within_threshold() {
        let result = mk_result(vec![], 150, Some(50), Some(0.001));
        let metric = EvalMetric::InferenceLatency { threshold_ms: 200 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert_eq!(value, 150.0);
        assert!(passed);
    }

    #[test]
    fn inference_latency_exceeds_threshold() {
        let result = mk_result(vec![], 300, Some(50), Some(0.001));
        let metric = EvalMetric::InferenceLatency { threshold_ms: 200 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert_eq!(value, 300.0);
        assert!(!passed);
    }

    #[test]
    fn time_to_first_token_from_first_step() {
        let result = mk_result(
            vec![
                StepRecord {
                    step_index: 0,
                    action_type: "click".into(),
                    action_params: serde_json::Value::Null,
                    thought: None,
                    duration_ms: 45,
                    success: true,
                    tool_calls: vec![],
                },
                StepRecord {
                    step_index: 1,
                    action_type: "type".into(),
                    action_params: serde_json::Value::Null,
                    thought: None,
                    duration_ms: 55,
                    success: true,
                    tool_calls: vec![],
                },
            ],
            100,
            Some(50),
            Some(0.001),
        );
        let metric = EvalMetric::TimeToFirstToken { threshold_ms: 100 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert_eq!(value, 45.0);
        assert!(passed);
    }

    #[test]
    fn time_to_first_token_fallback() {
        let result = mk_result(vec![], 120, Some(50), Some(0.001));
        let metric = EvalMetric::TimeToFirstToken { threshold_ms: 100 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert_eq!(value, 120.0); // fallback to duration_ms when no steps
        assert!(!passed);
    }
}
