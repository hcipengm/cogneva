//! D2 — 规划与推理质量指标计算（LLM-as-a-Judge）。

use crate::dataset::EvalCase;
use crate::metric::{EvalMetric, EvalResult};

/// 计算 D2 指标。
/// 核心指标（PQ, PA, LC）需要 LLM-as-a-Judge 评估。此处提供基于启发式的近似计算，
/// 精确评分需通过 [`crate::judge::PlanJudge`] / [`crate::judge::LogicJudge`] 完成。
pub fn compute(metric: &EvalMetric, _case: &EvalCase, result: &EvalResult) -> (f64, bool) {
    match metric {
        EvalMetric::PlanQuality { threshold } => {
            // 启发式：步骤数适中（3-10步）且无明显错误时给高分
            let step_count = result.steps.len() as f64;
            let error_count = result.steps.iter().filter(|s| !s.success).count() as f64;
            let has_replan = result
                .steps
                .windows(2)
                .any(|w| w[0].action_type == "replan" || w[1].action_type == "replan");
            let base = if (3.0..=10.0).contains(&step_count) {
                2.5
            } else if step_count > 10.0 {
                2.0
            } else {
                1.5
            };
            let penalty = error_count * 0.3 + if has_replan { 0.2 } else { 0.0 };
            let score = (base - penalty).clamp(0.0, 3.0) / 3.0;
            (score, score >= *threshold)
        }
        EvalMetric::PlanAdherence { threshold } => {
            // 启发式：无 replan 步骤且无错误恢复时给高分
            let total = result.steps.len().max(1) as f64;
            let deviated = result
                .steps
                .iter()
                .filter(|s| s.action_type == "replan" || !s.success)
                .count() as f64;
            let score = 1.0 - (deviated / total);
            (score, score >= *threshold)
        }
        EvalMetric::LogicalConsistency { threshold } => {
            // 启发式：无错误步骤时给高分
            let total = result.steps.len().max(1) as f64;
            let error_steps = result.steps.iter().filter(|s| !s.success).count() as f64;
            let score = 1.0 - (error_steps / total);
            (score, score >= *threshold)
        }
        _ => (0.0, true),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metric::StepRecord;

    fn mk_step(action_type: &str, success: bool) -> StepRecord {
        StepRecord {
            step_index: 0,
            action_type: action_type.into(),
            action_params: serde_json::Value::Null,
            thought: None,
            duration_ms: 10,
            success,
            tool_calls: vec![],
        }
    }

    fn mk_result(steps: Vec<StepRecord>, passed: bool) -> EvalResult {
        EvalResult {
            case_id: "t1".into(),
            passed,
            metrics: vec![],
            agent_output: serde_json::Value::Null,
            duration_ms: 100,
            token_usage: 50,
            cost: 0.001,
            error: None,
            steps,
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
    fn plan_quality_ideal_steps() {
        let result = mk_result(
            vec![
                mk_step("click", true),
                mk_step("type", true),
                mk_step("click", true),
            ],
            true,
        );
        let metric = EvalMetric::PlanQuality { threshold: 0.5 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert!(value > 0.5);
        assert!(passed);
    }

    #[test]
    fn plan_quality_too_many_steps() {
        let result = mk_result(vec![mk_step("click", true); 15], true);
        let metric = EvalMetric::PlanQuality { threshold: 0.8 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert!(value < 0.8);
        assert!(!passed);
    }

    #[test]
    fn plan_adherence_no_deviation() {
        let result = mk_result(vec![mk_step("click", true), mk_step("type", true)], true);
        let metric = EvalMetric::PlanAdherence { threshold: 0.9 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert_eq!(value, 1.0);
        assert!(passed);
    }

    #[test]
    fn plan_adherence_with_replan() {
        let result = mk_result(
            vec![
                mk_step("click", true),
                mk_step("replan", true),
                mk_step("type", true),
            ],
            true,
        );
        let metric = EvalMetric::PlanAdherence { threshold: 0.9 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert!(value < 1.0);
        assert!(!passed);
    }

    #[test]
    fn logical_consistency_no_errors() {
        let result = mk_result(vec![mk_step("click", true), mk_step("type", true)], true);
        let metric = EvalMetric::LogicalConsistency { threshold: 0.9 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert_eq!(value, 1.0);
        assert!(passed);
    }

    #[test]
    fn logical_consistency_with_errors() {
        let result = mk_result(vec![mk_step("click", true), mk_step("type", false)], false);
        let metric = EvalMetric::LogicalConsistency { threshold: 0.9 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert_eq!(value, 0.5);
        assert!(!passed);
    }
}
