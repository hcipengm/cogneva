//! D1 — 任务完成与结果质量指标计算。

use crate::dataset::EvalCase;
use crate::metric::{EvalMetric, EvalResult};

/// 计算 D1 指标。
pub fn compute(metric: &EvalMetric, _case: &EvalCase, result: &EvalResult) -> (f64, bool) {
    match metric {
        EvalMetric::StepAccuracy { threshold } => {
            let total = result.steps.len().max(1) as f64;
            let correct = result.steps.iter().filter(|s| s.success).count() as f64;
            let score = correct / total;
            (score, score >= *threshold)
        }
        EvalMetric::ClickAccuracy { threshold } => {
            let total = result
                .steps
                .iter()
                .filter(|s| s.action_type == "click")
                .count() as f64;
            let correct = result
                .steps
                .iter()
                .filter(|s| s.action_type == "click" && s.success)
                .count() as f64;
            let score = if total > 0.0 { correct / total } else { 1.0 };
            (score, score >= *threshold)
        }
        EvalMetric::TypeAccuracy { threshold } => {
            let total = result
                .steps
                .iter()
                .filter(|s| s.action_type == "type")
                .count() as f64;
            let correct = result
                .steps
                .iter()
                .filter(|s| s.action_type == "type" && s.success)
                .count() as f64;
            let score = if total > 0.0 { correct / total } else { 1.0 };
            (score, score >= *threshold)
        }
        EvalMetric::ScrollAccuracy { threshold } => {
            let total = result
                .steps
                .iter()
                .filter(|s| s.action_type == "scroll")
                .count() as f64;
            let correct = result
                .steps
                .iter()
                .filter(|s| s.action_type == "scroll" && s.success)
                .count() as f64;
            let score = if total > 0.0 { correct / total } else { 1.0 };
            (score, score >= *threshold)
        }
        EvalMetric::NavigateAccuracy { threshold } => {
            let total = result
                .steps
                .iter()
                .filter(|s| s.action_type == "navigate")
                .count() as f64;
            let correct = result
                .steps
                .iter()
                .filter(|s| s.action_type == "navigate" && s.success)
                .count() as f64;
            let score = if total > 0.0 { correct / total } else { 1.0 };
            (score, score >= *threshold)
        }
        EvalMetric::OperationF1 { threshold } => {
            // 简化为 StepAccuracy 的别名
            let total = result.steps.len().max(1) as f64;
            let correct = result.steps.iter().filter(|s| s.success).count() as f64;
            let score = correct / total;
            (score, score >= *threshold)
        }
        EvalMetric::ElementAccuracy { threshold } => {
            // 需要 expected element 信息，简化为 StepAccuracy
            let total = result.steps.len().max(1) as f64;
            let correct = result.steps.iter().filter(|s| s.success).count() as f64;
            let score = correct / total;
            (score, score >= *threshold)
        }
        EvalMetric::GoalFulfillment { threshold } => {
            // LLM-as-a-Judge 占位：基于 passed 状态近似
            let score = if result.passed { 1.0 } else { 0.0 };
            (score, score >= *threshold)
        }
        EvalMetric::TaskSuccessRate { threshold }
        | EvalMetric::StrictSuccessRate { threshold }
        | EvalMetric::PartialSuccessRate { threshold } => {
            // Dataset-level 指标，在单条 case 上退化为 passed
            let score = if result.passed { 1.0 } else { 0.0 };
            (score, score >= *threshold)
        }
        EvalMetric::PassAtK { threshold, .. } => {
            let score = if result.passed { 1.0 } else { 0.0 };
            (score, score >= *threshold)
        }
        EvalMetric::ResolveRate { threshold } => {
            let score = if result.passed { 1.0 } else { 0.0 };
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

    fn mk_result_with_steps(steps: Vec<StepRecord>, passed: bool) -> EvalResult {
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
    fn step_accuracy_all_success() {
        let result =
            mk_result_with_steps(vec![mk_step("click", true), mk_step("type", true)], true);
        let metric = EvalMetric::StepAccuracy { threshold: 0.5 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert_eq!(value, 1.0);
        assert!(passed);
    }

    #[test]
    fn step_accuracy_half_success() {
        let result =
            mk_result_with_steps(vec![mk_step("click", true), mk_step("type", false)], false);
        let metric = EvalMetric::StepAccuracy { threshold: 0.6 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert_eq!(value, 0.5);
        assert!(!passed);
    }

    #[test]
    fn click_accuracy_with_no_clicks() {
        let result = mk_result_with_steps(vec![mk_step("type", true)], true);
        let metric = EvalMetric::ClickAccuracy { threshold: 0.5 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert_eq!(value, 1.0);
        assert!(passed);
    }

    #[test]
    fn click_accuracy_mixed() {
        let result = mk_result_with_steps(
            vec![
                mk_step("click", true),
                mk_step("click", false),
                mk_step("type", true),
            ],
            false,
        );
        let metric = EvalMetric::ClickAccuracy { threshold: 0.5 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert_eq!(value, 0.5);
        assert!(passed);
    }

    #[test]
    fn goal_fulfillment_passed() {
        let result = mk_result_with_steps(vec![], true);
        let metric = EvalMetric::GoalFulfillment { threshold: 0.5 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert_eq!(value, 1.0);
        assert!(passed);
    }

    #[test]
    fn goal_fulfillment_failed() {
        let result = mk_result_with_steps(vec![], false);
        let metric = EvalMetric::GoalFulfillment { threshold: 0.5 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert_eq!(value, 0.0);
        assert!(!passed);
    }

    #[test]
    fn task_success_rate_passed() {
        let result = mk_result_with_steps(vec![], true);
        let metric = EvalMetric::TaskSuccessRate { threshold: 0.5 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert_eq!(value, 1.0);
        assert!(passed);
    }

    #[test]
    fn operation_f1_empty_steps() {
        let result = mk_result_with_steps(vec![], true);
        let metric = EvalMetric::OperationF1 { threshold: 0.5 };
        let (value, _passed) = compute(&metric, &mk_case(), &result);
        // empty steps -> total = 1, correct = 0 -> score = 0.0
        assert_eq!(value, 0.0);
    }

    #[test]
    fn navigate_accuracy_all_success() {
        let result = mk_result_with_steps(
            vec![mk_step("navigate", true), mk_step("navigate", true)],
            true,
        );
        let metric = EvalMetric::NavigateAccuracy { threshold: 0.5 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert_eq!(value, 1.0);
        assert!(passed);
    }
}
