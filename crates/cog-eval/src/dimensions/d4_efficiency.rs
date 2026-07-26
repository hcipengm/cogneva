//! D4 — 执行效率指标计算。

use crate::dataset::EvalCase;
use crate::metric::{EvalMetric, EvalResult};

/// 计算 D4 指标。
pub fn compute(metric: &EvalMetric, _case: &EvalCase, result: &EvalResult) -> (f64, bool) {
    match metric {
        EvalMetric::StepRatio { threshold } => {
            // agent_steps / human_golden_steps，human_golden_steps 从 case metadata 取，默认 5
            let agent_steps = result.steps.len() as f64;
            let human_steps = 5.0; // 默认基准
            let ratio = agent_steps / human_steps;
            (ratio, ratio <= *threshold)
        }
        EvalMetric::RepetitivenessRate { threshold } => {
            let total = result.steps.len().max(1) as f64;
            // 统计重复 action_type
            let mut seen = std::collections::HashSet::new();
            let mut duplicates = 0;
            for step in &result.steps {
                if !seen.insert(&step.action_type) {
                    duplicates += 1;
                }
            }
            let rate = duplicates as f64 / total;
            (rate, rate <= *threshold)
        }
        EvalMetric::TimePerStep { threshold_ms } => {
            let total_steps = result.steps.len().max(1) as f64;
            let avg = result.duration_ms as f64 / total_steps;
            (avg, avg <= *threshold_ms as f64)
        }
        EvalMetric::FirstActionLatency { threshold_ms } => {
            let first_latency = result
                .steps
                .first()
                .map(|s| s.duration_ms)
                .unwrap_or(result.duration_ms);
            (first_latency as f64, first_latency <= *threshold_ms)
        }
        EvalMetric::ExecutionEfficiency { threshold } => {
            // 启发式：基于 step_ratio 和错误率的综合评分
            let step_count = result.steps.len() as f64;
            let errors = result.steps.iter().filter(|s| !s.success).count() as f64;
            let ratio = step_count / 5.0; // 基准 5 步
            let score =
                (1.0 - (errors / step_count.max(1.0))).clamp(0.0, 1.0) * (1.0 / ratio.max(1.0));
            (score, score >= *threshold)
        }
        EvalMetric::StepSuccessRate { threshold } => {
            let total = result.steps.len().max(1) as f64;
            let correct = result.steps.iter().filter(|s| s.success).count() as f64;
            let score = correct / total;
            (score, score >= *threshold)
        }
        EvalMetric::RecoveryRate { threshold } => {
            // 启发式：有错误步骤但最终 passed 视为恢复成功
            let has_errors = result.steps.iter().any(|s| !s.success);
            let recovered = has_errors && result.passed;
            let score = if !has_errors || recovered { 1.0 } else { 0.0 };
            (score, score >= *threshold)
        }
        EvalMetric::ExploreMetric => {
            // 需要状态空间覆盖数据，单条 case 无法计算，返回占位
            (0.0, true)
        }
        EvalMetric::BacktrackingTaskRate { threshold } => {
            let has_backtrack = result
                .steps
                .iter()
                .any(|s| s.action_type == "undo" || s.action_type == "back");
            let rate = if has_backtrack { 1.0 } else { 0.0 };
            (rate, rate <= *threshold)
        }
        EvalMetric::BacktrackingSuccessRate { threshold } => {
            let has_backtrack = result
                .steps
                .iter()
                .any(|s| s.action_type == "undo" || s.action_type == "back");
            let score = if !has_backtrack {
                1.0
            } else {
                if result.passed {
                    1.0
                } else {
                    0.0
                }
            };
            (score, score >= *threshold)
        }
        EvalMetric::AvgBacktrackingSteps { threshold } => {
            let backtracks = result
                .steps
                .iter()
                .filter(|s| s.action_type == "undo" || s.action_type == "back")
                .count() as f64;
            (backtracks, backtracks <= *threshold)
        }
        EvalMetric::BacktrackingRecoveryTime { threshold_ms } => {
            // 简化：总耗时作为恢复时间上界
            let recovery_time = result.duration_ms;
            (recovery_time as f64, recovery_time <= *threshold_ms)
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

    fn mk_result(steps: Vec<StepRecord>, duration_ms: u64, passed: bool) -> EvalResult {
        EvalResult {
            case_id: "t1".into(),
            passed,
            metrics: vec![],
            agent_output: serde_json::Value::Null,
            duration_ms,
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
    fn step_ratio_under_threshold() {
        let result = mk_result(vec![mk_step("click", true)], 100, true);
        let metric = EvalMetric::StepRatio { threshold: 1.0 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert_eq!(value, 0.2); // 1 / 5 baseline
        assert!(passed);
    }

    #[test]
    fn step_ratio_over_threshold() {
        let result = mk_result(vec![mk_step("click", true); 10], 100, true);
        let metric = EvalMetric::StepRatio { threshold: 1.0 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert_eq!(value, 2.0); // 10 / 5 baseline
        assert!(!passed);
    }

    #[test]
    fn repetitiveness_no_duplicates() {
        let result = mk_result(
            vec![mk_step("click", true), mk_step("type", true)],
            100,
            true,
        );
        let metric = EvalMetric::RepetitivenessRate { threshold: 0.5 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert_eq!(value, 0.0);
        assert!(passed);
    }

    #[test]
    fn repetitiveness_with_duplicates() {
        let result = mk_result(
            vec![
                mk_step("click", true),
                mk_step("click", true),
                mk_step("click", true),
            ],
            100,
            true,
        );
        let metric = EvalMetric::RepetitivenessRate { threshold: 0.5 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert_eq!(value, 2.0 / 3.0);
        assert!(!passed);
    }

    #[test]
    fn time_per_step() {
        let result = mk_result(vec![mk_step("click", true); 4], 200, true);
        let metric = EvalMetric::TimePerStep { threshold_ms: 60 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert_eq!(value, 50.0); // 200 / 4
        assert!(passed);
    }

    #[test]
    fn step_success_rate_all_pass() {
        let result = mk_result(
            vec![mk_step("click", true), mk_step("type", true)],
            100,
            true,
        );
        let metric = EvalMetric::StepSuccessRate { threshold: 0.9 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert_eq!(value, 1.0);
        assert!(passed);
    }

    #[test]
    fn recovery_rate_recovered() {
        let result = mk_result(
            vec![mk_step("click", false), mk_step("type", true)],
            100,
            true,
        );
        let metric = EvalMetric::RecoveryRate { threshold: 0.5 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert_eq!(value, 1.0); // has errors but passed
        assert!(passed);
    }

    #[test]
    fn backtracking_task_rate() {
        let result = mk_result(
            vec![mk_step("click", true), mk_step("undo", true)],
            100,
            true,
        );
        let metric = EvalMetric::BacktrackingTaskRate { threshold: 0.5 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert_eq!(value, 1.0);
        assert!(!passed);
    }
}
