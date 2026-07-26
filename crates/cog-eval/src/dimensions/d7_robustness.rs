//! D7 — 鲁棒性与可靠性指标计算。
//! 大部分指标需要多次运行或压力测试数据，单条 case 层面仅提供启发式近似。

use crate::dataset::EvalCase;
use crate::metric::{EvalMetric, EvalResult};

/// 计算 D7 指标。
pub fn compute(metric: &EvalMetric, _case: &EvalCase, result: &EvalResult) -> (f64, bool) {
    match metric {
        EvalMetric::OutputConsistency { threshold } => {
            // 单条 case 无法计算多次一致性，退化为 passed 状态
            let score = if result.passed { 1.0 } else { 0.0 };
            (score, score >= *threshold)
        }
        EvalMetric::TrajectoryConsistency { threshold } => {
            // 需要多次运行对比，单条 case 退化为 passed
            let score = if result.passed { 1.0 } else { 0.0 };
            (score, score >= *threshold)
        }
        EvalMetric::ScoreConsistency { threshold } => {
            // 数据集级指标
            let score = 1.0;
            (score, score >= *threshold)
        }
        EvalMetric::ToolFailureRecovery { threshold } => {
            // 有工具调用失败但最终 passed 视为恢复
            let tool_failures = result
                .steps
                .iter()
                .filter(|s| !s.success && !s.tool_calls.is_empty())
                .count();
            let score = if tool_failures == 0 || result.passed {
                1.0
            } else {
                0.0
            };
            (score, score >= *threshold)
        }
        EvalMetric::EnvironmentAdaptationRate { threshold } => {
            let score = 1.0; // 占位：需要环境变化测试
            (score, score >= *threshold)
        }
        EvalMetric::HallucinationRate { threshold } => {
            // 启发式：输出中包含 "I don't know" 或 error 时可能未幻觉
            let output_str = result.agent_output.to_string().to_lowercase();
            let hallucinated = output_str.contains("i think")
                && !output_str.contains("source")
                && !output_str.contains("reference");
            let rate = if hallucinated { 1.0 } else { 0.0 };
            (rate, rate <= *threshold)
        }
        EvalMetric::ContextRetentionScore { threshold } => {
            // 启发式：步数少时上下文保留更好
            let step_count = result.steps.len() as f64;
            let score = (1.0 / (1.0 + step_count / 10.0)).clamp(0.0, 1.0);
            (score, score >= *threshold)
        }
        EvalMetric::LongTailSuccessRate { threshold }
        | EvalMetric::HighLoadSuccessRate { threshold }
        | EvalMetric::NoisyInputSuccessRate { threshold } => {
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

    fn mk_step(success: bool, tool_calls: Vec<String>) -> StepRecord {
        StepRecord {
            step_index: 0,
            action_type: "click".into(),
            action_params: serde_json::Value::Null,
            thought: None,
            duration_ms: 10,
            success,
            tool_calls,
        }
    }

    fn mk_result(
        steps: Vec<StepRecord>,
        passed: bool,
        agent_output: serde_json::Value,
    ) -> EvalResult {
        EvalResult {
            case_id: "t1".into(),
            passed,
            metrics: vec![],
            agent_output,
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
    fn output_consistency_passed() {
        let result = mk_result(vec![], true, serde_json::Value::Null);
        let metric = EvalMetric::OutputConsistency { threshold: 0.5 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert_eq!(value, 1.0);
        assert!(passed);
    }

    #[test]
    fn output_consistency_failed() {
        let result = mk_result(vec![], false, serde_json::Value::Null);
        let metric = EvalMetric::OutputConsistency { threshold: 0.5 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert_eq!(value, 0.0);
        assert!(!passed);
    }

    #[test]
    fn tool_failure_recovery_recovered() {
        let result = mk_result(
            vec![mk_step(false, vec!["tool1".into()]), mk_step(true, vec![])],
            true,
            serde_json::Value::Null,
        );
        let metric = EvalMetric::ToolFailureRecovery { threshold: 0.5 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert_eq!(value, 1.0);
        assert!(passed);
    }

    #[test]
    fn tool_failure_recovery_not_recovered() {
        let result = mk_result(
            vec![mk_step(false, vec!["tool1".into()])],
            false,
            serde_json::Value::Null,
        );
        let metric = EvalMetric::ToolFailureRecovery { threshold: 0.5 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert_eq!(value, 0.0);
        assert!(!passed);
    }

    #[test]
    fn hallucination_rate_no_hallucination() {
        let result = mk_result(vec![], true, serde_json::json!("source: reference"));
        let metric = EvalMetric::HallucinationRate { threshold: 0.1 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert_eq!(value, 0.0);
        assert!(passed);
    }

    #[test]
    fn hallucination_rate_detected() {
        let result = mk_result(vec![], true, serde_json::json!("i think this is correct"));
        let metric = EvalMetric::HallucinationRate { threshold: 0.1 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert_eq!(value, 1.0);
        assert!(!passed);
    }

    #[test]
    fn context_retention_few_steps() {
        let result = mk_result(
            vec![mk_step(true, vec![]); 2],
            true,
            serde_json::Value::Null,
        );
        let metric = EvalMetric::ContextRetentionScore { threshold: 0.5 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert!(value > 0.8);
        assert!(passed);
    }

    #[test]
    fn context_retention_many_steps() {
        let result = mk_result(
            vec![mk_step(true, vec![]); 20],
            true,
            serde_json::Value::Null,
        );
        let metric = EvalMetric::ContextRetentionScore { threshold: 0.5 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert!(value < 0.5);
        assert!(!passed);
    }
}
