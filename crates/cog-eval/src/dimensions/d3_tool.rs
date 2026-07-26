//! D3 — 工具使用质量指标计算。

use crate::dataset::EvalCase;
use crate::metric::{EvalMetric, EvalResult};

/// 计算 D3 指标。
pub fn compute(metric: &EvalMetric, _case: &EvalCase, result: &EvalResult) -> (f64, bool) {
    match metric {
        EvalMetric::ToolSelection { threshold } => {
            // 启发式：工具调用成功率作为近似
            let total_tool_calls: usize = result.steps.iter().map(|s| s.tool_calls.len()).sum();
            if total_tool_calls == 0 {
                return (1.0, true);
            }
            let error_tool_calls = result
                .steps
                .iter()
                .filter(|s| !s.success)
                .map(|s| s.tool_calls.len())
                .sum::<usize>();
            let score = 1.0 - (error_tool_calls as f64 / total_tool_calls as f64);
            (score, score >= *threshold)
        }
        EvalMetric::ToolCalling { threshold } => {
            // 启发式：参数语法有效性（工具调用无错误即认为语法有效）
            let total_tool_calls: usize = result.steps.iter().map(|s| s.tool_calls.len()).sum();
            if total_tool_calls == 0 {
                return (1.0, true);
            }
            let error_tool_calls = result
                .steps
                .iter()
                .filter(|s| !s.success)
                .map(|s| s.tool_calls.len())
                .sum::<usize>();
            let score = 1.0 - (error_tool_calls as f64 / total_tool_calls as f64);
            (score, score >= *threshold)
        }
        _ => (0.0, true),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metric::StepRecord;

    fn mk_step(action_type: &str, success: bool, tool_calls: Vec<String>) -> StepRecord {
        StepRecord {
            step_index: 0,
            action_type: action_type.into(),
            action_params: serde_json::Value::Null,
            thought: None,
            duration_ms: 10,
            success,
            tool_calls,
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
    fn tool_selection_no_tools() {
        let result = mk_result(vec![mk_step("click", true, vec![])], true);
        let metric = EvalMetric::ToolSelection { threshold: 0.5 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert_eq!(value, 1.0);
        assert!(passed);
    }

    #[test]
    fn tool_selection_all_success() {
        let result = mk_result(vec![mk_step("tool_call", true, vec!["tool1".into()])], true);
        let metric = EvalMetric::ToolSelection { threshold: 0.5 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert_eq!(value, 1.0);
        assert!(passed);
    }

    #[test]
    fn tool_selection_some_failed() {
        let result = mk_result(
            vec![
                mk_step("tool_call", true, vec!["tool1".into()]),
                mk_step("tool_call", false, vec!["tool2".into()]),
            ],
            false,
        );
        let metric = EvalMetric::ToolSelection { threshold: 0.6 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert_eq!(value, 0.5);
        assert!(!passed);
    }

    #[test]
    fn tool_calling_no_tools() {
        let result = mk_result(vec![mk_step("click", true, vec![])], true);
        let metric = EvalMetric::ToolCalling { threshold: 0.5 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert_eq!(value, 1.0);
        assert!(passed);
    }
}
