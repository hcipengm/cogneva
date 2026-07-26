//! 九大维度指标计算模块。
//! 每个模块提供 `compute` 函数，接收 EvalMetric 和 EvalResult，返回 (value, passed)。

pub mod d10_deployment;
pub mod d11_code_quality;
pub mod d12_ablation;
pub mod d13_defense;
pub mod d1_outcome;
pub mod d2_planning;
pub mod d3_tool;
pub mod d4_efficiency;
pub mod d5_observability;
pub mod d6_safety;
pub mod d7_robustness;
pub mod d8_collaboration;
pub mod d9_cost;

use crate::dataset::EvalCase;
use crate::metric::{EvalMetric, EvalResult};

/// 统一路由 —— 根据指标所属维度调用对应模块的 compute。
/// 每个模块对不属于自己的指标返回 `(0.0, true)`，本函数依次尝试所有维度，
/// 返回第一个非占位结果。如果全部返回占位，则最终返回 `(0.0, true)`。
type ComputeFn = fn(&EvalMetric, &EvalCase, &EvalResult) -> (f64, bool);

pub fn compute(metric: &EvalMetric, case: &EvalCase, result: &EvalResult) -> (f64, bool) {
    let dim_modules: Vec<ComputeFn> = vec![
        d1_outcome::compute,
        d2_planning::compute,
        d3_tool::compute,
        d4_efficiency::compute,
        d5_observability::compute,
        d6_safety::compute,
        d7_robustness::compute,
        d8_collaboration::compute,
        d9_cost::compute,
        d10_deployment::compute,
        d11_code_quality::compute,
        d12_ablation::compute,
        d13_defense::compute,
    ];

    for compute_fn in dim_modules {
        let (value, passed) = compute_fn(metric, case, result);
        // 占位值为 (0.0, true)，如果 value != 0.0 或 passed != true，说明该模块处理了此指标
        if value != 0.0 || !passed {
            return (value, passed);
        }
    }
    (0.0, true)
}

#[cfg(test)]
mod tests {
    use super::*;

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

    fn mk_result(passed: bool) -> EvalResult {
        EvalResult {
            case_id: "t1".into(),
            passed,
            metrics: vec![],
            agent_output: serde_json::Value::Null,
            duration_ms: 100,
            token_usage: 50,
            cost: 0.001,
            error: None,
            steps: vec![],
            trace_json: None,
        }
    }

    #[test]
    fn router_dispatches_d1_step_accuracy() {
        let case = mk_case();
        let result = mk_result(true);
        let metric = EvalMetric::StepAccuracy { threshold: 0.5 };
        let (value, passed) = compute(&metric, &case, &result);
        // 0 steps -> total = 1, correct = 0 -> score = 0.0
        assert_eq!(value, 0.0);
        assert!(!passed);
    }

    #[test]
    fn router_dispatches_d2_plan_quality() {
        let case = mk_case();
        let result = mk_result(true);
        let metric = EvalMetric::PlanQuality { threshold: 0.5 };
        let (value, passed) = compute(&metric, &case, &result);
        // 0 steps -> base=1.5, penalty=0 -> score=0.5
        assert_eq!(value, 0.5);
        assert!(passed);
    }

    #[test]
    fn router_dispatches_d3_tool_selection() {
        let case = mk_case();
        let result = mk_result(true);
        let metric = EvalMetric::ToolSelection { threshold: 0.5 };
        let (value, passed) = compute(&metric, &case, &result);
        // no tool calls -> returns (1.0, true)
        assert_eq!(value, 1.0);
        assert!(passed);
    }

    #[test]
    fn router_dispatches_d4_step_ratio() {
        let case = mk_case();
        let result = mk_result(true);
        let metric = EvalMetric::StepRatio { threshold: 0.5 };
        let (value, passed) = compute(&metric, &case, &result);
        // 0 steps / 5 baseline = 0.0
        assert_eq!(value, 0.0);
        assert!(passed);
    }

    #[test]
    fn router_dispatches_d5_snapshot() {
        let case = mk_case();
        let result = mk_result(true);
        let metric = EvalMetric::SnapshotReproducibility { threshold: 0.5 };
        let (value, passed) = compute(&metric, &case, &result);
        assert_eq!(value, 1.0);
        assert!(passed);
    }

    #[test]
    fn router_dispatches_d6_harmful_rate() {
        let case = mk_case();
        let result = mk_result(true);
        let metric = EvalMetric::HarmfulOutputRate { threshold: 0.1 };
        let (value, passed) = compute(&metric, &case, &result);
        assert_eq!(value, 0.0);
        assert!(passed);
    }

    #[test]
    fn router_dispatches_d7_output_consistency() {
        let case = mk_case();
        let result = mk_result(true);
        let metric = EvalMetric::OutputConsistency { threshold: 0.5 };
        let (value, passed) = compute(&metric, &case, &result);
        // passed=true -> score=1.0
        assert_eq!(value, 1.0);
        assert!(passed);
    }

    #[test]
    fn router_dispatches_d8_collaboration_success() {
        let case = mk_case();
        let result = mk_result(true);
        let metric = EvalMetric::CollaborationSuccessRate { threshold: 0.5 };
        let (value, passed) = compute(&metric, &case, &result);
        // passed=true -> score=1.0
        assert_eq!(value, 1.0);
        assert!(passed);
    }

    #[test]
    fn router_dispatches_d9_cost_per_step() {
        let case = mk_case();
        let result = mk_result(true);
        let metric = EvalMetric::CostPerStep { threshold: 0.5 };
        let (value, passed) = compute(&metric, &case, &result);
        // cost=0.001 / 1 step = 0.001
        assert_eq!(value, 0.001);
        assert!(passed);
    }
}
