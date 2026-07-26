//! D12 — 消融实验与进化指标计算。
//! delta 类指标越高越好（正增量）；EvolutionConvergenceRate 越低越好（更快收敛）。
//! 测量值由 AblationRunner / 进化跟踪器写入 agent_output[metric_name]。

use crate::dataset::EvalCase;
use crate::metric::{EvalMetric, EvalResult};

fn delta(result: &EvalResult, key: &str, threshold: f64) -> (f64, bool) {
    match result.agent_output.get(key).and_then(|v| v.as_f64()) {
        Some(v) => (v, v >= threshold),
        None => (0.0, true),
    }
}

pub fn compute(metric: &EvalMetric, _case: &EvalCase, result: &EvalResult) -> (f64, bool) {
    match metric {
        EvalMetric::AblationDeltaSelfReview { threshold } => {
            delta(result, "ablation_delta_self_review", *threshold)
        }
        EvalMetric::AblationDeltaPge { threshold } => {
            delta(result, "ablation_delta_pge", *threshold)
        }
        EvalMetric::AblationDeltaRalph { threshold } => {
            delta(result, "ablation_delta_ralph", *threshold)
        }
        EvalMetric::AblationDeltaMemory { threshold } => {
            delta(result, "ablation_delta_memory", *threshold)
        }
        EvalMetric::EvolutionImprovementDelta { threshold } => {
            delta(result, "evolution_improvement_delta", *threshold)
        }
        EvalMetric::EvolutionConvergenceRate { threshold } => {
            match result
                .agent_output
                .get("evolution_convergence_rate")
                .and_then(|v| v.as_f64())
            {
                // 收敛所需轮数越少越好
                Some(v) => (v, v <= *threshold),
                None => (0.0, true),
            }
        }
        EvalMetric::DualTrackCodeOnlyDelta { threshold } => {
            delta(result, "dual_track_code_only_delta", *threshold)
        }
        EvalMetric::DualTrackArtifactOnlyDelta { threshold } => {
            delta(result, "dual_track_artifact_only_delta", *threshold)
        }
        EvalMetric::DualTrackCombinedDelta { threshold } => {
            delta(result, "dual_track_combined_delta", *threshold)
        }
        EvalMetric::CrossSystemTransferability { threshold } => {
            delta(result, "cross_system_transferability", *threshold)
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
    fn pge_delta_passes_when_above_threshold() {
        let result = mk_result(serde_json::json!({"ablation_delta_pge": 0.08}));
        let metric = EvalMetric::AblationDeltaPge { threshold: 0.05 };
        let (v, p) = compute(&metric, &mk_case(), &result);
        assert_eq!(v, 0.08);
        assert!(p);
    }

    #[test]
    fn negative_delta_fails() {
        let result = mk_result(serde_json::json!({"ablation_delta_ralph": -0.02}));
        let metric = EvalMetric::AblationDeltaRalph { threshold: 0.0 };
        let (_, p) = compute(&metric, &mk_case(), &result);
        assert!(!p);
    }
}
