//! D8 — 多智能体协作指标计算。
//! 本维度所有指标均为多 Agent 场景专用，单条 case 层面仅返回占位值。
//! 实际值需在多 Agent 基准测试（MultiAgentBench）中采集原始数据后聚合。

use crate::dataset::EvalCase;
use crate::metric::{EvalMetric, EvalResult};

/// 计算 D8 指标。
pub fn compute(metric: &EvalMetric, _case: &EvalCase, result: &EvalResult) -> (f64, bool) {
    match metric {
        EvalMetric::TaskAssignmentAccuracy { threshold } => {
            let score = 1.0; // 占位：需 Crew/Squad 调度数据
            (score, score >= *threshold)
        }
        EvalMetric::InformationFlowEfficiency { threshold } => {
            let score = 1.0; // 占位：需 Agent 间消息日志
            (score, score >= *threshold)
        }
        EvalMetric::StanceConvergence { threshold } => {
            let score = 1.0; // 占位：需 Roundtable 投票数据
            (score, score >= *threshold)
        }
        EvalMetric::TotalStanceShift { threshold } => {
            let score = 0.0; // 占位：需 Roundtable 迭代数据
            (score, score <= *threshold)
        }
        EvalMetric::SemanticDiversity { threshold } => {
            let score = 0.5; // 占位：需推理路径 embedding
            (score, score >= *threshold)
        }
        EvalMetric::ConsensusEfficiency { threshold } => {
            let score = 1.0; // 占位：需实际轮次 / 理论最小轮次
            (score, score <= *threshold)
        }
        EvalMetric::CollaborationSuccessRate { threshold } => {
            let score = if result.passed { 1.0 } else { 0.0 };
            (score, score >= *threshold)
        }
        EvalMetric::GroupReflectionCoverage { threshold } => {
            let score = 1.0; // 占位：需参与反思的 Agent 比例
            (score, score >= *threshold)
        }
        EvalMetric::RoleConflictRate { threshold } => {
            let score = 0.0; // 占位：需角色边界检测
            (score, score <= *threshold)
        }
        EvalMetric::SelfOrganizationEfficiency { threshold } => {
            let score = 1.0; // 占位：需自组织 vs 中心化调度对比
            (score, score >= *threshold)
        }
        _ => (0.0, true),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{mk_case, mk_result};

    #[test]
    fn collaboration_success_rate_passed() {
        let result = mk_result(vec![], true);
        let metric = EvalMetric::CollaborationSuccessRate { threshold: 0.5 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert_eq!(value, 1.0);
        assert!(passed);
    }

    #[test]
    fn collaboration_success_rate_failed() {
        let result = mk_result(vec![], false);
        let metric = EvalMetric::CollaborationSuccessRate { threshold: 0.5 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert_eq!(value, 0.0);
        assert!(!passed);
    }

    #[test]
    fn total_stance_shift_within_threshold() {
        let result = mk_result(vec![], true);
        let metric = EvalMetric::TotalStanceShift { threshold: 0.5 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert_eq!(value, 0.0);
        assert!(passed);
    }

    #[test]
    fn role_conflict_rate_within_threshold() {
        let result = mk_result(vec![], true);
        let metric = EvalMetric::RoleConflictRate { threshold: 0.1 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert_eq!(value, 0.0);
        assert!(passed);
    }

    #[test]
    fn semantic_diversity_meets_threshold() {
        let result = mk_result(vec![], true);
        let metric = EvalMetric::SemanticDiversity { threshold: 0.4 };
        let (value, passed) = compute(&metric, &mk_case(), &result);
        assert_eq!(value, 0.5);
        assert!(passed);
    }
}
