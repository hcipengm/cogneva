//! 评估指标定义 —— 覆盖 D1-D9 九大维度 60+ 指标。

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// 单条 metric 结果。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricValue {
    pub metric: String,
    pub value: f64,
    pub passed: bool,
    pub threshold: Option<f64>,
}

/// 单步执行记录 —— 用于过程层指标（D2-D4）计算。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepRecord {
    pub step_index: usize,
    pub action_type: String,
    pub action_params: serde_json::Value,
    pub thought: Option<String>,
    pub duration_ms: u64,
    pub success: bool,
    pub tool_calls: Vec<String>,
}

/// 单条 case 的评估结果。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalResult {
    pub case_id: String,
    pub passed: bool,
    pub metrics: Vec<MetricValue>,
    pub agent_output: serde_json::Value,
    pub duration_ms: u64,
    pub token_usage: u64,
    pub cost: f64,
    pub error: Option<String>,
    pub steps: Vec<StepRecord>,
    pub trace_json: Option<serde_json::Value>,
}

impl EvalResult {
    pub fn aggregate(results: &[EvalResult]) -> HashMap<String, f64> {
        let mut sums: HashMap<String, (f64, u64)> = HashMap::new();
        for r in results {
            for m in &r.metrics {
                let entry = sums.entry(m.metric.clone()).or_insert((0.0, 0));
                entry.0 += m.value;
                entry.1 += 1;
            }
        }
        sums.into_iter()
            .map(|(k, (sum, count))| (k, sum / count.max(1) as f64))
            .collect()
    }

    /// 获取指定指标的值，不存在返回 None。
    pub fn metric_value(&self, name: &str) -> Option<f64> {
        self.metrics
            .iter()
            .find(|m| m.metric == name)
            .map(|m| m.value)
    }

    /// 获取指定指标是否通过。
    pub fn metric_passed(&self, name: &str) -> Option<bool> {
        self.metrics
            .iter()
            .find(|m| m.metric == name)
            .map(|m| m.passed)
    }
}

/// 评估指标类型 —— 九大维度 60+ 指标。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvalMetric {
    // ------------------------------------------------------------------
    // D1 — 任务完成与结果质量 (Outcome)
    // ------------------------------------------------------------------
    ExactMatch,
    SemanticSimilarity { threshold: f32 },
    ToolCallAccuracy,
    TaskSuccessRate { threshold: f64 },
    StrictSuccessRate { threshold: f64 },
    PartialSuccessRate { threshold: f64 },
    PassAtK { k: usize, threshold: f64 },
    ResolveRate { threshold: f64 },
    GoalFulfillment { threshold: f64 },
    StepAccuracy { threshold: f64 },
    ElementAccuracy { threshold: f64 },
    OperationF1 { threshold: f64 },
    ClickAccuracy { threshold: f64 },
    TypeAccuracy { threshold: f64 },
    ScrollAccuracy { threshold: f64 },
    NavigateAccuracy { threshold: f64 },

    // ------------------------------------------------------------------
    // D2 — 规划与推理质量 (Planning & Reasoning)
    // ------------------------------------------------------------------
    PlanQuality { threshold: f64 },
    PlanAdherence { threshold: f64 },
    LogicalConsistency { threshold: f64 },

    // ------------------------------------------------------------------
    // D3 — 工具使用质量 (Tool Use)
    // ------------------------------------------------------------------
    ToolSelection { threshold: f64 },
    ToolCalling { threshold: f64 },

    // ------------------------------------------------------------------
    // D4 — 执行效率 (Execution Efficiency)
    // ------------------------------------------------------------------
    StepRatio { threshold: f64 },
    RepetitivenessRate { threshold: f64 },
    TimePerStep { threshold_ms: u64 },
    FirstActionLatency { threshold_ms: u64 },
    ExecutionEfficiency { threshold: f64 },
    StepSuccessRate { threshold: f64 },
    RecoveryRate { threshold: f64 },
    ExploreMetric,
    BacktrackingTaskRate { threshold: f64 },
    BacktrackingSuccessRate { threshold: f64 },
    AvgBacktrackingSteps { threshold: f64 },
    BacktrackingRecoveryTime { threshold_ms: u64 },

    // ------------------------------------------------------------------
    // D5 — 可观测性与调试 (Observability)
    // ------------------------------------------------------------------
    SnapshotReproducibility { threshold: f64 },
    StateCoverage { threshold: f64 },
    SnapshotLatency { threshold_ms: u64 },
    CompressionRatio { threshold: f64 },
    StorageEfficiency { threshold: f64 },
    BacktraceTime { threshold_ms: u64 },
    EventStreamFidelity { threshold: f64 },
    EventCompleteness { threshold: f64 },
    StreamingSmoothness { threshold: f64 },
    UIRenderingLatency { threshold_ms: u64 },
    DebuggabilityIndex { threshold: f64 },
    ContextOverflowRate { threshold: f64 },
    InformationRetentionRate { threshold: f64 },
    MemoryTaskProficiencyRatio { threshold: f64 },
    SummarizationDistortionRate { threshold: f64 },
    LayerSwitchLatency { threshold_ms: u64 },

    // ------------------------------------------------------------------
    // D6 — 安全与对齐 (Safety & Alignment)
    // ------------------------------------------------------------------
    HarmfulOutputRate { threshold: f64 },
    CorrectRefusalRate { threshold: f64 },
    OverRefusalRate { threshold: f64 },
    PolicyComplianceRate { threshold: f64 },
    AdversarialSuccessRate { threshold: f64 },
    InstructionFollowingRate { threshold: f64 },
    FairnessBiasScore { threshold: f64 },
    TransparencyScore { threshold: f64 },

    // ------------------------------------------------------------------
    // D7 — 鲁棒性与可靠性 (Robustness)
    // ------------------------------------------------------------------
    OutputConsistency { threshold: f64 },
    TrajectoryConsistency { threshold: f64 },
    ScoreConsistency { threshold: f64 },
    ToolFailureRecovery { threshold: f64 },
    EnvironmentAdaptationRate { threshold: f64 },
    HallucinationRate { threshold: f64 },
    ContextRetentionScore { threshold: f64 },
    LongTailSuccessRate { threshold: f64 },
    HighLoadSuccessRate { threshold: f64 },
    NoisyInputSuccessRate { threshold: f64 },

    // ------------------------------------------------------------------
    // D8 — 多智能体协作 (Multi-Agent Collaboration)
    // ------------------------------------------------------------------
    TaskAssignmentAccuracy { threshold: f64 },
    InformationFlowEfficiency { threshold: f64 },
    StanceConvergence { threshold: f64 },
    TotalStanceShift { threshold: f64 },
    SemanticDiversity { threshold: f64 },
    ConsensusEfficiency { threshold: f64 },
    CollaborationSuccessRate { threshold: f64 },
    GroupReflectionCoverage { threshold: f64 },
    RoleConflictRate { threshold: f64 },
    SelfOrganizationEfficiency { threshold: f64 },

    // ------------------------------------------------------------------
    // D9 — 成本与资源效率 (Cost & Resource)
    // ------------------------------------------------------------------
    LatencyP50,
    LatencyP99,
    TokenEfficiency,
    CostPerTask,
    CostPerStep { threshold: f64 },
    TokenPerStep { threshold: f64 },
    InferenceLatency { threshold_ms: u64 },
    TimeToFirstToken { threshold_ms: u64 },

    // ------------------------------------------------------------------
    // D10 — 系统部署与运维 (Deployment & Ops)
    // 测量值约定：harness 将测量结果放入 agent_output 的对应 JSON key。
    // ------------------------------------------------------------------
    DeploymentTime { threshold_ms: u64 },
    DeploymentStepCount { threshold: f64 },
    FirstTimeSuccessRate { threshold: f64 },
    MttrPodCrash { threshold_ms: u64 },
    MttrNodeOffline { threshold_ms: u64 },
    MttrDiskFull { threshold_ms: u64 },
    ScaleElasticityP50 { threshold_ms: u64 },
    ScaleElasticityP99 { threshold_ms: u64 },
    ResourceOverheadCpu { threshold: f64 },
    ResourceOverheadMemory { threshold: f64 },
    GatewayLatencyP50 { threshold_ms: u64 },
    GatewayLatencyP99 { threshold_ms: u64 },
    StabilitySla { threshold: f64 },

    // ------------------------------------------------------------------
    // D11 — 代码质量与架构 (Code Quality & Architecture)
    // ------------------------------------------------------------------
    ClippyWarnCount { threshold: f64 },
    CargoTestPassRate { threshold: f64 },
    RegressionRate { threshold: f64 },
    CargoDenySecurityIssues { threshold: f64 },
    CrateDependencyCount { threshold: f64 },
    FaultIsolationSurvivalRate { threshold: f64 },
    PluginHotplugDowntime { threshold_ms: u64 },
    ArchitectureDriftIndex { threshold: f64 },
    NewPluginIntegrationCost { threshold: f64 },

    // ------------------------------------------------------------------
    // D12 — 消融实验与进化 (Ablation & Evolution)
    // ------------------------------------------------------------------
    AblationDeltaSelfReview { threshold: f64 },
    AblationDeltaPge { threshold: f64 },
    AblationDeltaRalph { threshold: f64 },
    AblationDeltaMemory { threshold: f64 },
    EvolutionImprovementDelta { threshold: f64 },
    EvolutionConvergenceRate { threshold: f64 },
    DualTrackCodeOnlyDelta { threshold: f64 },
    DualTrackArtifactOnlyDelta { threshold: f64 },
    DualTrackCombinedDelta { threshold: f64 },
    CrossSystemTransferability { threshold: f64 },

    // ------------------------------------------------------------------
    // D13 — 安全纵深防御 (Defense in Depth)
    // ------------------------------------------------------------------
    CredentialLeakInterceptRate { threshold: f64 },
    SandboxEscapeSuccessRate { threshold: f64 },
    AttackSurfaceReductionRate { threshold: f64 },
    BootstrapperCredentialZeroization { threshold: f64 },
    GatewayProxyLatencyLlm { threshold_ms: u64 },

    // ------------------------------------------------------------------
    // 自定义指标
    // ------------------------------------------------------------------
    Custom { name: String, evaluator: String },
}

impl EvalMetric {
    /// 指标名称（snake_case）。
    pub fn name(&self) -> String {
        match self {
            EvalMetric::ExactMatch => "exact_match".into(),
            EvalMetric::SemanticSimilarity { .. } => "semantic_similarity".into(),
            EvalMetric::ToolCallAccuracy => "tool_call_accuracy".into(),
            EvalMetric::TaskSuccessRate { .. } => "task_success_rate".into(),
            EvalMetric::StrictSuccessRate { .. } => "strict_success_rate".into(),
            EvalMetric::PartialSuccessRate { .. } => "partial_success_rate".into(),
            EvalMetric::PassAtK { .. } => "pass_at_k".into(),
            EvalMetric::ResolveRate { .. } => "resolve_rate".into(),
            EvalMetric::GoalFulfillment { .. } => "goal_fulfillment".into(),
            EvalMetric::StepAccuracy { .. } => "step_accuracy".into(),
            EvalMetric::ElementAccuracy { .. } => "element_accuracy".into(),
            EvalMetric::OperationF1 { .. } => "operation_f1".into(),
            EvalMetric::ClickAccuracy { .. } => "click_accuracy".into(),
            EvalMetric::TypeAccuracy { .. } => "type_accuracy".into(),
            EvalMetric::ScrollAccuracy { .. } => "scroll_accuracy".into(),
            EvalMetric::NavigateAccuracy { .. } => "navigate_accuracy".into(),
            EvalMetric::PlanQuality { .. } => "plan_quality".into(),
            EvalMetric::PlanAdherence { .. } => "plan_adherence".into(),
            EvalMetric::LogicalConsistency { .. } => "logical_consistency".into(),
            EvalMetric::ToolSelection { .. } => "tool_selection".into(),
            EvalMetric::ToolCalling { .. } => "tool_calling".into(),
            EvalMetric::StepRatio { .. } => "step_ratio".into(),
            EvalMetric::RepetitivenessRate { .. } => "repetitiveness_rate".into(),
            EvalMetric::TimePerStep { .. } => "time_per_step_ms".into(),
            EvalMetric::FirstActionLatency { .. } => "first_action_latency_ms".into(),
            EvalMetric::ExecutionEfficiency { .. } => "execution_efficiency".into(),
            EvalMetric::StepSuccessRate { .. } => "step_success_rate".into(),
            EvalMetric::RecoveryRate { .. } => "recovery_rate".into(),
            EvalMetric::ExploreMetric => "explore_metric".into(),
            EvalMetric::BacktrackingTaskRate { .. } => "backtracking_task_rate".into(),
            EvalMetric::BacktrackingSuccessRate { .. } => "backtracking_success_rate".into(),
            EvalMetric::AvgBacktrackingSteps { .. } => "avg_backtracking_steps".into(),
            EvalMetric::BacktrackingRecoveryTime { .. } => "backtracking_recovery_time_ms".into(),
            EvalMetric::SnapshotReproducibility { .. } => "snapshot_reproducibility".into(),
            EvalMetric::StateCoverage { .. } => "state_coverage".into(),
            EvalMetric::SnapshotLatency { .. } => "snapshot_latency_ms".into(),
            EvalMetric::CompressionRatio { .. } => "compression_ratio".into(),
            EvalMetric::StorageEfficiency { .. } => "storage_efficiency".into(),
            EvalMetric::BacktraceTime { .. } => "backtrace_time_ms".into(),
            EvalMetric::EventStreamFidelity { .. } => "event_stream_fidelity".into(),
            EvalMetric::EventCompleteness { .. } => "event_completeness".into(),
            EvalMetric::StreamingSmoothness { .. } => "streaming_smoothness".into(),
            EvalMetric::UIRenderingLatency { .. } => "ui_rendering_latency_ms".into(),
            EvalMetric::DebuggabilityIndex { .. } => "debuggability_index".into(),
            EvalMetric::ContextOverflowRate { .. } => "context_overflow_rate".into(),
            EvalMetric::InformationRetentionRate { .. } => "information_retention_rate".into(),
            EvalMetric::MemoryTaskProficiencyRatio { .. } => "memory_task_proficiency_ratio".into(),
            EvalMetric::SummarizationDistortionRate { .. } => {
                "summarization_distortion_rate".into()
            }
            EvalMetric::LayerSwitchLatency { .. } => "layer_switch_latency_ms".into(),
            EvalMetric::HarmfulOutputRate { .. } => "harmful_output_rate".into(),
            EvalMetric::CorrectRefusalRate { .. } => "correct_refusal_rate".into(),
            EvalMetric::OverRefusalRate { .. } => "over_refusal_rate".into(),
            EvalMetric::PolicyComplianceRate { .. } => "policy_compliance_rate".into(),
            EvalMetric::AdversarialSuccessRate { .. } => "adversarial_success_rate".into(),
            EvalMetric::InstructionFollowingRate { .. } => "instruction_following_rate".into(),
            EvalMetric::FairnessBiasScore { .. } => "fairness_bias_score".into(),
            EvalMetric::TransparencyScore { .. } => "transparency_score".into(),
            EvalMetric::OutputConsistency { .. } => "output_consistency".into(),
            EvalMetric::TrajectoryConsistency { .. } => "trajectory_consistency".into(),
            EvalMetric::ScoreConsistency { .. } => "score_consistency".into(),
            EvalMetric::ToolFailureRecovery { .. } => "tool_failure_recovery".into(),
            EvalMetric::EnvironmentAdaptationRate { .. } => "environment_adaptation_rate".into(),
            EvalMetric::HallucinationRate { .. } => "hallucination_rate".into(),
            EvalMetric::ContextRetentionScore { .. } => "context_retention_score".into(),
            EvalMetric::LongTailSuccessRate { .. } => "long_tail_success_rate".into(),
            EvalMetric::HighLoadSuccessRate { .. } => "high_load_success_rate".into(),
            EvalMetric::NoisyInputSuccessRate { .. } => "noisy_input_success_rate".into(),
            EvalMetric::TaskAssignmentAccuracy { .. } => "task_assignment_accuracy".into(),
            EvalMetric::InformationFlowEfficiency { .. } => "information_flow_efficiency".into(),
            EvalMetric::StanceConvergence { .. } => "stance_convergence".into(),
            EvalMetric::TotalStanceShift { .. } => "total_stance_shift".into(),
            EvalMetric::SemanticDiversity { .. } => "semantic_diversity".into(),
            EvalMetric::ConsensusEfficiency { .. } => "consensus_efficiency".into(),
            EvalMetric::CollaborationSuccessRate { .. } => "collaboration_success_rate".into(),
            EvalMetric::GroupReflectionCoverage { .. } => "group_reflection_coverage".into(),
            EvalMetric::RoleConflictRate { .. } => "role_conflict_rate".into(),
            EvalMetric::SelfOrganizationEfficiency { .. } => "self_organization_efficiency".into(),
            EvalMetric::LatencyP50 => "latency_ms".into(),
            EvalMetric::LatencyP99 => "latency_ms".into(),
            EvalMetric::TokenEfficiency => "token_efficiency".into(),
            EvalMetric::CostPerTask => "cost_usd".into(),
            EvalMetric::CostPerStep { .. } => "cost_per_step".into(),
            EvalMetric::TokenPerStep { .. } => "token_per_step".into(),
            EvalMetric::InferenceLatency { .. } => "inference_latency_ms".into(),
            EvalMetric::TimeToFirstToken { .. } => "time_to_first_token_ms".into(),
            // D10
            EvalMetric::DeploymentTime { .. } => "deployment_time_ms".into(),
            EvalMetric::DeploymentStepCount { .. } => "deployment_step_count".into(),
            EvalMetric::FirstTimeSuccessRate { .. } => "first_time_success_rate".into(),
            EvalMetric::MttrPodCrash { .. } => "mttr_pod_crash_ms".into(),
            EvalMetric::MttrNodeOffline { .. } => "mttr_node_offline_ms".into(),
            EvalMetric::MttrDiskFull { .. } => "mttr_disk_full_ms".into(),
            EvalMetric::ScaleElasticityP50 { .. } => "scale_elasticity_p50_ms".into(),
            EvalMetric::ScaleElasticityP99 { .. } => "scale_elasticity_p99_ms".into(),
            EvalMetric::ResourceOverheadCpu { .. } => "resource_overhead_cpu".into(),
            EvalMetric::ResourceOverheadMemory { .. } => "resource_overhead_memory".into(),
            EvalMetric::GatewayLatencyP50 { .. } => "gateway_latency_p50_ms".into(),
            EvalMetric::GatewayLatencyP99 { .. } => "gateway_latency_p99_ms".into(),
            EvalMetric::StabilitySla { .. } => "stability_sla".into(),
            // D11
            EvalMetric::ClippyWarnCount { .. } => "clippy_warn_count".into(),
            EvalMetric::CargoTestPassRate { .. } => "cargo_test_pass_rate".into(),
            EvalMetric::RegressionRate { .. } => "regression_rate".into(),
            EvalMetric::CargoDenySecurityIssues { .. } => "cargo_deny_security_issues".into(),
            EvalMetric::CrateDependencyCount { .. } => "crate_dependency_count".into(),
            EvalMetric::FaultIsolationSurvivalRate { .. } => "fault_isolation_survival_rate".into(),
            EvalMetric::PluginHotplugDowntime { .. } => "plugin_hotplug_downtime_ms".into(),
            EvalMetric::ArchitectureDriftIndex { .. } => "architecture_drift_index".into(),
            EvalMetric::NewPluginIntegrationCost { .. } => "new_plugin_integration_cost".into(),
            // D12
            EvalMetric::AblationDeltaSelfReview { .. } => "ablation_delta_self_review".into(),
            EvalMetric::AblationDeltaPge { .. } => "ablation_delta_pge".into(),
            EvalMetric::AblationDeltaRalph { .. } => "ablation_delta_ralph".into(),
            EvalMetric::AblationDeltaMemory { .. } => "ablation_delta_memory".into(),
            EvalMetric::EvolutionImprovementDelta { .. } => "evolution_improvement_delta".into(),
            EvalMetric::EvolutionConvergenceRate { .. } => "evolution_convergence_rate".into(),
            EvalMetric::DualTrackCodeOnlyDelta { .. } => "dual_track_code_only_delta".into(),
            EvalMetric::DualTrackArtifactOnlyDelta { .. } => {
                "dual_track_artifact_only_delta".into()
            }
            EvalMetric::DualTrackCombinedDelta { .. } => "dual_track_combined_delta".into(),
            EvalMetric::CrossSystemTransferability { .. } => "cross_system_transferability".into(),
            // D13
            EvalMetric::CredentialLeakInterceptRate { .. } => {
                "credential_leak_intercept_rate".into()
            }
            EvalMetric::SandboxEscapeSuccessRate { .. } => "sandbox_escape_success_rate".into(),
            EvalMetric::AttackSurfaceReductionRate { .. } => "attack_surface_reduction_rate".into(),
            EvalMetric::BootstrapperCredentialZeroization { .. } => {
                "bootstrapper_credential_zeroization".into()
            }
            EvalMetric::GatewayProxyLatencyLlm { .. } => "gateway_proxy_latency_llm_ms".into(),
            EvalMetric::Custom { name, .. } => name.clone(),
        }
    }

    /// 默认阈值（用于未显式配置时）。
    pub fn default_threshold(&self) -> Option<f64> {
        match self {
            EvalMetric::ExactMatch => Some(1.0),
            EvalMetric::SemanticSimilarity { threshold } => Some(*threshold as f64),
            EvalMetric::ToolCallAccuracy => Some(1.0),
            EvalMetric::TaskSuccessRate { threshold } => Some(*threshold),
            EvalMetric::StrictSuccessRate { threshold } => Some(*threshold),
            EvalMetric::PartialSuccessRate { threshold } => Some(*threshold),
            EvalMetric::PassAtK { threshold, .. } => Some(*threshold),
            EvalMetric::ResolveRate { threshold } => Some(*threshold),
            EvalMetric::GoalFulfillment { threshold } => Some(*threshold),
            EvalMetric::StepAccuracy { threshold } => Some(*threshold),
            EvalMetric::ElementAccuracy { threshold } => Some(*threshold),
            EvalMetric::OperationF1 { threshold } => Some(*threshold),
            EvalMetric::ClickAccuracy { threshold } => Some(*threshold),
            EvalMetric::TypeAccuracy { threshold } => Some(*threshold),
            EvalMetric::ScrollAccuracy { threshold } => Some(*threshold),
            EvalMetric::NavigateAccuracy { threshold } => Some(*threshold),
            EvalMetric::PlanQuality { threshold } => Some(*threshold),
            EvalMetric::PlanAdherence { threshold } => Some(*threshold),
            EvalMetric::LogicalConsistency { threshold } => Some(*threshold),
            EvalMetric::ToolSelection { threshold } => Some(*threshold),
            EvalMetric::ToolCalling { threshold } => Some(*threshold),
            EvalMetric::StepRatio { threshold } => Some(*threshold),
            EvalMetric::RepetitivenessRate { threshold } => Some(*threshold),
            EvalMetric::TimePerStep { threshold_ms } => Some(*threshold_ms as f64),
            EvalMetric::FirstActionLatency { threshold_ms } => Some(*threshold_ms as f64),
            EvalMetric::ExecutionEfficiency { threshold } => Some(*threshold),
            EvalMetric::StepSuccessRate { threshold } => Some(*threshold),
            EvalMetric::RecoveryRate { threshold } => Some(*threshold),
            EvalMetric::ExploreMetric => Some(0.6),
            EvalMetric::BacktrackingTaskRate { threshold } => Some(*threshold),
            EvalMetric::BacktrackingSuccessRate { threshold } => Some(*threshold),
            EvalMetric::AvgBacktrackingSteps { threshold } => Some(*threshold),
            EvalMetric::BacktrackingRecoveryTime { threshold_ms } => Some(*threshold_ms as f64),
            EvalMetric::SnapshotReproducibility { threshold } => Some(*threshold),
            EvalMetric::StateCoverage { threshold } => Some(*threshold),
            EvalMetric::SnapshotLatency { threshold_ms } => Some(*threshold_ms as f64),
            EvalMetric::CompressionRatio { threshold } => Some(*threshold),
            EvalMetric::StorageEfficiency { threshold } => Some(*threshold),
            EvalMetric::BacktraceTime { threshold_ms } => Some(*threshold_ms as f64),
            EvalMetric::EventStreamFidelity { threshold } => Some(*threshold),
            EvalMetric::EventCompleteness { threshold } => Some(*threshold),
            EvalMetric::StreamingSmoothness { threshold } => Some(*threshold),
            EvalMetric::UIRenderingLatency { threshold_ms } => Some(*threshold_ms as f64),
            EvalMetric::DebuggabilityIndex { threshold } => Some(*threshold),
            EvalMetric::ContextOverflowRate { threshold } => Some(*threshold),
            EvalMetric::InformationRetentionRate { threshold } => Some(*threshold),
            EvalMetric::MemoryTaskProficiencyRatio { threshold } => Some(*threshold),
            EvalMetric::SummarizationDistortionRate { threshold } => Some(*threshold),
            EvalMetric::LayerSwitchLatency { threshold_ms } => Some(*threshold_ms as f64),
            EvalMetric::HarmfulOutputRate { threshold } => Some(*threshold),
            EvalMetric::CorrectRefusalRate { threshold } => Some(*threshold),
            EvalMetric::OverRefusalRate { threshold } => Some(*threshold),
            EvalMetric::PolicyComplianceRate { threshold } => Some(*threshold),
            EvalMetric::AdversarialSuccessRate { threshold } => Some(*threshold),
            EvalMetric::InstructionFollowingRate { threshold } => Some(*threshold),
            EvalMetric::FairnessBiasScore { threshold } => Some(*threshold),
            EvalMetric::TransparencyScore { threshold } => Some(*threshold),
            EvalMetric::OutputConsistency { threshold } => Some(*threshold),
            EvalMetric::TrajectoryConsistency { threshold } => Some(*threshold),
            EvalMetric::ScoreConsistency { threshold } => Some(*threshold),
            EvalMetric::ToolFailureRecovery { threshold } => Some(*threshold),
            EvalMetric::EnvironmentAdaptationRate { threshold } => Some(*threshold),
            EvalMetric::HallucinationRate { threshold } => Some(*threshold),
            EvalMetric::ContextRetentionScore { threshold } => Some(*threshold),
            EvalMetric::LongTailSuccessRate { threshold } => Some(*threshold),
            EvalMetric::HighLoadSuccessRate { threshold } => Some(*threshold),
            EvalMetric::NoisyInputSuccessRate { threshold } => Some(*threshold),
            EvalMetric::TaskAssignmentAccuracy { threshold } => Some(*threshold),
            EvalMetric::InformationFlowEfficiency { threshold } => Some(*threshold),
            EvalMetric::StanceConvergence { threshold } => Some(*threshold),
            EvalMetric::TotalStanceShift { threshold } => Some(*threshold),
            EvalMetric::SemanticDiversity { threshold } => Some(*threshold),
            EvalMetric::ConsensusEfficiency { threshold } => Some(*threshold),
            EvalMetric::CollaborationSuccessRate { threshold } => Some(*threshold),
            EvalMetric::GroupReflectionCoverage { threshold } => Some(*threshold),
            EvalMetric::RoleConflictRate { threshold } => Some(*threshold),
            EvalMetric::SelfOrganizationEfficiency { threshold } => Some(*threshold),
            EvalMetric::LatencyP50 => Some(5000.0),
            EvalMetric::LatencyP99 => Some(30000.0),
            EvalMetric::TokenEfficiency => Some(0.5),
            EvalMetric::CostPerTask => Some(0.01),
            EvalMetric::CostPerStep { threshold } => Some(*threshold),
            EvalMetric::TokenPerStep { threshold } => Some(*threshold),
            EvalMetric::InferenceLatency { threshold_ms } => Some(*threshold_ms as f64),
            EvalMetric::TimeToFirstToken { threshold_ms } => Some(*threshold_ms as f64),
            // D10-D13：阈值型指标直接返回自带阈值
            EvalMetric::DeploymentTime { threshold_ms } => Some(*threshold_ms as f64),
            EvalMetric::DeploymentStepCount { threshold } => Some(*threshold),
            EvalMetric::FirstTimeSuccessRate { threshold } => Some(*threshold),
            EvalMetric::MttrPodCrash { threshold_ms } => Some(*threshold_ms as f64),
            EvalMetric::MttrNodeOffline { threshold_ms } => Some(*threshold_ms as f64),
            EvalMetric::MttrDiskFull { threshold_ms } => Some(*threshold_ms as f64),
            EvalMetric::ScaleElasticityP50 { threshold_ms } => Some(*threshold_ms as f64),
            EvalMetric::ScaleElasticityP99 { threshold_ms } => Some(*threshold_ms as f64),
            EvalMetric::ResourceOverheadCpu { threshold } => Some(*threshold),
            EvalMetric::ResourceOverheadMemory { threshold } => Some(*threshold),
            EvalMetric::GatewayLatencyP50 { threshold_ms } => Some(*threshold_ms as f64),
            EvalMetric::GatewayLatencyP99 { threshold_ms } => Some(*threshold_ms as f64),
            EvalMetric::StabilitySla { threshold } => Some(*threshold),
            EvalMetric::ClippyWarnCount { threshold } => Some(*threshold),
            EvalMetric::CargoTestPassRate { threshold } => Some(*threshold),
            EvalMetric::RegressionRate { threshold } => Some(*threshold),
            EvalMetric::CargoDenySecurityIssues { threshold } => Some(*threshold),
            EvalMetric::CrateDependencyCount { threshold } => Some(*threshold),
            EvalMetric::FaultIsolationSurvivalRate { threshold } => Some(*threshold),
            EvalMetric::PluginHotplugDowntime { threshold_ms } => Some(*threshold_ms as f64),
            EvalMetric::ArchitectureDriftIndex { threshold } => Some(*threshold),
            EvalMetric::NewPluginIntegrationCost { threshold } => Some(*threshold),
            EvalMetric::AblationDeltaSelfReview { threshold } => Some(*threshold),
            EvalMetric::AblationDeltaPge { threshold } => Some(*threshold),
            EvalMetric::AblationDeltaRalph { threshold } => Some(*threshold),
            EvalMetric::AblationDeltaMemory { threshold } => Some(*threshold),
            EvalMetric::EvolutionImprovementDelta { threshold } => Some(*threshold),
            EvalMetric::EvolutionConvergenceRate { threshold } => Some(*threshold),
            EvalMetric::DualTrackCodeOnlyDelta { threshold } => Some(*threshold),
            EvalMetric::DualTrackArtifactOnlyDelta { threshold } => Some(*threshold),
            EvalMetric::DualTrackCombinedDelta { threshold } => Some(*threshold),
            EvalMetric::CrossSystemTransferability { threshold } => Some(*threshold),
            EvalMetric::CredentialLeakInterceptRate { threshold } => Some(*threshold),
            EvalMetric::SandboxEscapeSuccessRate { threshold } => Some(*threshold),
            EvalMetric::AttackSurfaceReductionRate { threshold } => Some(*threshold),
            EvalMetric::BootstrapperCredentialZeroization { threshold } => Some(*threshold),
            EvalMetric::GatewayProxyLatencyLlm { threshold_ms } => Some(*threshold_ms as f64),
            EvalMetric::Custom { .. } => None,
        }
    }

    /// 指标所属评估维度（D1-D9）。
    pub fn dimension(&self) -> &'static str {
        match self {
            EvalMetric::ExactMatch
            | EvalMetric::SemanticSimilarity { .. }
            | EvalMetric::ToolCallAccuracy
            | EvalMetric::TaskSuccessRate { .. }
            | EvalMetric::StrictSuccessRate { .. }
            | EvalMetric::PartialSuccessRate { .. }
            | EvalMetric::PassAtK { .. }
            | EvalMetric::ResolveRate { .. }
            | EvalMetric::GoalFulfillment { .. }
            | EvalMetric::StepAccuracy { .. }
            | EvalMetric::ElementAccuracy { .. }
            | EvalMetric::OperationF1 { .. }
            | EvalMetric::ClickAccuracy { .. }
            | EvalMetric::TypeAccuracy { .. }
            | EvalMetric::ScrollAccuracy { .. }
            | EvalMetric::NavigateAccuracy { .. } => "D1",

            EvalMetric::PlanQuality { .. }
            | EvalMetric::PlanAdherence { .. }
            | EvalMetric::LogicalConsistency { .. } => "D2",

            EvalMetric::ToolSelection { .. } | EvalMetric::ToolCalling { .. } => "D3",

            EvalMetric::StepRatio { .. }
            | EvalMetric::RepetitivenessRate { .. }
            | EvalMetric::TimePerStep { .. }
            | EvalMetric::FirstActionLatency { .. }
            | EvalMetric::ExecutionEfficiency { .. }
            | EvalMetric::StepSuccessRate { .. }
            | EvalMetric::RecoveryRate { .. }
            | EvalMetric::ExploreMetric
            | EvalMetric::BacktrackingTaskRate { .. }
            | EvalMetric::BacktrackingSuccessRate { .. }
            | EvalMetric::AvgBacktrackingSteps { .. }
            | EvalMetric::BacktrackingRecoveryTime { .. }
            | EvalMetric::LatencyP50
            | EvalMetric::LatencyP99 => "D4",

            EvalMetric::SnapshotReproducibility { .. }
            | EvalMetric::StateCoverage { .. }
            | EvalMetric::SnapshotLatency { .. }
            | EvalMetric::CompressionRatio { .. }
            | EvalMetric::StorageEfficiency { .. }
            | EvalMetric::BacktraceTime { .. }
            | EvalMetric::EventStreamFidelity { .. }
            | EvalMetric::EventCompleteness { .. }
            | EvalMetric::StreamingSmoothness { .. }
            | EvalMetric::UIRenderingLatency { .. }
            | EvalMetric::DebuggabilityIndex { .. }
            | EvalMetric::ContextOverflowRate { .. }
            | EvalMetric::InformationRetentionRate { .. }
            | EvalMetric::MemoryTaskProficiencyRatio { .. }
            | EvalMetric::SummarizationDistortionRate { .. }
            | EvalMetric::LayerSwitchLatency { .. } => "D5",

            EvalMetric::HarmfulOutputRate { .. }
            | EvalMetric::CorrectRefusalRate { .. }
            | EvalMetric::OverRefusalRate { .. }
            | EvalMetric::PolicyComplianceRate { .. }
            | EvalMetric::AdversarialSuccessRate { .. }
            | EvalMetric::InstructionFollowingRate { .. }
            | EvalMetric::FairnessBiasScore { .. }
            | EvalMetric::TransparencyScore { .. } => "D6",

            EvalMetric::OutputConsistency { .. }
            | EvalMetric::TrajectoryConsistency { .. }
            | EvalMetric::ScoreConsistency { .. }
            | EvalMetric::ToolFailureRecovery { .. }
            | EvalMetric::EnvironmentAdaptationRate { .. }
            | EvalMetric::HallucinationRate { .. }
            | EvalMetric::ContextRetentionScore { .. }
            | EvalMetric::LongTailSuccessRate { .. }
            | EvalMetric::HighLoadSuccessRate { .. }
            | EvalMetric::NoisyInputSuccessRate { .. } => "D7",

            EvalMetric::TaskAssignmentAccuracy { .. }
            | EvalMetric::InformationFlowEfficiency { .. }
            | EvalMetric::StanceConvergence { .. }
            | EvalMetric::TotalStanceShift { .. }
            | EvalMetric::SemanticDiversity { .. }
            | EvalMetric::ConsensusEfficiency { .. }
            | EvalMetric::CollaborationSuccessRate { .. }
            | EvalMetric::GroupReflectionCoverage { .. }
            | EvalMetric::RoleConflictRate { .. }
            | EvalMetric::SelfOrganizationEfficiency { .. } => "D8",

            EvalMetric::TokenEfficiency
            | EvalMetric::CostPerTask
            | EvalMetric::CostPerStep { .. }
            | EvalMetric::TokenPerStep { .. }
            | EvalMetric::InferenceLatency { .. }
            | EvalMetric::TimeToFirstToken { .. } => "D9",

            EvalMetric::DeploymentTime { .. }
            | EvalMetric::DeploymentStepCount { .. }
            | EvalMetric::FirstTimeSuccessRate { .. }
            | EvalMetric::MttrPodCrash { .. }
            | EvalMetric::MttrNodeOffline { .. }
            | EvalMetric::MttrDiskFull { .. }
            | EvalMetric::ScaleElasticityP50 { .. }
            | EvalMetric::ScaleElasticityP99 { .. }
            | EvalMetric::ResourceOverheadCpu { .. }
            | EvalMetric::ResourceOverheadMemory { .. }
            | EvalMetric::GatewayLatencyP50 { .. }
            | EvalMetric::GatewayLatencyP99 { .. }
            | EvalMetric::StabilitySla { .. } => "D10",

            EvalMetric::ClippyWarnCount { .. }
            | EvalMetric::CargoTestPassRate { .. }
            | EvalMetric::RegressionRate { .. }
            | EvalMetric::CargoDenySecurityIssues { .. }
            | EvalMetric::CrateDependencyCount { .. }
            | EvalMetric::FaultIsolationSurvivalRate { .. }
            | EvalMetric::PluginHotplugDowntime { .. }
            | EvalMetric::ArchitectureDriftIndex { .. }
            | EvalMetric::NewPluginIntegrationCost { .. } => "D11",

            EvalMetric::AblationDeltaSelfReview { .. }
            | EvalMetric::AblationDeltaPge { .. }
            | EvalMetric::AblationDeltaRalph { .. }
            | EvalMetric::AblationDeltaMemory { .. }
            | EvalMetric::EvolutionImprovementDelta { .. }
            | EvalMetric::EvolutionConvergenceRate { .. }
            | EvalMetric::DualTrackCodeOnlyDelta { .. }
            | EvalMetric::DualTrackArtifactOnlyDelta { .. }
            | EvalMetric::DualTrackCombinedDelta { .. }
            | EvalMetric::CrossSystemTransferability { .. } => "D12",

            EvalMetric::CredentialLeakInterceptRate { .. }
            | EvalMetric::SandboxEscapeSuccessRate { .. }
            | EvalMetric::AttackSurfaceReductionRate { .. }
            | EvalMetric::BootstrapperCredentialZeroization { .. }
            | EvalMetric::GatewayProxyLatencyLlm { .. } => "D13",

            EvalMetric::Custom { .. } => "D9",
        }
    }
}
