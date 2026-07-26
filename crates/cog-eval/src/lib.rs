//! `cog-eval` — Agent 评估框架。
//! 没有 eval 的进化是盲目的。`cog-eval` 提供：
//! - Dataset：评估数据集管理（JSONL/YAML）
//! - Metric：九大维度 60+ 指标（D1-D9）
//! - Runner：批量运行 + 结果收集 + metric 计算
//! - Dimensions：分维度指标计算模块
//! - Judge：LLM-as-a-Judge 主观评分框架
//! - Comparator：A/B 对比 + 统计显著性检验
//! - Report：可视化报告生成（aggregate + per-case）
//! - Harness：CI 集成（`cargo test` 时自动跑回归）

pub mod ablation;
pub mod adapters;
pub mod comparator;
pub mod cross_system;
pub mod dataset;
pub mod dimensions;
pub mod gateway;
pub mod harness;
pub mod judge;
pub mod long_run;
pub mod metric;
pub mod plugin;
pub mod report;
pub mod runner;
pub mod service;
pub mod system_harness;
pub mod trend;

pub use ablation::{
    AblationConfig, AblationDelta, AblationExecutor, AblationGroup, AblationReport, AblationRunner,
    Component, GroupReport,
};
pub use adapters::{AgentBenchLoader, GaiaRunner, SweBenchRunner};
pub use comparator::{AbComparator, ComparisonReport, StatisticalTest};
pub use cross_system::{
    CrossSystemBenchmark, CrossSystemReport, ExternalSystem, MethodologyConfig, SystemScore,
};
pub use dataset::{EvalCase, EvalDataset};
pub use harness::{HarnessConfig, RegressionHarness};
pub use judge::{
    execution_efficiency_judge, goal_judge, logic_judge, plan_adherence_judge, plan_judge,
    safety_judge, tool_calling_judge, tool_selection_judge, Judge, LlmJudge,
};
pub use long_run::{
    CheckpointSample, DriftDetector, LinearFit, LongRunConfig, LongRunHarness, LongRunProbe,
    StabilityReport,
};
pub use metric::{EvalMetric, EvalResult, MetricValue, StepRecord};
pub use report::{DimensionSummary, EvalReport, MetricAggregate, PerCaseResult, ReportFormat};
pub use runner::{EvalRunner, RunnerConfig};
pub use system_harness::{
    D10Metrics, D13Metrics, DeployObservation, DeploymentTestConfig, FaultObservation, FaultType,
    MachineSpec, ScaleObservation, SecurityObservation, SecurityScenario, SystemEvalHarness,
    SystemRunner,
};
pub use trend::{
    render_ablation_stacked_bar, render_trend_chart, ConvergenceAnalysis, TrendPoint, TrendReport,
};
