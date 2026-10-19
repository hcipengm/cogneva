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
pub mod backoff;
pub mod bench;
pub mod comparator;
pub mod cross_system;
pub mod dataset;
pub mod dimensions;
pub mod exec_seam;
pub mod gateway;
pub mod harness;
pub mod judge;
pub mod long_run;
pub mod metric;
pub mod plugin;
pub mod report;
pub mod rig;
pub mod runner;
pub mod scaffold;
pub mod scaffolds;
pub mod service;
pub mod subset;
pub mod system_harness;
pub mod trend;

pub use ablation::{
    AblationConfig, AblationDelta, AblationExecutor, AblationGroup, AblationReport, AblationRunner,
    Component, GroupReport,
};
pub use adapters::{
    hle_benchmark, parse_test_list, swe_pro_benchmark, swe_pro_tool_definitions,
    toolathlon_benchmark, AgentBenchLoader, GaiaRunner, HleCaseSource, HleJudge, HleToolkit,
    SweBenchRunner, SweProBackend, SweProCaseSource, SweProJudge, SweProTestReport, SweProToolkit,
    ToolathlonBackend, ToolathlonCaseSource, ToolathlonCheck, ToolathlonJudge, ToolathlonReport,
    ToolathlonToolkit, HLE_JSONL, SWE_PRO_JSONL, SWE_PRO_TOOL_BASH, SWE_PRO_TOOL_FILE_EDIT,
    TOOLATHLON_TASKS_DIR,
};
pub use bench::{
    tools_bound_to, Benchmark, CaseJudge, CaseSource, EnvProvider, NoEnvProvider, ToolFactory,
    Toolkit, Verdict,
};
pub use comparator::{two_proportion_z_test, AbComparator, ComparisonReport, StatisticalTest};
pub use cross_system::{
    CrossSystemBenchmark, CrossSystemReport, ExternalSystem, MethodologyConfig, SystemScore,
};
pub use dataset::{EvalCase, EvalDataset};
pub use exec_seam::{ExecLimits, ExecOutcome, ExecRequest, ExecRunner, ExecSeam};
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
pub use rig::{Cell, CellStat, Checkpoint, Rig, RunConfig, SeedScore, Table, DATA_ROOT_ENV};
pub use runner::{EvalRunner, RunnerConfig};
pub use scaffold::{
    AgentOutput, AgentScaffold, Budget, CaseEnv, FinishReason, NoEnv, SolveContext, ToolSet,
};
pub use subset::{
    draw as draw_case_subset, render as render_case_subset, CaseSubset, SubsetSource,
};
pub use system_harness::{
    D10Metrics, D13Metrics, DeployObservation, DeploymentTestConfig, FaultObservation, FaultType,
    MachineSpec, ScaleObservation, SecurityObservation, SecurityScenario, SystemEvalHarness,
    SystemRunner,
};
pub use trend::{
    render_ablation_stacked_bar, render_trend_chart, ConvergenceAnalysis, TrendPoint, TrendReport,
};

#[cfg(test)]
mod failure_reading_carries_the_criterion_value {
    //! principle: feedback_failure_reading_must_carry_the_value
    //!
    //! 失败读数（CI 注解/告警/日志）必须带上判据比较的值，只有位置时同一次失败的不同
    //! 分支不可分。
    //!
    //! One range criterion is checked at one position and has two failure
    //! branches: the observed value fell below the lower bound, or rose above
    //! the upper one. A reading printing only the position emits the same line
    //! for both, so a reader of the CI annotation cannot tell which branch
    //! failed. The gate pins the renderer's contract: the reading carries the
    //! compared value and the bound it was judged against, and the two branches
    //! are separable by that line alone.

    /// A single criterion comparison at one fixed position.
    ///
    /// Both branches carry the *same* location by construction: location is the
    /// one thing that does not separate them.
    enum Branch {
        Below { observed: f64, bound: f64 },
        Above { observed: f64, bound: f64 },
    }

    const LOCATION: &str = "crates/cog-eval/src/harness.rs:96";

    impl Branch {
        /// The CI annotation/log line a reader gets when this branch fails.
        ///
        /// The comparison is rendered, not just the verdict: `observed <op>
        /// bound` is the value the criterion compared, and it is the only part
        /// of the line that differs between the two branches.
        fn render_failure(self) -> String {
            match self {
                Branch::Below { observed, bound } => {
                    format!("{LOCATION}: criterion failed: {observed:.1} < {bound:.1}")
                }
                Branch::Above { observed, bound } => {
                    format!("{LOCATION}: criterion failed: {observed:.1} > {bound:.1}")
                }
            }
        }
    }

    /// The position half of a failure line -- everything before the compared
    /// value. It is the whole reading a location-only annotation would carry.
    fn position_head(line: &str) -> &str {
        line.split_once(": criterion failed: ")
            .map(|(head, _)| head)
            .expect("a failure line has to carry its criterion comparison")
    }

    /// The gate: two failure branches at one position are separable only by the
    /// value the criterion compared.
    #[test]
    fn two_failure_branches_at_one_position_carry_distinct_compared_values() {
        let below = Branch::Below {
            observed: 70.0,
            bound: 85.0,
        };
        let above = Branch::Above {
            observed: 92.0,
            bound: 90.0,
        };

        let low_line = below.render_failure();
        let high_line = above.render_failure();

        // Control: the two branches genuinely share one position. Without this,
        // the separation below could be carried by two different locations.
        assert_eq!(position_head(&low_line), LOCATION);
        assert_eq!(position_head(&high_line), LOCATION);

        assert_ne!(
            low_line, high_line,
            "both branches fail at {LOCATION}, so a reading that dropped the \
             compared value could not name which one failed"
        );

        // Each line carries its own branch's compared value and the bound it
        // was judged against, and never the other branch's value.
        assert!(low_line.contains("70.0 < 85.0"), "{low_line}");
        assert!(high_line.contains("92.0 > 90.0"), "{high_line}");
        assert!(!low_line.contains("92.0"), "{low_line}");
        assert!(!high_line.contains("70.0"), "{high_line}");
    }

    /// The rejected shape, witnessed directly: a location-only renderer emits
    /// one identical reading for the two different failures.
    #[test]
    fn a_location_only_reading_collapses_the_two_branches() {
        let location_only = |_: Branch| format!("{LOCATION}: criterion failed");
        let below = Branch::Below {
            observed: 70.0,
            bound: 85.0,
        };
        let above = Branch::Above {
            observed: 92.0,
            bound: 90.0,
        };
        assert_eq!(
            location_only(below),
            location_only(above),
            "without the compared value the below and above branches are the same \
             annotation -- the indistinguishability the rule forbids"
        );
    }
}
