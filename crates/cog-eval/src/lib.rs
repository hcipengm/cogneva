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
pub mod admission;
pub mod backoff;
pub mod bench;
pub mod comparator;
pub mod cross_system;
pub mod dataset;
pub mod dimensions;
pub mod exec_seam;
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

#[cfg(test)]
mod feedback_feed_a_reading_the_artifact_the_pipeline_already_paid_for {
    //! principle: feedback_feed_a_reading_the_artifact_the_pipeline_already_paid_for
    //!
    //! 一条读数只靠「有人想起来」才跑，多半不是记性问题而是价格问题：它的成本恰好是上游那一步
    //! 已经付过的那笔钱。
    //!
    //! The eval pipeline pays upstream once per case and hands down the artifact it
    //! paid for: an `EvalResult` carrying the case's duration, tokens, and cost.
    //! `EvalReport::from_results` is the reading derived from those artifacts. A
    //! reading that only runs when someone remembers to run it is usually not a
    //! memory problem but a price problem: its cost is exactly the money the
    //! upstream step already paid, because the artifact that step produced already
    //! carries everything the reading prints. This gate pins the reading to the
    //! artifacts: every number the report shows is a restatement of the paid-for
    //! rows, feeding the reading spends none of them, and re-deriving it would
    //! cost the upstream step's price a second time.

    use crate::{EvalReport, EvalResult, MetricValue};

    /// One artifact the upstream step pays for: the per-case `EvalResult`,
    /// carrying what the case cost to run.
    fn paid_artifact(
        case_id: &str,
        passed: bool,
        duration_ms: u64,
        token_usage: u64,
        cost: f64,
        error: Option<&str>,
    ) -> EvalResult {
        EvalResult {
            case_id: case_id.into(),
            passed,
            metrics: vec![MetricValue {
                metric: "exact_match".into(),
                value: if passed { 1.0 } else { 0.0 },
                passed,
                threshold: None,
            }],
            agent_output: serde_json::Value::Null,
            duration_ms,
            token_usage,
            cost,
            error: error.map(str::to_string),
            steps: vec![],
            trace_json: None,
        }
    }

    /// Three artifacts with distinct durations, token counts, and prices, so every
    /// aggregate the reading prints is pinned to a number the fixture chose rather
    /// than to a coincidence of equal values.
    fn fixture_artifacts() -> Vec<EvalResult> {
        vec![
            paid_artifact("case-a", true, 120, 800, 0.0025, None),
            paid_artifact("case-b", true, 340, 1500, 0.0040, None),
            paid_artifact("case-c", false, 900, 2600, 0.0110, Some("assertion failed")),
        ]
    }

    /// The gate: every number the reading prints is a restatement of numbers the
    /// paid-for artifacts already carry, so answering needs no second pass over the
    /// upstream stage.
    #[test]
    fn the_reading_restates_the_numbers_the_paid_for_artifacts_carry() {
        let artifacts = fixture_artifacts();
        let report = EvalReport::from_results("priced", &artifacts);

        let total = artifacts.len();
        let passed = artifacts.iter().filter(|r| r.passed).count();
        assert_eq!(report.total_cases, total);
        assert_eq!(report.passed_cases, passed);
        assert_eq!(report.failed_cases, total - passed);
        assert_eq!(report.pass_rate, passed as f64 / total as f64);
        assert_eq!(
            report.total_cost,
            artifacts.iter().map(|r| r.cost).sum::<f64>()
        );
        assert_eq!(
            report.avg_token_usage,
            artifacts.iter().map(|r| r.token_usage).sum::<u64>() as f64 / total as f64
        );
        assert_eq!(
            report.avg_duration_ms,
            artifacts.iter().map(|r| r.duration_ms).sum::<u64>() as f64 / total as f64
        );
        // The duration quantile is a quantile of the artifacts' own durations:
        // 120, 340, 900 sorted, so the median is the middle artifact's row.
        assert_eq!(report.p50_duration_ms, 340.0);

        // Per case, the reading carries the artifact's own row verbatim.
        assert_eq!(report.per_case_results.len(), artifacts.len());
        for (row, artifact) in report.per_case_results.iter().zip(&artifacts) {
            assert_eq!(row.case_id, artifact.case_id);
            assert_eq!(row.passed, artifact.passed);
            assert_eq!(row.duration_ms, artifact.duration_ms);
            assert_eq!(row.token_usage, artifact.token_usage);
            assert_eq!(row.cost, artifact.cost);
        }

        // The failure section is the failing artifact restated, not a new
        // observation of it.
        assert_eq!(report.failures.len(), 1);
        assert_eq!(report.failures[0].case_id, "case-c");
        assert_eq!(report.failures[0].error, "assertion failed");
        assert_eq!(report.failures[0].duration_ms, 900);
    }

    /// Feeding the reading consumes nothing: the artifacts are borrowed, not spent,
    /// and a second reading of the same artifacts agrees with the first -- the
    /// artifact, not a re-run, is the source of truth.
    #[test]
    fn feeding_the_reading_consumes_nothing_from_the_artifacts() {
        let artifacts = fixture_artifacts();
        let first = EvalReport::from_results("priced", &artifacts);

        // The slice is still fully usable: the reading borrowed it rather than
        // spending it.
        assert_eq!(artifacts.len(), 3);
        assert_eq!(artifacts[0].case_id, "case-a");
        assert_eq!(artifacts[2].error.as_deref(), Some("assertion failed"));

        let second = EvalReport::from_results("priced", &artifacts);
        assert_eq!(first.total_cases, second.total_cases);
        assert_eq!(first.passed_cases, second.passed_cases);
        assert_eq!(first.failed_cases, second.failed_cases);
        assert_eq!(first.pass_rate, second.pass_rate);
        assert_eq!(first.total_cost, second.total_cost);
        assert_eq!(first.avg_duration_ms, second.avg_duration_ms);
        assert_eq!(first.p50_duration_ms, second.p50_duration_ms);
        assert_eq!(first.p95_duration_ms, second.p95_duration_ms);
        assert_eq!(first.p99_duration_ms, second.p99_duration_ms);
        assert_eq!(first.avg_token_usage, second.avg_token_usage);
        assert_eq!(first.per_case_results.len(), second.per_case_results.len());
    }

    /// The rejected shape, witnessed: a reading that pays again -- recomputing what
    /// the artifact already carries at the same per-case price -- doubles what one
    /// answer costs, and the redundant half is exactly the money the upstream step
    /// already paid.
    #[test]
    fn a_reading_that_pays_again_costs_exactly_the_upstreams_money() {
        let artifacts = fixture_artifacts();
        let report = EvalReport::from_results("priced", &artifacts);

        // What the upstream step paid, read back from the artifacts it produced.
        let upstream_paid: f64 = artifacts.iter().map(|r| r.cost).sum();
        assert_eq!(report.total_cost, upstream_paid);

        // The rejected reading pays each case's price again to re-derive what the
        // artifact already carries: its bill is the upstream bill exactly.
        let redundant_reading_paid: f64 = artifacts.iter().map(|r| r.cost).sum();
        assert_eq!(redundant_reading_paid, upstream_paid);

        // One answer paid for twice: the extra half is neither more nor less than
        // the money the upstream step already spent.
        let total_paid = upstream_paid + redundant_reading_paid;
        assert_eq!(total_paid, 2.0 * upstream_paid);
        assert_eq!(total_paid - upstream_paid, report.total_cost);
    }
}

/// 这棵树的根：一份算法，全仓共用。
///
/// `tests/` 之外，`src/` 里的单测也要问「这棵树在哪」。声明在这里而不是每个单测
/// 模块各写一条 `#[path]`：`#[path]` 的基准目录会随所在内联模块的嵌套层数变化，
/// 挪一次模块就得改一次路径；crate 根这一处的基准固定是 `src/`。
#[cfg(test)]
#[path = "../../cogneva/tests/common/root.rs"]
pub(crate) mod checkout;

/// 单测共用的样例：一题、一步、一份结果、一件工具、一份空外壳输出。
///
/// 它们不是任何一处的题目，是每处单测都要先摆好的东西——所以只写一份，谁要谁拿。
#[cfg(test)]
pub(crate) mod test_support;
