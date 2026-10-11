//! 双向验收：把「参考解全过」与「退化解全败」两臂各跑一遍整条链。
//!
//! 这一条检验的是**判据本身认不认得出对错**——链上只把「容器 / Compose」那一件换成
//! 一个按臂给结果的替身（真起容器归平台侧），数据、解析、判分规则、运行器、报表都是真的。
//! 参考臂若拿不满分，说明判分会把对的判错；退化臂若拿得到分，说明空答案也能过——两个
//! 方向都得验，只验一边会让「恒过」和「恒错」两种坏判分器各有一半机会蒙混过去。
//!
//! 只在真实取数产物在的时候跑；不在时打印 SKIP 而不是静默变绿。

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use cog_eval::adapters::swe_pro::parse_test_list;
use cog_eval::adapters::{SweProBackend, SweProTestReport, ToolathlonBackend, ToolathlonReport};
use cog_eval::{
    swe_pro_benchmark, toolathlon_benchmark, AgentOutput, AgentScaffold, CaseEnv, EvalCase,
    Benchmark, FinishReason, NoEnv, Rig, RunConfig, SolveContext, SweProToolkit, Table,
    ToolathlonToolkit,
    DATA_ROOT_ENV, SWE_PRO_JSONL, TOOLATHLON_TASKS_DIR,
};
use cog_core::LlmClient;

/// 占位上游：这一条链上没有任何一步该碰模型。
struct SilentLlm;

#[async_trait]
impl LlmClient for SilentLlm {
    async fn chat_stream(
        &self,
        _m: &[cog_core::Message],
        _o: &cog_core::ChatOptions,
    ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
        unreachable!("the rig never streams")
    }
    async fn complete_stream(
        &self,
        _p: &str,
        _o: &cog_core::CompleteOptions,
    ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
        unreachable!("the rig never streams")
    }
    async fn chat(
        &self,
        _m: &[cog_core::Message],
        _o: &cog_core::ChatOptions,
    ) -> cog_core::SFResult<cog_core::ChatResponse> {
        unreachable!("no scaffold here calls the backbone")
    }
    async fn health_check(&self) -> bool {
        false
    }
}

/// 交白卷的外壳。判分读的是测试结果 / 官方脚本退出码，不看它写了什么。
struct EmptyScaffold;

#[async_trait]
impl AgentScaffold for EmptyScaffold {
    fn name(&self) -> &str {
        "empty"
    }
    async fn solve(&self, _case: &EvalCase, _ctx: &SolveContext) -> anyhow::Result<AgentOutput> {
        Ok(AgentOutput {
            final_answer: String::new(),
            trace: vec![],
            tokens: Default::default(),
            finish: FinishReason::Answered,
        })
    }
}

/// SWE-bench Pro 的替身后端：`reference` 时把该题声明的测试全报通过（模拟打了金标补丁），
/// 否则一个都不通过（模拟空补丁）。
struct SweProArm {
    reference: bool,
}

fn declared_tests(case: &EvalCase) -> (Vec<String>, Vec<String>) {
    let field = |k: &str| {
        parse_test_list(case.metadata.get(k).map(String::as_str).unwrap_or("[]"))
            .unwrap_or_else(|e| panic!("{} {k}: {e}", case.id))
    };
    (field("fail_to_pass"), field("pass_to_pass"))
}

#[async_trait]
impl SweProBackend for SweProArm {
    async fn acquire(&self, _case: &EvalCase) -> anyhow::Result<Arc<dyn CaseEnv>> {
        Ok(Arc::new(NoEnv))
    }
    async fn run_tests(
        &self,
        _env: &Arc<dyn CaseEnv>,
        case: &EvalCase,
    ) -> anyhow::Result<SweProTestReport> {
        let (f2p, p2p) = declared_tests(case);
        assert!(!f2p.is_empty(), "{} has no fail_to_pass to run", case.id);
        let passed: BTreeSet<String> = if self.reference {
            f2p.into_iter().chain(p2p).collect()
        } else {
            BTreeSet::new()
        };
        Ok(SweProTestReport {
            passed,
            raw: format!("arm={} case={}", self.reference, case.id),
        })
    }
}

/// Toolathlon 的替身后端：`reference` 时官方脚本退出码 0，否则 1。
struct ToolathlonArm {
    reference: bool,
}

#[async_trait]
impl ToolathlonBackend for ToolathlonArm {
    async fn acquire(&self, _case: &EvalCase) -> anyhow::Result<Arc<dyn CaseEnv>> {
        Ok(Arc::new(NoEnv))
    }
    async fn evaluate(
        &self,
        _env: &Arc<dyn CaseEnv>,
        _case: &EvalCase,
    ) -> anyhow::Result<ToolathlonReport> {
        Ok(ToolathlonReport {
            exit_code: if self.reference { 0 } else { 1 },
            checks: vec![],
            raw: format!("arm={}", self.reference),
        })
    }
}

fn data_present(root: &Path, rel: &str) -> bool {
    root.join(rel).exists()
}

#[tokio::test]
async fn the_rig_scores_the_reference_arm_high_and_the_degenerate_arm_zero() {
    let Ok(root) = std::env::var(DATA_ROOT_ENV) else {
        eprintln!("SKIP: {DATA_ROOT_ENV} is not set, both directions were not run");
        return;
    };
    let root = std::path::PathBuf::from(root);
    let has_swe = data_present(&root, SWE_PRO_JSONL);
    let has_tool = data_present(&root, TOOLATHLON_TASKS_DIR);
    if !has_swe && !has_tool {
        eprintln!("SKIP: neither benchmark's real data is under {root:?}");
        return;
    }

    // 一臂一个 Rig：同名的两臂不能同表（同一列名会让读数无从归因）。
    async fn run_arm(root: std::path::PathBuf, bench: Benchmark) -> Table {
        Rig::new(Arc::new(SilentLlm), RunConfig::new(root))
            .with_scaffold(Arc::new(EmptyScaffold))
            .run(&[bench])
            .await
    }

    if has_swe {
        let reference = swe_pro_benchmark(
            Some(Arc::new(SweProArm { reference: true })),
            SweProToolkit::without_tools(),
        );
        let degenerate = swe_pro_benchmark(
            Some(Arc::new(SweProArm { reference: false })),
            SweProToolkit::without_tools(),
        );
        let high = run_arm(root.clone(), reference).await;
        let zero = run_arm(root.clone(), degenerate).await;
        let cell = |t: &Table| t.cell("empty", "swe-bench-pro").unwrap().clone();
        let (hi, lo) = (cell(&high), cell(&zero));
        assert_eq!(hi.per_seed[0].total, 731, "SWE-bench Pro public split");
        assert!(
            hi.per_seed.iter().all(|s| s.errored == 0),
            "the reference arm errored: {:?}",
            hi.per_seed.iter().map(|s| s.errored).collect::<Vec<_>>()
        );
        assert!(
            (hi.stat().mean - 1.0).abs() < 1e-9,
            "the reference arm must score full, got {}",
            hi.stat().mean
        );
        assert_eq!(lo.stat().mean, 0.0, "the degenerate arm must score zero");
        assert!(
            lo.per_seed.iter().all(|s| s.errored == 0),
            "the degenerate arm errored instead of failing: {:?}",
            lo.per_seed.iter().map(|s| s.errored).collect::<Vec<_>>()
        );
        // 两臂必须**每题都相反**：有一题两臂同判，判据就在那题上是瞎的。
        assert!(
            hi.per_seed[0].failures.is_empty(),
            "the reference arm named failures: {:?}",
            &hi.per_seed[0].failures[..hi.per_seed[0].failures.len().min(3)]
        );
        assert_eq!(
            lo.per_seed[0].failures.len(),
            731,
            "every case must be a failure in the degenerate arm"
        );
    }

    if has_tool {
        let reference = toolathlon_benchmark(
            Some(Arc::new(ToolathlonArm { reference: true })),
            ToolathlonToolkit::without_tools(),
        );
        let degenerate = toolathlon_benchmark(
            Some(Arc::new(ToolathlonArm { reference: false })),
            ToolathlonToolkit::without_tools(),
        );
        let high = run_arm(root.clone(), reference).await;
        let zero = run_arm(root.clone(), degenerate).await;
        let hi = high.cell("empty", "toolathlon").unwrap();
        let lo = zero.cell("empty", "toolathlon").unwrap();
        assert_eq!(hi.per_seed[0].total, 503, "Toolathlon GYM tasks");
        assert!(
            (hi.stat().mean - 1.0).abs() < 1e-9,
            "the reference arm must score full, got {}",
            hi.stat().mean
        );
        assert_eq!(lo.stat().mean, 0.0, "the degenerate arm must score zero");
        assert_eq!(
            lo.per_seed[0].errored, 0,
            "exit code 1 is a failure, not an error"
        );
    }
}
