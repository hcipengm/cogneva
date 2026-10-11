//! 数据目录旋钮的调用点：环境变量 → `RunConfig` → `Rig` → `CaseSource` 读目录。
//!
//! 这条链在仓库里没有第二个真正走过的地方——评测台今天没有 driver 二进制，`Rig` 只在
//! 测试里被构造。一个「写在文档里、没有任何读者」的旋钮和没有旋钮是一样的，所以这里
//! 让它端到端跑一次：只把 [`DATA_ROOT_ENV`] 指向一个目录，那个目录里的题就必须出现在
//! 表里。断言落在**题数**上，不是落在「配置对象里那个字段等于几」上——后者在旋钮接错
//! 的时候照样能绿。
//!
//! 这个文件是独立的 test binary，所以在这里设环境变量不会和别的测试抢。

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use cog_core::{ChatOptions, ChatResponse, CompleteOptions, Message, SFResult};
use cog_eval::{
    AgentOutput, AgentScaffold, Benchmark, Budget, CaseJudge, CaseSource, EnvProvider, EvalCase,
    FinishReason, NoEnv, Rig, RunConfig, SolveContext, ToolSet, Toolkit, Verdict, DATA_ROOT_ENV,
};

/// 上游永远不该被叫到：这几条路径只看数据从哪来，不看模型答什么。
struct NeverCalled;

#[async_trait]
impl cog_core::LlmClient for NeverCalled {
    async fn chat_stream(
        &self,
        _m: &[Message],
        _o: &ChatOptions,
    ) -> SFResult<cog_core::AssistantMessageEventStream> {
        unreachable!()
    }
    async fn complete_stream(
        &self,
        _p: &str,
        _o: &CompleteOptions,
    ) -> SFResult<cog_core::AssistantMessageEventStream> {
        unreachable!()
    }
    async fn chat(&self, _m: &[Message], _o: &ChatOptions) -> SFResult<ChatResponse> {
        unreachable!()
    }
    async fn health_check(&self) -> bool {
        false
    }
}

/// 题从 `<root>/cases.txt` 按行读：数据根本身就是这份文件所在的目录，读错目录题数就是 0。
struct FileSource;

#[async_trait]
impl CaseSource for FileSource {
    fn cases(&self, root: &Path) -> anyhow::Result<Vec<EvalCase>> {
        let body = std::fs::read_to_string(root.join("cases.txt"))?;
        Ok(body
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(|id| EvalCase {
                id: id.to_string(),
                name: id.to_string(),
                input: serde_json::json!(id),
                expected_output: None,
                expected_tools: None,
                tags: vec![],
                metrics: vec![],
                metadata: Default::default(),
            })
            .collect())
    }
}

struct OkEnv;
#[async_trait]
impl EnvProvider for OkEnv {
    async fn acquire(&self, _c: &EvalCase) -> anyhow::Result<Arc<dyn cog_eval::CaseEnv>> {
        Ok(Arc::new(NoEnv))
    }
}

struct NoTools;
impl Toolkit for NoTools {
    fn toolset(&self, _c: &EvalCase, _e: &Arc<dyn cog_eval::CaseEnv>) -> anyhow::Result<ToolSet> {
        Ok(ToolSet::empty())
    }
}

/// 判分器只认「答案等于题号」；它不认识外壳。
struct IdJudge;
#[async_trait]
impl CaseJudge for IdJudge {
    async fn judge(
        &self,
        case: &EvalCase,
        _env: &Arc<dyn cog_eval::CaseEnv>,
        output: &AgentOutput,
    ) -> anyhow::Result<Verdict> {
        Ok(if output.final_answer == case.id {
            Verdict::pass("echoed its id")
        } else {
            Verdict::fail("did not echo its id")
        })
    }
}

/// 把题号原样交回去的外壳：它自己当然知道题号，所以「这一格满分」只说明题真的到了它手上。
struct Echo;

#[async_trait]
impl AgentScaffold for Echo {
    fn name(&self) -> &str {
        "echo"
    }
    async fn solve(&self, case: &EvalCase, _ctx: &SolveContext) -> anyhow::Result<AgentOutput> {
        Ok(AgentOutput {
            final_answer: case.id.clone(),
            trace: vec![],
            tokens: Default::default(),
            finish: FinishReason::Answered,
        })
    }
}

fn benchmark() -> Benchmark {
    Benchmark {
        name: "hle".into(),
        metric: "pass@1".into(),
        cases: Arc::new(FileSource),
        env: Arc::new(OkEnv),
        tools: Arc::new(NoTools),
        judge: Arc::new(IdJudge),
        budget: Budget::default(),
    }
}

#[tokio::test]
async fn the_data_root_env_var_is_what_the_rig_reads_cases_from() {
    // 没设变量就是错误，不是回落：一条默认路径会把「数据根指错了」藏起来。
    std::env::remove_var(DATA_ROOT_ENV);
    let err = RunConfig::from_env().unwrap_err().to_string();
    assert!(err.contains(DATA_ROOT_ENV), "{err}");
    // 空串同样不是合法的数据根。
    std::env::set_var(DATA_ROOT_ENV, "   ");
    assert!(RunConfig::from_env().is_err());
    std::env::remove_var(DATA_ROOT_ENV);

    // 真目录里放三道题，把变量指过去，表里那一格就必须是三道、且全过。
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("cases.txt"), "c1\nc2\nc3\n").unwrap();
    std::env::set_var(DATA_ROOT_ENV, dir.path());

    let config = RunConfig::from_env().unwrap();
    assert_eq!(config.data_root.as_path(), dir.path());
    let table = Rig::new(Arc::new(NeverCalled), config)
        .with_scaffold(Arc::new(Echo))
        .run(&[benchmark()])
        .await;

    let cell = table.cell("echo", "hle").unwrap();
    assert_eq!(cell.per_seed[0].total, 3, "题是从环境变量指的那个目录读的");
    assert_eq!(cell.per_seed[0].resolved, 3);
    assert_eq!(cell.stat().mean, 1.0);
    assert_eq!(cell.errored(), 0);

    // 换一个目录，题数跟着变——上面那个 3 不是写死的。
    let other = tempfile::tempdir().unwrap();
    std::fs::write(other.path().join("cases.txt"), "c1\n").unwrap();
    std::env::set_var(DATA_ROOT_ENV, other.path());
    let table = Rig::new(Arc::new(NeverCalled), RunConfig::from_env().unwrap())
        .with_scaffold(Arc::new(Echo))
        .run(&[benchmark()])
        .await;
    assert_eq!(table.cell("echo", "hle").unwrap().per_seed[0].total, 1);

    std::env::remove_var(DATA_ROOT_ENV);
}
