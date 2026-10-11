//! 基准面：把「数据 / 环境 / 工具包 / 判分」四件绑到同一个名字上。
//!
//! 这四件**只跟基准绑定**，不跟方法绑定——方法那一侧是 [`crate::scaffold`]。两边
//! 在这条线上相遇的只有一次：一个 case 被交给一个外壳，外壳交回一份
//! [`AgentOutput`]，判分器读它。加一个基准＝照这一节加四个适配器，**不改外壳接口、
//! 不改运行器**；这条守住，扩容才一直便宜。

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use cog_core::{Tool, ToolDefinition, ToolImplementation};

use crate::dataset::EvalCase;
use crate::scaffold::{AgentOutput, Budget, CaseEnv, ToolSet};

/// 一个 case 的判词。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Verdict {
    /// 这一格要的那个 0/1：过就是 1。
    pub resolved: bool,
    /// 判分器给的判词。没通过时写清是**哪里**没过（哪条测试红了、脚本退出码几），
    /// 否则这一行的低分无从归因。
    pub detail: String,
    /// 判分器自报的读数（跑了多少条测试、判分脚本退出码、裁判模型名……）。
    /// 给人看、给报表用，**不参与打分**——打分只看 `resolved`。
    #[serde(default)]
    pub readings: HashMap<String, String>,
}

impl Verdict {
    pub fn pass(detail: impl Into<String>) -> Self {
        Self {
            resolved: true,
            detail: detail.into(),
            readings: HashMap::new(),
        }
    }

    pub fn fail(detail: impl Into<String>) -> Self {
        Self {
            resolved: false,
            detail: detail.into(),
            readings: HashMap::new(),
        }
    }
}

/// 数据适配器：基准自己的文件格式 → [`EvalCase`]，一个基准一个。
#[async_trait]
pub trait CaseSource: Send + Sync {
    /// `root` 是取数脚本的落点目录；适配器自己知道它在里面读哪个文件。
    /// 数据不进仓库（几个 GB 的 parquet / tarball），所以读的是运行时那个根。
    fn cases(&self, root: &Path) -> anyhow::Result<Vec<EvalCase>>;
}

/// 环境适配器：起这道题要跑的那个环境，一个基准一个。
#[async_trait]
pub trait EnvProvider: Send + Sync {
    /// 无环境基准（纯 API ＋ 工具的基准）返回 [`crate::scaffold::NoEnv`]，
    /// 而不是报「起不起来」——没有环境与起不来是两件事。
    async fn acquire(&self, case: &EvalCase) -> anyhow::Result<Arc<dyn CaseEnv>>;
}

/// 无环境基准的环境适配器。
///
/// 纯 API 的基准（HLE，以及将来同形的 GAIA）用它——它报的是「这道题本来就落在 API 上」，
/// 不是「环境起不来」。要真环境（容器、compose）的基准各写各的，别共用一个假的。
pub struct NoEnvProvider;

#[async_trait]
impl EnvProvider for NoEnvProvider {
    async fn acquire(&self, _case: &EvalCase) -> anyhow::Result<Arc<dyn CaseEnv>> {
        Ok(Arc::new(crate::scaffold::NoEnv))
    }
}

/// 按题造工具：拿到这道题的 `case` 与环境**之后**才定工具清单。
///
/// 与「构造期收一束现成工具」是两件事：容器里的 bash、某道题 compose 工程暴露出来的
/// 端口，都要按**这道题的**环境造，而构造期还不知道是哪道题，造不出来。工厂因此收
/// `case` 与 `env`，由 [`Toolkit::toolset`] 在拿到它们时现调。
///
/// 工厂住在基准各自的适配器里，评测台不认识任何基准——所以「工具怎么造」这件事
/// 不进入台子，也就不会让某一列变得不可比。
pub type ToolFactory =
    dyn Fn(&EvalCase, &Arc<dyn CaseEnv>) -> anyhow::Result<Vec<Tool>> + Send + Sync;

/// 把基准定义的工具面**接到这道题环境的执行面**上。
///
/// 定义（名字＋参数 schema）由基准出，执行落在**这道题自己的**环境里——这正是
/// [`ToolFactory`] 要的形状：承载工具的容器 / compose 工程，要到 `env` 拿到时才存在。
/// 名字**逐字**递给执行面，这里不改写、不翻译；执行面收到不认识的名字要自己报错，
/// 不许静默落到别的后端（落到平台沙盒里跑出来的不是这道题的答案）。
///
/// 环境没有执行面时**报错**，不回落成一束空工具：「这道题本来就没有环境」与「环境的
/// 执行面没接上」在分数上同形，必须在这一层就分开。
pub fn tools_bound_to(
    env: &Arc<dyn CaseEnv>,
    definitions: &[ToolDefinition],
) -> anyhow::Result<Vec<Tool>> {
    let executor = env.executor().ok_or_else(|| {
        anyhow::anyhow!(
            "environment `{}` offers no execution face, so a benchmark tool cannot run in it",
            env.id()
        )
    })?;
    Ok(definitions
        .iter()
        .map(|definition| {
            let executor = executor.clone();
            let name = definition.name.clone();
            Tool {
                name: definition.name.clone(),
                description: definition.description.clone(),
                parameters: definition.parameters.clone(),
                implementation: ToolImplementation::Native(Arc::new(move |arguments| {
                    let executor = executor.clone();
                    let name = name.clone();
                    Box::pin(async move { executor.execute(&name, arguments).await })
                })),
            }
        })
        .collect())
}

/// 工具包：这道题给外壳的工具面，一个基准一个。
///
/// 协议由基准定、**不由方法定**——所以它在基准这一侧，不在外壳那一侧。同一基准
/// 下四个外壳拿到的是同一束工具，否则表里的差就掺进了「谁的工具多」。
pub trait Toolkit: Send + Sync {
    /// 工具是绑在 `env` 上的闭包：命令要落在**这道题的**环境里，不是平台沙盒里。
    /// 这一层每次调用现造（见 [`ToolFactory`]），清单可以随题而变。
    fn toolset(&self, case: &EvalCase, env: &Arc<dyn CaseEnv>) -> anyhow::Result<ToolSet>;

    /// 外壳动作空间指名的那几件工具（`wanted` ＝ 外壳声明的名字），由**这道题的环境**供。
    ///
    /// 与 [`Self::toolset`] 分开，是因为两者消融不同：`toolset` 是基准外部工具面，无工具臂
    /// 关掉它；动作空间不参与消融，两臂都拿得到。评测台把两者并起来交给外壳。
    ///
    /// 默认交回空集＝这个基准的这道题供不出任何动作空间工具。今天组合根还没接工具实现，
    /// 所以每个基准都走默认，并集因此是恒等的——不改变任何一格的输出。
    fn action_tools(
        &self,
        _case: &EvalCase,
        _env: &Arc<dyn CaseEnv>,
        _wanted: &[&str],
    ) -> anyhow::Result<ToolSet> {
        Ok(ToolSet::empty())
    }
}

/// 判分适配器：读外壳交回的答案与环境，给一个 0/1。一个基准一个，**方法无关**。
#[async_trait]
pub trait CaseJudge: Send + Sync {
    /// 调用时环境还在（判分要看到外壳改过的那棵树）；回收由运行器在判分之后做。
    /// 判分器**不许**自己回收环境——它一回收，下一个读的人就没有现场了。
    async fn judge(
        &self,
        case: &EvalCase,
        env: &Arc<dyn CaseEnv>,
        output: &AgentOutput,
    ) -> anyhow::Result<Verdict>;
}

/// 一个基准 ＝ 四件适配器 ＋ 它的指标名与预算。
///
/// 这里**没有**外壳的位置：加了外壳，评测台就成了「我们的外壳跑别人的方法」，
/// 那一行出来的是混合体，不是那个方法。
pub struct Benchmark {
    pub name: String,
    /// 表头用的指标名（`pass@1` / `resolved@1`）。指标口径不同就不该混在一列里比。
    pub metric: String,
    pub cases: Arc<dyn CaseSource>,
    pub env: Arc<dyn EnvProvider>,
    pub tools: Arc<dyn Toolkit>,
    pub judge: Arc<dyn CaseJudge>,
    pub budget: Budget,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scaffold::{AgentScaffold, FinishReason, SolveContext};
    use cog_core::{ChatOptions, ChatResponse, Message, SFError, SFResult};
    use std::path::PathBuf;

    /// 替身环境：把一句话的执行面（或不给）按需接上。
    struct StubEnv {
        executor: Option<Arc<dyn cog_core::ToolExecutor>>,
    }

    #[async_trait]
    impl CaseEnv for StubEnv {
        fn id(&self) -> &str {
            "stub"
        }
        fn executor(&self) -> Option<Arc<dyn cog_core::ToolExecutor>> {
            self.executor.clone()
        }
        async fn teardown(&self) {}
    }

    /// 只认自己那一套工具名的执行面，并把收到的名字记下来。
    struct OnlyBash(std::sync::Mutex<Vec<String>>);

    #[async_trait]
    impl cog_core::ToolExecutor for OnlyBash {
        async fn execute(
            &self,
            name: &str,
            arguments: serde_json::Value,
        ) -> SFResult<serde_json::Value> {
            if name != "bash" {
                return Err(SFError::Validation(format!(
                    "this environment runs no `{name}`"
                )));
            }
            self.0.lock().unwrap().push(name.to_string());
            Ok(arguments)
        }
    }

    fn definition(name: &str) -> ToolDefinition {
        ToolDefinition {
            name: name.into(),
            description: name.into(),
            parameters: serde_json::json!({"type": "object"}),
        }
    }

    /// 定义由基准出、执行落在**这道题的环境**里；名字逐字到达执行面，回来的错也原样传上去。
    #[tokio::test]
    async fn a_benchmark_tool_runs_in_the_case_environment() {
        let executor = Arc::new(OnlyBash(Default::default()));
        let env: Arc<dyn CaseEnv> = Arc::new(StubEnv {
            executor: Some(executor.clone()),
        });
        let set = ToolSet::new(
            tools_bound_to(&env, &[definition("bash"), definition("python")]).unwrap(),
        )
        .unwrap();
        assert_eq!(set.definitions().len(), 2);

        let out = set
            .call("bash", serde_json::json!({"command": "ls"}))
            .await
            .unwrap();
        assert_eq!(out, serde_json::json!({"command": "ls"}));
        assert_eq!(executor.0.lock().unwrap().as_slice(), ["bash"]);

        // 执行面不认识的名字由**它**报错，不为这一格编一个空结果。
        let err = set
            .call("python", serde_json::json!({}))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("python"), "{err}");
    }

    /// 环境没有执行面是**报错**，不是给一束空工具：两者在分数上同形。
    #[tokio::test]
    async fn an_environment_without_an_execution_face_is_an_error() {
        let env: Arc<dyn CaseEnv> = Arc::new(StubEnv { executor: None });
        let err = tools_bound_to(&env, &[definition("bash")])
            .unwrap_err()
            .to_string();
        assert!(err.contains("no execution face"), "{err}");
    }

    struct MemorySource {
        cases: Vec<EvalCase>,
    }

    #[async_trait]
    impl CaseSource for MemorySource {
        fn cases(&self, _root: &Path) -> anyhow::Result<Vec<EvalCase>> {
            Ok(self.cases.clone())
        }
    }

    struct NoEnvProvider;

    #[async_trait]
    impl EnvProvider for NoEnvProvider {
        async fn acquire(&self, _case: &EvalCase) -> anyhow::Result<Arc<dyn CaseEnv>> {
            Ok(Arc::new(crate::scaffold::NoEnv))
        }
    }

    struct EchoToolkit;

    impl Toolkit for EchoToolkit {
        fn toolset(&self, _case: &EvalCase, _env: &Arc<dyn CaseEnv>) -> anyhow::Result<ToolSet> {
            Ok(ToolSet::empty())
        }
    }

    /// 判分器只看答案对不对——**它不认识任何外壳**，这正是它该有的样子。
    struct ExactJudge;

    #[async_trait]
    impl CaseJudge for ExactJudge {
        async fn judge(
            &self,
            case: &EvalCase,
            _env: &Arc<dyn CaseEnv>,
            output: &AgentOutput,
        ) -> anyhow::Result<Verdict> {
            let expected = case
                .expected_output
                .as_ref()
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            Ok(if output.final_answer == expected {
                Verdict::pass("exact match")
            } else {
                Verdict::fail(format!("want `{expected}`, got `{}`", output.final_answer))
            })
        }
    }

    fn case(id: &str, expected: &str) -> EvalCase {
        EvalCase {
            id: id.into(),
            name: id.into(),
            input: serde_json::json!("question?"),
            expected_output: Some(serde_json::json!(expected)),
            expected_tools: None,
            tags: vec![],
            metrics: vec![],
            metadata: Default::default(),
        }
    }

    fn benchmark(cases: Vec<EvalCase>) -> Benchmark {
        Benchmark {
            name: "toy".into(),
            metric: "pass@1".into(),
            cases: Arc::new(MemorySource { cases }),
            env: Arc::new(NoEnvProvider),
            tools: Arc::new(EchoToolkit),
            judge: Arc::new(ExactJudge),
            budget: Budget::default(),
        }
    }

    /// 只要一个能应答的 backbone：四条路径（这四件）都不该碰模型的能力边界。
    struct ScriptedLlm {
        answer: String,
    }

    #[async_trait]
    impl cog_core::LlmClient for ScriptedLlm {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _options: &ChatOptions,
        ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
            unimplemented!("the rig never streams")
        }
        async fn complete_stream(
            &self,
            _prompt: &str,
            _options: &cog_core::CompleteOptions,
        ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
            unimplemented!("the rig never streams")
        }
        async fn chat(
            &self,
            _messages: &[Message],
            _options: &ChatOptions,
        ) -> cog_core::SFResult<ChatResponse> {
            unimplemented!("unused by this test: {}", self.answer)
        }
        async fn health_check(&self) -> bool {
            true
        }
    }

    /// 一个最小的外壳：直接把题面当答案交回去。它在这里只用来证明四件能拼起来。
    struct EchoScaffold;

    #[async_trait]
    impl AgentScaffold for EchoScaffold {
        fn name(&self) -> &str {
            "echo"
        }
        async fn solve(&self, case: &EvalCase, _ctx: &SolveContext) -> anyhow::Result<AgentOutput> {
            Ok(AgentOutput {
                final_answer: case.input.as_str().unwrap_or_default().to_string(),
                trace: vec![],
                tokens: Default::default(),
                finish: FinishReason::Answered,
            })
        }
    }

    #[tokio::test]
    async fn the_four_adapters_compose_without_any_scaffold_knowledge() {
        let bench = benchmark(vec![case("c1", "question?"), case("c2", "other")]);
        let cases = bench.cases.cases(&PathBuf::from("/nonexistent")).unwrap();
        assert_eq!(cases.len(), 2);

        let env = bench.env.acquire(&cases[0]).await.unwrap();
        let tools = bench.tools.toolset(&cases[0], &env).unwrap();
        assert!(tools.is_empty());

        let ctx = SolveContext {
            llm: Arc::new(ScriptedLlm {
                answer: "unused".into(),
            }),
            tools: Arc::new(tools),
            env: env.clone(),
            budget: bench.budget,
            seed: 0,
        };
        let out = EchoScaffold.solve(&cases[0], &ctx).await.unwrap();
        let verdict = bench.judge.judge(&cases[0], &env, &out).await.unwrap();
        assert!(verdict.resolved, "{}", verdict.detail);

        // 判分器自己不许回收环境：这里回收，判分读到的还是那一棵树。
        env.teardown().await;

        let out2 = EchoScaffold.solve(&cases[1], &ctx).await.unwrap();
        let v2 = bench.judge.judge(&cases[1], &env, &out2).await.unwrap();
        assert!(!v2.resolved);
        assert!(v2.detail.contains("question?"), "{}", v2.detail);
    }

    #[test]
    fn the_metric_name_travels_with_the_benchmark() {
        // 口径不同的基准不能混在一列里比：`pass@1` 与 `resolved@1` 是两个词。
        assert_eq!(benchmark(vec![]).metric, "pass@1");
    }
}
