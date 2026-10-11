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

/// 工具包：这道题给外壳的工具面，一个基准一个。
///
/// 协议由基准定、**不由方法定**——所以它在基准这一侧，不在外壳那一侧。同一基准
/// 下四个外壳拿到的是同一束工具，否则表里的差就掺进了「谁的工具多」。
pub trait Toolkit: Send + Sync {
    /// 工具是绑在 `env` 上的闭包：命令要落在**这道题的**环境里，不是平台沙盒里。
    fn toolset(&self, case: &EvalCase, env: &Arc<dyn CaseEnv>) -> anyhow::Result<ToolSet>;
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
    use cog_core::{ChatOptions, ChatResponse, Message};
    use std::path::PathBuf;

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
