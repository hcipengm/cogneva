//! 外壳抽象：主表里「被测对象」的唯一接口。
//!
//! 主表的一行是一个**外壳**（把模型变成 agent 的那圈东西：prompt、工具调用循环、
//! 记忆、控制流），不是模型——模型是常数，四行共用同一个 backbone。所以评测台对
//! 外壳只认这一个 trait：加一行只加一个实现，运行器 / 判分 / 工具一行不改。
//!
//! 外壳拿到题面、工具面、环境和 backbone；**拿不到判分器**，判分在它返回之后由
//! 评测台做。判分器可见、或工具面可注册的外壳可以直接给自己补上缺的那块能力，
//! 那样这一行的数就不是这个方法的数了。

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use cog_core::{LlmClient, SFError, SFResult, Tool, ToolDefinition, ToolExecutor};

use crate::dataset::EvalCase;
use crate::metric::StepRecord;

/// 外壳跑完一题交回来的东西。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AgentOutput {
    /// 最终答案。脚本判分的基准这里可能是补丁文本或一条命令的输出，
    /// 判分器怎么读由基准定，外壳不管。
    pub final_answer: String,
    /// 过程记录。配套表里的步数、工具调用次数都从这里数出来，不另开计数器。
    pub trace: Vec<StepRecord>,
    pub tokens: cog_core::Usage,
    pub finish: FinishReason,
}

/// 外壳为什么停下。
///
/// 答错与「步数用尽」在分数上是同一格，在「这行为什么低」上却是两件事：上限是
/// 基准旋钮（四行同值），撞上限必须看得见，否则读不出是模型不行还是预算给少了。
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinishReason {
    /// 外壳自己认为答完了。
    Answered,
    /// 撞到步数上限。
    StepBudget,
    /// 撞到上下文上限。
    ContextBudget,
    /// 外壳自报的失败（上游报错、工具错误无法恢复……）。
    Failed,
}

/// 一次求解的预算。四行外壳、三个基准上每格都用同一份——换一个值，格子之间
/// 就不可比了。
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Budget {
    pub max_steps: usize,
    pub max_context_tokens: u32,
    pub temperature: f32,
}

impl Default for Budget {
    fn default() -> Self {
        // 逐类比照公开值，不额外加工具、不放松上限。
        Self {
            max_steps: 500,
            max_context_tokens: 512 * 1024,
            temperature: 1.0,
        }
    }
}

/// 一个 case 的运行环境。
///
/// 求解与判分**共用**它：判分要看到外壳改过的那棵树、那个数据库，换一个环境判就
/// 是在判另一道题。所以回收由判分之后的评测台做，判分器自己不许回收。
#[async_trait]
pub trait CaseEnv: Send + Sync {
    /// 环境标识（容器 id / compose 工程名）。进 trace，只为复现时能找回现场。
    fn id(&self) -> &str;
    /// 环境给的执行面。无环境基准返回 `None`。
    fn executor(&self) -> Option<Arc<dyn ToolExecutor>>;
    /// 回收。判分读过它之后调用；重复调用必须是空操作（一条 case 失败也要回收）。
    async fn teardown(&self);
}

/// 无环境基准（纯 API + 工具的基准）的环境。
///
/// 用空实现而不是 `Option<Box<dyn CaseEnv>>`：后者会把「这个基准本来就没有环境」
/// 与「环境没起起来」压成同一个形状，而这两件事一件正常、一件该报警。
pub struct NoEnv;

#[async_trait]
impl CaseEnv for NoEnv {
    fn id(&self) -> &str {
        "none"
    }
    fn executor(&self) -> Option<Arc<dyn ToolExecutor>> {
        None
    }
    async fn teardown(&self) {}
}

/// 外壳能拿到的工具面：给模型看的规格 ＋ 按名执行。
///
/// 设计把这个位置写成 `ToolRegistry`，这里收窄成**只能执行、不能注册**：工具协议
/// 由基准定、不由方法定。一个能注册工具的外壳可以给自己加上本来没有的工具（乃至
/// 判分器的能力），那一行就不是这个方法的数了。
#[derive(Debug)]
pub struct ToolSet {
    definitions: Vec<ToolDefinition>,
    tools: HashMap<String, Tool>,
}

impl ToolSet {
    /// 只接受 native 实现。
    ///
    /// 基准的工具都是绑在**这个 case 的环境**上的闭包（容器里的 bash、题目的
    /// 判分前脚本），所以它们必须是闭包本身，而不是一句「怎么跑」的声明——后者
    /// 要由评测台再解释一遍，而解释用的后端是平台沙盒，不是这道题的那个容器，
    /// 跑出来的就不是这道题的答案。声明式实现（WASM / shell）在这里直接拒绝，
    /// 免得它静默地跑到别处去。
    pub fn new(tools: Vec<Tool>) -> anyhow::Result<Self> {
        let mut map = HashMap::new();
        for tool in tools {
            if !matches!(tool.implementation, cog_core::ToolImplementation::Native(_)) {
                anyhow::bail!(
                    "tool `{}` is not a native implementation: the rig runs benchmark tools as \
                     closures bound to the case environment, not as declarations for a backend",
                    tool.name
                );
            }
            map.insert(tool.name.clone(), tool);
        }
        let definitions = map
            .values()
            .map(|t| ToolDefinition {
                name: t.name.clone(),
                description: t.description.clone(),
                parameters: t.parameters.clone(),
            })
            .collect();
        Ok(Self {
            definitions,
            tools: map,
        })
    }

    /// 空工具面（无工具的对照跑，以及不需要工具的基准）。
    pub fn empty() -> Self {
        Self {
            definitions: Vec::new(),
            tools: HashMap::new(),
        }
    }

    /// 给模型看的工具规格。
    pub fn definitions(&self) -> &[ToolDefinition] {
        &self.definitions
    }

    pub fn is_empty(&self) -> bool {
        self.tools.is_empty()
    }

    /// 按名执行。未知工具名是**错误**不是空结果：外壳喊了一个不存在的工具，
    /// 这一格的低分要能归因到外壳自己，而不是被吞成一次「工具返回空」。
    pub async fn call(
        &self,
        name: &str,
        arguments: serde_json::Value,
    ) -> SFResult<serde_json::Value> {
        let Some(tool) = self.tools.get(name) else {
            return Err(SFError::Validation(format!("unknown tool `{name}`")));
        };
        match &tool.implementation {
            cog_core::ToolImplementation::Native(handler) => handler(arguments).await,
            _ => Err(SFError::Internal(format!(
                "tool `{name}` is not a native implementation"
            ))),
        }
    }
}

/// 外壳求解一题时能拿到的一切。没有判分器，也没有可变的工具面。
pub struct SolveContext {
    /// backbone。四行共用同一个上游与同一个模型——它要是变量，表就不是方法 × 基准。
    pub llm: Arc<dyn LlmClient>,
    pub tools: Arc<ToolSet>,
    pub env: Arc<dyn CaseEnv>,
    pub budget: Budget,
    /// 这是同一格的三个独立运行里的哪一个。
    ///
    /// 请求本身没有种子这个旋钮，所以它不是一个上游参数：它标识的是**这一次运行**，
    /// 由外壳在自己的随机处（采样、人口初始化、示例顺序）取用，评测台负责保证同一格
    /// 的三个种子互不相同。读表时它只用来把三个数摊开成均值 ± 标准差。
    pub seed: u64,
}

/// 被测对象。主表里一行一个实现。
#[async_trait]
pub trait AgentScaffold: Send + Sync {
    /// 这一行在表里的名字。评测台按它建行，不按 Rust 类型名——换实现类型不该改表。
    fn name(&self) -> &str;

    async fn solve(&self, case: &EvalCase, ctx: &SolveContext) -> anyhow::Result<AgentOutput>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn native_tool(name: &str) -> Tool {
        Tool {
            name: name.into(),
            description: format!("{name} tool"),
            parameters: serde_json::json!({"type": "object"}),
            implementation: cog_core::ToolImplementation::Native(Arc::new(|args| {
                Box::pin(async move { Ok(serde_json::json!({"echo": args})) })
            })),
        }
    }

    #[tokio::test]
    async fn a_native_tool_executes_and_an_unknown_name_is_an_error() {
        let set = ToolSet::new(vec![native_tool("bash")]).unwrap();
        assert_eq!(set.definitions().len(), 1);
        assert_eq!(set.definitions()[0].name, "bash");
        assert_eq!(
            set.call("bash", serde_json::json!("ls")).await.unwrap(),
            serde_json::json!({"echo": "ls"})
        );
        // 喊一个不存在的工具必须报错，不能被吞成空结果。
        let err = set
            .call("python", serde_json::json!("1+1"))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("python"), "{err}");
    }

    #[test]
    fn a_declared_tool_is_refused_because_it_would_run_somewhere_else() {
        let shell_tool = Tool {
            name: "bash".into(),
            description: "run a command".into(),
            parameters: serde_json::json!({"type": "object"}),
            implementation: cog_core::ToolImplementation::Shell(cog_core::ShellOp::Command),
        };
        let err = ToolSet::new(vec![shell_tool]).unwrap_err();
        assert!(err.to_string().contains("native"), "{err}");
    }

    #[test]
    fn the_budget_is_the_same_for_every_row_and_column() {
        let b = Budget::default();
        assert_eq!(b.max_steps, 500);
        assert_eq!(b.max_context_tokens, 512 * 1024);
        assert_eq!(b.temperature, 1.0);
    }

    #[tokio::test]
    async fn the_env_less_benchmark_still_has_an_env_slot() {
        let env = NoEnv;
        assert_eq!(env.id(), "none");
        assert!(env.executor().is_none());
        env.teardown().await;
    }
}
