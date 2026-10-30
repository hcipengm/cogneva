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
use crate::exec_seam::ExecSeam;
use crate::metric::StepRecord;

/// 默认的上下文窗口：512K tokens。
///
/// 逐类比照公开模型值，不额外放宽；上下文是评测里最容易被「给多一点」吃掉
/// 可变性的地方——同一道题给不同流派不同的窗口，读数就不可比了。
// cap: derived — 逐类比照公开模型值，没有额外放宽
const DEFAULT_MAX_CONTEXT_TOKENS: u32 = 512 * 1024;

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
    /// 撞到组合根给这一行设的上限（时间 / CPU / 内存）。
    ///
    /// 只在**循环不在我们手里**的外壳上出现：外部流派的循环是别人写的，步数管不到它，
    /// 能管的只有时间与这台机器分给它的资源。它与 [`Failed`] 必须分开——`Failed` 是它
    /// 自己说做不下去，这一条是**我们设的界**把它收走了：前者要换方法，后者要问一句
    /// 上限是不是给少了。
    ///
    /// [`Failed`]: FinishReason::Failed
    ResourceLimit,
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
            max_context_tokens: DEFAULT_MAX_CONTEXT_TOKENS,
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
    /// 收回这个环境**本来的**那一个类型。
    ///
    /// 基准的后端起完环境、判分那一步又拿到同一个 `Arc<dyn CaseEnv>`：它要读的（容器句柄、
    /// compose 工程名）在具体类型上，不在这个接口上。拿不到就报错——**不许**顺手换一个执行面
    /// 顶上，那等于对着一个不相干的环境判这道题。实现一律是 `self`。
    fn as_any(&self) -> &dyn std::any::Any;
    /// 这个环境对外部流派的态度。
    ///
    /// 默认是**带理由的缺席**：一个环境不给这一位，说明的是「这个基准不托管外部程序」，
    /// 不是「外部流派在这道题上得零分」——后者是一个假读数，而它长得像真读数（补丁为空
    /// 就是没做对）。托管得了的后端覆写它，并给出自己那台机器。
    fn exec_seam(&self) -> ExecSeam {
        ExecSeam::Absent("this environment hosts no external agent program")
    }
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
    fn as_any(&self) -> &dyn std::any::Any {
        self
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

    /// 并集：把 `other` 里 `self` 没有的工具加进来（同名以 `self` 为准）。
    ///
    /// 这是「动作空间并入有效工具面」那一步：基准面（已按臂切过）在外壳动作空间上做并，
    /// 动作空间不参与消融，所以两臂做完并集都拿得到自己的动作空间。`other` 为空时原样
    /// 返回、不动定义序列——今天没有工具实现，走的就是这条，并集是恒等的。
    pub fn union(mut self, other: ToolSet) -> ToolSet {
        if other.tools.is_empty() {
            return self;
        }
        for (name, tool) in other.tools {
            self.tools.entry(name).or_insert(tool);
        }
        // 定义按名排序重建：并集不该让同一组工具因为谁的 `HashMap` 迭代序不同而给出两份
        // 不同的定义序列。
        let mut definitions: Vec<ToolDefinition> = self
            .tools
            .values()
            .map(|t| ToolDefinition {
                name: t.name.clone(),
                description: t.description.clone(),
                parameters: t.parameters.clone(),
            })
            .collect();
        definitions.sort_by(|a, b| a.name.cmp(&b.name));
        self.definitions = definitions;
        self
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
    /// 这一格在哪一行哪一列上。外壳未必用得到，走平台的那个要用它拼任务 id：
    /// id 里只有种子与题面的话，「同一个外壳改了代码重跑」会命中平台上一次那条
    /// 终态行，把旧代码的结果当成新代码的读数交回来——而表上完全看不出来。
    pub benchmark: String,
    pub scaffold: String,
}

/// 被测对象。主表里一行一个实现。
#[async_trait]
pub trait AgentScaffold: Send + Sync {
    /// 这一行在表里的名字。评测台按它建行，不按 Rust 类型名——换实现类型不该改表。
    fn name(&self) -> &str;

    /// 这个方法赖以行动、缺了它就不再是这个方法的东西（**动作空间**），按名声明。
    ///
    /// 消融（无工具臂）切的是**基准外部工具面**，不是这个：一个以代码解释器为动作空间
    /// 的方法，把解释器关掉就不再看是它自己，那一列量到的是另一个方法，不是「没有工具的
    /// 它」。所以动作空间**两臂都在**——评测台把 `(基准工具面 ∩ 臂) ∪ 动作空间` 作为这一
    /// 格的有效工具面交给外壳。切分按**角色**不按工具名：同一个名字在一个方法里是动作
    /// 空间本身、在另一个方法里只是外部工具，按名字一刀切会误伤。
    ///
    /// **声明权在外壳**，且只声明**名字**：谁提供这些工具（哪个沙盒、哪条搜索 API）由
    /// 组合根定，本 crate 不造实现。没有外部动作空间的方法返回空。
    ///
    /// **今天惰性**：组合根还没有任何工具实现，并集加不进东西——加了不改变任何一格的
    /// 输出。等工具实现到了，行为自动对。
    ///
    /// 外壳**拿不到**自己在哪一臂：[`SolveContext`] 上没有、也不许有「这是无工具臂」的
    /// 旗标——外壳一旦能分辨臂就能特判，中立性就死了。
    fn action_space(&self) -> &'static [&'static str] {
        &[]
    }

    /// 这一行在**哪些列**上有定义。
    ///
    /// 默认全列有定义（仓里那四行都是这样：换一道基准它们还是同一个方法）。要有这一位，
    /// 是因为有一类行的定义域本身就窄：吃「一棵树 ＋ 一个题面」、吐一个补丁的行，在一道
    /// 没有树的基准上不是「跑坏了」，而是**这一列本来就没有这一行**。缺了这一位，那种行
    /// 只能靠跑起来报错来表达自己不属于这里，而报错的形状（`errored`）读起来是「这一格
    /// 有缺陷要修」——把一次定义域外的事故记成一次缺陷，就该有人去修一个没坏的东西。
    ///
    /// 不在定义域里的格子**不建**，也不计分：不是零分，是没有这一格。
    fn applies_to(&self, _benchmark: &str) -> bool {
        true
    }

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
