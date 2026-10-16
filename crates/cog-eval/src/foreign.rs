//! 外部流派：把别人写的一个 agent 程序接进主表的那一行。
//!
//! 仓里四行外壳都是 Rust 代码，跑在评测台自己的进程里，靠 [`SolveContext`] 拿 backbone
//! 与环境。外部流派不是这样一段代码——它是别人写的一个程序，只能**执行**，不能链接。
//! 所以它要的不是第五种 [`AgentScaffold`]，而是一条**执行缝**：环境把「在这道题的那台
//! 机器上跑一支外部程序，题面进、补丁出」这件事交出来；行本身仍然是一个 `AgentScaffold`，
//! 评测台一行不用改。
//!
//! 缝长在**环境**上而不是组合根上：外部程序要跑在这道题的机器里（题面所依赖的那棵树
//! 就在那儿），换一台机器跑出来的补丁不是这道题的补丁。
//!
//! [`SolveContext`]: crate::scaffold::SolveContext
//! [`AgentScaffold`]: crate::scaffold::AgentScaffold

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::scaffold::FinishReason;

/// 一支外部流派跑一次的上限。
///
/// 三个值都由**组合根**给定（本 crate 只定义形状）：外部流派的循环不在我们手里，
/// 步数管不到它，能管的只有时间与这台机器分给它的资源。
///
/// 上限是**四行三列、所有种子共用的一个值**，与 [`Budget`] 同理：一个流派给自己多要
/// 一倍时间或一倍内存，它那一行就不再是同一张考卷下的读数。
///
/// [`Budget`]: crate::scaffold::Budget
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ForeignLimits {
    /// 墙上时钟上限。到点即失败，不算「没做出来」——见 [`ForeignFace`] 那一节。
    pub wall_clock: Duration,
    /// 容器能用的 CPU 核数。
    pub cpus: f32,
    /// 容器能用的内存字节数。
    pub memory_bytes: u64,
}

/// 外部流派在这个环境里跑一次所需的一切。
///
/// 题面与入口都给**文本**而不是路径：外部流派自己怎么读题、怎么落补丁由它定，
/// 我们只承诺「这段文字是这道题」与「这条命令能起它」。
pub struct ForeignRequest<'a> {
    /// 这一行在表里的名字。进 trace 与判词，不进外部程序（它不该知道自己叫什么）。
    pub flow: &'a str,
    /// 在环境里怎么起这支流派。
    pub entrypoint: &'a str,
    /// 题面：与判分读的**同一份**。外部流派看不到判分器，也看不到答案键。
    pub problem: &'a str,
    pub limits: ForeignLimits,
}

/// 外部流派交回来的东西。
#[derive(Debug, Clone)]
pub struct ForeignOutcome {
    /// 这一步在环境里留下的补丁（unified diff）。判分器怎么用它由基准定。
    pub patch: String,
    /// 它为什么停下。撞上限必须与「自己收工」分开，否则读不出是流派不行还是上限给少了。
    pub finish: FinishReason,
}

/// 一个能在**具体这个环境**里执行外部流派的东西。
///
/// 实现由基准的后端提供（谁拥有那台机器，谁才知道怎么在里面起程序）。本 crate 不造实现。
#[async_trait]
pub trait ForeignRunner: Send + Sync {
    async fn run(&self, req: ForeignRequest<'_>) -> anyhow::Result<ForeignOutcome>;
}

/// 这个环境对外部流派的态度：能跑，或者**因为一个具体理由**不能跑。
///
/// 不用 `Option<Arc<dyn ForeignRunner>>`：那样「这个基准本来就不托管外部程序」与
/// 「环境起不来」会压成同一个 `None`，而这两件事一件正常、一件该报警——同
/// [`NoEnv`] 存在的理由。理由是一句话而不是一个旗标，因为它要能被印进表里那行
/// `missing`，读的人据此知道该去修哪里。
///
/// [`NoEnv`]: crate::scaffold::NoEnv
pub enum ForeignFace {
    Ready(Arc<dyn ForeignRunner>),
    /// 装不下外部程序的理由。
    Absent(&'static str),
}

impl ForeignFace {
    /// 能不能跑。判据只看这一位，不看理由里的措辞。
    pub fn is_ready(&self) -> bool {
        matches!(self, Self::Ready(_))
    }

    /// 装不下时的理由；装得下返回 `None`。
    pub fn why_absent(&self) -> Option<&'static str> {
        match self {
            Self::Ready(_) => None,
            Self::Absent(why) => Some(why),
        }
    }
}
