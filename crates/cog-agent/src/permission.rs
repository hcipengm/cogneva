//! 工具执行之前的那一问：谁在问、问什么、答案怎么回到模型。
//!
//! 这一层只做三件事：把「这一轮处在哪种模式」写成闭集；把「哪些工具不问」写成
//! 一张显式的只读名单；在工具真正跑之前给出一个结论——允许、拒绝并带理由，或者
//! 交给人去问。它不关心问的人是谁：网页背后是一个连接，命令行背后是一个人，
//! 只要实现了 [`PermissionDecider`]，内核看到的都是同一次询问。
//!
//! 默认是问。名单只列「读了也不会改变任何东西」的工具，因为漏列的代价是多问一句
//! （可见、可补），而多列一个的代价是一次没人看见的写盘。

use std::sync::Arc;

/// 这一轮处在哪种模式下。闭集：不认识的模式不猜一个默认值，交回给调用方拒绝。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PermissionMode {
    /// 只读：任何会改变工作树或外部世界的工具都不执行，连问都不问。
    Plan,
    /// 默认：只读工具直接跑，其余每一样都先问人。
    #[default]
    Default,
    /// 文件编辑直接跑，其余（跑命令、提交、发消息）仍然问。
    AcceptEdits,
    /// 全不问。显式选择，且这个选择本身是读得出来的（见 [`PermissionMode::as_str`]）。
    Bypass,
}

impl PermissionMode {
    /// 模式名。闭集的每一格都要有一个名字，因为会话上可读的是这个名字，不是枚举。
    pub const NAMES: [&'static str; 4] = ["plan", "default", "acceptEdits", "bypass"];

    /// 从会话上读到的名字解析回来。不认识就是 `None`：一个拼错的模式名不该
    /// 静默落进「默认」，那会让一次手误看起来像一次没有发生的询问。
    pub fn parse(name: &str) -> Option<Self> {
        match name.trim() {
            "plan" => Some(Self::Plan),
            "default" => Some(Self::Default),
            "acceptEdits" => Some(Self::AcceptEdits),
            "bypass" => Some(Self::Bypass),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Plan => "plan",
            Self::Default => "default",
            Self::AcceptEdits => "acceptEdits",
            Self::Bypass => "bypass",
        }
    }
}

/// 一次询问的全部内容。
///
/// 参数一起带上，因为「写哪里」正是人要看的那一格：只报工具名的话，屏幕上出现的
/// 是一百次一模一样的「write_file」。
#[derive(Debug, Clone)]
pub struct PermissionAsk {
    pub call_id: String,
    pub agent_id: String,
    pub tool: String,
    pub arguments: serde_json::Value,
    pub mode: PermissionMode,
}

/// 问回来的结论。拒绝必须带理由：理由要交给模型，它据此换一条路，而不是把同一次
/// 调用再发一遍。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PermissionVerdict {
    Allow,
    Deny { reason: String },
}

impl PermissionVerdict {
    pub fn is_allowed(&self) -> bool {
        matches!(self, Self::Allow)
    }
}

/// 谁回答那一问。实现在持有「人」的那一侧：网页连接、命令行、或者一个明确
/// 声明「这里没人可问、按某个词直接判」的调用方。
#[async_trait::async_trait]
pub trait PermissionDecider: Send + Sync {
    async fn decide(&self, ask: &PermissionAsk) -> PermissionVerdict;
}

/// 读了也不会改变任何东西的工具。
///
/// 名单是显式的、按名字列的，不是按前缀或按「看起来像读」猜的：`raw_fetch` 与
/// `read_file` 只差一个词，而 `apply_patch` 与 `git_diff` 也一样。名单外的一律要问。
pub const READ_ONLY_TOOLS: &[&str] = &[
    "read_file",
    "grep",
    "search_code",
    "glob",
    "git_status",
    "git_diff",
    "todo",
    "list_agents",
    "read_task_ledger",
    "command_output",
    "raw_list",
    "raw_fetch",
];

/// 只改文件、不碰外部世界的工具。`acceptEdits` 模式放行的就是这一组。
pub const EDIT_TOOLS: &[&str] = &["write_file", "edit_file", "apply_patch"];

pub fn is_read_only(tool: &str) -> bool {
    READ_ONLY_TOOLS.contains(&tool)
}

pub fn is_edit(tool: &str) -> bool {
    EDIT_TOOLS.contains(&tool)
}

/// 不用问就能定的结论；`None` 表示这一问要交给人。
///
/// 四个模式各有一条独立的路，而不是「默认放行、例外再拦」：后者每加一个模式都要
/// 回头检查前一个模式漏了什么。
pub fn decided_without_asking(mode: PermissionMode, tool: &str) -> Option<PermissionVerdict> {
    match mode {
        PermissionMode::Bypass => Some(PermissionVerdict::Allow),
        PermissionMode::Plan => Some(if is_read_only(tool) {
            PermissionVerdict::Allow
        } else {
            PermissionVerdict::Deny {
                reason: format!(
                    "plan 模式只执行只读工具，`{tool}` 会改变工作树或外部世界，没有执行。\
                     要动手请先切到 default 模式。"
                ),
            }
        }),
        PermissionMode::AcceptEdits => {
            if is_read_only(tool) || is_edit(tool) {
                Some(PermissionVerdict::Allow)
            } else {
                None
            }
        }
        PermissionMode::Default => is_read_only(tool).then_some(PermissionVerdict::Allow),
    }
}

/// 把一次询问落成结论：先看模式能不能自己定，不能就问人；没有人可问时拒绝，
/// 而不是放行。放行等于把一个没人做过的决定记成一个做过的决定。
pub async fn decide(
    mode: PermissionMode,
    decider: Option<&Arc<dyn PermissionDecider>>,
    ask: &PermissionAsk,
) -> PermissionVerdict {
    if let Some(verdict) = decided_without_asking(mode, &ask.tool) {
        return verdict;
    }
    match decider {
        Some(decider) => decider.decide(ask).await,
        None => PermissionVerdict::Deny {
            reason: format!(
                "`{}` 在 {} 模式下需要人确认，但这一轮没有可以问的人（没有配置确认通道），\
                 所以没有执行。",
                ask.tool,
                mode.as_str()
            ),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 那张只读名单必须真的只装只读工具：名单里的每一个都该在工具注册表里，
    /// 且不属于会写盘的那一组。
    #[test]
    fn the_read_only_list_stays_read_only() {
        for tool in READ_ONLY_TOOLS {
            assert!(!EDIT_TOOLS.contains(tool), "{tool} 同时出现在两张名单里");
        }
        assert!(is_read_only("read_file"));
        assert!(!is_read_only("write_file"));
        assert!(!is_read_only("run_command"));
    }

    /// 换模式 ⇒ 换结论。这条是模式存在的理由：同一个工具在四种模式下得到四种待遇，
    /// 而不是四种名字指向同一个行为。
    #[test]
    fn the_mode_decides_what_the_tool_name_cannot() {
        let denied = |mode, tool: &str| {
            matches!(
                decided_without_asking(mode, tool),
                Some(PermissionVerdict::Deny { .. })
            )
        };
        assert!(denied(PermissionMode::Plan, "write_file"));
        assert!(!denied(PermissionMode::Plan, "read_file"));
        assert_eq!(
            decided_without_asking(PermissionMode::Bypass, "write_file"),
            Some(PermissionVerdict::Allow)
        );
        assert_eq!(
            decided_without_asking(PermissionMode::AcceptEdits, "write_file"),
            Some(PermissionVerdict::Allow)
        );
        assert_eq!(
            decided_without_asking(PermissionMode::AcceptEdits, "run_command"),
            None,
            "acceptEdits 只放行编辑，跑命令仍然要问"
        );
        assert_eq!(
            decided_without_asking(PermissionMode::Default, "run_command"),
            None
        );
        assert_eq!(
            decided_without_asking(PermissionMode::Default, "read_file"),
            Some(PermissionVerdict::Allow)
        );
        assert_eq!(
            decided_without_asking(PermissionMode::Plan, "read_file"),
            Some(PermissionVerdict::Allow)
        );
    }

    /// 模式名是可读的一面：解析与打印必须互逆，拼错的名字不许落进默认。
    #[test]
    fn the_mode_names_round_trip_and_a_typo_is_not_default() {
        for name in PermissionMode::NAMES {
            let mode = PermissionMode::parse(name).expect("闭集里的每个名字都该解析得回来");
            assert_eq!(mode.as_str(), name);
        }
        assert_eq!(PermissionMode::parse("defaultt"), None);
        assert_eq!(PermissionMode::parse(""), None);
    }

    /// 没有人可问时，需要问的那一步是拒绝而不是放行；且拒绝里带着「为什么没人」。
    #[tokio::test]
    async fn a_question_with_nobody_to_ask_is_refused_not_allowed() {
        let ask = PermissionAsk {
            call_id: "call-1".into(),
            agent_id: "worker".into(),
            tool: "write_file".into(),
            arguments: serde_json::json!({"path": "a.txt"}),
            mode: PermissionMode::Default,
        };
        let verdict = decide(PermissionMode::Default, None, &ask).await;
        match verdict {
            PermissionVerdict::Deny { reason } => {
                assert!(reason.contains("没有可以问的人"), "{reason}");
            }
            PermissionVerdict::Allow => panic!("没有人可问时必须拒绝"),
        }
    }
}
